import pandas as pd
from google import genai
from pydantic import BaseModel, Field
import os
import re
import time
import json
import shutil
from typing import Literal, get_args, Optional

from product_text_extract import extract_product_keys

# API key / model name are passed from the frontend
DEFAULT_MODEL_NAME = "models/gemini-3.1-flash-lite-preview"
# Gemini 整批失败后可选：Moonshot OpenAI 兼容接口（Kimi）
DEFAULT_MOONSHOT_MODEL = "kimi-k2-turbo-preview"
MOONSHOT_BASE_URL = "https://api.moonshot.cn/v1"

PackagingUnit = Literal[
    "袋",
    "盒",
    "瓶",
    "罐",
    "桶",
    "箱",
    "听",
    "杯",
    "支",
    "条",
    "片",
    "套",
    "枚",
    "个",
    "只",
    "包",
    "件",
    "板",
    "组",
    "卷",
    "未知",
]

class ProductInfo(BaseModel):
    """
    Gemini 新提取结构（写回 Excel 的新 6 列 A*）。

    说明：不再提取 品牌、核心名称、外观、材质。
    """

    net_content: str = Field(default="", description="单件净含量，标准单位 ml/L/g/kg，如 330ml/1.5L/18g")
    sell_quantity: str = Field(default="", description="售卖数量值，只保留数字，如 24/6/7/2/1，不带罐/瓶/个等单位")
    packaging_unit: PackagingUnit = Field(default="未知", description="包装单位，如 罐/瓶/袋/片/条/个/箱/包；不确定填 未知")
    color: list[str] = Field(default_factory=list, description="颜色（可多值）")
    size: list[str] = Field(default_factory=list, description="尺寸/长度/码数（可多值，如 240mm/17x25x8cm/XL）")
    model: str = Field(default="", description="型号（如 AB-123、X1），不确定留空")

class BatchResponse(BaseModel):
    items: list[ProductInfo]


ALLOWED_PACKAGING = frozenset(get_args(PackagingUnit))
EXTRACTION_SOURCE_COL = "A提取来源"
MODEL_EXTRACTION_SOURCE = "模型提取"
RULE_EXTRACTION_SOURCE = "规则兜底"


def _ai_log(log_tag: str, msg: str) -> None:
    """终端可检索前缀：[AI][主.xlsx] ..."""
    tag = (log_tag or "").strip() or "-"
    print(f"[AI][{tag}] {msg}", flush=True)


def _mark_extraction_source(items: list[ProductInfo], source: str) -> list[ProductInfo]:
    for item in items:
        item = _normalize_product_info(item)
        try:
            object.__setattr__(item, "_extraction_source", source)
        except Exception:
            pass
    return items


def _get_extraction_source(item: ProductInfo) -> str:
    return str(getattr(item, "_extraction_source", "") or "").strip()


_MODEL_EXCLUDE = {"大楷", "中楷", "小楷"}
_MODEL_DERIVE_PATTERNS = [
    # Bracketed size codes: 【L码】, 【XL码】, 【M码】
    (re.compile(r"【\s*((?:XXXL|XXL|XL|L|M|S)码)\s*】", re.IGNORECASE), 1),
    # Simple size/grade words used as option labels
    (re.compile(r"\b(小号|中号|大号)\b"), 1),
    (re.compile(r"\b(单层|双层|三层)\b"), 1),
]


def _postprocess_model(raw_model: str, name: str, spec: str) -> str:
    """
    Make A型号 closer to offline-script behavior:
    - Avoid treating calligraphy nib types like 中楷/大楷 as 型号.
    - If model missing, derive from common option labels in spec (e.g. 【L码】, 小号/中号/大号, 单层).
    """
    m = (raw_model or "").strip()
    if m in _MODEL_EXCLUDE:
        m = ""
    if m:
        return m
    text = f"{name} {spec}".strip()
    for pat, grp in _MODEL_DERIVE_PATTERNS:
        mm = pat.search(text)
        if mm:
            v = (mm.group(grp) or "").strip()
            # normalize casing for S/M/L/XL codes
            v = re.sub(r"(?i)^(xxxl|xxl|xl|l|m|s)码$", lambda x: x.group(1).upper() + "码", v)
            return v
    return ""


def _build_extraction_prompt(items) -> str:
    """与 Gemini 相同的抽取说明，供 Kimi 兜底复用。"""
    return f"""
    You are a highly accurate product attribute extractor.

    Return ONLY valid JSON that matches the provided response schema.
    Do NOT include any markdown, code fences, or explanations.

    Extract fields for each item:
    1. net_content (A单件净含量): per-unit net content only, standardized units: ml / L / g / kg (e.g. 330ml, 1.5L, 18g). If unclear, empty.
       Do NOT compute total net content.
    2. sell_quantity (A售卖数量): numeric quantity ONLY (e.g. 24, 6, 7, 2, 1). If unclear, empty.
       Do NOT include packaging unit in sell_quantity. Wrong: "24罐"; correct: "24".
    3. packaging_unit (A包装单位): the packaging unit corresponding to sell_quantity. Choose ONE from:
       ["袋","盒","瓶","罐","桶","箱","听","杯","支","条","片","套","枚","个","只","包","件","板","组","卷","未知"].
       Examples:
       - 330ml*24罐/箱 => sell_quantity=24, packaging_unit=罐
       - 7片/包 => sell_quantity=7, packaging_unit=片
       - 1个 => sell_quantity=1, packaging_unit=个
    4. color (A颜色): list of colors if explicitly stated.
    5. size (A尺寸): list of sizes/lengths or dimensions, keep units (mm/cm/m) and forms like 17x25x8cm, 直径19cm, and size codes like XL.
    6. model (A型号): model identifier if present (e.g. AB-123, X1).

    Normalization rules:
    - Convert full-width to half-width where applicable.
    - Standardize units: 毫升/ml/ML->ml; 升/L/l->L; 克/g/G->g; 千克/公斤/kg->kg
    - Treat x/×/* as multipliers. Example: 330ml*24罐/箱 => net_content=330ml, sell_quantity=24, packaging_unit=罐

    Top-level JSON shape: {{"items": [ ... ]}} — same length and order as input.

    Examples (few-shot). Follow the same output style:
    Input:
    [
      {{"name":"【整箱】雪碧 碳酸饮料 330ml*24罐/箱","spec":"330ml*24罐/箱"}},
      {{"name":"高洁丝 纯棉240mm*7片/包 极薄卫生巾","spec":"7片/包"}},
      {{"name":"礼袋 1个 礼品包装","spec":"礼袋17x25x8cm*1个"}}
    ]
    Output:
    {{
      "items":[
        {{"net_content":"330ml","sell_quantity":"24","packaging_unit":"罐","color":[],"size":[],"model":""}},
        {{"net_content":"","sell_quantity":"7","packaging_unit":"片","color":[],"size":["240mm"],"model":""}},
        {{"net_content":"","sell_quantity":"1","packaging_unit":"个","color":[],"size":["17x25x8cm"],"model":""}}
      ]
    }}

    Input items (JSON), keep output order exactly the same:
    {json.dumps(items, ensure_ascii=False, indent=2)}
    """


def _packaging_unit_from_sell_quantity(sell_quantity: str, default: str = "未知") -> str:
    s = (sell_quantity or "").strip()
    m = re.search(r"(袋|盒|瓶|罐|桶|箱|听|杯|支|条|片|套|枚|个|只|包|件|板|组|卷)$", s)
    unit = m.group(1) if m else (default or "未知")
    return unit if unit in ALLOWED_PACKAGING else "未知"


def _normalize_sell_quantity_value(sell_quantity: str) -> str:
    s = (sell_quantity or "").strip()
    if not s:
        return ""
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    return m.group(1) if m else ""


def _fill_packaging_from_sell_quantity(packaging_unit: str, sell_quantity: str) -> str:
    unit = (packaging_unit or "").strip()
    if unit and unit in ALLOWED_PACKAGING and unit != "未知":
        return unit
    return _packaging_unit_from_sell_quantity(sell_quantity, default=unit or "未知")


def _normalize_product_info(item: ProductInfo) -> ProductInfo:
    raw_sell = getattr(item, "sell_quantity", "") or ""
    item.sell_quantity = _normalize_sell_quantity_value(raw_sell)
    item.packaging_unit = _fill_packaging_from_sell_quantity(getattr(item, "packaging_unit", "") or "", raw_sell)
    return item


def _fallback_dimensions(text: str) -> list[str]:
    s = str(text or "")
    out: list[str] = []
    # 18*16*10cm / 25×20×12 cm
    pat3 = re.compile(
        r"(\d+(?:\.\d+)?)\s*[x×*]\s*(\d+(?:\.\d+)?)\s*[x×*]\s*(\d+(?:\.\d+)?)\s*(mm|cm|m)\b",
        re.IGNORECASE,
    )
    triple_spans: list[tuple[int, int]] = []
    for m in pat3.finditer(s):
        triple_spans.append(m.span())
        out.append(f"{m.group(1)}x{m.group(2)}x{m.group(3)}{m.group(4).lower()}")
    # 17x25cm / 10*20 cm
    pat2 = re.compile(
        r"(\d+(?:\.\d+)?)\s*[x×*]\s*(\d+(?:\.\d+)?)\s*(mm|cm|m)\b",
        re.IGNORECASE,
    )
    for m in pat2.finditer(s):
        if any(not (m.end() <= a or m.start() >= b) for a, b in triple_spans):
            continue
        out.append(f"{m.group(1)}x{m.group(2)}{m.group(3).lower()}")
    single_size_pat = re.compile(r"(\d+(?:\.\d+)?)\s*(mm|cm|m)\b", re.IGNORECASE)
    occupied = triple_spans[:]
    occupied.extend(m.span() for m in pat2.finditer(s))
    for m in single_size_pat.finditer(s):
        if any(not (m.end() <= a or m.start() >= b) for a, b in occupied):
            continue
        out.append(f"{m.group(1)}{m.group(2).lower()}")
    # 卫生巾等常见 “280*10片”：前一个数通常是长度 mm，后一个才是售卖数量。
    pat_pad = re.compile(r"(\d{2,4})\s*[x×*]\s*\d+\s*片")
    for m in pat_pad.finditer(s):
        out.append(f"{m.group(1)}mm")
    cleaned: list[str] = []
    seen: set[str] = set()
    for x in out:
        if x not in seen:
            seen.add(x)
            cleaned.append(x)
    return cleaned


def _fallback_sell_quantity(text: str, current: str) -> str:
    s = str(text or "")
    # Correct false merges such as 280*10片 -> 10片.
    m = re.search(r"\d{2,4}\s*[x×*]\s*(\d+(?:\.\d+)?)\s*(片|枚|个|只|包|袋)\b", s)
    if m:
        return f"{m.group(1)}{m.group(2)}"
    return current or ""


def _fallback_net_content(text: str, current: str) -> str:
    s = str(text or "")
    patterns = [
        (re.compile(r"(?i)(\d+(?:\.\d+)?)\s*(?:千克|公斤|kg)\b"), "kg"),
        (re.compile(r"(?i)(\d+(?:\.\d+)?)\s*(?:毫升|ml)\b"), "ml"),
        (re.compile(r"(?i)(\d+(?:\.\d+)?)\s*(?:克|g)\b"), "g"),
        (re.compile(r"(?i)(\d+(?:\.\d+)?)\s*(?:升|l)\b"), "L"),
    ]
    for pat, unit in patterns:
        m = pat.search(s)
        if m:
            return f"{m.group(1)}{unit}"
    return current or ""


def _fallback_colors(text: str, current: list[str]) -> list[str]:
    s = str(text or "")
    out: list[str] = []
    for c in current or []:
        v = (c or "").strip()
        if len(v) == 1:
            v = f"{v}色"
        if v and v not in out:
            out.append(v)
    color_pat = re.compile(r"(透明|米白|乳白|奶白|香槟|白色|黑色|灰色|银色|金色|红色|粉色|橙色|黄色|绿色|蓝色|紫色|棕色|咖色|白|黑|灰|银|金|红|粉|橙|黄|绿|蓝|紫|棕|咖)")
    for m in color_pat.finditer(s):
        v = m.group(1)
        if len(v) == 1:
            v = f"{v}色"
        if v not in out:
            out.append(v)
    return [v for v in out if not any(v != other and other.startswith(v) for other in out)]


def _clean_fallback_sizes(raw_sizes: list[str], text: str) -> list[str]:
    dims = _fallback_dimensions(text)
    out: list[str] = []
    for x in raw_sizes or []:
        v = (x or "").strip()
        if not v:
            continue
        # Drop concatenation artifacts from strings like 18*16*10cm after "*" was stripped.
        if re.fullmatch(r"\d{5,}(?:mm|cm|m)", v, flags=re.IGNORECASE):
            continue
        out.append(v)
    out = dims + out
    cleaned: list[str] = []
    seen: set[str] = set()
    for x in out:
        if x not in seen:
            seen.add(x)
            cleaned.append(x)
    return cleaned


def _fallback_model(text: str, current: str) -> str:
    cur = (current or "").strip()
    if cur:
        return cur
    s = str(text or "")
    for pat in (
        re.compile(r"(?:型号|货号|款号|model)\s*[:：]?\s*([A-Za-z0-9][A-Za-z0-9\-_/]{1,30})", re.IGNORECASE),
        re.compile(r"【\s*((?:XXXL|XXL|XL|L|M|S)码)\s*】", re.IGNORECASE),
        re.compile(r"\b((?:XXXL|XXL|XL|L|M|S)码)\b", re.IGNORECASE),
        re.compile(r"\b([A-Za-z]{1,6}[-_/]?\d{2,6}[A-Za-z0-9\-_/]{0,10})\b"),
    ):
        m = pat.search(s)
        if m:
            v = m.group(1).strip()
            return re.sub(r"(?i)^(xxxl|xxl|xl|l|m|s)码$", lambda x: x.group(1).upper() + "码", v)
    return ""


def _heuristic_product_info(item) -> ProductInfo:
    if isinstance(item, dict):
        name = item.get("name", "")
        spec = item.get("spec", "")
    else:
        name = str(item or "")
        spec = ""
    keys = extract_product_keys(name=name, spec=spec)
    raw_text = f"{name or ''} {spec or ''}".strip()
    net_content = _fallback_net_content(raw_text, keys.net_content or "")
    raw_sell_quantity = _fallback_sell_quantity(raw_text, keys.sell_quantity or "")
    sell_quantity = _normalize_sell_quantity_value(raw_sell_quantity)
    packaging_unit = _packaging_unit_from_sell_quantity(raw_sell_quantity)
    color = _fallback_colors(raw_text, list(keys.colors or ()))
    size = _clean_fallback_sizes(list(keys.size or ()), raw_text)
    model = _fallback_model(raw_text, keys.model or "")
    return ProductInfo(
        net_content=net_content,
        sell_quantity=sell_quantity,
        packaging_unit=packaging_unit,
        color=color,
        size=size,
        model=_postprocess_model(model, name or "", spec or ""),
    )


def _heuristic_batch(items, log_tag: str = "") -> list[ProductInfo]:
    out = [_heuristic_product_info(item) for item in items]
    non_empty_sell = sum(1 for x in out if (x.sell_quantity or "").strip())
    non_empty_net = sum(1 for x in out if (x.net_content or "").strip())
    _ai_log(
        log_tag,
        f"本地规则兜底完成: 条数={len(out)} A售卖非空={non_empty_sell} A净含量非空={non_empty_net}",
    )
    return _mark_extraction_source(out, RULE_EXTRACTION_SOURCE)


def _strip_markdown_json_fences(text: str) -> str:
    t = (text or "").strip()
    if not t.startswith("```"):
        return t
    lines = t.splitlines()
    lines = lines[1:]
    while lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


def _normalize_batch_dict_for_validate(data: dict) -> dict:
    if not isinstance(data, dict):
        return {"items": []}
    items = data.get("items")
    if not isinstance(items, list):
        return {"items": []}
    out_items = []
    for it in items:
        if not isinstance(it, dict):
            it = {}
        d = dict(it)
        pu = d.get("packaging_unit", "未知")
        if pu not in ALLOWED_PACKAGING:
            d["packaging_unit"] = "未知"
        for key in ("color", "size"):
            v = d.get(key)
            if v is None:
                d[key] = []
            elif isinstance(v, str):
                d[key] = [v.strip()] if str(v).strip() else []
            elif isinstance(v, list):
                d[key] = [str(x) for x in v if x is not None and str(x).strip()]
            else:
                d[key] = []
        for key in ("net_content", "sell_quantity", "model"):
            v = d.get(key, "")
            d[key] = "" if v is None else str(v).strip()
        d["packaging_unit"] = _fill_packaging_from_sell_quantity(d.get("packaging_unit", ""), d.get("sell_quantity", ""))
        d["sell_quantity"] = _normalize_sell_quantity_value(d.get("sell_quantity", ""))
        out_items.append(d)
    return {"items": out_items}


def extract_batch_moonshot(
    items,
    api_key: str,
    model_name: Optional[str] = None,
    max_retries: int = 5,
    log_tag: str = "",
):
    """Gemini 失败后的可选兜底：Moonshot Kimi（OpenAI 兼容 chat.completions）。"""
    if not items:
        return []
    try:
        from openai import OpenAI
    except ImportError:
        _ai_log(log_tag, "Kimi 兜底跳过: 未安装 openai（pip install openai），改用本地规则兜底")
        return _heuristic_batch(items, log_tag=log_tag)

    model = (model_name or os.environ.get("MOONSHOT_MODEL") or DEFAULT_MOONSHOT_MODEL).strip()
    client = OpenAI(api_key=api_key, base_url=MOONSHOT_BASE_URL)
    prompt = _build_extraction_prompt(items)
    _ai_log(log_tag, f"请求 Kimi(Moonshot) 兜底: model={model!r} items={len(items)}")

    for attempt in range(max_retries):
        try:
            r = client.chat.completions.create(
                model=model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a highly accurate product attribute extractor. "
                            "Return ONLY one JSON object with key \"items\" (array), no markdown fences."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
                response_format={"type": "json_object"},
            )
            text = (r.choices[0].message.content or "").strip()
            text = _strip_markdown_json_fences(text)
            data = json.loads(text)
            data = _normalize_batch_dict_for_validate(data)
            batch = BatchResponse.model_validate(data)
            if len(batch.items) != len(items):
                _ai_log(
                    log_tag,
                    f"Kimi attempt {attempt + 1}/{max_retries}: 条数不一致 {len(batch.items)} vs {len(items)}",
                )
                time.sleep(5)
                continue
            if batch.items:
                s0 = batch.items[0]
                _ai_log(
                    log_tag,
                    f"Kimi 兜底本批成功: 条数={len(batch.items)} 样例[0] sell={getattr(s0, 'sell_quantity', '')!r} "
                    f"pack={getattr(s0, 'packaging_unit', '')!r} net={getattr(s0, 'net_content', '')!r}",
                )
            else:
                _ai_log(log_tag, "Kimi 兜底本批成功: 条数=0（无条目）")
            return _mark_extraction_source(batch.items, MODEL_EXTRACTION_SOURCE)
        except Exception as e:
            err_msg = str(e)
            if "429" in err_msg or "rate" in err_msg.lower() or "限流" in err_msg:
                wait_time = (attempt + 1) * 20
                _ai_log(
                    log_tag,
                    f"Kimi attempt {attempt + 1}/{max_retries}: 限流/429，{wait_time}s 后重试 — {err_msg[:180]}",
                )
                time.sleep(wait_time)
            elif "503" in err_msg or "UNAVAILABLE" in err_msg:
                _ai_log(
                    log_tag,
                    f"Kimi attempt {attempt + 1}/{max_retries}: 503/繁忙 — {err_msg[:180]}",
                )
                time.sleep(15)
            else:
                _ai_log(log_tag, f"Kimi attempt {attempt + 1}/{max_retries}: {type(e).__name__}: {e}")
                time.sleep(8)

    _ai_log(log_tag, f"Kimi 兜底已放弃 batch_len={len(items)}，改用本地规则兜底")
    return _heuristic_batch(items, log_tag=log_tag)


def extract_batch_ai(
    items,
    api_key,
    model_name=None,
    max_retries=5,
    log_tag: str = "",
    fallback_api_key: Optional[str] = None,
    fallback_model: Optional[str] = None,
    allow_split: bool = True,
):
    client = genai.Client(api_key=api_key)
    model_name = (model_name or DEFAULT_MODEL_NAME).strip() or DEFAULT_MODEL_NAME
    _ai_log(log_tag, f"请求 Gemini batch: model={model_name!r} items={len(items)}")
    prompt = _build_extraction_prompt(items)

    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model=model_name,
                contents=prompt,
                config={
                    'response_mime_type': 'application/json',
                    'response_schema': BatchResponse,
                }
            )
            if response.parsed and hasattr(response.parsed, 'items'):
                parsed_items = response.parsed.items
                if len(parsed_items) == len(items):
                    if parsed_items:
                        s0 = parsed_items[0]
                        _ai_log(
                            log_tag,
                            f"本批成功: 条数={len(parsed_items)} 样例[0] sell={getattr(s0, 'sell_quantity', '')!r} "
                            f"pack={getattr(s0, 'packaging_unit', '')!r} net={getattr(s0, 'net_content', '')!r}",
                        )
                    else:
                        _ai_log(log_tag, "本批成功: 条数=0（无条目）")
                    return _mark_extraction_source(parsed_items, MODEL_EXTRACTION_SOURCE)
                else:
                    _ai_log(
                        log_tag,
                        f"attempt {attempt + 1}/{max_retries}: Result count mismatch "
                        f"({len(parsed_items)} vs {len(items)}). Retrying...",
                    )
            else:
                # 无 parsed 时此前静默重试，易导致「只有部分文件像提取成功」却看不到原因
                _ai_log(
                    log_tag,
                    f"attempt {attempt + 1}/{max_retries}: no usable parsed response "
                    f"(batch_len={len(items)}). Retrying...",
                )

        except Exception as e:
            err_msg = str(e)
            if "429" in err_msg or "RESOURCE_EXHAUSTED" in err_msg:
                wait_time = (attempt + 1) * 30
                _ai_log(
                    log_tag,
                    f"attempt {attempt + 1}/{max_retries}: Quota 429 / RESOURCE_EXHAUSTED. "
                    f"Sleep {wait_time}s — {err_msg[:200]}",
                )
                time.sleep(wait_time)
            elif "503" in err_msg or "UNAVAILABLE" in err_msg:
                wait_time = 15
                _ai_log(
                    log_tag,
                    f"attempt {attempt + 1}/{max_retries}: Server 503 / UNAVAILABLE. "
                    f"Sleep {wait_time}s — {err_msg[:200]}",
                )
                time.sleep(wait_time)
            else:
                _ai_log(log_tag, f"attempt {attempt + 1}/{max_retries}: Error — {type(e).__name__}: {e}")
                time.sleep(10)

    _ai_log(
        log_tag,
        f"已放弃 Gemini: {max_retries} 次尝试后仍失败 batch_len={len(items)}。",
    )
    if allow_split and len(items) > 30:
        mid = len(items) // 2
        _ai_log(log_tag, f"Gemini 大批次失败，自动拆分为 {mid}+{len(items) - mid} 后重试。")
        left = extract_batch_ai(
            items[:mid],
            api_key=api_key,
            model_name=model_name,
            max_retries=max(2, max_retries - 2),
            log_tag=log_tag,
            fallback_api_key=fallback_api_key,
            fallback_model=fallback_model,
            allow_split=True,
        )
        right = extract_batch_ai(
            items[mid:],
            api_key=api_key,
            model_name=model_name,
            max_retries=max(2, max_retries - 2),
            log_tag=log_tag,
            fallback_api_key=fallback_api_key,
            fallback_model=fallback_model,
            allow_split=True,
        )
        return left + right

    fk = (fallback_api_key or "").strip()
    if fk:
        _ai_log(log_tag, "改用 Kimi(Moonshot) 兜底本批…")
        return extract_batch_moonshot(
            items,
            api_key=fk,
            model_name=(fallback_model or "").strip() or None,
            max_retries=max_retries,
            log_tag=log_tag,
        )
    _ai_log(
        log_tag,
        "未配置 Kimi 兜底 Key：改用本地规则提取兜底，避免写入全空默认结果。",
    )
    return _heuristic_batch(items, log_tag=log_tag)


def safe_save(df, file_path):
    temp_path = file_path + ".resuming.xlsx"
    try:
        df.to_excel(temp_path, index=False, engine='openpyxl')
        shutil.move(temp_path, file_path)
        return True
    except Exception as e:
        print(f"Failed to safe save: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)
        return False


def _summarize_written_a_columns(df: pd.DataFrame, log_tag: str) -> None:
    """写回后统计，便于和「主/A 无值、B 有值」对照。"""
    n = len(df)
    if n == 0:
        return
    sell = df["A售卖数量"].astype(str).str.strip()
    ne_sell = (sell != "") & (sell.str.lower() != "nan")
    n_sell = int(ne_sell.sum())
    pack = df["A包装单位"].astype(str).str.strip()
    n_unk_pack = int((pack == "未知").sum())
    net = df["A单件净含量"].astype(str).str.strip()
    n_net = int(((net != "") & (net.str.lower() != "nan")).sum())
    _ai_log(
        log_tag,
        f"写回后全表统计: 行数={n} | A售卖非空={n_sell} | A净含量非空={n_net} | 包装为「未知」={n_unk_pack}",
    )
    if n_sell == 0 and n_unk_pack >= n * 0.9:
        _ai_log(
            log_tag,
            "提示: 售卖全空且包装几乎全为「未知」，高度疑似本文件 Gemini 未返回有效解析（与兜底 ProductInfo 一致），请向上翻看本文件 [AI][...] 报错/重试行。",
        )


def process_file_ai(
    file_path,
    api_key,
    batch_size=110,
    progress_cb=None,
    model_name=None,
    fallback_api_key: Optional[str] = None,
    fallback_model: Optional[str] = None,
):
    log_tag = os.path.basename(file_path) or "unknown.xlsx"
    _ai_log(log_tag, f"开始处理文件 path={file_path}")
    print(f"Loading {file_path}...")
    try:
        df = pd.read_excel(file_path, engine='openpyxl')
    except Exception as e:
        print(f"Failed to read {file_path} with openpyxl: {e}")
        return

    # Identify column names
    cols = df.columns.tolist()
    name_col = '商品名称'
    spec_col = '规格' if '规格' in cols else '规格名称'

    if name_col not in cols or spec_col not in cols:
        print(f"Required columns not found in {file_path}. Available: {cols}")
        return

    # Initialize target columns if they don't exist (new 6 columns + source marker)
    target_cols = ['A单件净含量', 'A售卖数量', 'A包装单位', 'A颜色', 'A尺寸', 'A型号']
    for col in target_cols + [EXTRACTION_SOURCE_COL]:
        if col not in df.columns:
            df[col] = ""
        else:
            df[col] = df[col].fillna("")

    # Drop legacy A* columns from previous extraction scheme (keep the sheet clean)
    legacy_cols = ['A商品名称', 'A规格', 'A材质口味', 'A使用场景', 'A功能标签']
    to_drop = [c for c in legacy_cols if c in df.columns]
    if to_drop:
        df.drop(columns=to_drop, inplace=True)

    # 1. Identify rows that need processing.
    # Use 商品名称 作为“是否有意义要处理”的判定（空名称直接跳过），不要再依赖 A商品名称。
    # We also combine Name + Spec for matching.
    df['_temp_input'] = df.apply(lambda r: f"{str(r[name_col]).strip()} {str(r[spec_col]).strip()}".strip() 
                               if str(r[spec_col]).lower() != 'nan' else str(r[name_col]).strip(), axis=1)
    
    name_ok = df[name_col].fillna("").astype(str).str.strip()
    name_ok = name_ok[(name_ok != "") & (name_ok.str.lower() != "nan")].index

    # 行需要处理的条件：商品名称非空，且任一目标列为空（首次或部分补全）
    def _cell_empty(v):
        s = "" if v is None else str(v).strip()
        return (s == "") or (s.lower() in ("nan", "none", "null"))

    need_cols = target_cols
    need_mask = df[need_cols].applymap(_cell_empty).any(axis=1) if need_cols else pd.Series(False, index=df.index)
    mask_to_process = df.index.isin(name_ok) & need_mask
    rows_to_process = df[mask_to_process]
    
    if rows_to_process.empty:
        _ai_log(log_tag, f"跳过: 无待处理行（认为 A* 已齐） path={file_path}")
        df.drop(columns=['_temp_input'], inplace=True)
        return

    # 2. Get unique inputs from those rows
    unique_inputs = rows_to_process['_temp_input'].unique().tolist()
    total_unique = len(unique_inputs)
    _ai_log(
        log_tag,
        f"待处理: 行数={len(rows_to_process)} 去重后条数={total_unique} batch_size={batch_size}",
    )

    # 3. Process unique items in batches
    results_map = {} # combined_name -> ProductInfo

    # Build a stable mapping: _temp_input -> (name, spec) from the original sheet columns.
    # This avoids truncation/heuristics that can materially change model inputs.
    tmp_map = {}
    for _, r in rows_to_process.iterrows():
        k = str(r.get('_temp_input', '')).strip()
        if not k or k in tmp_map:
            continue
        nm = str(r.get(name_col, '')).strip()
        sp = str(r.get(spec_col, '')).strip()
        if nm.lower() == 'nan': nm = ''
        if sp.lower() == 'nan': sp = ''
        tmp_map[k] = {"name": nm, "spec": sp}
    
    for i in range(0, total_unique, batch_size):
        batch_inputs = unique_inputs[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (total_unique - 1) // batch_size + 1
        _ai_log(log_tag, f"子批次 {batch_num}/{total_batches} 条数={len(batch_inputs)}")
        
        if progress_cb:
            progress_cb(batch_num, total_batches)

        # Build item list in the same order as batch_inputs using tmp_map.
        batch_items = [tmp_map.get(str(s).strip(), {"name": str(s).strip(), "spec": ""}) for s in batch_inputs]
        batch_results = extract_batch_ai(
            batch_items,
            api_key=api_key,
            model_name=model_name,
            log_tag=log_tag,
            fallback_api_key=fallback_api_key,
            fallback_model=fallback_model,
        )
        
        for name, res in zip(batch_inputs, batch_results):
            results_map[name] = res
        
        # We don't save every batch anymore to save time, but we could save every N batches if needed.
        # For now, let's keep it simple and save once at the end or every 5 batches.
        if (batch_num % 5 == 0) or (i + batch_size >= total_unique):
             # Sync back to main DF for rows in THIS batch (optional, but good for "safe" intermediate state)
             pass

    # 4. Map results back to the original DataFrame
    for name, res in results_map.items():
        row_mask = (df['_temp_input'] == name) & mask_to_process
        df.loc[row_mask, 'A单件净含量'] = getattr(res, "net_content", "") or ""
        df.loc[row_mask, 'A售卖数量'] = getattr(res, "sell_quantity", "") or ""
        df.loc[row_mask, 'A包装单位'] = getattr(res, "packaging_unit", "") or ""
        df.loc[row_mask, 'A颜色'] = " | ".join(getattr(res, "color", []) or [])
        df.loc[row_mask, 'A尺寸'] = " | ".join(getattr(res, "size", []) or [])
        # Model post-processing: keep closer to offline extractor output
        df.loc[row_mask, 'A型号'] = _postprocess_model(getattr(res, "model", "") or "", "", name)  # name==_temp_input here
        df.loc[row_mask, EXTRACTION_SOURCE_COL] = _get_extraction_source(res) or MODEL_EXTRACTION_SOURCE

    df.drop(columns=['_temp_input'], inplace=True)

    _summarize_written_a_columns(df, log_tag)

    # 5. Final Save
    if safe_save(df, file_path):
        _ai_log(log_tag, f"已保存: {file_path}")
    else:
        _ai_log(log_tag, f"CRITICAL 保存失败: {file_path}")


if __name__ == "__main__":
    base_dir = "/Users/admin/Documents/Antigravity_projects/pro_image/0307/0314"
    # Process the specific files requested by the user
    files = ["乐购达.xlsx", "沃玛希.xlsx", "优购哆0313.xlsx","犀牛.xlsx","AA百货.xlsx"]
    
    test_api_key = os.environ.get("GEMINI_API_KEY", "YOUR_API_KEY")
    
    for filename in files:
        file_to_process = os.path.join(base_dir, filename)

        if os.path.exists(file_to_process):
            process_file_ai(file_to_process, api_key=test_api_key, batch_size=110)
        else:
            print(f"File not found: {file_to_process}")
