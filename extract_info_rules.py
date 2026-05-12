import json
import re
from extract_info_schema import (
    ALLOWED_PACKAGING,
    MODEL_EXTRACTION_SOURCE,
    RULE_EXTRACTION_SOURCE,
    ProductInfo,
)
from product_text_extract import extract_product_keys


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
