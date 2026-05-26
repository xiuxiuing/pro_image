import pandas as pd
from google import genai
import os
import re
import time
import json
import threading
import uuid
from typing import Optional
import utils
from field_registry import detect_field_mapping, canonical_storage_field, REQUIRED_STANDARD_FIELDS, RULE_ATTRIBUTE_FIELDS

from extract_info_schema import (
    A_FIELD_COLUMNS,
    BatchResponse,
    DEFAULT_MOONSHOT_MODEL,
    EXTRACTION_SOURCE_COL,
    MODEL_EXTRACTION_SOURCE,
    MOONSHOT_BASE_URL,
)
from extract_info_rules import (
    _ai_log,
    _build_extraction_prompt,
    _get_extraction_source,
    _heuristic_batch,
    _mark_extraction_source,
    _normalize_batch_dict_for_validate,
    _postprocess_model,
    _strip_markdown_json_fences,
)

_INNER_COUNT_UNITS = {"片", "张", "枚", "粒", "颗", "条", "支", "个", "只", "根", "贴", "卷", "双", "副", "对"}
_OUTER_PACK_UNITS = {"盒", "包", "袋", "箱", "套", "组", "件", "份"}

_gemini_key_idx = 0
_gemini_key_lock = threading.Lock()
_deepseek_key_idx = 0
_deepseek_key_lock = threading.Lock()

# API key / model name are passed from the frontend
DEFAULT_MODEL_NAME = "models/gemini-3.1-flash-lite"
DEFAULT_DEEPSEEK_MODEL = "deepseek-chat"
DEEPSEEK_BASE_URL = "https://api.deepseek.com"
DEFAULT_EXTRACTION_BATCH_SIZE = int(os.environ.get("PROIMAGE_ASTAR_BATCH_SIZE", "60") or "60")


def _merge_model_with_rule_fallback(model_items, source_items, log_tag: str = ""):
    local_items = _heuristic_batch(source_items, log_tag=f"{log_tag}-rule-merge" if log_tag else "rule-merge")
    merged = []
    filled = 0
    corrected = 0
    for model_item, local_item in zip(model_items or [], local_items or []):
        for attr in (
            "core_category",
            "net_content",
            "sell_quantity",
            "size",
            "multidim_size",
        ):
            mv = getattr(model_item, attr, None)
            lv = getattr(local_item, attr, None)
            empty = not mv if not isinstance(mv, list) else len(mv) == 0
            if empty and lv:
                setattr(model_item, attr, lv)
                filled += 1
        pu = (getattr(model_item, "packaging_unit", "") or "").strip()
        if (not pu or pu == "未知") and (getattr(local_item, "packaging_unit", "") or "").strip() not in ("", "未知"):
            setattr(model_item, "packaging_unit", local_item.packaging_unit)
            filled += 1
        model_qty = (getattr(model_item, "sell_quantity", "") or "").strip()
        model_pack = (getattr(model_item, "packaging_unit", "") or "").strip()
        local_qty = (getattr(local_item, "sell_quantity", "") or "").strip()
        local_pack = (getattr(local_item, "packaging_unit", "") or "").strip()
        if (
            local_qty
            and local_pack in _INNER_COUNT_UNITS
            and model_pack in _OUTER_PACK_UNITS
            and (model_qty, model_pack) != (local_qty, local_pack)
        ):
            setattr(model_item, "sell_quantity", local_qty)
            setattr(model_item, "packaging_unit", local_pack)
            corrected += 1
        # Weak fields are not hard constraints, but filling them improves ranking/explainability.
        for attr in ("product_form", "key_attributes", "color"):
            mv = getattr(model_item, attr, None)
            lv = getattr(local_item, attr, None)
            empty = not mv if not isinstance(mv, list) else len(mv) == 0
            if empty and lv:
                setattr(model_item, attr, lv)
                filled += 1
        mv = getattr(model_item, "brand", None)
        lv = getattr(local_item, "brand", None)
        if not mv and lv:
            setattr(model_item, "brand", lv)
            filled += 1
        merged.append(model_item)
    if filled:
        _ai_log(log_tag, f"模型结果叠加本地兜底补齐字段 {filled} 处")
    if corrected:
        _ai_log(log_tag, f"模型结果按本地规格规则纠正售卖规格 {corrected} 处")
    return _mark_extraction_source(merged, MODEL_EXTRACTION_SOURCE)


def _split_api_keys(api_key: str) -> list[str]:
    keys = [k.strip() for k in re.split(r"[,，;；\s]+", str(api_key or "")) if k.strip()]
    return keys or [""]


def _model_provider_label(provider: str) -> str:
    p = (provider or "gemini").strip().lower()
    if p == "deepseek":
        return "DeepSeek"
    if p in ("kimi", "moonshot"):
        return "Kimi(Moonshot)"
    return "Gemini"


def _model_provider_default_model(provider: str) -> str:
    p = (provider or "gemini").strip().lower()
    if p == "deepseek":
        return DEFAULT_DEEPSEEK_MODEL
    if p in ("kimi", "moonshot"):
        return DEFAULT_MOONSHOT_MODEL
    return DEFAULT_MODEL_NAME


def _format_model_error(provider: str, model_name: str, exc: Exception) -> str:
    raw = str(exc) or exc.__class__.__name__
    low = raw.lower()
    label = _model_provider_label(provider)
    model = (model_name or _model_provider_default_model(provider)).strip()
    if any(marker in low for marker in ("connection", "timeout", "timed out", "network", "ssl", "proxy")):
        return f"{label} 连接失败，请检查网络/代理、API Key 权限和模型名（当前：{model}）：{raw}"
    if "api key" in low or "unauthorized" in low or "permission" in low or "401" in low or "403" in low:
        return f"{label} 鉴权失败或无模型权限（当前：{model}）：{raw}"
    if "not found" in low or "404" in low or "model" in low:
        return f"{label} 模型名不可用或当前 Key 无权限访问（当前：{model}）：{raw}"
    return raw


def _extract_batch_openai_compatible(
    items,
    api_key: str,
    model_name: str,
    base_url: str,
    provider: str,
    max_retries: int = 5,
    log_tag: str = "",
):
    if not items:
        return []
    try:
        from openai import OpenAI
    except ImportError:
        _ai_log(log_tag, f"{_model_provider_label(provider)} 跳过: 未安装 openai（pip install openai），改用本地规则兜底")
        return _heuristic_batch(items, log_tag=log_tag)

    global _deepseek_key_idx
    keys = _split_api_keys(api_key)
    model = (model_name or _model_provider_default_model(provider)).strip() or _model_provider_default_model(provider)
    prompt = _build_extraction_prompt(items)
    _ai_log(log_tag, f"请求 {_model_provider_label(provider)} batch: model={model!r} items={len(items)} keys_count={len(keys)}")

    for attempt in range(max_retries):
        with _deepseek_key_lock:
            current_key = keys[_deepseek_key_idx % len(keys)]
            _deepseek_key_idx += 1
        try:
            client = OpenAI(api_key=current_key, base_url=base_url, timeout=120.0, max_retries=2)
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
                    f"{_model_provider_label(provider)} attempt {attempt + 1}/{max_retries}: 条数不一致 "
                    f"{len(batch.items)} vs {len(items)}",
                )
                time.sleep(5)
                continue
            if batch.items:
                s0 = batch.items[0]
                _ai_log(
                    log_tag,
                    f"{_model_provider_label(provider)} 本批成功: 条数={len(batch.items)} "
                    f"样例[0] sell={getattr(s0, 'sell_quantity', '')!r} "
                    f"pack={getattr(s0, 'packaging_unit', '')!r} net={getattr(s0, 'net_content', '')!r}",
                )
            else:
                _ai_log(log_tag, f"{_model_provider_label(provider)} 本批成功: 条数=0（无条目）")
            return _merge_model_with_rule_fallback(batch.items, items, log_tag=log_tag)
        except Exception as e:
            err_msg = _format_model_error(provider, model, e)
            if "429" in err_msg or "rate" in err_msg.lower() or "限流" in err_msg:
                wait_time = (attempt + 1) * 20
                _ai_log(log_tag, f"{_model_provider_label(provider)} attempt {attempt + 1}/{max_retries}: 限流/429，{wait_time}s 后重试 — {err_msg[:180]}")
                time.sleep(wait_time)
            elif "503" in err_msg or "UNAVAILABLE" in err_msg:
                _ai_log(log_tag, f"{_model_provider_label(provider)} attempt {attempt + 1}/{max_retries}: 503/繁忙 — {err_msg[:180]}")
                time.sleep(15)
            else:
                _ai_log(log_tag, f"{_model_provider_label(provider)} attempt {attempt + 1}/{max_retries}: {type(e).__name__}: {err_msg}")
                time.sleep(8)

    _ai_log(log_tag, f"{_model_provider_label(provider)} 已放弃 batch_len={len(items)}，改用本地规则兜底")
    return _heuristic_batch(items, log_tag=log_tag)


def extract_batch_deepseek(
    items,
    api_key: str,
    model_name: Optional[str] = None,
    max_retries: int = 5,
    log_tag: str = "",
):
    base_url = (os.environ.get("DEEPSEEK_BASE_URL") or DEEPSEEK_BASE_URL).strip()
    return _extract_batch_openai_compatible(
        items,
        api_key=api_key,
        model_name=(model_name or DEFAULT_DEEPSEEK_MODEL),
        base_url=base_url,
        provider="deepseek",
        max_retries=max_retries,
        log_tag=log_tag,
    )


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
            return _merge_model_with_rule_fallback(batch.items, items, log_tag=log_tag)
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
    provider: str = "gemini",
    fallback_provider: str = "kimi",
    allow_split: bool = True,
):
    global _gemini_key_idx
    provider_norm = (provider or "gemini").strip().lower()
    if provider_norm == "deepseek":
        return extract_batch_deepseek(
            items,
            api_key=api_key,
            model_name=model_name,
            max_retries=max_retries,
            log_tag=log_tag,
        )
    if provider_norm not in ("gemini", ""):
        _ai_log(log_tag, f"未知模型类型 {provider!r}，按 Gemini 处理")

    keys = _split_api_keys(api_key)

    model_name = (model_name or DEFAULT_MODEL_NAME).strip() or DEFAULT_MODEL_NAME
    _ai_log(log_tag, f"请求 Gemini batch: model={model_name!r} items={len(items)} keys_count={len(keys)}")
    prompt = _build_extraction_prompt(items)

    for attempt in range(max_retries):
        with _gemini_key_lock:
            current_key = keys[_gemini_key_idx % len(keys)]
            _gemini_key_idx += 1
            
        client = genai.Client(api_key=current_key)
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
                    return _merge_model_with_rule_fallback(parsed_items, items, log_tag=log_tag)
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
            provider=provider_norm or "gemini",
            fallback_provider=fallback_provider,
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
            provider=provider_norm or "gemini",
            fallback_provider=fallback_provider,
            allow_split=True,
        )
        return left + right

    fk = (fallback_api_key or "").strip()
    if fk:
        fallback_provider_norm = (fallback_provider or "kimi").strip().lower()
        if fallback_provider_norm == "deepseek":
            _ai_log(log_tag, "改用 DeepSeek 兜底本批…")
            return extract_batch_deepseek(
                items,
                api_key=fk,
                model_name=(fallback_model or "").strip() or None,
                max_retries=max_retries,
                log_tag=log_tag,
            )
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
    temp_path = f"{file_path}.{os.getpid()}.{threading.get_ident()}.{uuid.uuid4().hex}.tmp.xlsx"
    try:
        df.to_excel(temp_path, index=False, engine='openpyxl')
        os.replace(temp_path, file_path)
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
    batch_size=None,
    progress_cb=None,
    model_name=None,
    fallback_api_key: Optional[str] = None,
    fallback_model: Optional[str] = None,
    provider: str = "gemini",
    fallback_provider: str = "kimi",
):
    if batch_size is None:
        batch_size = DEFAULT_EXTRACTION_BATCH_SIZE
    log_tag = os.path.basename(file_path) or "unknown.xlsx"
    _ai_log(log_tag, f"开始处理文件 path={file_path}")
    print(f"Loading {file_path}...")
    try:
        df = pd.read_excel(file_path, engine='openpyxl')
    except Exception as e:
        print(f"Failed to read {file_path} with openpyxl: {e}")
        return

    df.columns = [utils.clean_text_value(c) for c in df.columns]
    detected = detect_field_mapping(df.columns, standards=REQUIRED_STANDARD_FIELDS + RULE_ATTRIBUTE_FIELDS)
    for standard, info in detected.items():
        source_col = info.get("column")
        target_col = canonical_storage_field(standard)
        if not source_col or source_col not in df.columns or source_col == target_col:
            continue
        if target_col in df.columns:
            df[target_col] = df[target_col].where(df[target_col].fillna("").astype(str).str.strip() != "", df[source_col])
        else:
            df.rename(columns={source_col: target_col}, inplace=True)

    # Identify column names
    cols = df.columns.tolist()
    name_col = '商品名称'
    spec_col = '规格名称'

    if name_col not in cols or spec_col not in cols:
        print(f"Required columns not found in {file_path}. Available: {cols}")
        return

    # Initialize target columns if they don't exist (A* extraction columns + source marker)
    target_cols = list(A_FIELD_COLUMNS)
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
    need_mask = df[need_cols].map(_cell_empty).any(axis=1) if need_cols else pd.Series(False, index=df.index)
    
    if EXTRACTION_SOURCE_COL in df.columns:
        source_empty = df[EXTRACTION_SOURCE_COL].map(_cell_empty)
        need_mask = need_mask & source_empty

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
        tmp_map[k] = {
            "name": nm,
            "spec": sp,
            "l1": str(r.get("美团类目一级", "") or "").strip(),
            "l2": str(r.get("美团类目二级", "") or "").strip(),
            "l3": str(r.get("美团类目三级", "") or "").strip(),
        }
    
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
            provider=provider,
            fallback_provider=fallback_provider,
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
        df.loc[row_mask, 'A品牌'] = getattr(res, "brand", "") or ""
        # Model post-processing: keep closer to offline extractor output
        df.loc[row_mask, 'A型号'] = _postprocess_model(getattr(res, "model", "") or "", "", name)  # name==_temp_input here
        df.loc[row_mask, 'A核心品类'] = getattr(res, "core_category", "") or ""
        df.loc[row_mask, 'A商品形态'] = getattr(res, "product_form", "") or ""
        df.loc[row_mask, 'A关键属性词'] = " | ".join(getattr(res, "key_attributes", []) or [])
        df.loc[row_mask, 'A多维尺寸'] = " | ".join(getattr(res, "multidim_size", []) or [])
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
