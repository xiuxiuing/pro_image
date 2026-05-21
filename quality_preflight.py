from __future__ import annotations

import os
import json
import time
from typing import Dict, Iterable, List, Optional

import pandas as pd

import utils
from field_registry import (
    REQUIRED_STANDARD_FIELDS,
    RULE_ATTRIBUTE_FIELDS,
    canonical_storage_field,
    detect_field_mapping,
)


BLOCK_REQUIRED = {"skuId", "商品名称"}
CONFIRM_REQUIRED = set(REQUIRED_STANDARD_FIELDS) - BLOCK_REQUIRED
RULE_METRIC_TO_FIELD = {
    "net": "A单件净含量",
    "sell": "A售卖数量",
    "pack": "A包装单位",
    "color": "A颜色",
    "size": "A尺寸",
    "model": "A型号",
}


def _is_empty(value) -> bool:
    value = utils.clean_text_value(value)
    if value is None:
        return True
    s = str(value).strip()
    return not s or s.lower() in ("nan", "none", "null", "-")


def _read_df(path: str) -> pd.DataFrame:
    rows = utils.excel_to_list_dict(path)
    return pd.DataFrame(rows)


def _apply_mapping_to_df(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    for standard, source_col in (mapping or {}).items():
        if not source_col or source_col not in out.columns:
            continue
        target = canonical_storage_field(standard)
        if target in out.columns and target != source_col:
            out[target] = out[target].where(~out[target].map(_is_empty), out[source_col])
        elif target != source_col:
            out.rename(columns={source_col: target}, inplace=True)
    return out


def _non_empty_rate(df: pd.DataFrame, col: str) -> float:
    if df.empty or col not in df.columns:
        return 0.0
    return round(float((~df[col].map(_is_empty)).sum()) / max(1, len(df)), 4)


def enabled_rule_fields(rule_template: Optional[dict]) -> set:
    fields = set()
    if not isinstance(rule_template, dict):
        return fields
    for group in rule_template.get("rule_groups") or []:
        metrics = (group or {}).get("metrics") or {}
        for key, field in RULE_METRIC_TO_FIELD.items():
            if (metrics.get(key) or {}).get("en"):
                fields.add(field)
    return fields


def inspect_file(path: str, label: str, user_mappings: Optional[Dict[str, str]] = None, rule_template: Optional[dict] = None) -> dict:
    df = _read_df(path)
    headers = list(df.columns)
    detection = detect_field_mapping(headers, user_mappings=user_mappings)
    mapping = {
        std: info["column"]
        for std, info in detection.items()
        if info.get("column")
    }
    normalized = _apply_mapping_to_df(df, mapping)

    missing_block = [f for f in BLOCK_REQUIRED if not detection.get(f, {}).get("column")]
    missing_confirm = [f for f in CONFIRM_REQUIRED if not detection.get(f, {}).get("column")]
    suggested = [
        info for f, info in detection.items()
        if f in REQUIRED_STANDARD_FIELDS and not info.get("column") and info.get("suggestions")
    ]
    metrics = {
        "rows": int(len(df)),
        "fields": {},
        "rule_fields": {},
    }
    for field in REQUIRED_STANDARD_FIELDS:
        storage = canonical_storage_field(field)
        metrics["fields"][field] = {
            "column": detection.get(field, {}).get("column", ""),
            "rate": _non_empty_rate(normalized, storage),
        }
    for field in RULE_ATTRIBUTE_FIELDS:
        storage = canonical_storage_field(field)
        metrics["rule_fields"][field] = {
            "column": detection.get(field, {}).get("column", ""),
            "rate": _non_empty_rate(normalized, storage),
        }

    issues = []
    level = "ok"
    enabled_fields = enabled_rule_fields(rule_template)
    if metrics["rows"] == 0:
        issues.append({"level": "block", "message": f"{label} 没有有效数据行"})
        level = "block"
    for f in missing_block:
        issues.append({"level": "block", "message": f"{label} 缺少必需列：{f}"})
        level = "block"
    for f in missing_confirm:
        issues.append({"level": "confirm", "message": f"{label} 未识别到列：{f}"})
        if level != "block":
            level = "confirm"
    for f, item in metrics["fields"].items():
        if item["column"] and item["rate"] < 0.5 and f in BLOCK_REQUIRED:
            issues.append({"level": "block", "message": f"{label} 字段 {f} 非空率过低：{item['rate']:.0%}"})
            level = "block"
        elif item["column"] and item["rate"] < 0.7:
            issues.append({"level": "confirm", "message": f"{label} 字段 {f} 非空率偏低：{item['rate']:.0%}"})
            if level != "block":
                level = "confirm"
    for f, item in metrics["rule_fields"].items():
        if f in enabled_fields and item["rate"] < 0.5:
            issues.append({"level": "confirm", "message": f"{label} 当前规则启用字段 {f} 覆盖率偏低：{item['rate']:.0%}"})
            if level != "block":
                level = "confirm"
        elif item["column"] and item["rate"] < 0.5:
            issues.append({"level": "warn", "message": f"{label} 规则字段 {f} 覆盖率偏低：{item['rate']:.0%}"})
            if level == "ok":
                level = "warn"

    return {
        "label": label,
        "path": path,
        "level": level,
        "headers": headers,
        "mapping": detection,
        "confirmed_mapping": mapping,
        "suggested": suggested,
        "metrics": metrics,
        "enabled_rule_fields": sorted(enabled_fields),
        "issues": issues,
    }


def inspect_files(files: Iterable[dict], user_mappings: Optional[Dict[str, Dict[str, str]]] = None, rule_template: Optional[dict] = None) -> dict:
    user_mappings = user_mappings or {}
    items = []
    level_rank = {"ok": 0, "warn": 1, "confirm": 2, "block": 3}
    overall = "ok"
    for item in files:
        key = item.get("key") or item.get("label") or os.path.basename(item["path"])
        report = inspect_file(item["path"], item.get("label") or key, user_mappings.get(key) or {}, rule_template=rule_template)
        report["key"] = key
        items.append(report)
        if level_rank[report["level"]] > level_rank[overall]:
            overall = report["level"]
    return {
        "status": "ok",
        "level": overall,
        "items": items,
        "can_continue": overall in ("ok", "warn", "confirm"),
        "requires_confirmation": overall == "confirm",
    }


def normalize_file_for_analysis(src_path: str, dst_path: str, user_mapping: Optional[Dict[str, str]] = None) -> str:
    df = _read_df(src_path)
    detection = detect_field_mapping(list(df.columns), user_mappings=user_mapping or {})
    mapping = {std: info["column"] for std, info in detection.items() if info.get("column")}
    out = _apply_mapping_to_df(df, mapping)
    os.makedirs(os.path.dirname(dst_path), exist_ok=True)
    out.to_excel(dst_path, index=False)
    return dst_path


def _rate(numerator: int, denominator: int) -> float:
    return round(float(numerator) / max(1, int(denominator or 0)), 4)


def build_quality_report(preflight: Optional[dict] = None, analysis_metrics: Optional[dict] = None, context: Optional[dict] = None) -> dict:
    preflight = preflight or {}
    analysis_metrics = analysis_metrics or {}
    context = context or {}
    download_total = download_success = 0
    vector_total = vector_success = 0
    for src in analysis_metrics.get("sources") or []:
        dl = src.get("download") or {}
        download_total += int(dl.get("total") or 0)
        download_success += int(dl.get("success") or 0)
        for key in ("image_index", "text_index"):
            idx = src.get(key) or {}
            if idx.get("reused"):
                continue
            vector_total += int(idx.get("total") or 0)
            vector_success += int(idx.get("vectors") or 0)
    q = (analysis_metrics.get("query") or {})
    dl = q.get("download") or {}
    download_total += int(dl.get("total") or 0)
    download_success += int(dl.get("success") or 0)
    for key in ("image_vectors", "text_vectors"):
        item = q.get(key) or {}
        vector_total += int(item.get("total") or 0)
        vector_success += int(item.get("vectors") or 0)

    match_total = rule_rejected = vector_candidates = 0
    for src in ((analysis_metrics.get("matching") or {}).get("sources") or []):
        match_total += int(src.get("matched") or 0)
        rule_rejected += int(src.get("rule_rejected") or 0)
        vector_candidates += int(src.get("vector_candidates") or 0)

    warnings = []
    for item in preflight.get("items") or []:
        for issue in item.get("issues") or []:
            if issue.get("level") in ("warn", "confirm", "block"):
                warnings.append(issue.get("message", ""))
    if download_total and _rate(download_success, download_total) < 0.8:
        warnings.append(f"图片下载成功率偏低：{_rate(download_success, download_total):.0%}")
    if vector_total and _rate(vector_success, vector_total) < 0.8:
        warnings.append(f"AI生成成功率偏低：{_rate(vector_success, vector_total):.0%}")

    return {
        "version": 1,
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "context": context,
        "summary": {
            "preflight_level": preflight.get("level", "unknown"),
            "download_success_rate": _rate(download_success, download_total),
            "download_success": download_success,
            "download_total": download_total,
            "vector_success_rate": _rate(vector_success, vector_total),
            "vector_success": vector_success,
            "vector_total": vector_total,
            "matched": match_total,
            "rule_rejected": rule_rejected,
            "vector_candidates": vector_candidates,
            "warning_count": len([w for w in warnings if w]),
        },
        "warnings": [w for w in warnings if w],
        "preflight": preflight,
        "analysis": analysis_metrics,
    }


def save_quality_report(report: dict, path: str) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    return path
