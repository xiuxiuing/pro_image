#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import random
import re
import sys
from pathlib import Path
from typing import Iterable

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from extract_info_ai2 import (  # noqa: E402
    DEFAULT_DEEPSEEK_MODEL,
    DEFAULT_MODEL_NAME,
    extract_batch_ai,
)
from extract_info_rules import _heuristic_batch  # noqa: E402
from extract_info_schema import A_FIELD_COLUMNS, ProductInfo  # noqa: E402
from field_registry import detect_field_mapping  # noqa: E402


PROVIDER_PREFIX = {"deepseek": "DeepSeek", "gemini": "Gemini", "local": "本地规则"}
PRODUCT_INFO_TO_A_FIELD = {
    "core_category": "A核心品类",
    "net_content": "A单件净含量",
    "sell_quantity": "A售卖数量",
    "packaging_unit": "A包装单位",
    "size": "A尺寸",
    "multidim_size": "A多维尺寸",
    "model": "A型号",
    "product_form": "A商品形态",
    "key_attributes": "A关键属性词",
    "color": "A颜色",
}


def _clean(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def _join(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        return " | ".join(_clean(x) for x in value if _clean(x))
    return _clean(value)


def _mapped(row: pd.Series, mapping: dict, field: str) -> str:
    col = (mapping.get(field) or {}).get("column") or ""
    return _clean(row.get(col)) if col else ""


def _read_products(files: Iterable[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    wanted = [
        "skuId",
        "商品名称",
        "规格名称",
        "美团类目一级",
        "美团类目二级",
        "美团类目三级",
    ]
    for file in files:
        path = Path(file).expanduser()
        df = pd.read_excel(path)
        mapping = detect_field_mapping(df.columns, standards=wanted)
        rows = []
        for idx, row in df.iterrows():
            name = _mapped(row, mapping, "商品名称")
            spec = _mapped(row, mapping, "规格名称")
            if not name and not spec:
                continue
            rows.append(
                {
                    "来源文件": path.name,
                    "源行号": int(idx) + 2,
                    "skuId": _mapped(row, mapping, "skuId"),
                    "商品名称": name,
                    "规格名称": spec,
                    "美团类目一级": _mapped(row, mapping, "美团类目一级"),
                    "美团类目二级": _mapped(row, mapping, "美团类目二级"),
                    "美团类目三级": _mapped(row, mapping, "美团类目三级"),
                }
            )
        frames.append(pd.DataFrame(rows))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _correction_keys(path: str) -> tuple[set[str], set[str]]:
    if not path:
        return set(), set()
    p = Path(path).expanduser()
    if not p.exists():
        return set(), set()
    df = pd.read_excel(p)
    skus: set[str] = set()
    cats: set[str] = set()
    for col in df.columns:
        col_s = str(col)
        vals = {_clean(x) for x in df[col].dropna().tolist()}
        if any(k in col_s.lower() for k in ("sku", "skuid")) or "规格ID" in col_s:
            skus.update(x for x in vals if x)
        if "类目" in col_s or "分类" in col_s:
            cats.update(x for x in vals if x)
    return skus, cats


def _sample_rows(df: pd.DataFrame, mode: str, sample_size: int, corrections: str = "", seed: int = 20260525) -> pd.DataFrame:
    if df.empty:
        return df
    n = min(sample_size, len(df))
    if mode == "random":
        return df.sample(n=n, random_state=seed).reset_index(drop=True)
    if mode == "corrections":
        skus, cats = _correction_keys(corrections)
        mask = pd.Series(False, index=df.index)
        if skus:
            mask = mask | df["skuId"].astype(str).isin(skus)
        if cats:
            for col in ("美团类目一级", "美团类目二级", "美团类目三级"):
                mask = mask | df[col].astype(str).isin(cats)
        picked = df[mask]
        if len(picked) >= n:
            return picked.sample(n=n, random_state=seed).reset_index(drop=True)
        rest = df[~mask].sample(n=min(n - len(picked), len(df[~mask])), random_state=seed)
        return pd.concat([picked, rest], ignore_index=True).head(n)

    # highfreq: prioritize frequent l1/l2/l3 combinations, then sample inside them.
    grouped = (
        df.groupby(["美团类目一级", "美团类目二级", "美团类目三级"], dropna=False)
        .size()
        .sort_values(ascending=False)
    )
    chunks: list[pd.DataFrame] = []
    per_group = max(1, n // min(10, max(1, len(grouped))))
    for key, _cnt in grouped.head(20).items():
        mask = (
            (df["美团类目一级"] == key[0])
            & (df["美团类目二级"] == key[1])
            & (df["美团类目三级"] == key[2])
        )
        part = df[mask]
        chunks.append(part.sample(n=min(per_group, len(part)), random_state=seed))
        if sum(len(x) for x in chunks) >= n:
            break
    picked = pd.concat(chunks, ignore_index=True) if chunks else df.iloc[0:0]
    if len(picked) < n:
        rest = df.drop(picked.index, errors="ignore")
        if len(rest):
            picked = pd.concat(
                [picked, rest.sample(n=min(n - len(picked), len(rest)), random_state=seed)],
                ignore_index=True,
            )
    return picked.head(n).reset_index(drop=True)


def _items_from_rows(rows: pd.DataFrame) -> list[dict]:
    return [
        {
            "name": r["商品名称"],
            "spec": r["规格名称"],
            "l1": r["美团类目一级"],
            "l2": r["美团类目二级"],
            "l3": r["美团类目三级"],
        }
        for _, r in rows.iterrows()
    ]


def _batch_provider(provider: str, items: list[dict], args) -> list[ProductInfo] | None:
    if provider == "local":
        return _heuristic_batch(items, log_tag="抽取质检-local")
    batch_size = max(1, int(getattr(args, "batch_size", 60) or 60))
    chunks = [items[i : i + batch_size] for i in range(0, len(items), batch_size)]
    results: list[ProductInfo] = []
    if provider == "deepseek":
        key = args.deepseek_key or os.environ.get("DEEPSEEK_API_KEY", "")
        if not key:
            print("[抽取质检] DeepSeek 缺少 Key，跳过", flush=True)
            return None
        for idx, chunk in enumerate(chunks, start=1):
            print(f"[抽取质检] DeepSeek 分批 {idx}/{len(chunks)}: {len(chunk)} 条", flush=True)
            results.extend(
                extract_batch_ai(
                    chunk,
                    key,
                    model_name=args.deepseek_model or DEFAULT_DEEPSEEK_MODEL,
                    max_retries=args.max_retries,
                    log_tag=f"抽取质检-deepseek-{idx}",
                    provider="deepseek",
                )
            )
        return results
    if provider == "gemini":
        key = args.gemini_key or os.environ.get("GEMINI_API_KEY", "")
        if not key:
            print("[抽取质检] Gemini 缺少 Key，跳过", flush=True)
            return None
        for idx, chunk in enumerate(chunks, start=1):
            print(f"[抽取质检] Gemini 分批 {idx}/{len(chunks)}: {len(chunk)} 条", flush=True)
            results.extend(
                extract_batch_ai(
                    chunk,
                    key,
                    model_name=args.gemini_model or DEFAULT_MODEL_NAME,
                    max_retries=args.max_retries,
                    log_tag=f"抽取质检-gemini-{idx}",
                    provider="gemini",
                    fallback_api_key="",
                )
            )
        return results
    raise ValueError(f"unknown provider: {provider}")


def _field_value(item: ProductInfo, a_field: str) -> str:
    for attr, field in PRODUCT_INFO_TO_A_FIELD.items():
        if field == a_field:
            return _join(getattr(item, attr, ""))
    return ""


def _issue_flags(row: pd.Series, item: ProductInfo | None) -> list[str]:
    text = f"{row.get('商品名称', '')} {row.get('规格名称', '')}"
    flags: list[str] = []
    if not item:
        return flags
    if not item.core_category:
        flags.append("核心品类漏抽")
    if item.packaging_unit == "未知" and item.sell_quantity:
        flags.append("包装单位未知")
    if not item.sell_quantity and re.search(r"\d+\s*(袋|盒|瓶|罐|桶|箱|听|杯|支|条|片|套|枚|个|只|包|件|根|张|双|副|板|组|卷)", text):
        flags.append("数量疑似漏抽")
    if any(re.search(r"\d\s*[x×*]\s*\d", str(x), flags=re.IGNORECASE) for x in item.size or []):
        flags.append("多维尺寸误入A尺寸")
    has_multidim_hint = re.search(r"\d+(?:\.\d+)?\s*[x×*]\s*\d+", text)
    has_pack_count_hint = re.search(r"\d{2,4}\s*[x×*]\s*\d+\s*(片|枚|个|只|包|袋|瓶|罐|支|条)\b", text)
    if has_multidim_hint and not has_pack_count_hint and not item.multidim_size:
        flags.append("多维尺寸疑似漏抽")
    if re.search(r"(建议|适合|体重|斤以内).{0,12}\d+(?:\.\d+)?\s*(斤|kg|千克|公斤)", text) and item.net_content:
        flags.append("净含量疑似误抽体重")
    if re.fullmatch(r"(?i)(xxxl|xxl|xl|l|m|s)码?", item.model or ""):
        flags.append("尺码误入A型号")
    return flags


def _build_report(rows: pd.DataFrame, provider_results: dict[str, list[ProductInfo] | None]) -> pd.DataFrame:
    out_rows = []
    local_items = provider_results.get("local")
    for idx, row in rows.iterrows():
        out = row.to_dict()
        for provider, items in provider_results.items():
            label = PROVIDER_PREFIX[provider]
            if not items:
                for field in A_FIELD_COLUMNS:
                    out[f"{label}_{field}"] = ""
                continue
            item = items[idx]
            for field in A_FIELD_COLUMNS:
                out[f"{label}_{field}"] = _field_value(item, field)
        all_flags: list[str] = []
        for provider, items in provider_results.items():
            item = items[idx] if items else None
            for flag in _issue_flags(row, item):
                all_flags.append(f"{PROVIDER_PREFIX[provider]}:{flag}")
        out["字段可用性判断"] = "需复核" if all_flags else "可用"
        out["问题类型"] = "、".join(all_flags)
        out_rows.append(out)
    return pd.DataFrame(out_rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="V2 十个 A*字段抽取质量抽样报告")
    parser.add_argument("--files", nargs="+", required=True, help="待抽样商品 Excel 文件")
    parser.add_argument("--output", required=True, help="输出 xlsx 路径")
    parser.add_argument("--sample-size", type=int, default=100)
    parser.add_argument("--mode", choices=["highfreq", "corrections", "random"], default="random")
    parser.add_argument("--corrections", default="", help="订正数据 xlsx，用于 corrections 抽样")
    parser.add_argument("--providers", default="deepseek,gemini,local", help="逗号分隔：deepseek,gemini,local")
    parser.add_argument("--deepseek-key", default="")
    parser.add_argument("--gemini-key", default="")
    parser.add_argument("--deepseek-model", default=DEFAULT_DEEPSEEK_MODEL)
    parser.add_argument("--gemini-model", default=DEFAULT_MODEL_NAME)
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--seed", type=int, default=20260525)
    parser.add_argument("--batch-size", type=int, default=60, help="模型抽取分批大小，避免大样本 JSON 截断")
    args = parser.parse_args()

    random.seed(args.seed)
    df = _read_products(args.files)
    rows = _sample_rows(df, args.mode, args.sample_size, corrections=args.corrections, seed=args.seed)
    items = _items_from_rows(rows)
    providers = [p.strip().lower() for p in args.providers.split(",") if p.strip()]
    if "local" not in providers:
        providers.append("local")

    provider_results: dict[str, list[ProductInfo] | None] = {}
    for provider in providers:
        print(f"[抽取质检] 开始 {provider}: {len(items)} 条", flush=True)
        provider_results[provider] = _batch_provider(provider, items, args)

    report = _build_report(rows, provider_results)
    output = Path(args.output).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    report.to_excel(output, index=False)
    print(f"[抽取质检] 已输出: {output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
