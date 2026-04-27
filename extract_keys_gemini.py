"""
用 Gemini 从「商品名称 + 规格」抽取结构化字段（方案验证脚本）。

特点：
- 读取 xlsx/csv（支持非标准表头：按内容启发式猜“名称列/规格列/三级类目列”）
- 将每行构造成 {name, spec, category3} items，批量调用 Gemini
- 强制 JSON schema 返回（Pydantic），并输出带字段列的 xlsx/csv

运行示例：

  export GEMINI_API_KEY="xxx"
  python3 extract_keys_gemini.py "/Users/user/Desktop/拆分逻辑测试清单.xlsx" \
    -o "/Users/user/Desktop/拆分逻辑测试清单_gemini.xlsx"

只生成输入（不调用）：

  python3 extract_keys_gemini.py "/Users/user/Desktop/拆分逻辑测试清单.xlsx" --dry-run \
    -o "/Users/user/Desktop/拆分逻辑测试清单_gemini.xlsx"
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from typing import Literal

import pandas as pd
from google import genai
from pydantic import BaseModel, Field


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


class ProductKeyExtract(BaseModel):
    name: str = ""
    spec: str = ""
    category3: str = ""

    net_content: str = Field(default="", description="单件净含量：ml/L/g/kg")
    sell_quantity: str = Field(default="", description="售卖数量：如 24/6/7/2/1")
    packaging_unit: PackagingUnit = Field(default="未知", description="包装单位（罐/瓶/袋/片/条/个/箱…），不确定填 未知")

    color: list[str] = Field(default_factory=list)
    size: list[str] = Field(default_factory=list)
    model: str = ""

    # 这两个字段用于调试与评估，不作为你“统一字段集”的输出要求，但保留会更好排查。
    spec_text: str = ""
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)


class BatchProductKeyExtractResponse(BaseModel):
    items: list[ProductKeyExtract]


def _read_table(path: str) -> pd.DataFrame:
    if path.lower().endswith(".csv"):
        return pd.read_csv(path, encoding="utf-8-sig")
    return pd.read_excel(path, engine="openpyxl")


def _guess_name_spec_category_cols(df: pd.DataFrame) -> tuple[object, object, object | None]:
    """
    优先使用标准列名；否则按内容猜：
    - name: 更长、中文占比更高、非 URL/纯数字
    - spec: 更短，且包含数值+单位 / 乘号 / 包装单位
    - category3: 倾向短中文词（2-8 字），且位于“类目”层级列（如三级类目/美团三级类目）
    """
    cols = {str(c).strip(): c for c in df.columns}

    name_key = None
    for k in ("商品名称", "名称", "name", "菜单名"):
        if k in cols:
            name_key = cols[k]
            break

    spec_key = None
    for k in ("规格", "规格名称", "spec"):
        if k in cols:
            spec_key = cols[k]
            break

    cat3_key = None
    for k in ("美团三级类目", "美团类目三级", "三级类目", "类目三级", "category3"):
        if k in cols:
            cat3_key = cols[k]
            break

    if name_key is not None and spec_key is not None:
        return name_key, spec_key, cat3_key

    sample = df.head(300)
    url_pat = re.compile(r"^https?://", re.IGNORECASE)
    num_pat = re.compile(r"^\d+(\.\d+)?$")
    qty_pat = re.compile(
        r"(?i)\d+(?:\.\d+)?(?:ml|g|kg|l|mm|cm)\b|(?:\*|×|x)\s*\d+|/\s*(?:袋|包|片|罐|瓶|箱|件|盒|听|支|条)|\d+\s*(?:袋|包|片|罐|瓶|箱|件|盒|听|支|条)\b"
    )
    cat_pat = re.compile(r"^[\u4e00-\u9fff]{2,12}$")

    best_name = (None, -1.0)
    best_spec = (None, -1.0)
    best_cat3 = (None, -1.0)

    for c in sample.columns:
        s = sample[c].astype(str).fillna("").map(lambda x: x.strip())
        non_empty = s[(s != "") & (s.str.lower() != "nan")]
        if non_empty.empty:
            continue

        url_ratio = float(non_empty.map(lambda x: bool(url_pat.match(x))).mean())
        if url_ratio > 0.6:
            continue

        avg_len = float(non_empty.map(len).mean())
        cn_ratio = float(
            non_empty.map(lambda x: sum("\u4e00" <= ch <= "\u9fff" for ch in x) / max(1, len(x))).mean()
        )
        num_ratio = float(non_empty.map(lambda x: bool(num_pat.match(x))).mean())
        qty_ratio = float(non_empty.map(lambda x: bool(qty_pat.search(x))).mean())

        # name
        name_score = (avg_len * cn_ratio) - (50.0 * num_ratio) + (qty_ratio * 5.0)
        if name_score > best_name[1]:
            best_name = (c, name_score)

        # spec
        spec_score = (qty_ratio * 100.0) - (avg_len * 0.35)
        if spec_score > best_spec[1]:
            best_spec = (c, spec_score)

        # category3
        cat_ratio = float(non_empty.map(lambda x: bool(cat_pat.match(x))).mean())
        cat_score = (cat_ratio * 100.0) - (avg_len * 2.0)
        if cat_score > best_cat3[1]:
            best_cat3 = (c, cat_score)

    if name_key is None:
        name_key = best_name[0]
    if spec_key is None:
        spec_key = best_spec[0]
    if cat3_key is None and best_cat3[1] >= 10.0:
        cat3_key = best_cat3[0]

    if name_key is None or spec_key is None:
        raise SystemExit("无法识别「商品名称/规格」列。请将列名改为「商品名称」「规格」。")

    return name_key, spec_key, cat3_key


def _build_prompt(items_json: str) -> str:
    # few-shot + 规则（精简版，便于稳定输出）
    return f"""
You are a highly accurate product attribute extractor.

Return ONLY valid JSON that matches the provided response schema.
Do NOT include any markdown, code fences, or explanations.

Fields to extract:
- net_content: per-unit net content only, standardized units: ml / L / g / kg (e.g. 330ml, 1.5L, 18g). If unclear, empty.
- sell_quantity: number + selling unit (e.g. 24罐, 6瓶, 7片, 2条, 1个). If unclear, empty.
- packaging_unit: choose ONE from ["袋","盒","瓶","罐","桶","箱","听","杯","支","条","片","套","枚","个","只","包","件","板","组","卷","未知"].
  Use the selling unit of the quantity (e.g. 330ml*24罐/箱 => 罐 ; 7片/包 => 片 ; 1个 => 个).
- size: include mm/cm/m dimensions like 17x25x8cm, 240mm, 直径19cm, and size codes like XL.
- spec_text: cleaned spec string (keep key numbers/units/multipliers; remove marketing noise).
- confidence: 0.0~1.0

Normalization:
- Convert full-width to half-width where applicable.
- Standardize units: 毫升/ml/ML->ml; 升/L/l->L; 克/g/G->g; 千克/公斤/kg->kg
- Treat x/×/* as multipliers.
- Do NOT compute total net content.

Examples (few-shot):
Input:
[
  {{"name":"【整箱】雪碧 碳酸饮料 330ml*24罐/箱","spec":"330ml*24罐/箱","category3":"碳酸饮料"}},
  {{"name":"高洁丝 纯棉240mm*7片/包 极薄卫生巾","spec":"7片/包","category3":"卫生巾"}},
  {{"name":"礼袋 1个 礼品包装","spec":"礼袋17x25x8cm*1个","category3":"礼品包装"}}
]
Output:
{{
  "items":[
    {{"name":"【整箱】雪碧 碳酸饮料 330ml*24罐/箱","spec":"330ml*24罐/箱","category3":"碳酸饮料","net_content":"330ml","sell_quantity":"24罐","packaging_unit":"罐","color":[],"size":[],"model":"","spec_text":"330ml*24罐/箱","confidence":0.95}},
    {{"name":"高洁丝 纯棉240mm*7片/包 极薄卫生巾","spec":"7片/包","category3":"卫生巾","net_content":"","sell_quantity":"7片","packaging_unit":"片","color":[],"size":["240mm"],"model":"","spec_text":"240mm*7片/包","confidence":0.92}},
    {{"name":"礼袋 1个 礼品包装","spec":"礼袋17x25x8cm*1个","category3":"礼品包装","net_content":"","sell_quantity":"1个","packaging_unit":"个","color":[],"size":["17x25x8cm"],"model":"","spec_text":"17x25x8cm*1个","confidence":0.90}}
  ]
}}

Now process the following input items (JSON). Keep output order the same:
{items_json}
""".strip()


def _call_gemini_extract(
    items: list[dict],
    api_key: str,
    model_name: str,
    max_retries: int = 5,
) -> list[ProductKeyExtract]:
    client = genai.Client(api_key=api_key)
    prompt = _build_prompt(json.dumps(items, ensure_ascii=False, indent=2))
    for attempt in range(max_retries):
        try:
            resp = client.models.generate_content(
                model=model_name,
                contents=prompt,
                config={"response_mime_type": "application/json", "response_schema": BatchProductKeyExtractResponse},
            )
            parsed = resp.parsed
            if parsed and hasattr(parsed, "items") and len(parsed.items) == len(items):
                return parsed.items
            time.sleep(3)
        except Exception as e:
            msg = str(e)
            if "429" in msg or "RESOURCE_EXHAUSTED" in msg:
                time.sleep((attempt + 1) * 20)
            elif "503" in msg or "UNAVAILABLE" in msg:
                time.sleep(10)
            else:
                time.sleep(5)
    return [ProductKeyExtract(**{"name": it.get("name", ""), "spec": it.get("spec", ""), "category3": it.get("category3", "")}) for it in items]


def main() -> None:
    ap = argparse.ArgumentParser(description="Gemini 结构化抽取（商品名称+规格）")
    ap.add_argument("path", help="输入 xlsx/csv")
    ap.add_argument("-o", "--output", required=True, help="输出 xlsx/csv")
    ap.add_argument("--model", default="models/gemini-3.1-flash-lite-preview", help="Gemini 模型名")
    ap.add_argument("--batch-size", type=int, default=50)
    ap.add_argument("--dry-run", action="store_true", help="只输出将发送的 items JSON（不调用 Gemini）")
    args = ap.parse_args()

    df = _read_table(args.path)
    name_col, spec_col, cat3_col = _guess_name_spec_category_cols(df)

    rows: list[dict] = []
    for _, r in df.iterrows():
        name = str(r.get(name_col, "")).strip()
        spec = str(r.get(spec_col, "")).strip()
        if name.lower() == "nan":
            name = ""
        if spec.lower() == "nan":
            spec = ""
        if not name and not spec:
            continue
        cat3 = ""
        if cat3_col is not None:
            cat3 = str(r.get(cat3_col, "")).strip()
            if cat3.lower() == "nan":
                cat3 = ""
        rows.append({"name": name, "spec": spec, "category3": cat3})

    # 批量处理
    out_records: list[dict] = []
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if (not args.dry_run) and (not api_key):
        raise SystemExit("未设置环境变量 GEMINI_API_KEY。你也可以加 --dry-run 先生成输入。")

    for i in range(0, len(rows), args.batch_size):
        batch = rows[i : i + args.batch_size]
        if args.dry_run:
            # 不调用，输出空字段
            extracted = [ProductKeyExtract(**b) for b in batch]
        else:
            extracted = _call_gemini_extract(batch, api_key=api_key, model_name=args.model)
        for b, ex in zip(batch, extracted):
            d = ex.model_dump()
            # 输出字段统一为中文列名（你指定的 10 个字段）
            out_records.append(
                {
                    "商品名称": b.get("name", ""),
                    "规格": b.get("spec", ""),
                    "单件净含量": d.get("net_content", ""),
                    "售卖数量": d.get("sell_quantity", ""),
                    "包装单位": d.get("packaging_unit", ""),
                    "颜色": " | ".join(d.get("color") or []),
                    "尺寸": " | ".join(d.get("size") or []),
                    "型号": d.get("model", ""),
                }
            )

    out_df = pd.DataFrame(out_records)
    if args.output.lower().endswith(".csv"):
        out_df.to_csv(args.output, index=False, encoding="utf-8-sig")
    else:
        out_df.to_excel(args.output, index=False, engine="openpyxl")
    print(f"已写入: {args.output}")


if __name__ == "__main__":
    main()

