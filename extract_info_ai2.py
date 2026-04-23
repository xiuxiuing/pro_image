import pandas as pd
from google import genai
from pydantic import BaseModel, Field
import os
import re
import time
import json
import shutil
from typing import Literal

# API key / model name are passed from the frontend
DEFAULT_MODEL_NAME = "models/gemini-3.1-flash-lite-preview"

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
    sell_quantity: str = Field(default="", description="售卖数量，如 24/6/7/2/1")
    packaging_unit: PackagingUnit = Field(default="未知", description="包装单位，如 罐/瓶/袋/片/条/个/箱/包；不确定填 未知")
    color: list[str] = Field(default_factory=list, description="颜色（可多值）")
    size: list[str] = Field(default_factory=list, description="尺寸/长度/码数（可多值，如 240mm/17x25x8cm/XL）")
    model: str = Field(default="", description="型号（如 AB-123、X1），不确定留空")

class BatchResponse(BaseModel):
    items: list[ProductInfo]

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

def extract_batch_ai(items, api_key, model_name=None, max_retries=5):
    client = genai.Client(api_key=api_key)
    model_name = (model_name or DEFAULT_MODEL_NAME).strip() or DEFAULT_MODEL_NAME
    prompt = f"""
    You are a highly accurate product attribute extractor.

    Return ONLY valid JSON that matches the provided response schema.
    Do NOT include any markdown, code fences, or explanations.

    Extract fields for each item:
    1. net_content (A单件净含量): per-unit net content only, standardized units: ml / L / g / kg (e.g. 330ml, 1.5L, 18g). If unclear, empty.
       Do NOT compute total net content.
    2. sell_quantity (A售卖数量): number + selling unit (e.g. 24罐, 6瓶, 7片, 2条, 1个). If unclear, empty.
    3. packaging_unit (A包装单位): the unit used in sell_quantity. Choose ONE from:
       ["袋","盒","瓶","罐","桶","箱","听","杯","支","条","片","套","枚","个","只","包","件","板","组","卷","未知"].
       Examples:
       - 330ml*24罐/箱 => sell_quantity=24罐, packaging_unit=罐
       - 7片/包 => sell_quantity=7片, packaging_unit=片
       - 1个 => sell_quantity=1个, packaging_unit=个
    4. color (A颜色): list of colors if explicitly stated.
    5. size (A尺寸): list of sizes/lengths or dimensions, keep units (mm/cm/m) and forms like 17x25x8cm, 直径19cm, and size codes like XL.
    6. model (A型号): model identifier if present (e.g. AB-123, X1).

    Normalization rules:
    - Convert full-width to half-width where applicable.
    - Standardize units: 毫升/ml/ML->ml; 升/L/l->L; 克/g/G->g; 千克/公斤/kg->kg
    - Treat x/×/* as multipliers. Example: 330ml*24罐/箱 => net_content=330ml, sell_quantity=24罐, packaging_unit=罐

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
        {{"net_content":"330ml","sell_quantity":"24罐","packaging_unit":"罐","color":[],"size":[],"model":""}},
        {{"net_content":"","sell_quantity":"7片","packaging_unit":"片","color":[],"size":["240mm"],"model":""}},
        {{"net_content":"","sell_quantity":"1个","packaging_unit":"个","color":[],"size":["17x25x8cm"],"model":""}}
      ]
    }}

    Input items (JSON), keep output order exactly the same:
    {json.dumps(items, ensure_ascii=False, indent=2)}
    """

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
                    return parsed_items
                else:
                    print(f"Result count mismatch ({len(parsed_items)} vs {len(items)}). Retrying...")

        except Exception as e:
            err_msg = str(e)
            if "429" in err_msg or "RESOURCE_EXHAUSTED" in err_msg:
                wait_time = (attempt + 1) * 30
                print(f"Quota issue (429). Retrying in {wait_time}s... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
            elif "503" in err_msg or "UNAVAILABLE" in err_msg:
                wait_time = 15
                print(f"Server busy (503). Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"Error in AI extraction: {e}")
                time.sleep(10)

    return [ProductInfo() for _ in items]


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


def process_file_ai(file_path, api_key, batch_size=110, progress_cb=None, model_name=None):
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

    # Initialize target columns if they don't exist (new 6 columns)
    target_cols = ['A单件净含量', 'A售卖数量', 'A包装单位', 'A颜色', 'A尺寸', 'A型号']
    for col in target_cols:
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
        print(f"All rows in {file_path} are already processed. Skipping AI extraction.")
        df.drop(columns=['_temp_input'], inplace=True)
        return

    # 2. Get unique inputs from those rows
    unique_inputs = rows_to_process['_temp_input'].unique().tolist()
    total_unique = len(unique_inputs)
    print(f"Total rows to process: {len(rows_to_process)}")
    print(f"Unique items to process via AI: {total_unique}")

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
        print(f"Processing batch {batch_num}/{total_batches} ({len(batch_inputs)} unique items)...")
        
        if progress_cb:
            progress_cb(batch_num, total_batches)

        # Build item list in the same order as batch_inputs using tmp_map.
        batch_items = [tmp_map.get(str(s).strip(), {"name": str(s).strip(), "spec": ""}) for s in batch_inputs]
        batch_results = extract_batch_ai(batch_items, api_key=api_key, model_name=model_name)
        
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

    df.drop(columns=['_temp_input'], inplace=True)

    # 5. Final Save
    if safe_save(df, file_path):
        print(f"Finished processing {file_path} and saved results.")
    else:
        print(f"CRITICAL: Failed to save results for {file_path}")


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
