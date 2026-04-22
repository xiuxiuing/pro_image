import pandas as pd
from google import genai
from pydantic import BaseModel
import os
import time
import json
import shutil
from openai import OpenAI


# API key / model name are passed from the frontend
DEFAULT_MODEL_NAME = "kimi-k2.5"

class ProductInfo(BaseModel):
    product_name: str
    spec: str
    usage_scenario: str
    functional_tags: str

class BatchResponse(BaseModel):
    items: list[ProductInfo]

def extract_batch_ai(names, api_key, model_name=None, max_retries=5):
    client = client = OpenAI(
    api_key=api_key,  # 请确保设置了环境变量
    base_url="https://api.moonshot.cn/v1"
    )
    model_name = (model_name or DEFAULT_MODEL_NAME).strip() or DEFAULT_MODEL_NAME
    prompt = f"""
    Extract high-accuracy details from the following product descriptions.
    
    Rules:
    1. Product Name (商品名称): The actual name of the product, excluding brand, marketing tags, and specifications.
    2. Specification (规格): The size, weight, or quantity details (e.g. '500ml', '240g/袋').
    3. Usage Scenario (使用场景): Where or how the product is typically used (e.g. '居家', '户外', '办公').
    4. Functional Tags (功能标签): Key functions or benefits (e.g. '便携', '保鲜', '控油').
    
    Descriptions:
    {json.dumps(names, ensure_ascii=False, indent=2)}
    
    Return a list of objects matching the input order.
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
                items = response.parsed.items
                if len(items) == len(names):
                    return items
                else:
                    print(f"Result count mismatch ({len(items)} vs {len(names)}). Retrying...")

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

    return [ProductInfo(product_name="", spec="", usage_scenario="", functional_tags="")
            for _ in names]


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

    # Initialize target columns if they don't exist
    target_cols = ['A商品名称', 'A规格', 'A使用场景', 'A功能标签']
    for col in target_cols:
        if col not in df.columns:
            df[col] = ""
        else:
            df[col] = df[col].fillna("")

    # 1. Identify rows that need processing (where 'A商品名称' is empty)
    # We also combine Name + Spec for matching
    df['_temp_input'] = df.apply(lambda r: f"{str(r[name_col]).strip()} {str(r[spec_col]).strip()}".strip() 
                               if str(r[spec_col]).lower() != 'nan' else str(r[name_col]).strip(), axis=1)
    
    mask_to_process = (df['A商品名称'] == "") | (df['A商品名称'].isna())
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
    
    for i in range(0, total_unique, batch_size):
        batch_inputs = unique_inputs[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (total_unique - 1) // batch_size + 1
        print(f"Processing batch {batch_num}/{total_batches} ({len(batch_inputs)} unique items)...")
        
        if progress_cb:
            progress_cb(batch_num, total_batches)

        batch_results = extract_batch_ai(batch_inputs, api_key=api_key, model_name=model_name)
        
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
        df.loc[row_mask, 'A商品名称'] = res.product_name
        df.loc[row_mask, 'A规格'] = res.spec
        df.loc[row_mask, 'A使用场景'] = res.usage_scenario
        df.loc[row_mask, 'A功能标签'] = res.functional_tags

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
