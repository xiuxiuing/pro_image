import pandas as pd
from google import genai
from pydantic import BaseModel
import os
import time
import json
import shutil

# API key / model name are passed from the frontend
DEFAULT_MODEL_NAME = "models/gemini-3.1-flash-lite-preview"

class ProductInfo(BaseModel):
    brand: str
    product_name: str
    spec: str
    material_flavor: str
    usage_scenario: str
    functional_tags: str

class BatchResponse(BaseModel):
    items: list[ProductInfo]

def extract_batch_ai(names, api_key, model_name=None, max_retries=5):
    client = genai.Client(api_key=api_key)
    model_name = (model_name or DEFAULT_MODEL_NAME).strip() or DEFAULT_MODEL_NAME
    prompt = f"""
    Extract high-accuracy details from the following product descriptions.
    
    Rules:
    1. Brand (品牌): The manufacturer or brand name (e.g. '农夫山泉'). If not clear, leave empty.
    2. Product Name (商品名称): The actual name of the product, excluding brand, marketing tags, and specifications.
    3. Specification (规格): The size, weight, or quantity details (e.g. '500ml', '240g/袋').
    4. Material/Flavor (材质口味): The material (for non-food) or flavor (for food) of the product (e.g. '不锈钢', '水蜜桃味').
    5. Usage Scenario (使用场景): Where or how the product is typically used (e.g. '居家', '户外', '办公').
    6. Functional Tags (功能标签): Key functions or benefits (e.g. '便携', '保鲜', '控油').
    
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

    return [ProductInfo(brand="", product_name="", spec="", material_flavor="", usage_scenario="", functional_tags="")
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

    # Initialize/Clear columns to ensure full re-extraction
    target_cols = ['A品牌', 'A商品名称', 'A规格', 'A材质口味', 'A使用场景', 'A功能标签']
    
    for col in target_cols:
        df[col] = ""  # Clear old data

    # Identify rows that need processing
    rows_to_process = df.index.tolist()

    total_to_process = len(rows_to_process)
    print(f"Total rows in file: {len(df)}")
    print(f"Processing all {total_to_process} rows...")

    # Process in batches
    for i in range(0, total_to_process, batch_size):

        batch_indices = rows_to_process[i:i + batch_size]

        # Concatenate '商品名称' and '规格' as input for AI
        batch_inputs = []
        for idx in batch_indices:
            name = str(df.loc[idx, name_col]).strip()
            spec = str(df.loc[idx, spec_col]).strip()
            # Combine them if both exist, otherwise use what's available
            combined = f"{name} {spec}".strip() if spec and spec.lower() != 'nan' else name
            batch_inputs.append(combined)

        batch_num = i // batch_size + 1
        total_batches = (total_to_process - 1) // batch_size + 1
        print(f"Processing batch {batch_num}/{total_batches} ({len(batch_indices)} rows)...")
        if progress_cb:
            progress_cb(batch_num, total_batches)

        results = extract_batch_ai(batch_inputs, api_key=api_key, model_name=model_name)

        df.loc[batch_indices, 'A品牌'] = [item.brand for item in results]
        df.loc[batch_indices, 'A商品名称'] = [item.product_name for item in results]
        df.loc[batch_indices, 'A规格'] = [item.spec for item in results]
        df.loc[batch_indices, 'A材质口味'] = [item.material_flavor for item in results]
        df.loc[batch_indices, 'A使用场景'] = [item.usage_scenario for item in results]
        df.loc[batch_indices, 'A功能标签'] = [item.functional_tags for item in results]

        # Save progress every batch
        if safe_save(df, file_path):
            print(f"Saved progress to {file_path} (Safe)")

        time.sleep(1)

    print(f"Finished processing {file_path}")


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
