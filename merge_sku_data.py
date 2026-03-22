import pandas as pd
import os

# Updated paths
base_path = "/Users/admin/Documents/Antigravity_projects/pro_image/0307/sys_0314/"
source_data_path = "/Users/admin/Documents/Antigravity_projects/pro_image/0307/0314/"

mappings = {
    "output_031511.xlsx": [
        {"key_col": "skuId", "file": "优购哆.xlsx", "prefix": "", "use_prefix_match": True},
        {"key_col": "0skuId", "file": "乐购达.xlsx", "prefix": "0"},
        {"key_col": "1skuId", "file": "沃玛希.xlsx", "prefix": "1"},
        {"key_col": "2skuId", "file": "犀牛.xlsx", "prefix": "2"},
        {"key_col": "3skuId", "file": "AA百货.xlsx", "prefix": "3"},
    ],
    "output_030822.xlsx": [
        {"key_col": "skuId", "file": "乐购达.xlsx", "prefix": ""},
        {"key_col": "0skuId", "file": "沃玛希.xlsx", "prefix": "0"},
        {"key_col": "1skuId", "file": "犀牛.xlsx", "prefix": "1"},
        {"key_col": "2skuId", "file": "AA百货.xlsx", "prefix": "2"},
    ]
}

def merge_file(target_filename):
    input_file = os.path.join(base_path, target_filename)
    output_filename = target_filename.replace(".xlsx", "_merged.xlsx")
    final_output_file = os.path.join(base_path, output_filename)
    
    if not os.path.exists(input_file):
        print(f"Error: {target_filename} not found.")
        return

    print(f"\n--- Processing {target_filename} ---")
    df_main = pd.read_excel(input_file)
    
    file_mapping = mappings.get(target_filename, [])

    for m in file_mapping:
        key_col = m['key_col']
        filename = m['file']
        prefix = m['prefix']
        use_prefix_match = m.get('use_prefix_match', False)
        
        if key_col not in df_main.columns:
            print(f"Note: Column {key_col} not found in {target_filename}. Skipping {filename}.")
            continue
            
        file_path = os.path.join(source_data_path, filename)
        if not os.path.exists(file_path):
            print(f"Warning: File {filename} not found. Skipping.")
            continue
            
        print(f"Merging {filename} into {key_col} with prefix '{prefix}'{' (16-digit match)' if use_prefix_match else ''}...")
        df_source = pd.read_excel(file_path)
        
        if 'SKUID' not in df_source.columns:
            print(f"Error: SKUID column not found in {filename}. Skipping.")
            continue
            
        df_source_tmp = df_source.copy()
        
        # Rename source columns
        rename_dict = {col: f"{prefix}{col}" for col in df_source_tmp.columns}
        df_source_tmp = df_source_tmp.rename(columns=rename_dict)
        source_skuid_col = f"{prefix}SKUID"
        
        # Prepare join keys
        df_main['join_key'] = df_main[key_col].astype(str).str.replace(r'\.0$', '', regex=True)
        df_source_tmp['join_key'] = df_source_tmp[source_skuid_col].astype(str).str.replace(r'\.0$', '', regex=True)
        
        if use_prefix_match:
            df_main['join_key'] = df_main['join_key'].str[:16]
            df_source_tmp['join_key'] = df_source_tmp['join_key'].str[:16]
            df_source_tmp = df_source_tmp.drop_duplicates(subset=['join_key'])

        # Perform left join
        df_main = df_main.merge(df_source_tmp, on='join_key', how='left')
        
        df_main = df_main.drop(columns=['join_key'])

    print(f"Saving to {output_filename}...")
    df_main.to_excel(final_output_file, index=False)
    print(f"Done! Match count for first mapped store: {df_main[file_mapping[0]['prefix'] + 'SKUID'].notnull().sum() if file_mapping else 'N/A'}")

if __name__ == "__main__":
    for fname in mappings.keys():
        merge_file(fname)
