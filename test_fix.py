import os
import sqlite3
import pandas as pd
from data_mgr import DataManager

base_dir = '/Users/user/Desktop/个人/github/pro_image'
dm = DataManager(base_dir)

# Trigger re-import for project 11
print('Activating project 11...')
dm.activate_project(11)
print('Project 11 activated.')

# Ensure data is loaded (which calls _import_to_sqlite if metadata is missing)
dm.load_data() 
print('Data loaded/imported.')

# Verification Check
def check_db(pid, sku_id):
    db_path = os.path.join(base_dir, 'pro_image.db')
    conn = sqlite3.connect(db_path)
    
    print(f'\n--- Checking SKU {sku_id} in project {pid} ---')
    
    # Check link counts
    cur = conn.execute(f"SELECT COUNT(*) FROM product_links WHERE project_id={pid} AND main_sku_id='{sku_id}'")
    link_count = cur.fetchone()[0]
    print(f"Total links found for SKU: {link_count}")

    # Check store_id mapping
    df_links = pd.read_sql(f"SELECT main_sku_id, store_id, comp_sku_id FROM product_links WHERE project_id={pid} AND main_sku_id='{sku_id}'", conn)
    print("\nLinks table entries:")
    print(df_links)
    
    print('\nChecking alignment with comp_products:')
    for idx, row in df_links.iterrows():
        sid, comp_sku = row['store_id'], row['comp_sku_id']
        cur = conn.execute(f"SELECT COUNT(*) FROM comp_products WHERE project_id={pid} AND store_id='{sid}' AND skuId='{comp_sku}'")
        found = cur.fetchone()[0] > 0
        print(f"Link store_id={sid}, comp_sku={comp_sku} -> Found in comp_products: {found}")
    
    conn.close()

check_db(11, '2032366391512076288')
