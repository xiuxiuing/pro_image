import pandas as pd
import os
import zipfile
import shutil
import tempfile
import time

class DataManager:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.output_file = os.path.join(base_dir, "output_030822.xlsx")
        self.target_file = os.path.join(base_dir, "乐购达.xlsx")
        self.source_files = [
            os.path.join(base_dir, "沃玛希.xlsx"),
            os.path.join(base_dir, "犀牛.xlsx"),
            os.path.join(base_dir, "AA百货.xlsx")
        ]
        self.store_names = ["沃玛希", "犀牛", "AA百货"]
        self.main_store_name = "乐购达"
        
        self.grid_df = None
        self.store_dfs = {} # Store name -> DataFrame
        self.load_data()

    def load_data(self):
        # Load the main comparison output
        if os.path.exists(self.output_file):
            self.grid_df = pd.read_excel(self.output_file)
            self.grid_df = self.grid_df.astype(object)
            # Add a 'status' column if not exists (for Eliminate/Eliminated)
            if '淘汰标记' not in self.grid_df.columns:
                self.grid_df['淘汰标记'] = 0 # 0: Normal, 1: Eliminated
            if '是否淘汰' not in self.grid_df.columns:
                self.grid_df['是否淘汰'] = ""
        else:
            print(f"Warning: {self.output_file} not found.")
            self.grid_df = pd.DataFrame()

        # Load individual store data for manual linking
        for i, file_path in enumerate(self.source_files):
            store_name = self.store_names[i]
            if os.path.exists(file_path):
                df = pd.read_excel(file_path)
                self.store_dfs[str(i)] = {
                    "name": store_name,
                    "df": df
                }
                # Initialize 'is_new' column for each store in grid if not exists
                prefix = str(i)
                col_name = f"{prefix}是否新增"
                if col_name not in self.grid_df.columns:
                    self.grid_df[col_name] = ""

    def get_grid_data(self):
        # Fill NaN with empty string to ensure valid JSON serialization
        return self.grid_df.fillna("").to_dict(orient='records')

    def get_store_products(self, store_id):
        if store_id in self.store_dfs:
            return self.store_dfs[store_id]["df"].fillna("").to_dict(orient='records')
        return []

    def _safe_set(self, row_idx, col, val):
        if col not in self.grid_df.columns:
            self.grid_df[col] = pd.Series(dtype=object)
        elif self.grid_df[col].dtype != object:
            self.grid_df[col] = self.grid_df[col].astype(object)
        self.grid_df.loc[row_idx, col] = val

    def update_cell(self, row_idx, update_data):
        """
        update_data: dict of column: value
        """
        for col, val in update_data.items():
            self._safe_set(row_idx, col, val)

    def eliminate_product(self, row_idx, status):
        self._safe_set(row_idx, '淘汰标记', status)
        self._safe_set(row_idx, '是否淘汰', "是" if status == 1 else "否")
        return True

    def mark_as_new(self, row_idx, store_id, is_new):
        col_name = f"{store_id}是否新增"
        self._safe_set(row_idx, col_name, "是" if is_new else "否")
        return True

    def price_match(self, row_idx, store_id):
        # Find the activity price of the competitor and sync to main's NEW fields
        prefix = str(store_id)
        act_col = f"{prefix}活动价"
        orig_col = f"{prefix}原价"
        
        updated = False
        if act_col in self.grid_df.columns:
            val = self.grid_df.loc[row_idx, act_col]
            try:
                # User logic: -0.1 if >= 0.3, else unchanged
                num_val = float(val)
                if num_val >= 0.3:
                    new_val = round(num_val - 0.1, 2)
                else:
                    new_val = num_val
                val = new_val
            except:
                pass
            self._safe_set(row_idx, '新活动价', val)
            updated = True
        
        if orig_col in self.grid_df.columns:
            self._safe_set(row_idx, '新售价', self.grid_df.loc[row_idx, orig_col])
            updated = True
        
        if updated:
            store_name = self.store_dfs[str(store_id)]["name"]
            self._safe_set(row_idx, '跟价店', store_name)
            
        return updated

    def manual_link(self, row_idx, store_id, product_data):
        """
        product_data: data from the source store excel
        """
        prefix = str(store_id)
        # Mapping fields from source to output structure
        # Source fields: skuId/SKUID, 图片, 商品名称, 规格/规格名称, 单件折扣价/新活动价, 单件原价/新售价, 销售/月销量, 条码/商品条码
        
        def get_val(d, keys):
            for k in keys:
                if k in d: return d[k]
            return ""

        mapping = {
            "skuId": ["skuid", "SKUID"],
            "主图链接": ["图片"],
            "菜单名": ["商品名称"],
            "规格名": ["规格", "规格名称"],
            "活动价": ["单件折扣价", "新活动价"],
            "原价": ["单件原价", "新售价"],
            "销售": ["销售", "月销量"],
            "条码": ["条码", "商品条码"],
            "三级类目": ["美团类目三级", "三级类目"]
        }

        for out_key, src_keys in mapping.items():
            full_key = f"{prefix}{out_key}"
            val = get_val(product_data, src_keys)
            self._safe_set(row_idx, full_key, val)
        
        # Also update similarity and match description
        self._safe_set(row_idx, f"{prefix}相似度", 1.0)
        self._safe_set(row_idx, f"{prefix}匹配", "手动关联")
        
        return True

    def unlink_product(self, row_idx, store_id):
        prefix = str(store_id)
        keys_to_clear = [
            "skuId", "主图链接", "菜单名", "规格名", "活动价", "原价", "销售", "条码", "三级类目", 
            "相似度", "匹配", "是否新增"
        ]
        for key in keys_to_clear:
            full_key = f"{prefix}{key}"
            if full_key in self.grid_df.columns:
                self._safe_set(row_idx, full_key, "")
        
        return True

    def save_to_excel(self, filename="output_modified.xlsx"):
        path = os.path.join(self.base_dir, filename)
        self.grid_df.to_excel(path, index=False)
        return path

    def save_separate_exports(self):
        """
        Splits grid_df into separate store files and returns the path to a ZIP archive.
        """
        temp_dir = tempfile.mkdtemp()
        try:
            # 1. Export Main Store
            main_cols = ['skuId', '条码', '菜单名', '规格名', '主图链接', '活动价', '原价', '新活动价', '新售价', '销售', '三级类目', '是否淘汰']
            # Filter existing columns only
            main_cols = [c for c in main_cols if c in self.grid_df.columns]
            main_df = self.grid_df[main_cols].copy()
            main_path = os.path.join(temp_dir, f"主店_{self.main_store_name}.xlsx")
            main_df.to_excel(main_path, index=False)

            # 2. Export Competitor Stores
            for i, store_name in enumerate(self.store_names):
                prefix = str(i)
                # Find all columns starting with this prefix
                comp_cols = [c for c in self.grid_df.columns if c.startswith(prefix)]
                if not comp_cols:
                    continue
                
                comp_df = self.grid_df[comp_cols].copy()
                
                # Add main store SKU ID to competitor data
                if 'skuId' in self.grid_df.columns:
                    comp_df.insert(0, f"{prefix}主店SKU", self.grid_df['skuId'])
                
                # Remove prefix from column names
                comp_df.columns = [c[len(prefix):] for c in comp_df.columns]
                
                comp_path = os.path.join(temp_dir, f"竞店_{store_name}.xlsx")
                comp_df.to_excel(comp_path, index=False)

            # 3. Create ZIP
            zip_filename = f"对比成果_{time.strftime('%Y%m%d_%H%M%S')}.zip"
            zip_path = os.path.join(self.base_dir, zip_filename)
            
            with zipfile.ZipFile(zip_path, 'w') as zf:
                for root, _, files in os.walk(temp_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        zf.write(file_path, arcname=file)
            
            return zip_path
        finally:
            shutil.rmtree(temp_dir)
