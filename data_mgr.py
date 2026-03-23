import pandas as pd
import os
import zipfile
import shutil
import tempfile
import time
import threading

class DataManager:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.output_file = os.path.join(base_dir, "output_031511.xlsx")
        self.target_file = os.path.join(base_dir, "优购哆.xlsx")
        self.source_files = [
            os.path.join(base_dir, "乐购达.xlsx"),
            os.path.join(base_dir, "沃玛希.xlsx"),
            os.path.join(base_dir, "犀牛.xlsx"),
            os.path.join(base_dir, "AA百货.xlsx")
        ]
        self.store_names = ["乐购达", "沃玛希", "犀牛", "AA百货"]
        self.main_store_name = "优购哆"
        
        self.grid_df = None
        self.store_dfs = {} # Store name -> DataFrame
        
        self._save_lock = threading.Lock()
        
        self.load_data()

    def update_config(self, target_file=None, source_files=None, output_file=None):
        """Updates the current file configuration and reloads data."""
        if target_file:
            self.target_file = os.path.abspath(target_file)
            self.main_store_name = os.path.basename(self.target_file).replace(".xlsx", "").replace(".xls", "")
        
        if source_files:
            self.source_files = [os.path.abspath(f) for f in source_files]
            self.store_names = [os.path.basename(f).replace(".xlsx", "").replace(".xls", "") for f in self.source_files]
        
        if output_file:
            self.output_file = os.path.abspath(output_file)
        
        self.load_data()

    def load_data(self):
        # Load the main comparison output
        if os.path.exists(self.output_file):
            import utils
            data = utils.excel_to_list_dict(self.output_file)
            self.grid_df = pd.DataFrame(data)
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
                import utils
                data = utils.excel_to_list_dict(file_path)
                df = pd.DataFrame(data)
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

    def _save_to_disk(self):
        """Persists the current in-memory grid_df to disk in a background thread without blocking the UI."""
        if self.output_file and self.grid_df is not None:
            # Take a snapshot while holding the lock (very fast)
            with self._save_lock:
                snapshot = self.grid_df.copy()
            
            def save_task(df_to_save, target_path):
                # This runs in background and DOES NOT hold self._save_lock during the slow to_excel()
                try:
                    import utils
                    utils.write_dict_list_to_excel(df_to_save.fillna("").to_dict(orient='records'), target_path)
                    print(f"DEBUG: Background autosave completed: {target_path}")
                except Exception as e:
                    print(f"ERROR: Background autosave failed: {e}")

            # Start background thread
            thread = threading.Thread(target=save_task, args=(snapshot, self.output_file))
            thread.daemon = True
            thread.start()

    def update_cell(self, row_idx, update_data):
        """
        update_data: dict of column: value
        """
        for col, val in update_data.items():
            self._safe_set(row_idx, col, val)
        self._save_to_disk()

    def eliminate_product(self, row_idx, status):
        self._safe_set(row_idx, '淘汰标记', status)
        self._safe_set(row_idx, '是否淘汰', "是" if status == 1 else "否")
        self._save_to_disk()
        return True

    def mark_as_new(self, row_idx, store_id, is_new):
        col_name = f"{store_id}是否新增"
        self._safe_set(row_idx, col_name, "是" if is_new else "否")
        self._save_to_disk()
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
            self._save_to_disk()
            
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
        self._save_to_disk()
        
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
        
        self._save_to_disk()
        return True

    def save_to_excel(self):
        """
        Saves the current grid_df to a single merged Excel file.
        """
        filename = f"对比分析全量成果_{time.strftime('%Y%m%d_%H%M%S')}.xlsx"
        path = os.path.join(self.base_dir, filename)
        
        # Clean up internal technical keys for the final export
        export_df = self.grid_df.copy()
        internal_to_drop = ['__idx', '淘汰标记']
        cols_to_drop = [c for c in internal_to_drop if c in export_df.columns]
        if cols_to_drop:
            export_df.drop(columns=cols_to_drop, inplace=True)
            
        import utils
        utils.write_dict_list_to_excel(export_df.fillna("").to_dict(orient='records'), path)
        return path

    def save_separate_exports(self):
        """
        Splits grid_df into separate store files and returns the path to a ZIP archive.
        """
        temp_dir = tempfile.mkdtemp()
        try:
            internal_keys = ['淘汰标记', '__idx']
            
            # 1. Export Main Store
            # Ensure standard editing columns exist
            for col in ['新活动价', '新售价', '跟价店', '是否淘汰']:
                if col not in self.grid_df.columns:
                    self.grid_df[col] = ""
            
            # Add addition summaries for each competitor store
            for i, store_name in enumerate(self.store_names):
                prefix = str(i)
                new_col = f"{prefix}是否新增"
                summary_col = f"{store_name}新增"
                if new_col in self.grid_df.columns:
                    self.grid_df[summary_col] = self.grid_df[new_col]
                else:
                    self.grid_df[summary_col] = ""

            # Get all columns that don't start with a digit and aren't internal
            main_cols = [c for c in self.grid_df.columns if (not c or not c[0].isdigit()) and c not in internal_keys]
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
                
                # Add main store SKU ID to competitor data for reference
                if 'skuId' in self.grid_df.columns:
                    comp_df.insert(0, "主店SKU", self.grid_df['skuId'])
                
                # Remove prefix from column names
                raw_cols = []
                standardized_to_remove = {'skuId', '主图链接', '菜单名', '规格名', '活动价', '原价', '销售', '条码', '三级类目'}
                
                new_col_names = []
                for c in comp_df.columns:
                    if c == "主店SKU":
                        new_col_names.append(c)
                        continue
                        
                    base_name = c[len(prefix):]
                    # If this is a standardized key we added, and the original column name is already present, we might skip it or keep it.
                    # As a general rule, if the original column exists without the standardized mapping, we keep the original.
                    # But the simplest is to just strip prefixes and let duplicates happen (Excel handles them or we can deduplicate).
                    new_col_names.append(base_name)
                
                comp_df.columns = new_col_names
                
                # Basic deduplication of columns if any (optional but good)
                comp_df = comp_df.loc[:, ~comp_df.columns.duplicated()]
                
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

    def export_new_items(self):
        """
        Exports all competitor products marked as "新增" ("是") into Sheet 1,
        and all main store products marked as "淘汰" ("是") into Sheet 2.
        Records operation time in both.
        """
        all_new_items_dfs = []
        op_time = time.strftime('%Y-%m-%d %H:%M:%S')

        # --- 1. New Competitor Items (Sheet 1) ---
        for i, store_name in enumerate(self.store_names):
            prefix = str(i)
            new_col = f"{prefix}是否新增"
            
            if new_col not in self.grid_df.columns:
                continue
                
            store_new_df = self.grid_df[self.grid_df[new_col] == "是"].copy()
            if store_new_df.empty:
                continue
            
            comp_cols = [c for c in store_new_df.columns if c.startswith(prefix)]
            if not comp_cols:
                continue
                
            comp_df = store_new_df[comp_cols].copy()
            comp_df.insert(0, "竞品店铺", store_name)
            
            if 'skuId' in store_new_df.columns:
                comp_df.insert(0, "主店SKU", store_new_df['skuId'])
            
            new_col_names = []
            for c in comp_df.columns:
                if c in ["主店SKU", "竞品店铺"]:
                    new_col_names.append(c)
                    continue
                new_col_names.append(c[len(prefix):])
                
            comp_df.columns = new_col_names
            comp_df = comp_df.loc[:, ~comp_df.columns.duplicated()]
            all_new_items_dfs.append(comp_df)
            
        if all_new_items_dfs:
            final_df = pd.concat(all_new_items_dfs, ignore_index=True)
        else:
            final_df = pd.DataFrame(columns=["主店SKU", "竞品店铺", "skuId", "主图链接", "菜单名", "规格名", "活动价", "原价", "销售", "条码"])

        # Add Operation Time
        final_df["操作时间"] = op_time

        # --- 2. Eliminated Main Store Items (Sheet 2) ---
        if '是否淘汰' in self.grid_df.columns:
            eliminated_df = self.grid_df[self.grid_df['是否淘汰'] == "是"].copy()
        else:
            eliminated_df = pd.DataFrame()
            
        if not eliminated_df.empty:
            internal_keys = ['淘汰标记', '__idx']
            main_cols = [c for c in eliminated_df.columns if (not c or not c[0].isdigit()) and c not in internal_keys]
            eliminated_df = eliminated_df[main_cols].copy()
            eliminated_df["操作时间"] = op_time
        else:
            # Empty dataframe with default main store columns
            eliminated_df = pd.DataFrame(columns=["skuId", "主图链接", "菜单名", "规格名", "活动价", "原价", "销售", "条码", "操作时间"])

        # --- 3. Save to Multi-sheet Excel ---
        filename = f"新增竞品数据_{time.strftime('%Y%m%d_%H%M%S')}.xlsx"
        path = os.path.join(self.base_dir, filename)
        
        import utils
        sheet_data = {
            "新增(竞店)": final_df.fillna("").to_dict(orient='records'),
            "淘汰(主店)": eliminated_df.fillna("").to_dict(orient='records')
        }
        
        utils.write_multisheet_dict_to_excel(sheet_data, path)
        
        return path
