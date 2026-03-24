import pandas as pd
import os
import zipfile
import shutil
import tempfile
import time
import threading
import sqlite3

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
        
        self.db_path = os.path.join(base_dir, "pro_image.db")
        self.grid_df = None
        self.store_dfs = {} 
        self._db_lock = threading.Lock()
        
        self._init_db()
        self.load_data()

    def _get_conn(self):
        return sqlite3.connect(self.db_path, check_same_thread=False)

    def _init_db(self):
        with self._get_conn() as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS meta_info (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')

    def update_config(self, target_file=None, source_files=None, output_file=None):
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
        needs_import = True
        current_mtime = str(os.path.getmtime(self.output_file)) if os.path.exists(self.output_file) else "0"
        
        with self._get_conn() as conn:
            try:
                cur = conn.cursor()
                cur.execute("SELECT value FROM meta_info WHERE key='output_file'")
                res_file = cur.fetchone()
                cur.execute("SELECT value FROM meta_info WHERE key='output_mtime'")
                res_mtime = cur.fetchone()
                if res_file and res_file[0] == self.output_file and res_mtime and res_mtime[0] == current_mtime:
                    needs_import = False
            except Exception:
                pass
                
        if needs_import and os.path.exists(self.output_file):
            print(f"Importing {self.output_file} to SQLite...")
            self._import_to_sqlite()
        
        self._reconstruct_from_sqlite()

    def _import_to_sqlite(self):
        import utils
        import ssl
        try:
            data = utils.excel_to_list_dict(self.output_file)
            df = pd.DataFrame(data)
        except Exception as e:
            print("Import err:", e)
            return

        mappings = {
            '图片': '主图链接', 'SKUID': 'skuId', '商品名称': '商品名称', '菜单名': '商品名称',
            'A商品名称': '商品名称', '规格': '规格名称', '规格名称': '规格名称', '规格名': '规格名称',
            'A规格': '规格名称', '新售价': '原价', '零售价': '原价', '美团外卖渠道售价': '原价',
            '月销量': '销售', '销售': '销售', '条码': '商品条码', '商品条码': '商品条码',
            '美团类目三级': '美团类目三级', '三级类目': '美团类目三级'
        }
        
        base_mappings = mappings.copy()
        for i in range(10):
            p = str(i)
            for k, v in base_mappings.items():
                mappings[p+k] = p+v

        for src, dst in mappings.items():
            if src in df.columns:
                if dst in df.columns and src != dst:
                    df[dst] = df[dst].fillna(df[src])
                    df.drop(columns=[src], inplace=True)
                else:
                    df.rename(columns={src: dst}, inplace=True)

        internal_keys = ['淘汰标记', '是否淘汰', '新活动价', '新售价', '跟价店', '__idx']
        main_cols = [c for c in df.columns if (not c or not c[0].isdigit()) and c not in internal_keys]
        main_cols += [c for c in internal_keys if c in df.columns]

        main_df = df[main_cols].copy()
        
        # Ensure critical columns exist
        for c in ['淘汰标记', '是否淘汰', '新活动价', '新售价', '跟价店']:
            if c not in main_df.columns:
                main_df[c] = ""
        main_df['淘汰标记'] = pd.to_numeric(main_df['淘汰标记'], errors='coerce').fillna(0).astype(int)

        if 'skuId' not in main_df.columns:
            main_df.insert(0, 'skuId', [str(i) for i in range(len(main_df))])
        
        main_df['skuId'] = main_df['skuId'].astype(str).replace(['nan', 'None', 'nan.0'], '')
        main_df['_row_orig_idx'] = range(len(main_df))

        links = []
        comp_data = []

        for i, store_name in enumerate(self.store_names):
            prefix = str(i)
            comp_cols = [c for c in df.columns if c.startswith(prefix)]
            if not comp_cols: continue
            
            for idx, row in df.iterrows():
                main_sku = main_df.loc[idx, 'skuId']
                comp_sku = row.get(f"{prefix}skuId")
                
                if pd.notna(comp_sku) and str(comp_sku).strip().lower() not in ["", "nan", "none", "nan.0"]:
                    links.append({
                        'main_sku_id': str(main_sku), 'store_id': prefix, 'comp_sku_id': str(comp_sku),
                        'similarity': row.get(f"{prefix}相似度"), 'match_type': row.get(f"{prefix}匹配"), 'is_new_add': row.get(f"{prefix}是否新增", "")
                    })
                    c_data = {'store_id': prefix}
                    for c in comp_cols:
                        if c not in [f"{prefix}相似度", f"{prefix}匹配", f"{prefix}是否新增"]:
                            base_name = c[len(prefix):]
                            c_data[base_name] = row[c]
                    comp_data.append(c_data)

        links_df = pd.DataFrame(links)
        comp_df = pd.DataFrame(comp_data)

        # Merge with individual store source files
        all_comps = []
        for i, path in enumerate(self.source_files):
            if os.path.exists(path):
                c_data_full = utils.excel_to_list_dict(path)
                cdf = pd.DataFrame(c_data_full)
                for src, dst in mappings.items():
                    if src in cdf.columns:
                        if dst in cdf.columns and src != dst:
                            cdf[dst] = cdf[dst].fillna(cdf[src])
                            cdf.drop(columns=[src], inplace=True)
                        else:
                            cdf.rename(columns={src: dst}, inplace=True)
                cdf['store_id'] = str(i)
                if 'skuId' in cdf.columns:
                    cdf['skuId'] = cdf['skuId'].astype(str)
                all_comps.append(cdf)
                
        if all_comps:
            comp_df_full = pd.concat(all_comps, ignore_index=True)
            if 'skuId' not in comp_df_full.columns:
                 comp_df_full['skuId'] = [str(i) for i in range(len(comp_df_full))]
            comp_df_full.drop_duplicates(subset=['store_id', 'skuId'], inplace=True, keep='last')
            if not comp_df.empty:
                comp_df['skuId'] = comp_df['skuId'].astype(str)
                comp_df = pd.concat([comp_df, comp_df_full], ignore_index=True).drop_duplicates(subset=['store_id', 'skuId'], keep='first')
            else:
                comp_df = comp_df_full
        elif not comp_df.empty:
            comp_df['skuId'] = comp_df['skuId'].astype(str)
            comp_df.drop_duplicates(subset=['store_id', 'skuId'], inplace=True)

        with self._db_lock:
            with self._get_conn() as conn:
                main_df.to_sql('main_products', conn, index=False, if_exists='replace')
                if not links_df.empty:
                    links_df.to_sql('product_links', conn, index=False, if_exists='replace')
                else:
                    conn.execute("CREATE TABLE IF NOT EXISTS product_links (main_sku_id TEXT, store_id TEXT, comp_sku_id TEXT, similarity REAL, match_type TEXT, is_new_add TEXT)")
                
                if not comp_df.empty:
                    comp_df.to_sql('comp_products', conn, index=False, if_exists='replace')
                else:
                    conn.execute("CREATE TABLE IF NOT EXISTS comp_products (store_id TEXT, skuId TEXT)")
                
                current_mtime = str(os.path.getmtime(self.output_file)) if os.path.exists(self.output_file) else "0"
                conn.execute("REPLACE INTO meta_info (key, value) VALUES ('output_file', ?)", (self.output_file,))
                conn.execute("REPLACE INTO meta_info (key, value) VALUES ('output_mtime', ?)", (current_mtime,))

    def _reconstruct_from_sqlite(self):
        with self._db_lock:
            with self._get_conn() as conn:
                try:
                    self.main_df = pd.read_sql("SELECT * FROM main_products ORDER BY _row_orig_idx ASC", conn)
                    links_df = pd.read_sql("SELECT * FROM product_links", conn)
                    comp_df = pd.read_sql("SELECT * FROM comp_products", conn)
                except Exception as e:
                    print("DB Reconstruction err:", e)
                    self.grid_df = pd.DataFrame()
                    return

        self.store_dfs = {}
        for i, store_name in enumerate(self.store_names):
            prefix = str(i)
            st_df = comp_df[comp_df['store_id'] == prefix].copy() if not comp_df.empty else pd.DataFrame()
            if not st_df.empty:
                st_df.drop(columns=['store_id'], inplace=True)
            self.store_dfs[prefix] = {"name": store_name, "df": st_df}

        grid = self.main_df.copy()
        if not links_df.empty and not comp_df.empty:
            for i, store_name in enumerate(self.store_names):
                prefix = str(i)
                store_links = links_df[links_df['store_id'] == prefix].copy()
                if store_links.empty: continue
                
                st_df = self.store_dfs[prefix]["df"]
                if st_df.empty: continue
                
                merged_comp = pd.merge(store_links, st_df, left_on='comp_sku_id', right_on='skuId', how='left')
                
                rename_dict = {
                    'similarity': f"{prefix}相似度",
                    'match_type': f"{prefix}匹配",
                    'is_new_add': f"{prefix}是否新增",
                    'main_sku_id': 'main_sku_id'
                }
                drop_cols = ['store_id', 'comp_sku_id']
                for c in merged_comp.columns:
                    if c not in rename_dict and c not in drop_cols:
                        rename_dict[c] = f"{prefix}{c}"
                        
                merged_comp.rename(columns=rename_dict, inplace=True)
                # Drop unwanted columns
                cols_to_drop = [c for c in drop_cols if c in merged_comp.columns]
                if cols_to_drop:
                    merged_comp.drop(columns=cols_to_drop, inplace=True)
                
                # No more 1:1 restriction here to allow all links to be merged
                # merged_comp.drop_duplicates(subset=['main_sku_id'], inplace=True)
                        
                grid = pd.merge(grid, merged_comp, left_on='skuId', right_on='main_sku_id', how='left')
                if 'main_sku_id' in grid.columns:
                    grid.drop(columns=['main_sku_id'], inplace=True)

        self.grid_df = grid

    def get_grid_data(self):
        if self.grid_df is None or self.grid_df.empty:
            return []
        if '_row_orig_idx' in self.grid_df.columns:
            export_df = self.grid_df.drop(columns=['_row_orig_idx'])
        else:
            export_df = self.grid_df
        return export_df.fillna("").to_dict(orient='records')

    def get_store_products(self, store_id):
        if store_id in self.store_dfs and not self.store_dfs[store_id]["df"].empty:
            return self.store_dfs[store_id]["df"].fillna("").to_dict(orient='records')
        return []

    def _ensure_column(self, conn, table, col_name, default_val=""):
        try:
            conn.execute(f"SELECT `{col_name}` FROM `{table}` LIMIT 1")
        except sqlite3.OperationalError:
            conn.execute(f"ALTER TABLE `{table}` ADD COLUMN `{col_name}` TEXT")

    def update_cell(self, row_idx, update_data):
        if row_idx >= len(self.grid_df): return
        main_sku_id = self.grid_df.loc[row_idx, 'skuId']
        with self._db_lock:
            with self._get_conn() as conn:
                for col, val in update_data.items():
                    self._ensure_column(conn, "main_products", col)
                    conn.execute(f"UPDATE main_products SET `{col}` = ? WHERE skuId = ?", (val, str(main_sku_id)))
        self._reconstruct_from_sqlite()

    def eliminate_product(self, row_idx, status):
        main_sku_id = self.grid_df.loc[row_idx, 'skuId']
        is_elim = "是" if status == 1 else "否"
        with self._db_lock:
            with self._get_conn() as conn:
                self._ensure_column(conn, "main_products", "淘汰标记")
                self._ensure_column(conn, "main_products", "是否淘汰")
                conn.execute("UPDATE main_products SET `淘汰标记`=?, `是否淘汰`=? WHERE skuId=?", (status, is_elim, str(main_sku_id)))
        self._reconstruct_from_sqlite()

    def mark_as_new(self, row_idx, store_id, is_new):
        main_sku_id = self.grid_df.loc[row_idx, 'skuId']
        is_new_str = "是" if is_new else "否"
        with self._db_lock:
            with self._get_conn() as conn:
                self._ensure_column(conn, "product_links", "is_new_add")
                conn.execute("UPDATE product_links SET is_new_add=? WHERE main_sku_id=? AND store_id=?", (is_new_str, str(main_sku_id), str(store_id)))
        self._reconstruct_from_sqlite()

    def price_match(self, row_idx, store_id):
        main_sku_id = self.grid_df.loc[row_idx, 'skuId']
        prefix = str(store_id)
        act_col = f"{prefix}活动价"
        orig_col = f"{prefix}原价"
        
        updated = False
        new_act = ""
        new_orig = ""
        store_name = ""
        
        if act_col in self.grid_df.columns:
            val = self.grid_df.loc[row_idx, act_col]
            try:
                num_val = float(val)
                new_act = round(num_val - 0.1, 2) if num_val >= 0.3 else num_val
            except:
                new_act = val
            updated = True
            
        if orig_col in self.grid_df.columns:
            new_orig = self.grid_df.loc[row_idx, orig_col]
            updated = True
            
        if updated:
            store_name = self.store_dfs[str(store_id)]["name"]
            with self._db_lock:
                with self._get_conn() as conn:
                    self._ensure_column(conn, "main_products", "新活动价")
                    self._ensure_column(conn, "main_products", "新售价")
                    self._ensure_column(conn, "main_products", "跟价店")
                    conn.execute("UPDATE main_products SET `新活动价`=?, `新售价`=?, `跟价店`=? WHERE skuId=?", 
                                (new_act, new_orig, store_name, str(main_sku_id)))
            self._reconstruct_from_sqlite()
            
        return updated

    def manual_link(self, row_idx, store_id, product_data):
        main_sku_id = self.grid_df.loc[row_idx, 'skuId']
        comp_sku_id = product_data.get('skuId') or product_data.get('skuid') or product_data.get('SKUID')
        if not comp_sku_id: return False
        
        with self._db_lock:
            with self._get_conn() as conn:
                conn.execute("DELETE FROM product_links WHERE main_sku_id=? AND store_id=?", (str(main_sku_id), str(store_id)))
                conn.execute("""
                    INSERT INTO product_links (main_sku_id, store_id, comp_sku_id, similarity, match_type, is_new_add)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (str(main_sku_id), str(store_id), str(comp_sku_id), 1.0, '手动关联', '否'))
        self._reconstruct_from_sqlite()
        return True

    def unlink_product(self, row_idx, store_id):
        main_sku_id = self.grid_df.loc[row_idx, 'skuId']
        with self._db_lock:
            with self._get_conn() as conn:
                conn.execute("DELETE FROM product_links WHERE main_sku_id=? AND store_id=?", (str(main_sku_id), str(store_id)))
        self._reconstruct_from_sqlite()
        return True

    def _calculate_margins(self):
        if self.grid_df is None or '采购价' not in self.grid_df.columns: return

        def calc_margin(act_val, purch_val):
            try:
                a, p = float(act_val), float(purch_val)
                if a > 0: return f"{round((a - p) / a * 100, 2)}%"
            except: pass
            return "-"

        self.grid_df['现在毛利'] = [calc_margin(row.get('活动价'), row.get('采购价')) for _, row in self.grid_df.iterrows()]
        if '新活动价' in self.grid_df.columns:
            self.grid_df['跟价毛利'] = [calc_margin(row.get('新活动价'), row.get('采购价')) for _, row in self.grid_df.iterrows()]

        for i in range(len(self.store_names)):
            prefix = str(i)
            comp_act_col = f"{prefix}活动价"
            if comp_act_col in self.grid_df.columns:
                self.grid_df[f"{prefix}现在毛利"] = [calc_margin(row.get(comp_act_col), row.get('采购价')) for _, row in self.grid_df.iterrows()]
            if '新活动价' in self.grid_df.columns:
                self.grid_df[f"{prefix}跟价毛利"] = [calc_margin(row.get('新活动价'), row.get('采购价')) for _, row in self.grid_df.iterrows()]

    def save_to_excel(self):
        filename = f"对比分析全量成果_{time.strftime('%Y%m%d_%H%M%S')}.xlsx"
        path = os.path.join(self.base_dir, filename)
        export_df = self.grid_df.copy()
        internal_to_drop = ['__idx', '淘汰标记', '_row_orig_idx']
        cols_to_drop = [c for c in internal_to_drop if c in export_df.columns]
        if cols_to_drop: export_df.drop(columns=cols_to_drop, inplace=True)
            
        import utils
        utils.write_dict_list_to_excel(export_df.fillna("").to_dict(orient='records'), path)
        return path

    def save_separate_exports(self):
        temp_dir = tempfile.mkdtemp()
        try:
            internal_keys = ['淘汰标记', '__idx', '_row_orig_idx']
            for col in ['新活动价', '新售价', '跟价店', '是否淘汰']:
                if col not in self.grid_df.columns: self.grid_df[col] = ""
            
            self._calculate_margins()
            
            for i, store_name in enumerate(self.store_names):
                prefix = str(i)
                new_col, summary_col = f"{prefix}是否新增", f"{store_name}新增"
                if new_col in self.grid_df.columns:
                    self.grid_df[summary_col] = self.grid_df[new_col]
                else: self.grid_df[summary_col] = ""

            main_cols = [c for c in self.grid_df.columns if (not c or not c[0].isdigit()) and c not in internal_keys]
            main_df = self.grid_df[main_cols].copy()
            main_path = os.path.join(temp_dir, f"主店_{self.main_store_name}.xlsx")
            main_df.to_excel(main_path, index=False)

            for i, store_name in enumerate(self.store_names):
                prefix = str(i)
                comp_cols = [c for c in self.grid_df.columns if c.startswith(prefix)]
                if not comp_cols: continue
                
                comp_df = self.grid_df[comp_cols].copy()
                if 'skuId' in self.grid_df.columns:
                    comp_df.insert(0, "主店SKU", self.grid_df['skuId'])
                
                new_col_names = []
                for c in comp_df.columns:
                    if c == "主店SKU":
                        new_col_names.append(c)
                        continue
                    new_col_names.append(c[len(prefix):])
                
                comp_df.columns = new_col_names
                comp_df = comp_df.loc[:, ~comp_df.columns.duplicated()]
                comp_path = os.path.join(temp_dir, f"竞店_{store_name}.xlsx")
                comp_df.to_excel(comp_path, index=False)

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
        all_new_items_dfs = []
        op_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self._calculate_margins()

        for i, store_name in enumerate(self.store_names):
            prefix = str(i)
            new_col = f"{prefix}是否新增"
            if new_col not in self.grid_df.columns: continue
                
            store_new_df = self.grid_df[self.grid_df[new_col] == "是"].copy()
            if store_new_df.empty: continue
            
            comp_cols = [c for c in store_new_df.columns if c.startswith(prefix)]
            if not comp_cols: continue
                
            comp_df = store_new_df[comp_cols].copy()
            comp_df.insert(0, "竞品店铺", store_name)
            if 'skuId' in store_new_df.columns:
                comp_df.insert(0, "主店SKU", store_new_df['skuId'])
            
            new_col_names = []
            for c in comp_df.columns:
                if c in ["主店SKU", "竞品店铺"]: new_col_names.append(c)
                else: new_col_names.append(c[len(prefix):])
                
            comp_df.columns = new_col_names
            comp_df = comp_df.loc[:, ~comp_df.columns.duplicated()]
            all_new_items_dfs.append(comp_df)
            
        if all_new_items_dfs:
            final_df = pd.concat(all_new_items_dfs, ignore_index=True)
        else:
            final_df = pd.DataFrame(columns=["主店SKU", "竞品店铺", "skuId", "主图链接", "菜单名", "规格名", "活动价", "原价", "销售", "条码"])
        final_df["操作时间"] = op_time

        mask = pd.Series(False, index=self.grid_df.index)
        if '是否淘汰' in self.grid_df.columns:
            mask |= (self.grid_df['是否淘汰'] == "是")
        if '跟价店' in self.grid_df.columns:
            mask |= (self.grid_df['跟价店'].notna() & (self.grid_df['跟价店'] != ""))
            
        eliminated_df = self.grid_df[mask].copy()
        if not eliminated_df.empty:
            internal_keys = ['淘汰标记', '__idx', '_row_orig_idx']
            main_cols = [c for c in eliminated_df.columns if (not c or not c[0].isdigit()) and c not in internal_keys]
            eliminated_df = eliminated_df[main_cols].copy()
            eliminated_df["操作时间"] = op_time
        else:
            eliminated_df = pd.DataFrame(columns=["skuId", "主图链接", "菜单名", "规格名", "活动价", "原价", "销售", "条码", "操作时间"])

        filename = f"新增竞品数据_{time.strftime('%Y%m%d_%H%M%S')}.xlsx"
        path = os.path.join(self.base_dir, filename)
        import utils
        sheet_data = {
            "新增(竞店)": final_df.fillna("").to_dict(orient='records'),
            "淘汰(主店)": eliminated_df.fillna("").to_dict(orient='records')
        }
        utils.write_multisheet_dict_to_excel(sheet_data, path)
        return path

