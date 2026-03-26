import pandas as pd
import os
import zipfile
import shutil
import tempfile
import time
import threading
import sqlite3
import utils

# --- Constants ---
INTERNAL_COLUMNS = ['淘汰标记', '是否淘汰', '新活动价', '新售价', '跟价店']
INTERNAL_EXPORT_KEYS = ['__idx', '淘汰标记', '_row_orig_idx']

FIELD_MAPPINGS = {
    '图片': '主图链接', 'SKUID': 'skuId', '商品名称': '商品名称', '菜单名': '商品名称',
    'A商品名称': '商品名称', '规格': '规格名称', '规格名称': '规格名称', '规格名': '规格名称',
    'A规格': '规格名称', '美团外卖渠道售价': '原价',
    '活动价': '活动价', '单件折扣价': '活动价',
    '月销量': '销售', '销售': '销售', '条码': '商品条码', '商品条码': '商品条码',
    '美团类目三级': '美团类目三级', '三级类目': '美团类目三级'
}

# --- Core Database Columns ---
CORE_MAIN_COLUMNS = [
    'project_id', 'skuId', '_row_orig_idx', '商品名称', '规格名称', '原价', '活动价', '销售', 
    '主图链接', '商品条码', 'SPUID', '美团类目三级', '采购价', '采购单价', '采购链接',
    '淘汰标记', '是否淘汰', '新活动价', '新售价', '跟价店', '现在毛利', '跟价毛利'
]

CORE_COMP_COLUMNS = [
    'project_id', 'store_id', 'skuId', '商品名称', '规格名称', '原价', '活动价', '销售', 
    '主图链接', '商品条码', 'SPUID', '美团类目三级'
]

MAPPING_VERSION = "3.0" # Bumped to trigger full re-import with structure cleanup

class DataManager:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.db_path = os.path.join(base_dir, "pro_image.db")
        self._db_lock = threading.RLock()
        
        # Project-specific state
        self.active_project_id = 1
        self.active_project_name = "默认项目"
        self.target_file = ""
        self.output_file = ""
        self.project_dir = "" # Base dir for this project's files
        self.source_files = []
        self.store_names = []
        self.main_store_name = ""
        
        self.grid_df = None
        self.main_df = None
        self.store_dfs = {} 
        
        self._init_db()
        self._load_active_project()
        self.load_data()

    def _get_project_dirs(self, pid):
        """Returns standard subdirectories for a project."""
        pdir = os.path.join(self.base_dir, "uploads", f"project_{pid}")
        return {
            "root": pdir,
            "sources": os.path.join(pdir, "sources"),
            "outputs": os.path.join(pdir, "outputs"),
            "cache": os.path.join(pdir, "cache")
        }

    def _ensure_project_dirs(self, pid):
        """Ensures all standard project subdirectories exist."""
        dirs = self._get_project_dirs(pid)
        for _, path in dirs.items():
            os.makedirs(path, exist_ok=True)
        return dirs

    def _load_active_project(self):
        with self._db_lock:
            conn = self._get_conn()
            try:
                # Get active project
                cur = conn.execute("SELECT id, name FROM projects WHERE is_active = 1 LIMIT 1")
                row = cur.fetchone()
                if not row:
                    cur = conn.execute("SELECT id, name FROM projects LIMIT 1")
                    row = cur.fetchone()
                
                if row:
                    self.active_project_id, self.active_project_name = row[0], row[1]
                    
                    # Ensure dirs exist and set project_dir
                    dirs = self._ensure_project_dirs(self.active_project_id)
                    self.project_dir = dirs["root"]
                    
                    # Load files
                    cur = conn.execute("SELECT type, local_path, store_name FROM project_files WHERE project_id = ? ORDER BY id ASC", (self.active_project_id,))
                    files = cur.fetchall()
                    
                    self.source_files = []
                    self.store_names = []
                    for f_type, path, store in files:
                        if f_type == 'main':
                            self.target_file = path
                            self.main_store_name = store
                        elif f_type == 'comp':
                            self.source_files.append(path)
                            self.store_names.append(store)
                    
                    # Output file defaults to outputs/ subfolder
                    self.output_file = os.path.join(dirs["outputs"], f"output_{self.active_project_id}.xlsx")
            finally:
                conn.close()

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _init_db(self):
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    # Meta and Project Management
                    conn.execute("CREATE TABLE IF NOT EXISTS meta_info (key TEXT PRIMARY KEY, value TEXT)")
                    conn.execute("CREATE TABLE IF NOT EXISTS projects (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, is_active INTEGER DEFAULT 0)")
                    conn.execute("CREATE TABLE IF NOT EXISTS project_files (id INTEGER PRIMARY KEY AUTOINCREMENT, project_id INTEGER, type TEXT, local_path TEXT, store_name TEXT, FOREIGN KEY(project_id) REFERENCES projects(id))")
                    
                    # Core Data Tables (with project_id awareness)
                    main_cols = ", ".join([f"{c} TEXT" for c in CORE_MAIN_COLUMNS if c not in ['project_id', 'skuId', '_row_orig_idx']])
                    conn.execute(f"CREATE TABLE IF NOT EXISTS main_products (project_id INTEGER, skuId TEXT, _row_orig_idx INT, {main_cols}, PRIMARY KEY(project_id, skuId))")
                    
                    conn.execute("CREATE TABLE IF NOT EXISTS product_links (project_id INTEGER, main_sku_id TEXT, store_id TEXT, comp_sku_id TEXT, similarity REAL, match_type TEXT, is_new_add TEXT)")
                    
                    comp_cols = ", ".join([f"{c} TEXT" for c in CORE_COMP_COLUMNS if c not in ['project_id', 'store_id', 'skuId']])
                    conn.execute(f"CREATE TABLE IF NOT EXISTS comp_products (project_id INTEGER, store_id TEXT, skuId TEXT, {comp_cols})")
                    
                    # Migration: Add project_id to existing tables if missing
                    tables = ["main_products", "product_links", "comp_products"]
                    for table in tables:
                        cursor = conn.execute(f"PRAGMA table_info({table})")
                        cols = [c[1] for c in cursor.fetchall()]
                        if "project_id" not in cols:
                            conn.execute(f"ALTER TABLE {table} ADD COLUMN project_id INTEGER DEFAULT 1")
                            print(f"Added project_id to {table}")
                    
                    # Initialize default project if none exist
                    cursor = conn.execute("SELECT COUNT(*) FROM projects")
                    if cursor.fetchone()[0] == 0:
                        conn.execute("INSERT INTO projects (id, name, is_active) VALUES (1, '默认项目', 1)")
                        print("Created default project.")
            finally:
                conn.close()

    def update_config(self, target_file=None, source_files=None, output_file=None):
        if target_file:
            self.target_file = os.path.abspath(target_file)
            self.main_store_name = os.path.basename(self.target_file).replace(".xlsx", "").replace(".xls", "")
        if source_files:
            self.source_files = [os.path.abspath(f) for f in source_files]
            self.store_names = [os.path.basename(f).replace(".xlsx", "").replace(".xls", "") for f in self.source_files]
        if output_file:
            self.output_file = os.path.abspath(output_file)
        
        # Persist to project_files table if active
        if self.active_project_id:
            with self._db_lock:
                conn = self._get_conn()
                try:
                    with conn:
                        if target_file:
                            conn.execute("DELETE FROM project_files WHERE project_id = ? AND type = 'main'", (self.active_project_id,))
                            conn.execute("INSERT INTO project_files (project_id, type, local_path, store_name) VALUES (?, ?, ?, ?)",
                                        (self.active_project_id, 'main', self.target_file, self.main_store_name))
                        if source_files:
                            conn.execute("DELETE FROM project_files WHERE project_id = ? AND type = 'comp'", (self.active_project_id,))
                            for i, path in enumerate(self.source_files):
                                conn.execute("INSERT INTO project_files (project_id, type, local_path, store_name) VALUES (?, ?, ?, ?)",
                                            (self.active_project_id, 'comp', path, self.store_names[i]))
                finally:
                    conn.close()

        self.load_data()

    def load_data(self):
        if not self.active_project_id: return
        needs_import = True
        current_mtime = str(os.path.getmtime(self.output_file)) if os.path.exists(self.output_file) else "0"
        
        # Project-specific metadata keys
        file_key = f"proj_{self.active_project_id}_file"
        mtime_key = f"proj_{self.active_project_id}_mtime"
        
        with self._db_lock:
            with self._get_conn() as conn:
                try:
                    cur = conn.cursor()
                    cur.execute("SELECT value FROM meta_info WHERE key=?", (file_key,))
                    res_file = cur.fetchone()
                    cur.execute("SELECT value FROM meta_info WHERE key=?", (mtime_key,))
                    res_mtime = cur.fetchone()
                    cur.execute("SELECT value FROM meta_info WHERE key='mapping_version'")
                    res_ver = cur.fetchone()
                    
                    # Check if products actually exist for this project
                    cur.execute("SELECT COUNT(*) FROM main_products WHERE project_id=?", (self.active_project_id,))
                    count_main = cur.fetchone()[0]
                    cur.execute("SELECT COUNT(*) FROM comp_products WHERE project_id=?", (self.active_project_id,))
                    count_comp = cur.fetchone()[0]
                    
                    if res_file and res_file[0] == self.output_file and res_mtime and res_mtime[0] == current_mtime:
                        if res_ver and res_ver[0] == MAPPING_VERSION and (count_main > 0 or count_comp > 0):
                            needs_import = False
                except: pass
                
        if needs_import:
            # Update metadata
            with self._db_lock:
                with self._get_conn() as conn:
                    with conn:
                        conn.execute("INSERT OR REPLACE INTO meta_info (key, value) VALUES (?, ?)", (file_key, self.output_file))
                        conn.execute("INSERT OR REPLACE INTO meta_info (key, value) VALUES (?, ?)", (mtime_key, current_mtime))
                        conn.execute("INSERT OR REPLACE INTO meta_info (key, value) VALUES ('mapping_version', ?)", (MAPPING_VERSION,))
            
            # Re-import from source files if they exist
            if os.path.exists(self.target_file) or self.source_files:
                print(f"Triggering Re-import for Project {self.active_project_id}...")
                self._import_to_sqlite()
        
        self._reconstruct_from_sqlite()

    def _apply_mappings(self, df, mappings):
        """Standardizes column names in a DataFrame based on provided mappings."""
        import numpy as np
        for src, dst in mappings.items():
            if src in df.columns:
                if dst in df.columns and src != dst:
                    # Treat "" as NaN to allow fillna to work
                    # Only fill if dst is empty or NaN
                    df[dst] = df[dst].replace('', np.nan).fillna(df[src].replace('', np.nan))
                    df.drop(columns=[src], inplace=True)
                else:
                    df.rename(columns={src: dst}, inplace=True)
        return df

    def _import_to_sqlite(self):
        """
        New logic:
        1. main_products: From self.target_file (Source Main Store)
        2. comp_products: From self.source_files (Source Competitor Stores)
        3. product_links: From self.output_file (Result File)
        """
        # Phase 0: Clear existing data for this project
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    conn.execute("DELETE FROM main_products WHERE project_id = ?", (self.active_project_id,))
                    conn.execute("DELETE FROM product_links WHERE project_id = ?", (self.active_project_id,))
                    conn.execute("DELETE FROM comp_products WHERE project_id = ?", (self.active_project_id,))
            finally:
                conn.close()

        # Phase 1: Import Main Store Data (Full)
        if os.path.exists(self.target_file):
            print(f"Importing Main Store: {self.target_file}")
            main_data = utils.excel_to_list_dict(self.target_file)
            main_df = pd.DataFrame(main_data)
            main_df = self._apply_mappings(main_df, FIELD_MAPPINGS)
            
            # Ensure SKU ID
            sku_ids = []
            for idx, row in main_df.iterrows():
                sid = utils.get_sku_id(row.to_dict())
                if not sid: sid = f"auto_{idx}"
                sku_ids.append(sid)
            main_df['skuId'] = sku_ids
            main_df['project_id'] = self.active_project_id
            main_df['_row_orig_idx'] = range(len(main_df))
            
            # Filter and normalize columns
            for c in CORE_MAIN_COLUMNS:
                if c not in main_df.columns: main_df[c] = None
            main_df = main_df[CORE_MAIN_COLUMNS]
            
            # Remove duplicates for primary key (project_id, skuId)
            main_df = main_df.drop_duplicates(subset=['project_id', 'skuId'], keep='first')
            
            
            with self._db_lock:
                conn = self._get_conn()
                try:
                    with conn:
                        # Append to existing structure, ensuring columns match
                        main_df.to_sql('main_products', conn, index=False, if_exists='append')
                finally:
                    conn.close()
            self.main_df = main_df

        # Phase 2: Import Competitor Store Data (Full)
        for i, path in enumerate(self.source_files):
            if os.path.exists(path):
                print(f"Importing Competitor Store [{i}]: {path}")
                comp_data = utils.excel_to_list_dict(path)
                cdf = pd.DataFrame(comp_data)
                cdf = self._apply_mappings(cdf, FIELD_MAPPINGS)
                cdf['store_id'] = str(i)
                cdf['project_id'] = self.active_project_id
                
                sku_ids = []
                for idx, row in cdf.iterrows():
                    sid = utils.get_sku_id(row.to_dict())
                    if not sid: sid = f"auto_{i}_{idx}"
                    sku_ids.append(sid)
                cdf['skuId'] = sku_ids
                
                # Filter and normalize columns
                for c in CORE_COMP_COLUMNS:
                    if c not in cdf.columns: cdf[c] = None
                cdf = cdf[CORE_COMP_COLUMNS]
                
                # Deduplicate before insert (though comp_products has no PK, it's cleaner)
                cdf = cdf.drop_duplicates(subset=['project_id', 'store_id', 'skuId'], keep='first')
                
                with self._db_lock:
                    conn = self._get_conn()
                    try:
                        with conn:
                            cdf.to_sql('comp_products', conn, index=False, if_exists='append')
                    finally:
                        conn.close()

        # Phase 3: Import Product Links (from Result/Output File)
        if os.path.exists(self.output_file):
            print(f"Importing Links from Result: {self.output_file}")
            res_data = utils.excel_to_list_dict(self.output_file)
            res_df = pd.DataFrame(res_data)
            
            # --- Smart Store ID Mapping ---
            # 1. Build a SKU -> store_id map for all competitor products in this project
            with self._db_lock:
                conn = self._get_conn()
                try:
                    comp_skus_df = pd.read_sql("SELECT skuId, store_id FROM comp_products WHERE project_id = ?", conn, params=(self.active_project_id,))
                finally:
                    conn.close()
            
            sku_to_store = {}
            if not comp_skus_df.empty:
                for _, row in comp_skus_df.iterrows():
                    sku_to_store[str(row['skuId'])] = str(row['store_id'])
            
            # 2. Detect mapping for each prefix in res_df
            prefix_to_store_map = {}
            for i in range(10):
                p = str(i)
                col = f"{p}skuId"
                if col in res_df.columns:
                    # Sample unique non-empty SKUs from this prefix column
                    samples = res_df[col].dropna().astype(str).unique().tolist()
                    hits = {} # store_id -> count
                    for s in samples[:100]: # Sample first 100 to be efficient
                        s_clean = ""
                        try: s_clean = str(int(float(s)))
                        except: s_clean = s.strip()
                        
                        if s_clean in sku_to_store:
                            sid = sku_to_store[s_clean]
                            hits[sid] = hits.get(sid, 0) + 1
                    
                    if hits:
                        best_sid = max(hits, key=hits.get)
                        prefix_to_store_map[p] = best_sid
                        print(f"Detected prefix [{p}] matches store_id [{best_sid}] with {hits[best_sid]} hits")
                    else:
                        # Fallback to direct mapping
                        prefix_to_store_map[p] = p
            
            # Apply mappings for column names based on detected prefixes
            final_mappings = FIELD_MAPPINGS.copy()
            for p in prefix_to_store_map.keys():
                for k, v in FIELD_MAPPINGS.items():
                    final_mappings[p+k] = p+v
            res_df = self._apply_mappings(res_df, final_mappings)
            
            links = []
            for idx, row in res_df.iterrows():
                row_dict = row.to_dict()
                main_sku = utils.get_sku_id(row_dict)
                if not main_sku: main_sku = f"auto_{idx}"

                for p, sid in prefix_to_store_map.items():
                    comp_sku_col = f"{p}skuId"
                    if comp_sku_col in res_df.columns:
                        comp_sku_val = row_dict.get(comp_sku_col)
                        comp_sku = ""
                        if comp_sku_val is not None:
                            try:
                                comp_sku = str(int(float(comp_sku_val)))
                            except:
                                comp_sku = str(comp_sku_val).strip()
                        
                        if comp_sku and comp_sku.lower() not in ["", "nan", "none", "nan.0"]:
                            links.append({
                                'project_id': self.active_project_id,
                                'main_sku_id': str(main_sku),
                                'store_id': sid, # Use detected store ID
                                'comp_sku_id': str(comp_sku),
                                'similarity': row_dict.get(f"{p}相似度", 1.0),
                                'match_type': row_dict.get(f"{p}匹配", "未知"),
                                'is_new_add': row_dict.get(f"{p}是否新增", "否")
                            })
            
            if links:
                links_df = pd.DataFrame(links)
                with self._db_lock:
                    conn = self._get_conn()
                    try:
                        with conn:
                            links_df.to_sql('product_links', conn, index=False, if_exists='append')
                    finally:
                        conn.close()

        # Phase 4: Update Metadata
        current_mtime = str(os.path.getmtime(self.output_file)) if os.path.exists(self.output_file) else "0"
        file_key = f"proj_{self.active_project_id}_file"
        mtime_key = f"proj_{self.active_project_id}_mtime"
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    conn.execute("REPLACE INTO meta_info (key, value) VALUES (?, ?)", (file_key, self.output_file))
                    conn.execute("REPLACE INTO meta_info (key, value) VALUES (?, ?)", (mtime_key, current_mtime))
            except Exception as e:
                print("DB Metadata Update err:", e)
            finally:
                conn.close()

    def _reconstruct_from_sqlite(self):
        if not self.active_project_id: return
        with self._db_lock:
            conn = self._get_conn()
            try:
                self.main_df = pd.read_sql("SELECT * FROM main_products WHERE project_id = ? ORDER BY _row_orig_idx ASC", conn, params=(self.active_project_id,))
                links_df = pd.read_sql("SELECT * FROM product_links WHERE project_id = ?", conn, params=(self.active_project_id,))
                comp_df = pd.read_sql("SELECT * FROM comp_products WHERE project_id = ?", conn, params=(self.active_project_id,))
            except Exception as e:
                print("DB Reconstruction err:", e); self.grid_df = pd.DataFrame(); return
            finally:
                conn.close()

        self.store_dfs = {}
        for i, store_name in enumerate(self.store_names):
            prefix = str(i)
            st_df = comp_df[comp_df['store_id'] == prefix].copy() if not comp_df.empty else pd.DataFrame()
            if not st_df.empty: st_df.drop(columns=['store_id'], inplace=True)
            self.store_dfs[prefix] = {"name": store_name, "df": st_df}

        if self.main_df is None: self.grid_df = pd.DataFrame(); return
        if self.main_df.empty: self.grid_df = pd.DataFrame(); return
            
        grid = self.main_df.copy()
        if not links_df.empty and not comp_df.empty:
            for i, store_name in enumerate(self.store_names):
                prefix = str(i)
                store_links = links_df[links_df['store_id'] == prefix].copy()
                if store_links.empty: continue
                
                st_df = self.store_dfs[prefix]["df"]
                if st_df.empty: continue
                
                merged_comp = pd.merge(store_links, st_df, left_on='comp_sku_id', right_on='skuId', how='left')
                
                rename_dict = {'similarity': f"{prefix}相似度", 'match_type': f"{prefix}匹配", 'is_new_add': f"{prefix}是否新增", 'main_sku_id': 'main_sku_id'}
                drop_cols = ['store_id', 'comp_sku_id']
                for c in merged_comp.columns:
                    col_name = str(c)
                    if col_name not in rename_dict and col_name not in drop_cols: rename_dict[col_name] = f"{prefix}{col_name}"
                        
                merged_comp.rename(columns=rename_dict, inplace=True)
                cols_to_drop = [c for c in drop_cols if c in merged_comp.columns]
                if cols_to_drop: merged_comp.drop(columns=cols_to_drop, inplace=True)
                
                grid = pd.merge(grid, merged_comp, left_on='skuId', right_on='main_sku_id', how='left')
                if 'main_sku_id' in grid.columns: grid.drop(columns=['main_sku_id'], inplace=True)

        self.grid_df = grid

    def get_grid_data(self):
        if self.grid_df is None or self.grid_df.empty: return {"items": [], "total": 0}
        # Backward compatibility for old calls, but returning paginated for safety
        return self.get_paginated_grid(page=1, limit=50)

    def get_paginated_grid(self, page=1, limit=50, search="", mode="all"):
        if self.grid_df is None or self.grid_df.empty:
            return {"items": [], "total": 0, "page": page, "pages": 0}

        df = self.grid_df.copy()

        # 1. Search Filter
        if search:
            search = str(search).lower()
            mask = df.apply(lambda row: any(search in str(v).lower() for v in row), axis=1)
            df = df[mask]

        # 2. Mode Filter
        if mode == "diff":
            def has_diff(row):
                main_act = 0
                try: main_act = float(row.get('活动价', 0))
                except: pass
                if main_act <= 0: return False
                
                for i in range(len(self.store_names)):
                    prefix = str(i)
                    comp_act = 0
                    try: comp_act = float(row.get(f"{prefix}活动价", 0))
                    except: pass
                    if comp_act > 0 and abs(main_act - comp_act) > 0.01:
                        return True
                return False
            
            df = df[df.apply(has_diff, axis=1)]

        total = len(df)
        pages = (total + limit - 1) // limit
        start = (page - 1) * limit
        end = start + limit

        items = df.iloc[start:end].fillna("").to_dict(orient='records')
        
        return {
            "items": items,
            "total": total,
            "page": page,
            "limit": limit,
            "pages": pages
        }

    def get_store_products(self, store_id):
        if store_id in self.store_dfs and not self.store_dfs[store_id]["df"].empty:
            return self.store_dfs[store_id]["df"].fillna("").to_dict(orient='records')
        return []

    def get_unlinked_products(self):
        """Find SKUs in comp_products NOT in product_links for each store"""
        if not self.active_project_id: return {}
        unlinked = {}
        with self._db_lock:
            conn = self._get_conn()
            try:
                for i, _ in enumerate(self.store_names):
                    prefix = str(i)
                    query = """
                        SELECT * FROM comp_products 
                        WHERE project_id = ? AND store_id = ?
                        AND skuId NOT IN (
                            SELECT comp_sku_id FROM product_links 
                            WHERE project_id = ? AND store_id = ?
                        )
                    """
                    df = pd.read_sql(query, conn, params=(self.active_project_id, prefix, self.active_project_id, prefix))
                    unlinked[prefix] = df.fillna("").to_dict(orient='records')
            except Exception as e:
                print("Error fetching unlinked products:", e)
            finally:
                conn.close()
        return unlinked

    def _ensure_column(self, conn, table, col_name):
        try: conn.execute(f"SELECT `{col_name}` FROM `{table}` LIMIT 1")
        except sqlite3.OperationalError: conn.execute(f"ALTER TABLE `{table}` ADD COLUMN `{col_name}` TEXT")

    def update_cell(self, row_idx, update_data):
        if self.grid_df is None or row_idx >= len(self.grid_df): return
        main_sku_id = self.grid_df.loc[row_idx, 'skuId']
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    for col, val in update_data.items():
                        self._ensure_column(conn, "main_products", col)
                        conn.execute(f"UPDATE main_products SET `{col}` = ? WHERE project_id = ? AND skuId = ?", 
                                    (val, self.active_project_id, str(main_sku_id)))
            finally:
                conn.close()
        self._reconstruct_from_sqlite()

    def eliminate_product(self, row_idx, status):
        main_sku_id = self.grid_df.loc[row_idx, 'skuId']
        is_elim = "是" if status == 1 else "否"
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    self._ensure_column(conn, "main_products", "淘汰标记")
                    self._ensure_column(conn, "main_products", "是否淘汰")
                    conn.execute("UPDATE main_products SET `淘汰标记`=?, `是否淘汰`=? WHERE project_id = ? AND skuId=?", 
                                (status, is_elim, self.active_project_id, str(main_sku_id)))
            finally:
                conn.close()
        self._reconstruct_from_sqlite()

    def mark_as_new(self, row_idx, store_id, is_new, sku_id=None):
        is_new_str = "是" if is_new else "否"
        
        # If sku_id is provided, we use it directly (important for Unlinked Pool mode)
        # Otherwise, we fall back to row_idx lookup
        comp_sku_id = sku_id
        if not comp_sku_id and row_idx is not None:
            prefix = str(store_id)
            comp_sku_id = self.grid_df.loc[row_idx, f"{prefix}skuId"]

        if not comp_sku_id:
            return

        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    # 1. Update existing links in product_links
                    self._ensure_column(conn, "product_links", "is_new_add")
                    conn.execute("UPDATE product_links SET is_new_add=? WHERE project_id = ? AND store_id=? AND comp_sku_id=?", 
                                (is_new_str, self.active_project_id, str(store_id), str(comp_sku_id)))
                    
                    # 2. ALSO update comp_products table if the column exists (for unlinked items)
                    # We ensure the column exists first
                    self._ensure_column(conn, "comp_products", "is_new_add")
                    conn.execute("UPDATE comp_products SET is_new_add=? WHERE project_id=? AND store_id=? AND skuId=?",
                                (is_new_str, self.active_project_id, str(store_id), str(comp_sku_id)))
            finally:
                conn.close()
        self._reconstruct_from_sqlite()

    def price_match(self, row_idx, store_id):
        main_sku_id = self.grid_df.loc[row_idx, 'skuId']
        prefix = str(store_id)
        act_col, orig_col = f"{prefix}活动价", f"{prefix}原价"
        
        updated, new_act, new_orig = False, "", ""
        if act_col in self.grid_df.columns:
            val = self.grid_df.loc[row_idx, act_col]
            try:
                num_val = float(val)
                new_act = round(num_val - 0.1, 2) if num_val >= 0.3 else num_val
            except: new_act = val
            updated = True
            
        if orig_col in self.grid_df.columns:
            new_orig = self.grid_df.loc[row_idx, orig_col]; updated = True
            
        if updated:
            store_name = self.store_dfs[str(store_id)]["name"]
            with self._db_lock:
                conn = self._get_conn()
                try:
                    with conn:
                        for c in ["新活动价", "新售价", "跟价店"]: self._ensure_column(conn, "main_products", c)
                        conn.execute("UPDATE main_products SET `新活动价`=?, `新售价`=?, `跟价店`=? WHERE project_id = ? AND skuId=?", 
                                    (new_act, new_orig, store_name, self.active_project_id, str(main_sku_id)))
                finally:
                    conn.close()
            self._reconstruct_from_sqlite()
        return updated

    def manual_add_new(self, row_idx, store_id, comp_sku_id):
        main_sku_id = self.grid_df.loc[row_idx, 'skuId']
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    conn.execute("DELETE FROM product_links WHERE project_id = ? AND main_sku_id=? AND store_id=?", 
                                (self.active_project_id, str(main_sku_id), str(store_id)))
                    conn.execute("INSERT INTO product_links (project_id, main_sku_id, store_id, comp_sku_id, similarity, match_type, is_new_add) VALUES (?, ?, ?, ?, 1.0, '手动新增', '是')",
                                (self.active_project_id, str(main_sku_id), str(store_id), str(comp_sku_id)))
            finally:
                conn.close()
        self._reconstruct_from_sqlite()

    def manual_link(self, row_idx, store_id, product_data):
        main_sku_id = self.grid_df.loc[row_idx, 'skuId']
        comp_sku_id = product_data.get('skuId') or product_data.get('skuid') or product_data.get('SKUID')
        if not comp_sku_id: return False
        
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    conn.execute("DELETE FROM product_links WHERE project_id = ? AND main_sku_id=? AND store_id=?", 
                                (self.active_project_id, str(main_sku_id), str(store_id)))
                    conn.execute("""
                        INSERT INTO product_links (project_id, main_sku_id, store_id, comp_sku_id, similarity, match_type, is_new_add)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (self.active_project_id, str(main_sku_id), str(store_id), str(comp_sku_id), 1.0, '手动关联', '否'))
            finally:
                conn.close()
        self._reconstruct_from_sqlite(); return True

    def unlink_product(self, row_idx, store_id):
        main_sku_id = self.grid_df.loc[row_idx, 'skuId']
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    conn.execute("DELETE FROM product_links WHERE project_id = ? AND main_sku_id=? AND store_id=?", 
                                (self.active_project_id, str(main_sku_id), str(store_id)))
            except Exception as e:
                print("DB Unlink err:", e)
            finally:
                conn.close()
        self._reconstruct_from_sqlite(); return True

    def _calculate_margins(self):
        if self.grid_df is None or '采购价' not in self.grid_df.columns: return

        def calc(a, p):
            try:
                a, p = float(a), float(p)
                if a > 0: return f"{round((a - p) / a * 100, 2)}%"
            except: pass
            return "-"

        self.grid_df['现在毛利'] = [calc(r.get('活动价'), r.get('采购价')) for _, r in self.grid_df.iterrows()]
        if '新活动价' in self.grid_df.columns:
            self.grid_df['跟价毛利'] = [calc(r.get('新活动价'), r.get('采购价')) for _, r in self.grid_df.iterrows()]

        for i in range(len(self.store_names)):
            prefix = str(i)
            comp_act_col = f"{prefix}活动价"
            if comp_act_col in self.grid_df.columns:
                self.grid_df[f"{prefix}现在毛利"] = [calc(r.get(comp_act_col), r.get('采购价')) for _, r in self.grid_df.iterrows()]
            if '新活动价' in self.grid_df.columns:
                self.grid_df[f"{prefix}跟价毛利"] = [calc(r.get('新活动价'), r.get('采购价')) for _, r in self.grid_df.iterrows()]

    def save_to_excel(self):
        filename = f"对比分析全量成果_{time.strftime('%Y%m%d_%H%M%S')}.xlsx"
        dirs = self._get_project_dirs(self.active_project_id)
        path = os.path.join(dirs["outputs"], filename)
        
        export_df = self.grid_df.copy()
        cols_to_drop = [c for c in INTERNAL_EXPORT_KEYS if c in export_df.columns]
        if cols_to_drop: export_df.drop(columns=cols_to_drop, inplace=True)
        utils.write_dict_list_to_excel(export_df.fillna("").to_dict(orient='records'), path)
        return path

    def save_separate_exports(self):
        temp_dir = tempfile.mkdtemp()
        try:
            for col in ['新活动价', '新售价', '跟价店', '是否淘汰']:
                if col not in self.grid_df.columns: self.grid_df[col] = ""
            self._calculate_margins()
            
            for i, store_name in enumerate(self.store_names):
                prefix = str(i)
                new_col, summ_col = f"{prefix}是否新增", f"{store_name}新增"
                self.grid_df[summ_col] = self.grid_df[new_col] if new_col in self.grid_df.columns else ""

            # 1. Main store export
            main_cols = [c for c in self.grid_df.columns if (not c or not c[0].isdigit()) and c not in INTERNAL_EXPORT_KEYS]
            self.grid_df[main_cols].to_excel(os.path.join(temp_dir, f"主店_{self.main_store_name}.xlsx"), index=False)

            # 2. Linked competitor store exports
            for i, store_name in enumerate(self.store_names):
                prefix = str(i)
                comp_cols = [c for c in self.grid_df.columns if c.startswith(prefix)]
                if not comp_cols: continue
                comp_df = self.grid_df[comp_cols].copy()
                if 'skuId' in self.grid_df.columns: comp_df.insert(0, "主店SKU", self.grid_df['skuId'])
                comp_df.columns = [c if c == "主店SKU" else c[len(prefix):] for c in comp_df.columns]
                comp_df.loc[:, ~comp_df.columns.duplicated()].to_excel(os.path.join(temp_dir, f"竞店_{store_name}.xlsx"), index=False)

            # 3. Unlinked pool export
            unlinked_data = []
            with self._get_conn() as conn:
                for i, store_name in list(enumerate(self.store_names)):
                    prefix = str(i)
                    query = """
                        SELECT * FROM comp_products 
                        WHERE project_id = ? AND store_id = ?
                        AND skuId NOT IN (
                            SELECT comp_sku_id FROM product_links 
                            WHERE project_id = ? AND store_id = ?
                        )
                    """
                    df = pd.read_sql(query, conn, params=(self.active_project_id, prefix, self.active_project_id, prefix))
                    if not df.empty:
                        df.insert(0, "竞品店铺", store_name)
                        unlinked_data.append(df)
            
                if not pool_df.empty:
                    pool_df.to_excel(os.path.join(temp_dir, "未关联商品池.xlsx"), index=False)
            
            dirs = self._get_project_dirs(self.active_project_id)
            zip_path = os.path.join(dirs["outputs"], f"对比成果_{time.strftime('%Y%m%d_%H%M%S')}.zip")
            with zipfile.ZipFile(zip_path, 'w') as zf:
                for root, _, files in os.walk(temp_dir):
                    for file in files: zf.write(os.path.join(root, file), arcname=file)
            return zip_path
        finally: shutil.rmtree(temp_dir)

    def export_new_items(self):
        op_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self._calculate_margins()
        
        # Mapping prefix to store name
        store_map = {str(i): name for i, name in enumerate(self.store_names)}
        
        # 1. Fetch ALL products marked as "New" from comp_products (includes unlinked ones)
        all_new_data = []
        with self._get_conn() as conn:
            query = "SELECT * FROM comp_products WHERE project_id = ? AND is_new_add = '是'"
            all_comp_new_df = pd.read_sql(query, conn, params=(self.active_project_id,))
            if not all_comp_new_df.empty:
                # Add store name and main store link info if available
                all_comp_new_df['竞品店铺'] = all_comp_new_df['store_id'].map(store_map)
                
                # Fetch links to get Main Store SKU if linked
                link_query = "SELECT comp_sku_id, main_sku_id FROM product_links WHERE project_id = ?"
                links = pd.read_sql(link_query, conn, params=(self.active_project_id,))
                
                # Merge to get Main SKU
                merged = all_comp_new_df.merge(links, left_on='skuId', right_on='comp_sku_id', how='left')
                merged.rename(columns={'main_sku_id': '主店SKU'}, inplace=True)
                
                # Ensure core columns
                cols = ['主店SKU', '竞品店铺', 'skuId', '主图链接', '商品名称', '规格名称', '活动价', '原价', '销售', '条码']
                final_new_df = merged[[c for c in cols if c in merged.columns]].copy()
                for c in cols:
                    if c not in final_new_df.columns: final_new_df[c] = ""
                all_new_data = final_new_df.fillna("").to_dict(orient='records')

        final_df = pd.DataFrame(all_new_data) if all_new_data else pd.DataFrame(columns=["主店SKU", "竞品店铺", "skuId", "主图链接", "商品名称", "规格名称", "活动价", "原价", "销售", "条码"])
        final_df["操作时间"] = op_time

        # 2. Main store eliminated items
        mask = pd.Series(False, index=self.grid_df.index)
        if '是否淘汰' in self.grid_df.columns: mask |= (self.grid_df['是否淘汰'] == "是")
        if '跟价店' in self.grid_df.columns: mask |= (self.grid_df['跟价店'].notna() & (self.grid_df['跟价店'] != ""))
            
        elim_df = self.grid_df[mask].copy()
        if not elim_df.empty:
            main_cols = [c for c in elim_df.columns if (not c or not c[0].isdigit()) and c not in INTERNAL_EXPORT_KEYS]
            elim_df = elim_df[main_cols].copy(); elim_df["操作时间"] = op_time
        else: elim_df = pd.DataFrame(columns=["skuId", "主图链接", "商品名称", "规格名称", "活动价", "原价", "销售", "条码", "操作时间"])

        dirs = self._get_project_dirs(self.active_project_id)
        path = os.path.join(dirs["outputs"], f"新增竞品数据_{time.strftime('%Y%m%d_%H%M%S')}.xlsx")
        sheet_data = {"新增(竞店)": final_df.fillna("").to_dict(orient='records'), "淘汰(主店)": elim_df.fillna("").to_dict(orient='records')}
        utils.write_multisheet_dict_to_excel(sheet_data, path); return path

    def list_projects(self):
        with self._db_lock:
            conn = self._get_conn()
            try:
                cur = conn.execute("SELECT id, name, created_at, is_active FROM projects ORDER BY created_at DESC")
                projs = []
                for row in cur.fetchall():
                    pid = row[0]
                    # Count SKUs
                    count_cur = conn.execute("SELECT COUNT(*) FROM main_products WHERE project_id = ?", (pid,))
                    sku_count = count_cur.fetchone()[0]
                    projs.append({
                        "id": pid, "name": row[1], "created_at": row[2], 
                        "is_active": row[3], "sku_count": sku_count
                    })
                return projs
            finally:
                conn.close()

    def create_project(self, name, main_file_info, comp_files_info):
        """
        main_file_info: {'path': ..., 'store_name': ...}
        comp_files_info: [{'path': ..., 'store_name': ...}, ...]
        """
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    # Create project
                    cur = conn.execute("INSERT INTO projects (name) VALUES (?)", (name,))
                    pid = cur.lastrowid
                    
                    # Add files
                    conn.execute("INSERT INTO project_files (project_id, type, local_path, store_name) VALUES (?, ?, ?, ?)",
                                (pid, 'main', main_file_info['path'], main_file_info['store_name']))
                    for f in comp_files_info:
                        conn.execute("INSERT INTO project_files (project_id, type, local_path, store_name) VALUES (?, ?, ?, ?)",
                                    (pid, 'comp', f['path'], f['store_name']))
                    
                    return pid
            finally:
                conn.close()

    def activate_project(self, project_id):
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    conn.execute("UPDATE projects SET is_active = 0")
                    conn.execute("UPDATE projects SET is_active = 1 WHERE id = ?", (project_id,))
            finally:
                conn.close()
        self._load_active_project()
        self.load_data()
        return True

    def delete_project(self, project_id):
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    conn.execute("DELETE FROM main_products WHERE project_id = ?", (project_id,))
                    conn.execute("DELETE FROM product_links WHERE project_id = ?", (project_id,))
                    conn.execute("DELETE FROM comp_products WHERE project_id = ?", (project_id,))
                    conn.execute("DELETE FROM project_files WHERE project_id = ?", (project_id,))
                    conn.execute("DELETE FROM projects WHERE id = ?", (project_id,))
            finally:
                conn.close()

        # Delete project folder on disk
        dirs = self._get_project_dirs(project_id)
        if os.path.exists(dirs["root"]):
            try:
                shutil.rmtree(dirs["root"])
                print(f"Deleted project folder: {dirs['root']}")
            except Exception as e:
                print(f"Error deleting folder {dirs['root']}: {e}")
        
        if self.active_project_id == project_id:
            # Find any other project to activate
            with self._get_conn() as conn:
                res = conn.execute("SELECT id FROM projects LIMIT 1").fetchone()
                if res:
                    self.activate_project(res[0])
                else:
                    self.active_project_id = None
                    self.grid_df = pd.DataFrame()
        return True
