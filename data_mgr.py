import pandas as pd
import os
import re
import zipfile
import shutil
import tempfile
import time
import threading
import sqlite3
import hashlib
import gc
import json
import utils

# --- Constants ---
INTERNAL_COLUMNS = ['淘汰标记', '是否淘汰', '新活动价', '新售价', '跟价店']
INTERNAL_EXPORT_KEYS = ['__idx', '淘汰标记', '_row_orig_idx', 'ref_name_store', 'ref_image_store']

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
    '淘汰标记', '是否淘汰', '新活动价', '新售价', '跟价店', '现价毛利', '跟价毛利'
]

CORE_COMP_COLUMNS = [
    'project_id', 'store_id', 'skuId', '商品名称', '规格名称', '原价', '活动价', '销售', 
    '主图链接', '商品条码', 'SPUID', '美团类目三级'
]

MAPPING_VERSION = "3.0" # Bumped to trigger full re-import with structure cleanup

_SAFE_COL_RE = re.compile(r'^[\w\u4e00-\u9fff]+$')

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
                    conn.execute("CREATE TABLE IF NOT EXISTS projects (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, is_active INTEGER DEFAULT 0, status TEXT DEFAULT 'ready', analysis_started_at TEXT)")
                    conn.execute("CREATE TABLE IF NOT EXISTS project_files (id INTEGER PRIMARY KEY AUTOINCREMENT, project_id INTEGER, type TEXT, local_path TEXT, store_name TEXT, FOREIGN KEY(project_id) REFERENCES projects(id))")
                    
                    # Core Data Tables (with project_id awareness)
                    main_cols = ", ".join([f"{c} TEXT" for c in CORE_MAIN_COLUMNS if c not in ['project_id', 'skuId', '_row_orig_idx']])
                    conn.execute(f"CREATE TABLE IF NOT EXISTS main_products (project_id INTEGER, skuId TEXT, _row_orig_idx INT, {main_cols}, PRIMARY KEY(project_id, skuId))")
                    
                    conn.execute("CREATE TABLE IF NOT EXISTS product_links (project_id INTEGER, main_sku_id TEXT, store_id TEXT, comp_sku_id TEXT, similarity REAL, match_type TEXT, is_new_add TEXT)")
                    
                    comp_cols = ", ".join([f"{c} TEXT" for c in CORE_COMP_COLUMNS if c not in ['project_id', 'store_id', 'skuId']])
                    conn.execute(f"CREATE TABLE IF NOT EXISTS comp_products (project_id INTEGER, store_id TEXT, skuId TEXT, {comp_cols})")
                    
                    # --- Robust Migration: Add missing columns to all core tables ---
                    # 1. Check main_products
                    cursor = conn.execute("PRAGMA table_info(main_products)")
                    existing_main = [c[1] for c in cursor.fetchall()]
                    for col in CORE_MAIN_COLUMNS:
                        if col not in existing_main:
                            conn.execute(f"ALTER TABLE main_products ADD COLUMN {col} TEXT")
                            print(f"Migration: Added missing column [{col}] to main_products")

                    if "is_handled" not in existing_main:
                        conn.execute("ALTER TABLE main_products ADD COLUMN is_handled TEXT DEFAULT '0'")
                        print("Migration: Added is_handled column to main_products")

                    for ref_col in ("ref_name_store", "ref_image_store"):
                        if ref_col not in existing_main:
                            conn.execute(f"ALTER TABLE main_products ADD COLUMN {ref_col} TEXT DEFAULT ''")
                            print(f"Migration: Added {ref_col} column to main_products")

                    # 2. Check comp_products
                    cursor = conn.execute("PRAGMA table_info(comp_products)")
                    existing_comp = [c[1] for c in cursor.fetchall()]
                    for col in CORE_COMP_COLUMNS:
                        if col not in existing_comp:
                            conn.execute(f"ALTER TABLE comp_products ADD COLUMN {col} TEXT")
                            print(f"Migration: Added missing column [{col}] to comp_products")

                    # 3. Check extra columns (status, project_id etc)
                    tables_to_check = ["main_products", "product_links", "comp_products"]
                    for table in tables_to_check:
                        cursor = conn.execute(f"PRAGMA table_info({table})")
                        cols = [c[1] for c in cursor.fetchall()]
                        if "project_id" not in cols:
                            conn.execute(f"ALTER TABLE {table} ADD COLUMN project_id INTEGER DEFAULT 1")
                            print(f"Migration: Added project_id to {table}")

                    # 4. Check projects table extra columns
                    cursor = conn.execute("PRAGMA table_info(projects)")
                    proj_cols = [c[1] for c in cursor.fetchall()]
                    if "status" not in proj_cols:
                        conn.execute("ALTER TABLE projects ADD COLUMN status TEXT DEFAULT 'ready'")
                    if "analysis_started_at" not in proj_cols:
                        conn.execute("ALTER TABLE projects ADD COLUMN analysis_started_at TEXT")

                    # Performance indexes for unlinked-pool / grid queries
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_product_links_lookup ON product_links(project_id, store_id, comp_sku_id)")
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_comp_products_store ON comp_products(project_id, store_id)")
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_product_links_main ON product_links(project_id, main_sku_id)")

                    # Initialize default project if none exist
                    cursor = conn.execute("SELECT COUNT(*) FROM projects")
                    if cursor.fetchone()[0] == 0:
                        conn.execute("INSERT INTO projects (id, name, is_active) VALUES (1, '默认项目', 1)")
                        print("Created default project.")

                    # Startup recovery: fix orphaned 'analyzing' status from crashed runs
                    for row in conn.execute("SELECT id FROM projects WHERE status = 'analyzing'").fetchall():
                        orphan_pid = row[0]
                        output_path = os.path.join(self.base_dir, "uploads",
                                                   f"project_{orphan_pid}", "outputs",
                                                   f"output_{orphan_pid}.xlsx")
                        new_status = 'ready' if os.path.exists(output_path) else 'failed'
                        conn.execute("UPDATE projects SET status = ?, analysis_started_at = NULL WHERE id = ?",
                                     (new_status, orphan_pid))
                        print(f"Startup recovery: project {orphan_pid} → {new_status}")
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
        current_hash = self._file_sha256(self.output_file) if os.path.exists(self.output_file) else "0"
        
        # Project-specific metadata keys
        file_key = f"proj_{self.active_project_id}_file"
        mtime_key = f"proj_{self.active_project_id}_mtime"
        hash_key = f"proj_{self.active_project_id}_hash"
        
        with self._db_lock:
            with self._get_conn() as conn:
                try:
                    cur = conn.cursor()
                    cur.execute("SELECT value FROM meta_info WHERE key=?", (file_key,))
                    res_file = cur.fetchone()
                    cur.execute("SELECT value FROM meta_info WHERE key=?", (mtime_key,))
                    res_mtime = cur.fetchone()
                    cur.execute("SELECT value FROM meta_info WHERE key=?", (hash_key,))
                    res_hash = cur.fetchone()
                    cur.execute("SELECT value FROM meta_info WHERE key='mapping_version'")
                    res_ver = cur.fetchone()
                    
                    # Check if products actually exist for this project
                    cur.execute("SELECT COUNT(*) FROM main_products WHERE project_id=?", (self.active_project_id,))
                    count_main = cur.fetchone()[0]
                    cur.execute("SELECT COUNT(*) FROM comp_products WHERE project_id=?", (self.active_project_id,))
                    count_comp = cur.fetchone()[0]
                    cur.execute("SELECT COUNT(*) FROM product_links WHERE project_id=?", (self.active_project_id,))
                    count_links = cur.fetchone()[0]

                    # If output file is missing but DB already has links, keep current DB state
                    # instead of wiping links during re-import.
                    if (not os.path.exists(self.output_file)) and count_main > 0 and count_comp > 0 and count_links > 0:
                        needs_import = False
                    
                    if (
                        res_file and res_file[0] == self.output_file and
                        res_mtime and res_mtime[0] == current_mtime and
                        res_hash and res_hash[0] == current_hash
                    ):
                        if res_ver and res_ver[0] == MAPPING_VERSION and (count_main > 0 or count_comp > 0):
                            needs_import = False
                except Exception as e:
                    print(f"Meta check warn (project {self.active_project_id}): {e}")
                
        if needs_import:
            # Update metadata
            with self._db_lock:
                with self._get_conn() as conn:
                    with conn:
                        conn.execute("INSERT OR REPLACE INTO meta_info (key, value) VALUES (?, ?)", (file_key, self.output_file))
                        conn.execute("INSERT OR REPLACE INTO meta_info (key, value) VALUES (?, ?)", (mtime_key, current_mtime))
                        conn.execute("INSERT OR REPLACE INTO meta_info (key, value) VALUES (?, ?)", (hash_key, current_hash))
                        conn.execute("INSERT OR REPLACE INTO meta_info (key, value) VALUES ('mapping_version', ?)", (MAPPING_VERSION,))
            
            # Re-import from source files if they exist
            if os.path.exists(self.target_file) or self.source_files:
                print(f"Triggering Re-import for Project {self.active_project_id}...")
                self._import_to_sqlite()
        
        self._reconstruct_from_sqlite()

    def _file_sha256(self, file_path, chunk_size=1024 * 1024):
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                sha256.update(chunk)
        return sha256.hexdigest()

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
        Transactional import: prepare all data in memory first, then write in a
        single atomic transaction. If any step fails, nothing is changed in DB.
        """
        pid = self.active_project_id
        has_output_file = os.path.exists(self.output_file)

        # ── Phase 1: Prepare main store data (memory only) ──
        main_df = None
        if os.path.exists(self.target_file):
            print(f"Importing Main Store: {self.target_file}")
            main_data = utils.excel_to_list_dict(self.target_file)
            main_df = pd.DataFrame(main_data)
            main_df = self._apply_mappings(main_df, FIELD_MAPPINGS)

            sku_ids = []
            for idx, row in main_df.iterrows():
                sid = utils.get_sku_id(row.to_dict())
                if not sid: sid = f"auto_{idx}"
                sku_ids.append(sid)
            main_df['skuId'] = sku_ids
            main_df['project_id'] = pid
            main_df['_row_orig_idx'] = range(len(main_df))

            for c in CORE_MAIN_COLUMNS:
                if c not in main_df.columns: main_df[c] = None
            main_df = main_df[CORE_MAIN_COLUMNS]
            main_df = main_df.drop_duplicates(subset=['project_id', 'skuId'], keep='first')

        # ── Phase 2: Prepare competitor store data (memory only) ──
        comp_dfs = []
        sku_to_store = {}
        for i, path in enumerate(self.source_files):
            if os.path.exists(path):
                print(f"Importing Competitor Store [{i}]: {path}")
                comp_data = utils.excel_to_list_dict(path)
                cdf = pd.DataFrame(comp_data)
                cdf = self._apply_mappings(cdf, FIELD_MAPPINGS)
                cdf['store_id'] = str(i)
                cdf['project_id'] = pid

                sku_ids = []
                for idx, row in cdf.iterrows():
                    sid = utils.get_sku_id(row.to_dict())
                    if not sid: sid = f"auto_{i}_{idx}"
                    sku_ids.append(sid)
                cdf['skuId'] = sku_ids

                for c in CORE_COMP_COLUMNS:
                    if c not in cdf.columns: cdf[c] = None
                cdf = cdf[CORE_COMP_COLUMNS]
                cdf = cdf.drop_duplicates(subset=['project_id', 'store_id', 'skuId'], keep='first')
                comp_dfs.append(cdf)

                for _, row in cdf.iterrows():
                    sku_to_store[str(row['skuId'])] = str(row['store_id'])

        # ── Phase 3: Prepare links data (memory only, uses sku_to_store from Phase 2) ──
        links_df = None
        if has_output_file:
            print(f"Importing Links from Result: {self.output_file}")
            res_data = utils.excel_to_list_dict(self.output_file)
            res_df = pd.DataFrame(res_data)

            prefix_to_store_map = {}
            for i in range(10):
                p = str(i)
                col = f"{p}skuId"
                if col in res_df.columns:
                    samples = res_df[col].dropna().astype(str).unique().tolist()
                    hits = {}
                    for s in samples[:100]:
                        s_clean = ""
                        if s is not None:
                            s_str = str(s).strip()
                            s_clean = s_str[:-2] if s_str.endswith(".0") else s_str
                        if s_clean in sku_to_store:
                            sid = sku_to_store[s_clean]
                            hits[sid] = hits.get(sid, 0) + 1
                    if hits:
                        best_sid = max(hits, key=hits.get)
                        prefix_to_store_map[p] = best_sid
                        print(f"Detected prefix [{p}] matches store_id [{best_sid}] with {hits[best_sid]} hits")
                    else:
                        prefix_to_store_map[p] = p

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
                            s_str = str(comp_sku_val).strip()
                            comp_sku = s_str[:-2] if s_str.endswith(".0") else s_str
                        if comp_sku and comp_sku.lower() not in ["", "nan", "none", "nan.0"]:
                            links.append({
                                'project_id': pid,
                                'main_sku_id': str(main_sku),
                                'store_id': sid,
                                'comp_sku_id': str(comp_sku),
                                'similarity': row_dict.get(f"{p}相似度", 1.0),
                                'match_type': row_dict.get(f"{p}匹配", "未知"),
                                'is_new_add': row_dict.get(f"{p}是否新增", "否")
                            })
            if links:
                links_df = pd.DataFrame(links)

        # ── Phase 4: Atomic DB write — single transaction ──
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    conn.execute("DELETE FROM main_products WHERE project_id = ?", (pid,))
                    conn.execute("DELETE FROM comp_products WHERE project_id = ?", (pid,))
                    if has_output_file:
                        conn.execute("DELETE FROM product_links WHERE project_id = ?", (pid,))

                    if main_df is not None:
                        main_df.to_sql('main_products', conn, index=False, if_exists='append')
                        self.main_df = main_df
                    for cdf in comp_dfs:
                        cdf.to_sql('comp_products', conn, index=False, if_exists='append')
                    if links_df is not None and not links_df.empty:
                        links_df.to_sql('product_links', conn, index=False, if_exists='append')

                    current_mtime = str(os.path.getmtime(self.output_file)) if has_output_file else "0"
                    file_key = f"proj_{pid}_file"
                    mtime_key = f"proj_{pid}_mtime"
                    conn.execute("REPLACE INTO meta_info (key, value) VALUES (?, ?)", (file_key, self.output_file))
                    conn.execute("REPLACE INTO meta_info (key, value) VALUES (?, ?)", (mtime_key, current_mtime))
                print(f"Import complete for project {pid} (atomic transaction).")
            except Exception as e:
                print(f"Import FAILED for project {pid}, transaction rolled back: {e}")
                raise
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

        # SQLite numeric-like TEXT fields may be inferred as int by pandas.
        # Normalize join/filter keys to string to avoid silent merge misses.
        if not links_df.empty:
            for col in ['store_id', 'main_sku_id', 'comp_sku_id']:
                if col in links_df.columns:
                    links_df[col] = links_df[col].astype(str)
        if not comp_df.empty:
            for col in ['store_id', 'skuId']:
                if col in comp_df.columns:
                    comp_df[col] = comp_df[col].astype(str)
        if self.main_df is not None and not self.main_df.empty and 'skuId' in self.main_df.columns:
            self.main_df['skuId'] = self.main_df['skuId'].astype(str)

        self.store_dfs = {}
        for i, store_name in enumerate(self.store_names):
            prefix = str(i)
            st_df = comp_df[comp_df['store_id'] == prefix].copy() if not comp_df.empty else pd.DataFrame()
            if not st_df.empty:
                # Keep competitor product fields only; remove aux columns to avoid _x/_y suffix noise.
                st_df.drop(columns=['store_id', 'project_id', 'is_new_add'], inplace=True, errors='ignore')
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

                sim_col = 'similarity' if 'similarity' in merged_comp.columns else 'similarity_x'
                match_col = 'match_type' if 'match_type' in merged_comp.columns else 'match_type_x'
                new_col = 'is_new_add'
                if new_col not in merged_comp.columns:
                    if 'is_new_add_x' in merged_comp.columns:
                        new_col = 'is_new_add_x'
                    elif 'is_new_add_y' in merged_comp.columns:
                        new_col = 'is_new_add_y'

                rename_dict = {'main_sku_id': 'main_sku_id'}
                if sim_col in merged_comp.columns:
                    rename_dict[sim_col] = f"{prefix}相似度"
                if match_col in merged_comp.columns:
                    rename_dict[match_col] = f"{prefix}匹配"
                if new_col in merged_comp.columns:
                    rename_dict[new_col] = f"{prefix}是否新增"
                drop_cols = ['store_id', 'comp_sku_id']
                for c in merged_comp.columns:
                    col_name = str(c)
                    if col_name in ['is_new_add_x', 'is_new_add_y', 'project_id_x', 'project_id_y']:
                        continue
                    if col_name not in rename_dict and col_name not in drop_cols:
                        rename_dict[col_name] = f"{prefix}{col_name}"
                        
                merged_comp.rename(columns=rename_dict, inplace=True)
                cols_to_drop = [c for c in drop_cols if c in merged_comp.columns]
                if cols_to_drop: merged_comp.drop(columns=cols_to_drop, inplace=True)
                
                grid = pd.merge(grid, merged_comp, left_on='skuId', right_on='main_sku_id', how='left')
                if 'main_sku_id' in grid.columns: grid.drop(columns=['main_sku_id'], inplace=True)

        self.grid_df = grid

    def _get_spu_count(self):
        """Count of unique 商品名称 in main_products for the active project."""
        with self._db_lock:
            conn = self._get_conn()
            try:
                cur = conn.execute(
                    "SELECT COUNT(DISTINCT 商品名称) FROM main_products WHERE project_id = ? AND 商品名称 IS NOT NULL AND 商品名称 != ''",
                    (self.active_project_id,)
                )
                return cur.fetchone()[0]
            finally:
                conn.close()

    def _spu_count_from_grid_df(self, df):
        """当前筛选结果中不重复「商品名称」数量，与 total（行数）同一套过滤条件。"""
        if df is None or df.empty or '商品名称' not in df.columns:
            return 0
        s = df['商品名称'].astype(str).str.strip()
        s = s[(s != '') & (s.str.lower() != 'nan')]
        return int(s.nunique())

    def get_grid_data(self):
        if self.grid_df is None or self.grid_df.empty: return {"items": [], "total": 0}
        # Backward compatibility for old calls, but returning paginated for safety
        return self.get_paginated_grid(page=1, limit=50)

    def _grid_filter_col_mask(self, df, col, needle):
        """Substring match on col and optional prefixed columns (0col, 1col, ...)."""
        needle = str(needle).strip().lower()
        if not needle:
            return pd.Series(True, index=df.index)
        parts = []
        if col in df.columns:
            parts.append(df[col].astype(str).str.lower().str.contains(needle, regex=False, na=False))
        for p in range(len(self.store_names) + 5):
            pc = f"{p}{col}"
            if pc in df.columns:
                parts.append(df[pc].astype(str).str.lower().str.contains(needle, regex=False, na=False))
        if not parts:
            return pd.Series(True, index=df.index)
        m = parts[0]
        for q in parts[1:]:
            m = m | q
        return m

    def _grid_negative_sales_mask(self, df):
        """竞店销量 > 主店销量 — 任一侧满足即保留该行。"""

        def row_ok(row):
            try:
                main_s = float(row.get("销售", 0) or 0)
            except (ValueError, TypeError):
                main_s = 0.0
            for i in range(len(self.store_names)):
                p = str(i)
                try:
                    cs = float(row.get(f"{p}销售", 0) or 0)
                except (ValueError, TypeError):
                    cs = 0.0
                if cs > main_s:
                    return True
            return False

        return df.apply(row_ok, axis=1)

    def get_paginated_grid(self, page=1, limit=50, search="", mode="all", filters_json=None,
                           sort_field="", sort_order="desc", negative_sales_only=False):
        if self.grid_df is None or self.grid_df.empty:
            return {"items": [], "total": 0, "page": page, "pages": 0, "spu_count": 0}

        df = self.grid_df.copy()

        # 1. Search Filter
        if search:
            search = str(search).lower()
            mask = df.apply(lambda row: any(search in str(v).lower() for v in row), axis=1)
            df = df[mask]

        # 2. Mode Filter
        if mode == "no_link":
            comp_sku_cols = [f"{i}skuId" for i in range(len(self.store_names)) if f"{i}skuId" in df.columns]
            if comp_sku_cols:
                mask = df[comp_sku_cols].apply(
                    lambda row: all(pd.isna(v) or str(v).strip() in ('', 'nan', 'None') for v in row), axis=1
                )
                df = df[mask]
            else:
                pass

        if mode == "unhandled":
            if 'is_handled' in df.columns:
                df = df[df['is_handled'].fillna('0').astype(str) != '1']

        if mode == "diff":
            def has_diff(row):
                main_act = 0
                try: main_act = float(row.get('活动价', 0))
                except (ValueError, TypeError): pass
                if main_act <= 0: return False

                for i in range(len(self.store_names)):
                    prefix = str(i)
                    comp_act = 0
                    try: comp_act = float(row.get(f"{prefix}活动价", 0))
                    except (ValueError, TypeError): pass
                    if comp_act > 0 and abs(main_act - comp_act) > 0.01:
                        return True
                return False

            df = df[df.apply(has_diff, axis=1)]

        # 3. Advanced filters (per-column 筛选 in filter popup)
        if filters_json:
            try:
                filters = json.loads(filters_json) if isinstance(filters_json, str) else (filters_json or {})
            except json.JSONDecodeError:
                filters = {}
            if isinstance(filters, dict):
                for col, raw in filters.items():
                    if not col or not isinstance(col, str):
                        continue
                    if not re.match(r"^[\w\u4e00-\u9fff]+$", col):
                        continue
                    val = (raw or "").strip()
                    if not val:
                        continue
                    df = df[self._grid_filter_col_mask(df, col, val)]

        # 4. 负销量：竞店销量 > 主店销量
        if negative_sales_only:
            df = df[self._grid_negative_sales_mask(df)]

        # 5. Sort (e.g. 销售, 0销售, 1销售)
        sf = (sort_field or "").strip()
        if sf and sf in df.columns:
            asc = str(sort_order).lower() == "asc"
            num = pd.to_numeric(df[sf], errors="coerce").fillna(0)
            df = df.assign(__sort_k=num).sort_values("__sort_k", ascending=asc).drop(columns=["__sort_k"])

        total = len(df)
        pages = (total + limit - 1) // limit if limit else 0
        page = max(1, int(page))
        limit = max(1, int(limit))
        start = (page - 1) * limit
        end = start + limit

        items = df.iloc[start:end].fillna("").to_dict(orient='records')

        return {
            "items": items,
            "total": total,
            "page": page,
            "limit": limit,
            "pages": pages,
            "spu_count": self._spu_count_from_grid_df(df),
        }

    def get_store_products(self, store_id):
        if store_id in self.store_dfs and not self.store_dfs[store_id]["df"].empty:
            return self.store_dfs[store_id]["df"].fillna("").to_dict(orient='records')
        return []

    def get_unlinked_products(self):
        """Backward-compatible full fetch for unlinked pool."""
        if not self.active_project_id:
            return {}
        out = {}
        with self._db_lock:
            conn = self._get_conn()
            try:
                for i, _ in enumerate(self.store_names):
                    sid = str(i)
                    q = """
                        SELECT cp.*
                        FROM comp_products cp
                        WHERE cp.project_id = ? AND cp.store_id = ?
                          AND NOT EXISTS (
                            SELECT 1 FROM product_links pl
                            WHERE pl.project_id = cp.project_id
                              AND pl.store_id = cp.store_id
                              AND pl.comp_sku_id = cp.skuId
                          )
                    """
                    df = pd.read_sql(q, conn, params=(self.active_project_id, sid))
                    out[sid] = df.fillna("").to_dict(orient='records')
            finally:
                conn.close()
        return out

    def _build_unlinked_virtual_row(self, idx, main_df, store_slices, link_map):
        row = {}
        if idx < len(main_df):
            m = main_df.iloc[idx].to_dict()
            for k, v in m.items():
                row[k] = "" if pd.isna(v) else v
            sku = str(row.get("skuId", ""))
            row["__linked_count"] = link_map.get(sku, 0)
        else:
            row["__linked_count"] = 0
        for i in range(len(self.store_names)):
            sid = str(i)
            sdf = store_slices[sid]
            if idx < len(sdf):
                c = sdf.iloc[idx].to_dict()
                for k, v in c.items():
                    row[f"{sid}{k}"] = "" if pd.isna(v) else v
                row[f"{sid}是否新增"] = row.get(f"{sid}is_new_add", "否") or "否"
                row[f"{sid}__from_pool"] = "1"
            else:
                row[f"{sid}__from_pool"] = ""
        return row

    def _unlinked_row_passes_filters(self, row, filters_dict):
        for col, raw in (filters_dict or {}).items():
            if not col or not isinstance(col, str):
                continue
            if not re.match(r"^[\w\u4e00-\u9fff]+$", col):
                continue
            val = (raw or "").strip()
            if not val:
                continue
            val = val.lower()
            ok = False
            if col in row and val in str(row.get(col, "")).lower():
                ok = True
            else:
                for p in range(len(self.store_names) + 5):
                    pc = f"{p}{col}"
                    if pc in row and val in str(row.get(pc, "")).lower():
                        ok = True
                        break
            if not ok:
                return False
        return True

    def _unlinked_row_negative_sales(self, row):
        try:
            main_s = float(row.get("销售", 0) or 0)
        except (ValueError, TypeError):
            main_s = 0.0
        for i in range(len(self.store_names)):
            p = str(i)
            try:
                cs = float(row.get(f"{p}销售", 0) or 0)
            except (ValueError, TypeError):
                cs = 0.0
            if cs > main_s:
                return True
        return False

    def _unlinked_need_full_scan(self, filters_dict, negative_sales_only):
        if negative_sales_only:
            return True
        for k, v in (filters_dict or {}).items():
            if not (v or "").strip():
                continue
            if k == "美团类目三级":
                continue
            return True
        return False

    def get_unlinked_pool_page(self, page=1, limit=30, search="", category3="", sort_store_id="", sort_order="desc",
                               filters_json=None, negative_sales_only=False):
        """
        Returns virtual-row aligned unlinked pool:
        - first column: main products (sales desc)
        - following columns: each store's unlinked products (sales desc)
        """
        if not self.active_project_id:
            return {"items": [], "total": 0, "page": page, "limit": limit, "pages": 0, "spu_count": 0}

        page = max(1, int(page))
        limit = max(1, min(int(limit), 100))
        offset = (page - 1) * limit
        search_like = f"%{search.strip()}%" if search else None
        try:
            filters_dict = json.loads(filters_json) if (filters_json and str(filters_json).strip()) else {}
        except json.JSONDecodeError:
            filters_dict = {}
        if not isinstance(filters_dict, dict):
            filters_dict = {}
        cat_from_filter = (filters_dict.get("美团类目三级") or "").strip()
        cat_like = f"%{(category3 or cat_from_filter).strip()}%" if (category3 or cat_from_filter) else None

        def _num_expr(col):
            return f"CAST(COALESCE(NULLIF({col}, ''), '0') AS REAL)"

        with self._db_lock:
            conn = self._get_conn()
            try:
                main_where = ["project_id = ?"]
                main_params = [self.active_project_id]
                if search_like:
                    main_where.append("(skuId LIKE ? OR 商品名称 LIKE ? OR 规格名称 LIKE ?)")
                    main_params.extend([search_like, search_like, search_like])
                if cat_like:
                    main_where.append("美团类目三级 LIKE ?")
                    main_params.append(cat_like)
                main_where_sql = " AND ".join(main_where)

                link_cnt = pd.read_sql(
                    """
                    SELECT main_sku_id, COUNT(*) AS cnt
                    FROM product_links
                    WHERE project_id = ?
                    GROUP BY main_sku_id
                    """,
                    conn,
                    params=(self.active_project_id,),
                )
                link_map = {str(r["main_sku_id"]): int(r["cnt"]) for _, r in link_cnt.iterrows()} if not link_cnt.empty else {}

                need_full = self._unlinked_need_full_scan(filters_dict, negative_sales_only)
                spu_count = self._get_spu_count()

                def load_store_slice(sid, lim, off):
                    where = [
                        "cp.project_id = ?",
                        "cp.store_id = ?",
                        """NOT EXISTS (
                            SELECT 1 FROM product_links pl
                            WHERE pl.project_id = cp.project_id
                              AND pl.store_id = cp.store_id
                              AND pl.comp_sku_id = cp.skuId
                        )""",
                    ]
                    params = [self.active_project_id, sid]
                    if search_like:
                        where.append("(cp.skuId LIKE ? OR cp.商品名称 LIKE ? OR cp.规格名称 LIKE ?)")
                        params.extend([search_like, search_like, search_like])
                    if cat_like:
                        where.append("cp.美团类目三级 LIKE ?")
                        params.append(cat_like)
                    where_sql = " AND ".join(where)
                    sid_desc = "DESC"
                    if str(sort_store_id) == sid:
                        sid_desc = "ASC" if str(sort_order).lower() == "asc" else "DESC"
                    lim_sql = f"LIMIT {int(lim)} OFFSET {int(off)}" if lim is not None else ""
                    return pd.read_sql(
                        f"""
                        SELECT cp.*
                        FROM comp_products cp
                        WHERE {where_sql}
                        ORDER BY {_num_expr('cp.销售')} {sid_desc}, cp.skuId ASC
                        {lim_sql}
                        """,
                        conn,
                        params=tuple(params),
                    )

                if need_full:
                    main_df = pd.read_sql(
                        f"""
                        SELECT * FROM main_products
                        WHERE {main_where_sql}
                        ORDER BY {_num_expr('销售')} DESC, _row_orig_idx ASC
                        """,
                        conn,
                        params=tuple(main_params),
                    )
                    store_slices = {}
                    for i, _ in enumerate(self.store_names):
                        sid = str(i)
                        store_slices[sid] = load_store_slice(sid, None, None)

                    lens = [len(main_df)] + [len(store_slices[str(i)]) for i in range(len(self.store_names))]
                    raw_total = max(lens) if lens else 0
                    indices = []
                    spu_names = set()
                    for idx in range(raw_total):
                        row = self._build_unlinked_virtual_row(idx, main_df, store_slices, link_map)
                        if not self._unlinked_row_passes_filters(row, filters_dict):
                            continue
                        if negative_sales_only and not self._unlinked_row_negative_sales(row):
                            continue
                        indices.append(idx)
                        nm = row.get('商品名称')
                        if nm is not None:
                            t = str(nm).strip()
                            if t and t.lower() != 'nan':
                                spu_names.add(t)
                    total = len(indices)
                    spu_count = len(spu_names)
                    pages = (total + limit - 1) // limit if total else 0
                    page_indices = indices[offset : offset + limit]
                    items = []
                    for idx in page_indices:
                        items.append(self._build_unlinked_virtual_row(idx, main_df, store_slices, link_map))
                else:
                    main_count = conn.execute(f"SELECT COUNT(*) FROM main_products WHERE {main_where_sql}", tuple(main_params)).fetchone()[0]
                    main_df = pd.read_sql(
                        f"""
                        SELECT * FROM main_products
                        WHERE {main_where_sql}
                        ORDER BY {_num_expr('销售')} DESC, _row_orig_idx ASC
                        LIMIT ? OFFSET ?
                        """,
                        conn,
                        params=tuple(main_params + [limit, offset]),
                    )
                    store_slices = {}
                    store_counts = []
                    for i, _ in enumerate(self.store_names):
                        sid = str(i)
                        where = [
                            "cp.project_id = ?",
                            "cp.store_id = ?",
                            """NOT EXISTS (
                                SELECT 1 FROM product_links pl
                                WHERE pl.project_id = cp.project_id
                                  AND pl.store_id = cp.store_id
                                  AND pl.comp_sku_id = cp.skuId
                            )""",
                        ]
                        params = [self.active_project_id, sid]
                        if search_like:
                            where.append("(cp.skuId LIKE ? OR cp.商品名称 LIKE ? OR cp.规格名称 LIKE ?)")
                            params.extend([search_like, search_like, search_like])
                        if cat_like:
                            where.append("cp.美团类目三级 LIKE ?")
                            params.append(cat_like)
                        where_sql = " AND ".join(where)
                        count = conn.execute(f"SELECT COUNT(*) FROM comp_products cp WHERE {where_sql}", tuple(params)).fetchone()[0]
                        store_counts.append(count)
                        store_slices[sid] = load_store_slice(sid, limit, offset)

                    total = max([main_count] + store_counts) if (self.store_names or main_count) else 0
                    pages = (total + limit - 1) // limit if total else 0
                    page_rows = max(0, min(limit, total - offset))
                    items = []
                    for idx in range(page_rows):
                        items.append(self._build_unlinked_virtual_row(idx, main_df, store_slices, link_map))
            finally:
                conn.close()

        return {"items": items, "total": total, "page": page, "limit": limit, "pages": pages, "spu_count": spu_count}

    def get_main_products_page(self, page=1, limit=50, search=""):
        if not self.active_project_id:
            return {"items": [], "total": 0, "page": page, "limit": limit, "pages": 0}
        page = max(1, int(page))
        limit = max(1, min(int(limit), 100))
        offset = (page - 1) * limit
        like = f"%{search.strip()}%" if search else None
        where = ["project_id = ?"]
        params = [self.active_project_id]
        if like:
            where.append("(skuId LIKE ? OR 商品名称 LIKE ? OR 规格名称 LIKE ?)")
            params.extend([like, like, like])
        where_sql = " AND ".join(where)
        with self._db_lock:
            conn = self._get_conn()
            try:
                total = conn.execute(f"SELECT COUNT(*) FROM main_products WHERE {where_sql}", tuple(params)).fetchone()[0]
                df = pd.read_sql(
                    f"""
                    SELECT skuId, 商品名称, 规格名称, 主图链接, 活动价, 原价, 销售, 美团类目三级, _row_orig_idx
                    FROM main_products
                    WHERE {where_sql}
                    ORDER BY CAST(COALESCE(NULLIF(销售, ''), '0') AS REAL) DESC, _row_orig_idx ASC
                    LIMIT ? OFFSET ?
                    """,
                    conn,
                    params=tuple(params + [limit, offset])
                )
                link_cnt = pd.read_sql(
                    """
                    SELECT main_sku_id, COUNT(*) AS cnt
                    FROM product_links
                    WHERE project_id = ?
                    GROUP BY main_sku_id
                    """,
                    conn,
                    params=(self.active_project_id,)
                )
            finally:
                conn.close()
        if not df.empty:
            link_map = {str(r["main_sku_id"]): int(r["cnt"]) for _, r in link_cnt.iterrows()} if not link_cnt.empty else {}
            df["__linked_count"] = df["skuId"].astype(str).map(lambda x: link_map.get(x, 0))
        else:
            df["__linked_count"] = pd.Series(dtype="int64")
        pages = (total + limit - 1) // limit if total else 0
        return {"items": df.fillna("").to_dict(orient="records"), "total": total, "page": page, "limit": limit, "pages": pages}

    def _ensure_column(self, conn, table, col_name):
        try: conn.execute(f"SELECT `{col_name}` FROM `{table}` LIMIT 1")
        except sqlite3.OperationalError: conn.execute(f"ALTER TABLE `{table}` ADD COLUMN `{col_name}` TEXT")

    def _patch_grid_main(self, main_sku_id, updates):
        """In-place patch of grid_df for a main product row. Avoids full reconstruct."""
        if self.grid_df is None or self.grid_df.empty:
            return
        mask = self.grid_df['skuId'].astype(str) == str(main_sku_id)
        if not mask.any():
            return
        for col, val in updates.items():
            if col not in self.grid_df.columns:
                self.grid_df[col] = ""
            self.grid_df.loc[mask, col] = val

    def _patch_grid_comp(self, store_id, comp_sku_id, updates):
        """In-place patch of grid_df for a comp product column. Avoids full reconstruct."""
        if self.grid_df is None or self.grid_df.empty:
            return
        prefix = str(store_id)
        comp_col = f"{prefix}skuId"
        if comp_col not in self.grid_df.columns:
            return
        mask = self.grid_df[comp_col].astype(str) == str(comp_sku_id)
        if not mask.any():
            return
        for col, val in updates.items():
            target = f"{prefix}{col}"
            if target not in self.grid_df.columns:
                self.grid_df[target] = ""
            self.grid_df.loc[mask, target] = val

    def _get_grid_row_by_main_sku(self, main_sku_id):
        if self.grid_df is None or main_sku_id in [None, ""]:
            return None
        rows = self.grid_df[self.grid_df['skuId'].astype(str) == str(main_sku_id)]
        if rows.empty:
            return None
        return rows.iloc[0]

    def update_cell(self, main_sku_id, update_data):
        if not main_sku_id:
            return
        safe_data = {k: v for k, v in update_data.items() if _SAFE_COL_RE.match(k)}
        if not safe_data:
            return
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    for col, val in safe_data.items():
                        self._ensure_column(conn, "main_products", col)
                        conn.execute(f"UPDATE main_products SET `{col}` = ? WHERE project_id = ? AND skuId = ?", 
                                    (val, self.active_project_id, str(main_sku_id)))
            finally:
                conn.close()
        self._patch_grid_main(main_sku_id, safe_data)

    def eliminate_product(self, main_sku_id, status):
        if not main_sku_id:
            return False
        status = int(status) if status else 0
        is_elim = "是" if status == 1 else "否"
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    self._ensure_column(conn, "main_products", "淘汰标记")
                    self._ensure_column(conn, "main_products", "是否淘汰")
                    conn.execute("UPDATE main_products SET `淘汰标记`=?, `是否淘汰`=? WHERE project_id = ? AND skuId=?", 
                                (str(status), is_elim, self.active_project_id, str(main_sku_id)))
            finally:
                conn.close()
        self._patch_grid_main(main_sku_id, {'淘汰标记': str(status), '是否淘汰': is_elim})
        return True

    def toggle_handled(self, sku_id, handled=True):
        val = '1' if handled else '0'
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    conn.execute(
                        "UPDATE main_products SET is_handled=? WHERE project_id=? AND skuId=?",
                        (val, self.active_project_id, str(sku_id)),
                    )
            finally:
                conn.close()
        if self.grid_df is not None and 'skuId' in self.grid_df.columns:
            mask = self.grid_df['skuId'] == str(sku_id)
            self.grid_df.loc[mask, 'is_handled'] = val

    def set_ref(self, sku_id, field, store_id):
        """Set or clear a reference mark. field is 'name' or 'image'. store_id='' to clear."""
        col = 'ref_name_store' if field == 'name' else 'ref_image_store'
        val = str(store_id) if store_id else ''
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    conn.execute(
                        f"UPDATE main_products SET {col}=? WHERE project_id=? AND skuId=?",
                        (val, self.active_project_id, str(sku_id)),
                    )
            finally:
                conn.close()
        if self.grid_df is not None and 'skuId' in self.grid_df.columns:
            mask = self.grid_df['skuId'] == str(sku_id)
            self.grid_df.loc[mask, col] = val

    def mark_as_new(self, store_id, comp_sku_id, is_new):
        if not comp_sku_id:
            return False
        is_new_str = "是" if is_new else "否"
        changed = False
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    self._ensure_column(conn, "product_links", "is_new_add")
                    cur = conn.execute(
                        "UPDATE product_links SET is_new_add=? WHERE project_id = ? AND store_id=? AND comp_sku_id=?",
                        (is_new_str, self.active_project_id, str(store_id), str(comp_sku_id))
                    )
                    changed = cur.rowcount > 0

                    self._ensure_column(conn, "comp_products", "is_new_add")
                    comp_cur = conn.execute(
                        "UPDATE comp_products SET is_new_add=? WHERE project_id=? AND store_id=? AND skuId=?",
                        (is_new_str, self.active_project_id, str(store_id), str(comp_sku_id))
                    )
                    changed = changed or (comp_cur.rowcount > 0)
            finally:
                conn.close()
        if changed:
            self._patch_grid_comp(store_id, comp_sku_id, {'是否新增': is_new_str})
        return changed

    def price_match(self, main_sku_id, store_id):
        if not main_sku_id:
            return None
        prefix = str(store_id)
        act_col, orig_col = f"{prefix}活动价", f"{prefix}原价"
        row = self._get_grid_row_by_main_sku(main_sku_id)
        if row is None:
            return None
        
        new_act, new_orig = "", ""
        updated = False
        if act_col in self.grid_df.columns:
            val = row.get(act_col)
            try:
                num_val = float(val)
                new_act = round(num_val - 0.1, 2) if num_val >= 0.3 else num_val
            except (ValueError, TypeError): new_act = val
            updated = True
            
        if orig_col in self.grid_df.columns:
            new_orig = row.get(orig_col)
            updated = True
            
        if not updated:
            return None

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
        self._patch_grid_main(main_sku_id, {'新活动价': new_act, '新售价': new_orig, '跟价店': store_name})

        def fmt(v):
            if pd.isna(v) or v == "": return ""
            try: return float(v)
            except (ValueError, TypeError): return str(v)

        return {"new_act": fmt(new_act), "new_orig": fmt(new_orig), "store_name": store_name}

    def clear_price_match(self, main_sku_id):
        if not main_sku_id:
            return False
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    conn.execute(
                        "UPDATE main_products SET `新活动价`='', `新售价`='', `跟价店`='' WHERE project_id=? AND skuId=?",
                        (self.active_project_id, str(main_sku_id)),
                    )
            finally:
                conn.close()
        self._patch_grid_main(main_sku_id, {'新活动价': '', '新售价': '', '跟价店': ''})
        return True

    def manual_link(self, main_sku_id, store_id, comp_sku_id):
        if not main_sku_id or not comp_sku_id:
            return False
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
        self._reconstruct_from_sqlite()
        return True

    def unlink_product(self, main_sku_id, store_id):
        if not main_sku_id:
            return False
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
        self._reconstruct_from_sqlite()
        return True

    def _calculate_margins(self):
        if self.grid_df is None or '采购价' not in self.grid_df.columns: return

        def calc(a, p):
            try:
                a, p = float(a), float(p)
                if a > 0: return f"{round((a - p) / a * 100, 2)}%"
            except (ValueError, TypeError): pass
            return "-"

        self.grid_df['现价毛利'] = [calc(r.get('活动价'), r.get('采购价')) for _, r in self.grid_df.iterrows()]
        if '新活动价' in self.grid_df.columns:
            self.grid_df['跟价毛利'] = [calc(r.get('新活动价'), r.get('采购价')) for _, r in self.grid_df.iterrows()]

        for i in range(len(self.store_names)):
            prefix = str(i)
            comp_act_col = f"{prefix}活动价"
            if comp_act_col in self.grid_df.columns:
                self.grid_df[f"{prefix}现价毛利"] = [calc(r.get(comp_act_col), r.get('采购价')) for _, r in self.grid_df.iterrows()]
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
            store_id_to_name = {str(i): name for i, name in enumerate(self.store_names)}
            main_cols = [c for c in self.grid_df.columns if (not c or not c[0].isdigit()) and c not in INTERNAL_EXPORT_KEYS]
            main_export = self.grid_df[main_cols].copy()
            ref_name_col = self.grid_df['ref_name_store'].fillna('') if 'ref_name_store' in self.grid_df.columns else pd.Series('', index=self.grid_df.index)
            ref_image_col = self.grid_df['ref_image_store'].fillna('') if 'ref_image_store' in self.grid_df.columns else pd.Series('', index=self.grid_df.index)

            main_export['名称参考店铺'] = ref_name_col.map(lambda v: store_id_to_name.get(str(v), '') if v else '')
            main_export['参考商品名称'] = [
                self.grid_df.at[i, str(v) + '商品名称'] if v and (str(v) + '商品名称') in self.grid_df.columns else ''
                for i, v in ref_name_col.items()
            ]
            main_export['图片参考店铺'] = ref_image_col.map(lambda v: store_id_to_name.get(str(v), '') if v else '')
            main_export['参考图片链接'] = [
                self.grid_df.at[i, str(v) + '主图链接'] if v and (str(v) + '主图链接') in self.grid_df.columns else ''
                for i, v in ref_image_col.items()
            ]
            main_export.to_excel(os.path.join(temp_dir, f"主店_{self.main_store_name}.xlsx"), index=False)

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
            with self._db_lock, self._get_conn() as conn:
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
            
                if unlinked_data:
                    pool_df = pd.concat(unlinked_data, ignore_index=True)
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
        
        # 1. Fetch ALL products marked as "New" from comp_products (includes linked + unlinked)
        all_new_data = []
        with self._db_lock, self._get_conn() as conn:
            query = "SELECT * FROM comp_products WHERE project_id = ? AND is_new_add = '是'"
            all_comp_new_df = pd.read_sql(query, conn, params=(self.active_project_id,))
            if not all_comp_new_df.empty:
                # Add store name and main store link info if available
                all_comp_new_df['竞品店铺'] = all_comp_new_df['store_id'].map(store_map)
                
                # Fetch links to get Main Store SKU if linked
                link_query = "SELECT comp_sku_id, main_sku_id, store_id FROM product_links WHERE project_id = ?"
                links = pd.read_sql(link_query, conn, params=(self.active_project_id,))
                
                # Merge to get Main SKU (store-aware to avoid cross-store false joins)
                merged = all_comp_new_df.merge(
                    links,
                    left_on=['skuId', 'store_id'],
                    right_on=['comp_sku_id', 'store_id'],
                    how='left'
                )
                merged.rename(columns={'main_sku_id': '主店SKU'}, inplace=True)
                merged['来源'] = merged['主店SKU'].apply(lambda v: '已匹配' if str(v).strip() not in ['', 'nan', 'None'] else '未匹配池')
                
                # Ensure core columns
                cols = ['来源', '主店SKU', '竞品店铺', 'skuId', '主图链接', '商品名称', '规格名称', '活动价', '原价', '销售', '条码']
                final_new_df = merged[[c for c in cols if c in merged.columns]].copy()
                for c in cols:
                    if c not in final_new_df.columns: final_new_df[c] = ""
                all_new_data = final_new_df.fillna("").to_dict(orient='records')

        final_df = pd.DataFrame(all_new_data) if all_new_data else pd.DataFrame(columns=["主店SKU", "竞品店铺", "skuId", "主图链接", "商品名称", "规格名称", "活动价", "原价", "销售", "条码"])
        final_df["操作时间"] = op_time

        # 2. Main store: eliminated / price-matched / ref-marked items
        mask = pd.Series(False, index=self.grid_df.index)
        if '是否淘汰' in self.grid_df.columns: mask |= (self.grid_df['是否淘汰'] == "是")
        if '跟价店' in self.grid_df.columns: mask |= (self.grid_df['跟价店'].notna() & (self.grid_df['跟价店'] != ""))
        if 'ref_name_store' in self.grid_df.columns: mask |= (self.grid_df['ref_name_store'].fillna('') != '')
        if 'ref_image_store' in self.grid_df.columns: mask |= (self.grid_df['ref_image_store'].fillna('') != '')
            
        elim_df = self.grid_df[mask].copy()
        if not elim_df.empty:
            main_cols = [c for c in elim_df.columns if (not c or not c[0].isdigit()) and c not in INTERNAL_EXPORT_KEYS]
            elim_export = elim_df[main_cols].copy()
            ref_name_s = elim_df['ref_name_store'].fillna('') if 'ref_name_store' in elim_df.columns else pd.Series('', index=elim_df.index)
            ref_image_s = elim_df['ref_image_store'].fillna('') if 'ref_image_store' in elim_df.columns else pd.Series('', index=elim_df.index)
            elim_export['名称参考店铺'] = ref_name_s.map(lambda v: store_map.get(str(v), '') if v else '')
            elim_export['参考商品名称'] = [
                elim_df.at[i, str(v) + '商品名称'] if v and (str(v) + '商品名称') in elim_df.columns else ''
                for i, v in ref_name_s.items()
            ]
            elim_export['图片参考店铺'] = ref_image_s.map(lambda v: store_map.get(str(v), '') if v else '')
            elim_export['参考图片链接'] = [
                elim_df.at[i, str(v) + '主图链接'] if v and (str(v) + '主图链接') in elim_df.columns else ''
                for i, v in ref_image_s.items()
            ]
            elim_export["操作时间"] = op_time
            elim_df = elim_export
        else: elim_df = pd.DataFrame(columns=["skuId", "主图链接", "商品名称", "规格名称", "活动价", "原价", "销售", "条码", "操作时间"])

        dirs = self._get_project_dirs(self.active_project_id)
        path = os.path.join(dirs["outputs"], f"新增竞品数据_{time.strftime('%Y%m%d_%H%M%S')}.xlsx")
        sheet_data = {"新增(竞店)": final_df.fillna("").to_dict(orient='records'), "淘汰(主店)": elim_df.fillna("").to_dict(orient='records')}
        utils.write_multisheet_dict_to_excel(sheet_data, path); return path

    def list_projects(self):
        with self._db_lock:
            conn = self._get_conn()
            try:
                cur = conn.execute("""
                    SELECT p.id, p.name, p.created_at, p.is_active,
                           COALESCE(m.cnt, 0) AS sku_count, p.status,
                           p.analysis_started_at
                    FROM projects p
                    LEFT JOIN (
                        SELECT project_id, COUNT(*) AS cnt
                        FROM main_products
                        GROUP BY project_id
                    ) m ON m.project_id = p.id
                    ORDER BY p.created_at DESC
                """)
                return [
                    {"id": r[0], "name": r[1], "created_at": r[2],
                     "is_active": r[3], "sku_count": r[4],
                     "status": r[5] or "ready",
                     "analysis_started_at": r[6]}
                    for r in cur.fetchall()
                ]
            finally:
                conn.close()

    def create_project(self, name, main_file_info, comp_files_info, status='ready'):
        """
        main_file_info: {'path': ..., 'store_name': ...}
        comp_files_info: [{'path': ..., 'store_name': ...}, ...]
        """
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    analysis_started = time.strftime('%Y-%m-%d %H:%M:%S') if status == 'analyzing' else None
                    cur = conn.execute("INSERT INTO projects (name, status, analysis_started_at) VALUES (?, ?, ?)",
                                       (name, status, analysis_started))
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

    def update_project_status(self, project_id, status):
        """Thread-safe status update using an independent connection (safe for background threads)."""
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("UPDATE projects SET status = ?, analysis_started_at = NULL WHERE id = ?",
                         (status, project_id))
            conn.commit()
        finally:
            conn.close()

    def activate_project(self, project_id, skip_load=False):
        self.grid_df = None
        self.main_df = None
        self.store_dfs = {}
        gc.collect()

        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    conn.execute("UPDATE projects SET is_active = 0")
                    conn.execute("UPDATE projects SET is_active = 1 WHERE id = ?", (project_id,))
            finally:
                conn.close()
        self._load_active_project()
        if not skip_load:
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
