import os
import hashlib
import pandas as pd
import utils
from data_mgr_base import MAPPING_VERSION, FIELD_MAPPINGS, CORE_MAIN_COLUMNS, CORE_COMP_COLUMNS

class DataManagerImportMixin:
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
