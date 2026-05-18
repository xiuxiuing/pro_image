import os
import hashlib
import pandas as pd
import utils
from utils import clean_text_value
from data_mgr_base import MAPPING_VERSION, FIELD_MAPPINGS, CORE_MAIN_COLUMNS, CORE_COMP_COLUMNS

class DataManagerImportMixin:
    def _normalize_dataframe_text(self, df):
        """Clean common Excel text artifacts before storing or parsing keys."""
        if df is None or df.empty:
            return df
        for col in df.columns:
            df[col] = df[col].map(clean_text_value)
        return df

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

        if self.active_project_id and (target_file or source_files):
            self.import_project_sources(self.active_project_id)
        else:
            self.load_data()

    def load_data(self):
        if not self.active_project_id:
            return
        self._reconstruct_from_sqlite()

    def import_project_sources(self, project_id):
        """Import only the uploaded main/competitor source files for a project."""
        self.activate_project(project_id, skip_load=True)
        self._import_to_sqlite(import_links=False)
        self.grid_df = None
        self.main_df = None
        self.store_dfs = {}
        self.rebuild_analysis_snapshot()

    def parse_links_from_output(self, project_id, output_file):
        """Parse an analysis/output workbook into standard product_links rows."""
        if not output_file or not os.path.exists(output_file):
            return None
        with self._db_lock:
            conn = self._get_conn()
            try:
                rows = conn.execute(
                    "SELECT id FROM project_files WHERE project_id = ? AND type = 'comp' ORDER BY id ASC",
                    (project_id,),
                ).fetchall()
                store_count = len(rows)
                if store_count == 0:
                    store_rows = conn.execute(
                        "SELECT DISTINCT store_id FROM comp_products WHERE project_id = ? ORDER BY CAST(store_id AS INTEGER)",
                        (project_id,),
                    ).fetchall()
                    store_ids = [str(r[0]) for r in store_rows]
                else:
                    store_ids = [str(i) for i in range(store_count)]

                comp_dfs = []
                for sid in store_ids:
                    cdf = pd.read_sql(
                        "SELECT * FROM comp_products WHERE project_id = ? AND store_id = ?",
                        conn, params=(project_id, sid)
                    )
                    comp_dfs.append(cdf)
            finally:
                conn.close()

        print(f"Importing Links from Result: {output_file}")
        res_data = utils.excel_to_list_dict(output_file)
        res_df = pd.DataFrame(res_data)
        if res_df.empty:
            return None
        res_df = self._normalize_dataframe_text(res_df)

        prefix_to_store_map = self._detect_output_prefix_to_store_map(res_df, comp_dfs)
        final_mappings = FIELD_MAPPINGS.copy()
        for p in prefix_to_store_map.keys():
            for k, v in FIELD_MAPPINGS.items():
                final_mappings[p + k] = p + v
        res_df = self._apply_mappings(res_df, final_mappings)

        links = []
        for idx, row in res_df.iterrows():
            row_dict = row.to_dict()
            main_sku = utils.get_sku_id(row_dict)
            if not main_sku:
                main_sku = f"auto_{idx}"

            for p, sid in prefix_to_store_map.items():
                comp_sku_col = f"{p}skuId"
                if comp_sku_col not in res_df.columns:
                    continue
                comp_sku_val = row_dict.get(comp_sku_col)
                comp_sku = ""
                if comp_sku_val is not None:
                    s_str = str(comp_sku_val).strip()
                    comp_sku = s_str[:-2] if s_str.endswith(".0") else s_str
                if comp_sku and comp_sku.lower() not in ["", "nan", "none", "nan.0"]:
                    links.append({
                        'project_id': project_id,
                        'main_sku_id': str(main_sku),
                        'store_id': sid,
                        'comp_sku_id': str(comp_sku),
                        'similarity': row_dict.get(f"{p}相似度", 1.0),
                        'match_type': row_dict.get(f"{p}匹配", "未知"),
                        'is_new_add': row_dict.get(f"{p}是否新增", "否")
                    })
        return pd.DataFrame(links) if links else None

    def _prepare_links_from_output(self, output_file):
        return self.parse_links_from_output(self.active_project_id, output_file)

    def replace_project_links(self, project_id, links_df, categories=None):
        """Replace product links for a project, optionally scoped to main-store categories."""
        cleaned_categories = []
        for c in categories or []:
            c = clean_text_value(c)
            if c is None:
                continue
            c = str(c).strip()
            if c:
                cleaned_categories.append(c)
        categories = cleaned_categories
        if links_df is not None and not links_df.empty:
            links_df = links_df.copy()
            links_df["project_id"] = project_id

        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    if categories:
                        placeholders = ",".join(["?"] * len(categories))
                        rows = conn.execute(
                            f"""
                            SELECT skuId FROM main_products
                            WHERE project_id = ? AND trim(COALESCE(美团类目三级, '')) IN ({placeholders})
                            """,
                            [project_id] + categories,
                        ).fetchall()
                        main_skus = [str(r[0]) for r in rows if r[0] is not None]
                        if not main_skus:
                            return
                        sku_placeholders = ",".join(["?"] * len(main_skus))
                        conn.execute(
                            f"DELETE FROM product_links WHERE project_id = ? AND main_sku_id IN ({sku_placeholders})",
                            [project_id] + main_skus,
                        )
                        if links_df is not None and not links_df.empty:
                            scoped = links_df[links_df["main_sku_id"].astype(str).isin(main_skus)]
                            if not scoped.empty:
                                scoped.to_sql('product_links', conn, index=False, if_exists='append')
                    else:
                        conn.execute("DELETE FROM product_links WHERE project_id = ?", (project_id,))
                        if links_df is not None and not links_df.empty:
                            links_df.to_sql('product_links', conn, index=False, if_exists='append')
                    conn.execute("DELETE FROM project_analysis_snapshots WHERE project_id = ?", (project_id,))
            finally:
                conn.close()

        if self.active_project_id == project_id:
            self._reconstruct_from_sqlite()

    def import_project_links_from_output(self, output_file, categories=None):
        """Replace product links from an output file, optionally limited to main-store category names."""
        pid = self.active_project_id
        links_df = self.parse_links_from_output(pid, output_file)
        self.replace_project_links(pid, links_df, categories=categories)

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

    def _clean_sku_value(self, value):
        if value is None:
            return ""
        try:
            if pd.isna(value):
                return ""
        except (TypeError, ValueError):
            pass
        s = str(clean_text_value(value)).strip()
        if s.endswith(".0"):
            s = s[:-2]
        if s.lower() in ["", "nan", "none", "nan.0"]:
            return ""
        return s

    def _detect_output_prefix_to_store_map(self, res_df, comp_dfs):
        """
        Map output column prefixes (0skuId/1skuId/...) to the actual store_id.

        Manual uploads can provide a result file whose competitor column order is
        different from the current project file order. Matching by column index
        then imports links under the wrong store. We instead compare each output
        prefix's SKU values with every competitor source SKU set and choose a
        one-to-one assignment by strongest overlap, falling back to index order
        only when the result file has no usable SKU evidence.
        """
        prefixes = []
        for col in res_df.columns:
            col_s = str(col)
            if col_s.endswith("skuId") and col_s[:-5].isdigit():
                prefixes.append(col_s[:-5])
        prefixes = sorted(set(prefixes), key=lambda x: int(x))

        store_sku_sets = {}
        for idx, cdf in enumerate(comp_dfs):
            if cdf is None or cdf.empty or "skuId" not in cdf.columns:
                store_sku_sets[str(idx)] = set()
                continue
            store_sku_sets[str(idx)] = {
                self._clean_sku_value(v)
                for v in cdf["skuId"].tolist()
                if self._clean_sku_value(v)
            }

        prefix_sku_sets = {}
        for p in prefixes:
            col = f"{p}skuId"
            prefix_sku_sets[p] = {
                self._clean_sku_value(v)
                for v in res_df[col].tolist()
                if self._clean_sku_value(v)
            }

        candidates = []
        for p in prefixes:
            p_skus = prefix_sku_sets.get(p, set())
            for sid, s_skus in store_sku_sets.items():
                overlap = len(p_skus & s_skus)
                ratio = overlap / max(1, len(p_skus))
                same_index = 1 if p == sid else 0
                distance = abs(int(p) - int(sid))
                candidates.append((overlap, ratio, same_index, -distance, p, sid))

        assigned_prefixes = set()
        assigned_stores = set()
        prefix_to_store_map = {}
        for overlap, ratio, same_index, neg_distance, p, sid in sorted(candidates, reverse=True):
            if overlap <= 0:
                continue
            if p in assigned_prefixes or sid in assigned_stores:
                continue
            prefix_to_store_map[p] = sid
            assigned_prefixes.add(p)
            assigned_stores.add(sid)
            print(
                f"Link import: column prefix [{p}*] → store_id [{sid}] "
                f"(sku overlap={overlap}, ratio={ratio:.2%})",
                flush=True,
            )

        for p in prefixes:
            if p in assigned_prefixes:
                continue
            # Safe fallback for generated files where output order already matches
            # project order, or for empty columns with no SKU evidence.
            if p in store_sku_sets and p not in assigned_stores:
                prefix_to_store_map[p] = p
                assigned_prefixes.add(p)
                assigned_stores.add(p)
                print(f"Link import: column prefix [{p}*] → store_id [{p}] (index fallback)", flush=True)

        return prefix_to_store_map

    def _import_to_sqlite(self, import_links=False):
        """
        Transactional import: prepare all data in memory first, then write in a
        single atomic transaction. If any step fails, nothing is changed in DB.
        """
        pid = self.active_project_id
        import_output_links = bool(import_links and os.path.exists(self.output_file))

        # ── Phase 1: Prepare main store data (memory only) ──
        main_df = None
        if os.path.exists(self.target_file):
            print(f"Importing Main Store: {self.target_file}")
            main_data = utils.excel_to_list_dict(self.target_file)
            main_df = pd.DataFrame(main_data)
            main_df = self._apply_mappings(main_df, FIELD_MAPPINGS)
            main_df = self._normalize_dataframe_text(main_df)

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
            for c in ["商品名称", "规格名称"]:
                main_df[c] = main_df[c].fillna("").astype(str)
            main_df = main_df.drop_duplicates(
                subset=['project_id', 'skuId', '商品名称', '规格名称'],
                keep='first',
            )

        # ── Phase 2: Prepare competitor store data (memory only) ──
        comp_dfs = []
        for i, path in enumerate(self.source_files):
            if os.path.exists(path):
                print(f"Importing Competitor Store [{i}]: {path}")
                comp_data = utils.excel_to_list_dict(path)
                cdf = pd.DataFrame(comp_data)
                cdf = self._apply_mappings(cdf, FIELD_MAPPINGS)
                cdf = self._normalize_dataframe_text(cdf)
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

        # ── Phase 3: Prepare links only for legacy explicit imports. Normal
        # analysis writes product_links via replace_project_links().
        links_df = None
        if import_output_links:
            links_df = self.parse_links_from_output(pid, self.output_file)

        # ── Phase 4: Atomic DB write — single transaction ──
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    conn.execute("DELETE FROM main_products WHERE project_id = ?", (pid,))
                    conn.execute("DELETE FROM comp_products WHERE project_id = ?", (pid,))
                    if import_output_links:
                        conn.execute("DELETE FROM product_links WHERE project_id = ?", (pid,))

                    if main_df is not None:
                        main_df.to_sql('main_products', conn, index=False, if_exists='append')
                        self.main_df = main_df
                    for cdf in comp_dfs:
                        cdf.to_sql('comp_products', conn, index=False, if_exists='append')
                    if links_df is not None and not links_df.empty:
                        links_df.to_sql('product_links', conn, index=False, if_exists='append')

                    conn.execute("REPLACE INTO meta_info (key, value) VALUES ('mapping_version', ?)", (MAPPING_VERSION,))
                print(f"Import complete for project {pid} (atomic transaction).")
            except Exception as e:
                print(f"Import FAILED for project {pid}, transaction rolled back: {e}")
                raise
            finally:
                conn.close()
