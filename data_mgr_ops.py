import time
import os
import shutil
import pandas as pd
from data_mgr_base import _SAFE_COL_RE, INTERNAL_EXPORT_KEYS

class DataManagerOpsMixin:
    def _resolve_project_id(self, project_id=None):
        return int(project_id or self.active_project_id or 0)

    def _ensure_column(self, conn, table, col_name):
        cols = [c.lower() for c in conn.get_table_columns(table)]
        if col_name.lower() not in cols:
            conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col_name}" TEXT')

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

    def update_cell(self, main_sku_id, update_data, project_id=None):
        pid = self._resolve_project_id(project_id)
        if not main_sku_id:
            return
        safe_data = {k: v for k, v in update_data.items() if _SAFE_COL_RE.match(k)}
        if not pid or not safe_data:
            return
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    for col, val in safe_data.items():
                        self._ensure_column(conn, "main_products", col)
                        conn.execute(f'UPDATE main_products SET "{col}" = ? WHERE project_id = ? AND "skuId" = ?', 
                                    (val, pid, str(main_sku_id)))
            finally:
                conn.close()
        if pid == self.active_project_id:
            self._patch_grid_main(main_sku_id, safe_data)
        self.invalidate_analysis_snapshot(pid)

    def eliminate_product(self, main_sku_id, status, project_id=None):
        pid = self._resolve_project_id(project_id)
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
                    conn.execute('UPDATE main_products SET "淘汰标记"=?, "是否淘汰"=? WHERE project_id = ? AND "skuId"=?', 
                                (str(status), is_elim, pid, str(main_sku_id)))
            finally:
                conn.close()
        if pid == self.active_project_id:
            self._patch_grid_main(main_sku_id, {'淘汰标记': str(status), '是否淘汰': is_elim})
        self.invalidate_analysis_snapshot(pid)
        return True

    def toggle_handled(self, sku_id, handled=True, project_id=None):
        pid = self._resolve_project_id(project_id)
        val = '1' if handled else '0'
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    conn.execute(
                        "UPDATE main_products SET is_handled=? WHERE project_id=? AND skuId=?",
                        (val, pid, str(sku_id)),
                    )
            finally:
                conn.close()
        if pid == self.active_project_id and self.grid_df is not None and 'skuId' in self.grid_df.columns:
            mask = self.grid_df['skuId'] == str(sku_id)
            self.grid_df.loc[mask, 'is_handled'] = val
        self.invalidate_analysis_snapshot(pid)

    def set_ref(self, sku_id, field, store_id, project_id=None):
        """Set or clear a reference mark. field is 'name' or 'image'. store_id='' to clear."""
        pid = self._resolve_project_id(project_id)
        col = 'ref_name_store' if field == 'name' else 'ref_image_store'
        val = str(store_id) if store_id else ''
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    conn.execute(
                        f"UPDATE main_products SET {col}=? WHERE project_id=? AND skuId=?",
                        (val, pid, str(sku_id)),
                    )
            finally:
                conn.close()
        if pid == self.active_project_id and self.grid_df is not None and 'skuId' in self.grid_df.columns:
            mask = self.grid_df['skuId'] == str(sku_id)
            self.grid_df.loc[mask, col] = val

    def mark_as_new(self, store_id, comp_sku_id, is_new, project_id=None):
        pid = self._resolve_project_id(project_id)
        if not comp_sku_id:
            return False
        if isinstance(is_new, str):
            is_new_str = is_new
            ignore_str = "否" if is_new != "否" else None
        else:
            is_new_str = "是" if is_new else "否"
            ignore_str = "否" if is_new else None
        changed = False
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    self._ensure_column(conn, "product_links", "is_new_add")
                    cur = conn.execute(
                        "UPDATE product_links SET is_new_add=? WHERE project_id = ? AND store_id=? AND comp_sku_id=?",
                        (is_new_str, pid, str(store_id), str(comp_sku_id))
                    )
                    changed = cur.rowcount > 0

                    self._ensure_column(conn, "comp_products", "is_new_add")
                    self._ensure_column(conn, "comp_products", "is_ignored")
                    if ignore_str is not None:
                        comp_cur = conn.execute(
                            "UPDATE comp_products SET is_new_add=?, is_ignored=? WHERE project_id=? AND store_id=? AND skuId=?",
                            (is_new_str, ignore_str, pid, str(store_id), str(comp_sku_id))
                        )
                    else:
                        comp_cur = conn.execute(
                            "UPDATE comp_products SET is_new_add=? WHERE project_id=? AND store_id=? AND skuId=?",
                            (is_new_str, pid, str(store_id), str(comp_sku_id))
                        )
                    changed = changed or (comp_cur.rowcount > 0)
            finally:
                conn.close()
        if changed and pid == self.active_project_id:
            updates = {'是否新增': is_new_str}
            if ignore_str is not None:
                updates['是否不处理'] = ignore_str
            self._patch_grid_comp(store_id, comp_sku_id, updates)
        if changed:
            self.invalidate_analysis_snapshot(pid)
        return changed

    def mark_as_ignored(self, store_id, comp_sku_id, is_ignored, project_id=None):
        pid = self._resolve_project_id(project_id)
        if not comp_sku_id:
            return False
        ignore_str = "是" if is_ignored else "否"
        new_str = "否" if is_ignored else None
        changed = False
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    self._ensure_column(conn, "comp_products", "is_ignored")
                    self._ensure_column(conn, "comp_products", "is_new_add")
                    if new_str is not None:
                        comp_cur = conn.execute(
                            "UPDATE comp_products SET is_ignored=?, is_new_add=? WHERE project_id=? AND store_id=? AND skuId=?",
                            (ignore_str, new_str, pid, str(store_id), str(comp_sku_id))
                        )
                        self._ensure_column(conn, "product_links", "is_new_add")
                        conn.execute(
                            "UPDATE product_links SET is_new_add=? WHERE project_id = ? AND store_id=? AND comp_sku_id=?",
                            (new_str, pid, str(store_id), str(comp_sku_id))
                        )
                    else:
                        comp_cur = conn.execute(
                            "UPDATE comp_products SET is_ignored=? WHERE project_id=? AND store_id=? AND skuId=?",
                            (ignore_str, pid, str(store_id), str(comp_sku_id))
                        )
                    changed = changed or (comp_cur.rowcount > 0)
            finally:
                conn.close()
        if changed and pid == self.active_project_id:
            updates = {'是否不处理': ignore_str}
            if new_str is not None:
                updates['是否新增'] = new_str
            self._patch_grid_comp(store_id, comp_sku_id, updates)
        if changed:
            self.invalidate_analysis_snapshot(pid)
        return changed

    def price_match(self, main_sku_id, store_id, match_act=True, match_orig=True, project_id=None):
        pid = self._resolve_project_id(project_id)
        if pid != self.active_project_id:
            return None
        if not main_sku_id:
            return None
        prefix = str(store_id)
        act_col, orig_col = f"{prefix}活动价", f"{prefix}原价"
        row = self._get_grid_row_by_main_sku(main_sku_id)
        if row is None:
            return None
        
        new_act, new_orig = "", ""
        updated = False
        updates = {}
        if match_act and act_col in self.grid_df.columns:
            val = row.get(act_col)
            try:
                num_val = float(val)
                new_act = round(num_val - 0.1, 2) if num_val >= 0.3 else num_val
            except (ValueError, TypeError): new_act = val
            updates['新活动价'] = new_act
            updated = True
            
        if match_orig and orig_col in self.grid_df.columns:
            new_orig = row.get(orig_col)
            updates['新售价'] = new_orig
            updated = True
            
        if not updated:
            return None

        store_name = self.store_dfs[str(store_id)]["name"]
        updates['跟价店'] = store_name

        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    for c in updates.keys(): self._ensure_column(conn, "main_products", c)
                    set_clause = ", ".join([f'"{c}"=?' for c in updates.keys()])
                    conn.execute(f'UPDATE main_products SET {set_clause} WHERE project_id = ? AND "skuId"=?', 
                                (*updates.values(), pid, str(main_sku_id)))
            finally:
                conn.close()
        self._patch_grid_main(main_sku_id, updates)
        self.invalidate_analysis_snapshot(pid)

        def fmt(v):
            import pandas as pd
            if pd.isna(v) or v == "": return ""
            try: return float(v)
            except (ValueError, TypeError): return str(v)

        ret = {"store_name": store_name}
        if match_act:
            ret["new_act"] = fmt(new_act)
        if match_orig:
            ret["new_orig"] = fmt(new_orig)
        return ret

    def clear_price_match(self, main_sku_id, project_id=None):
        pid = self._resolve_project_id(project_id)
        if not main_sku_id:
            return False
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    conn.execute(
                        'UPDATE main_products SET "新活动价"=\'\', "新售价"=\'\', "跟价店"=\'\' WHERE project_id=? AND "skuId"=?',
                        (pid, str(main_sku_id)),
                    )
            finally:
                conn.close()
        if pid == self.active_project_id:
            self._patch_grid_main(main_sku_id, {'新活动价': '', '新售价': '', '跟价店': ''})
        self.invalidate_analysis_snapshot(pid)
        return True

    def manual_link(self, main_sku_id, store_id, comp_sku_id, project_id=None):
        pid = self._resolve_project_id(project_id)
        if not main_sku_id or not comp_sku_id:
            return False
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    old_link = conn.execute(
                        """
                        SELECT comp_sku_id, similarity, match_type
                        FROM product_links
                        WHERE project_id = ? AND main_sku_id = ? AND store_id = ?
                        LIMIT 1
                        """,
                        (pid, str(main_sku_id), str(store_id)),
                    ).fetchone()
                    old_comp_sku_id = old_link[0] if old_link else ""
                    old_similarity = old_link[1] if old_link else None
                    old_match_type = old_link[2] if old_link else ""
                    error_type = "错配" if str(old_comp_sku_id or "").strip() else "漏配"
                    conn.execute(
                        """
                        DELETE FROM product_links
                        WHERE project_id = ?
                          AND store_id = ?
                          AND (
                              main_sku_id = ?
                              OR comp_sku_id = ?
                          )
                        """,
                        (pid, str(store_id), str(main_sku_id), str(comp_sku_id)),
                    )
                    conn.execute("""
                        INSERT INTO product_links (project_id, main_sku_id, store_id, comp_sku_id, similarity, match_type, is_new_add)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (pid, str(main_sku_id), str(store_id), str(comp_sku_id), 1.0, '手动关联', '否'))
                    conn.execute(
                        """
                        INSERT INTO manual_link_corrections (
                            project_id, main_sku_id, store_id,
                            old_comp_sku_id, old_similarity, old_match_type,
                            new_comp_sku_id, new_similarity, new_match_type, error_type, created_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            pid,
                            str(main_sku_id),
                            str(store_id),
                            str(old_comp_sku_id or ""),
                            old_similarity,
                            str(old_match_type or ""),
                            str(comp_sku_id),
                            1.0,
                            "手动关联",
                            error_type,
                            __import__("time").strftime("%Y-%m-%d %H:%M:%S"),
                        ),
                    )
            finally:
                conn.close()
        if pid == self.active_project_id:
            self._reconstruct_from_sqlite()
            self.refresh_workbench_summary_snapshot()
        self.invalidate_analysis_snapshot(pid)
        return True

    def unlink_product(self, main_sku_id, store_id, project_id=None):
        pid = self._resolve_project_id(project_id)
        if not main_sku_id:
            return False
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    old_link = conn.execute(
                        """
                        SELECT comp_sku_id, similarity, match_type
                        FROM product_links
                        WHERE project_id = ? AND main_sku_id = ? AND store_id = ?
                        LIMIT 1
                        """,
                        (pid, str(main_sku_id), str(store_id)),
                    ).fetchone()
                    conn.execute("DELETE FROM product_links WHERE project_id = ? AND main_sku_id=? AND store_id=?", 
                                (pid, str(main_sku_id), str(store_id)))
                    if old_link:
                        conn.execute(
                            """
                            INSERT INTO manual_link_corrections (
                                project_id, main_sku_id, store_id,
                                old_comp_sku_id, old_similarity, old_match_type,
                                new_comp_sku_id, new_similarity, new_match_type, error_type, created_at
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                pid,
                                str(main_sku_id),
                                str(store_id),
                                str(old_link[0] or ""),
                                old_link[1],
                                str(old_link[2] or ""),
                                "",
                                None,
                                "",
                                "错配",
                                __import__("time").strftime("%Y-%m-%d %H:%M:%S"),
                            ),
                        )
            except Exception as e:
                print("DB Unlink err:", e)
            finally:
                conn.close()
        if pid == self.active_project_id:
            self._reconstruct_from_sqlite()
            self.refresh_workbench_summary_snapshot()
        self.invalidate_analysis_snapshot(pid)
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

    def list_projects(self):
        with self._db_lock:
            conn = self._get_conn()
            try:
                cur = conn.execute("""
                    SELECT p.id, p.name, p.created_at, p.is_active,
                           COALESCE(m.cnt, 0) AS sku_count, p.status,
                           p.analysis_started_at, p.rule_template_id
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
                     "analysis_started_at": r[6],
                     "rule_template_id": r[7]}
                    for r in cur.fetchall()
                ]
            finally:
                conn.close()

    def create_project(self, name, main_file_info, comp_files_info, status='ready', match_config_json="", rule_template_id=None):
        """
        main_file_info: {'path': ..., 'store_name': ...}
        comp_files_info: [{'path': ..., 'store_name': ...}, ...]
        """
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    now = time.strftime('%Y-%m-%d %H:%M:%S')
                    analysis_started = now if status in ('analyzing', 'creating') else None
                    rtid = rule_template_id
                    if rtid is None:
                        rtid = self._default_rule_template_id(conn)
                    cur = conn.execute(
                        "INSERT INTO projects (name, created_at, status, analysis_started_at, match_config, rule_template_id) VALUES (?, ?, ?, ?, ?, ?)",
                        (name, now, status, analysis_started, match_config_json or "", rtid),
                    )
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
        conn = self._get_conn()
        try:
            conn.execute("UPDATE projects SET status = ?, analysis_started_at = NULL WHERE id = ?",
                         (status, project_id))
            conn.commit()
        finally:
            conn.close()

    def activate_project(self, project_id, skip_load=False):
        import gc
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
        import shutil
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    conn.execute("DELETE FROM main_products WHERE project_id = ?", (project_id,))
                    conn.execute("DELETE FROM product_links WHERE project_id = ?", (project_id,))
                    conn.execute("DELETE FROM comp_products WHERE project_id = ?", (project_id,))
                    conn.execute("DELETE FROM project_files WHERE project_id = ?", (project_id,))
                    conn.execute("DELETE FROM project_analysis_snapshots WHERE project_id = ?", (project_id,))
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
