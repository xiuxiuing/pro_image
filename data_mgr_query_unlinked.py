import json
import re
import unicodedata
import pandas as pd


class DataManagerUnlinkedQueryMixin:
    SEARCH_TOKEN_LIMIT = 5

    def _normalize_search_text(self, value):
        if value is None:
            return ""
        try:
            if pd.isna(value):
                return ""
        except (TypeError, ValueError):
            pass
        return unicodedata.normalize("NFKC", str(value)).casefold().strip()

    def _search_tokens(self, search, limit=None):
        text = self._normalize_search_text(search)
        if not text:
            return []
        max_tokens = self.SEARCH_TOKEN_LIMIT if limit is None else int(limit)
        tokens = []
        seen = set()
        for token in re.split(r"[\s,，;；、]+", text):
            token = token.strip()
            if not token or token in seen:
                continue
            seen.add(token)
            tokens.append(token)
            if len(tokens) >= max_tokens:
                break
        return tokens

    def _row_matches_search_tokens(self, row, tokens):
        if not tokens:
            return True
        haystack = " ".join(self._normalize_search_text(v) for v in row)
        return all(token in haystack for token in tokens)

    def _add_search_token_clauses(self, where, params, columns, tokens):
        for token in tokens:
            where.append("(" + " OR ".join(f"LOWER(COALESCE({col}, '')) LIKE ?" for col in columns) + ")")
            params.extend([f"%{token}%" for _ in columns])

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
                row[f"{sid}是否不处理"] = row.get(f"{sid}is_ignored", "否") or "否"
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

    def _empty_unlinked_store_stats(self):
        return {str(i): {"spu_count": 0, "sku_count": 0} for i in range(len(self.store_names))}

    def _add_unlinked_store_stat_from_row(self, stat_sets, row):
        for i in range(len(self.store_names)):
            sid = str(i)
            sku = str(row.get(f"{sid}skuId", "") or "").strip()
            if not sku or sku.lower() in ("nan", "none"):
                continue
            stat_sets[sid]["sku"].add(sku)
            name = str(row.get(f"{sid}商品名称", "") or "").strip()
            if name and name.lower() != "nan":
                stat_sets[sid]["spu"].add(name)

    def _unlinked_store_stats_from_sets(self, stat_sets):
        return {
            sid: {"spu_count": len(v["spu"]), "sku_count": len(v["sku"])}
            for sid, v in stat_sets.items()
        }

    def get_unlinked_pool_page(self, page=1, limit=30, search="", category3="", sort_store_id="", sort_order="desc",
                               filters_json=None, negative_sales_only=False):
        """
        Returns virtual-row aligned unlinked pool:
        - first column: main products (sales desc)
        - following columns: each store's unlinked products (sales desc)
        """
        if not self.active_project_id:
            return {"items": [], "total": 0, "page": page, "limit": limit, "pages": 0, "spu_count": 0, "store_stats": {}}

        page = max(1, int(page))
        limit = max(1, min(int(limit), 100))
        offset = (page - 1) * limit
        search_tokens = self._search_tokens(search)
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
                if search_tokens:
                    self._add_search_token_clauses(main_where, main_params, ["skuId", "商品名称", "规格名称"], search_tokens)
                if cat_like:
                    main_where.append("美团类目三级 LIKE ?")
                    main_params.append(cat_like)
                main_where_sql = " AND ".join(main_where)

                link_cnt = self._read_sql(
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
                    if search_tokens:
                        self._add_search_token_clauses(where, params, ["cp.skuId", "cp.商品名称", "cp.规格名称"], search_tokens)
                    if cat_like:
                        where.append("cp.美团类目三级 LIKE ?")
                        params.append(cat_like)
                    where_sql = " AND ".join(where)
                    sid_desc = "DESC"
                    if str(sort_store_id) == sid:
                        sid_desc = "ASC" if str(sort_order).lower() == "asc" else "DESC"
                    lim_sql = f"LIMIT {int(lim)} OFFSET {int(off)}" if lim is not None else ""
                    return self._read_sql(
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
                    main_df = self._read_sql(
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
                    store_stat_sets = {str(i): {"spu": set(), "sku": set()} for i in range(len(self.store_names))}
                    for idx in page_indices:
                        items.append(self._build_unlinked_virtual_row(idx, main_df, store_slices, link_map))
                    for idx in indices:
                        self._add_unlinked_store_stat_from_row(
                            store_stat_sets,
                            self._build_unlinked_virtual_row(idx, main_df, store_slices, link_map),
                        )
                    store_stats = self._unlinked_store_stats_from_sets(store_stat_sets)
                else:
                    main_count = conn.execute(f"SELECT COUNT(*) FROM main_products WHERE {main_where_sql}", tuple(main_params)).fetchone()[0]
                    main_df = self._read_sql(
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
                    store_stats = {}
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
                        if search_tokens:
                            self._add_search_token_clauses(where, params, ["cp.skuId", "cp.商品名称", "cp.规格名称"], search_tokens)
                        if cat_like:
                            where.append("cp.美团类目三级 LIKE ?")
                            params.append(cat_like)
                        where_sql = " AND ".join(where)
                        count = conn.execute(f"SELECT COUNT(*) FROM comp_products cp WHERE {where_sql}", tuple(params)).fetchone()[0]
                        store_counts.append(count)
                        stat_row = conn.execute(
                            f"""
                            SELECT
                                COUNT(DISTINCT NULLIF(TRIM(cp.商品名称), '')) AS spu_count,
                                COUNT(DISTINCT NULLIF(TRIM(cp.skuId), '')) AS sku_count
                            FROM comp_products cp
                            WHERE {where_sql}
                            """,
                            tuple(params),
                        ).fetchone()
                        store_stats[sid] = {
                            "spu_count": int(stat_row[0] or 0),
                            "sku_count": int(stat_row[1] or 0),
                        }
                        store_slices[sid] = load_store_slice(sid, limit, offset)

                    total = max([main_count] + store_counts) if (self.store_names or main_count) else 0
                    pages = (total + limit - 1) // limit if total else 0
                    spu_count = conn.execute(
                        f"SELECT COUNT(DISTINCT 商品名称) FROM main_products WHERE {main_where_sql}",
                        tuple(main_params)
                    ).fetchone()[0]
                    page_rows = max(0, min(limit, total - offset))
                    items = []
                    for idx in range(page_rows):
                        items.append(self._build_unlinked_virtual_row(idx, main_df, store_slices, link_map))
            finally:
                conn.close()

        summary = self.get_workbench_summary()
        summary_store_stats = {
            sid: (store.get("unlinked") or {})
            for sid, store in (summary.get("stores") or {}).items()
        }
        return {
            "items": items,
            "total": total,
            "page": page,
            "limit": limit,
            "pages": pages,
            "spu_count": spu_count,
            "store_stats": summary_store_stats or store_stats,
        }
