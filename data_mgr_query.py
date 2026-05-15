import json
import re
import pandas as pd
from data_mgr_query_unlinked import DataManagerUnlinkedQueryMixin
from utils import clean_text_value

class DataManagerQueryMixin(DataManagerUnlinkedQueryMixin):
    CATEGORY3_ALIASES = ("美团类目三级", "美团三级类目", "三级类目", "美团三级分类", "三级分类", "美团分类三级")

    def _statistics_category_series(self, df):
        if df is None or df.empty:
            return pd.Series([], dtype=str)
        for col in self.CATEGORY3_ALIASES:
            if col in df.columns:
                s = df[col].fillna("").map(clean_text_value).astype(str).str.strip()
                if (s[(s != "") & (s.str.lower() != "nan")]).any():
                    return s
        return pd.Series([""] * len(df), index=df.index, dtype=str)

    def _statistics_number(self, value):
        if value is None:
            return 0.0
        try:
            if pd.isna(value):
                return 0.0
        except (TypeError, ValueError):
            pass
        s = str(value).strip().replace(",", "").replace("￥", "").replace("¥", "")
        if s.endswith("%"):
            s = s[:-1]
        if s.lower() in ("", "nan", "none", "-"):
            return 0.0
        try:
            return float(s)
        except (TypeError, ValueError):
            return 0.0

    def _statistics_metric_pack(self, df):
        if df is None or df.empty or "商品名称" not in df.columns:
            return {"sales": 0.0, "sales_amount": 0.0, "spu": 0, "active_rate": 0.0}

        work = df.copy()
        work["__name"] = work["商品名称"].fillna("").astype(str).str.strip()
        work = work[(work["__name"] != "") & (work["__name"].str.lower() != "nan")]
        if work.empty:
            return {"sales": 0.0, "sales_amount": 0.0, "spu": 0, "active_rate": 0.0}

        work = work.drop_duplicates(subset=["__name"], keep="first")
        sales = work["销售"].apply(self._statistics_number) if "销售" in work.columns else pd.Series([], dtype=float)
        price = work["活动价"].apply(self._statistics_number) if "活动价" in work.columns else pd.Series([], dtype=float)
        spu = int(len(work))
        active = int((sales >= 1).sum()) if spu else 0
        return {
            "sales": float(sales.sum()) if not sales.empty else 0.0,
            "sales_amount": float((sales * price).sum()) if not sales.empty else 0.0,
            "spu": spu,
            "active_rate": round((active / spu * 100), 2) if spu else 0.0,
        }

    def get_statistics_products(self, category, source_type="main", store_id=""):
        category = str(category or "").strip()
        source_type = str(source_type or "main").strip()
        store_id = str(store_id or "").strip()
        if not self.active_project_id or not category:
            return {"items": [], "category": category, "source_type": source_type, "source_name": ""}

        with self._db_lock:
            conn = self._get_conn()
            try:
                if source_type == "competitor_unique":
                    df = pd.read_sql(
                        "SELECT * FROM comp_products WHERE project_id = ? AND store_id = ?",
                        conn,
                        params=(self.active_project_id, store_id),
                    )
                    source_name = self.store_names[int(store_id)] if store_id.isdigit() and int(store_id) < len(self.store_names) else "竞店"
                else:
                    df = pd.read_sql(
                        "SELECT * FROM main_products WHERE project_id = ? ORDER BY _row_orig_idx ASC",
                        conn,
                        params=(self.active_project_id,),
                    )
                    source_name = self.main_store_name or "主店"
            finally:
                conn.close()

        if df.empty:
            return {"items": [], "category": category, "source_type": source_type, "source_name": source_name}

        df["__cat3"] = self._statistics_category_series(df)
        df["__sales_num"] = df["销售"].apply(self._statistics_number) if "销售" in df.columns else 0
        df = df[(df["__cat3"] == category) & (df["__sales_num"] >= 1)].copy()
        if df.empty:
            return {"items": [], "category": category, "source_type": source_type, "source_name": source_name}

        def val(row, col):
            value = row.get(col, "")
            try:
                if pd.isna(value):
                    return ""
            except (TypeError, ValueError):
                pass
            return "" if value is None else str(value)

        items = []
        for _, row in df.iterrows():
            items.append({
                "image": val(row, "主图链接"),
                "name": val(row, "商品名称"),
                "spec": val(row, "规格名称"),
                "activity_price": val(row, "活动价"),
                "original_price": val(row, "原价"),
                "sales": val(row, "销售"),
            })

        return {
            "items": items,
            "category": category,
            "source_type": source_type,
            "source_name": source_name,
        }

    def get_statistics(self):
        if not self.active_project_id:
            return {"items": [], "tabs": [], "stores": [], "main_store": ""}

        with self._db_lock:
            conn = self._get_conn()
            try:
                main_df = pd.read_sql(
                    "SELECT * FROM main_products WHERE project_id = ? ORDER BY _row_orig_idx ASC",
                    conn,
                    params=(self.active_project_id,),
                )
                comp_df = pd.read_sql(
                    "SELECT * FROM comp_products WHERE project_id = ?",
                    conn,
                    params=(self.active_project_id,),
                )
            finally:
                conn.close()

        if main_df.empty:
            return {
                "items": [],
                "tabs": [],
                "stores": [{"id": str(i), "name": n} for i, n in enumerate(self.store_names)],
                "main_store": self.main_store_name or "主店",
            }

        main_df["__cat3"] = self._statistics_category_series(main_df)
        main_df = main_df[(main_df["__cat3"] != "") & (main_df["__cat3"].str.lower() != "nan")]
        if not comp_df.empty:
            comp_df["__cat3"] = self._statistics_category_series(comp_df)
            if "store_id" in comp_df.columns:
                comp_df["store_id"] = comp_df["store_id"].astype(str)
        else:
            comp_df["__cat3"] = ""

        store_count = 1 + len(self.store_names)
        metric_keys = ["sales", "sales_amount", "spu", "active_rate"]
        main_categories = list(dict.fromkeys(main_df["__cat3"].tolist()))
        main_category_set = set(main_categories)

        def empty_totals():
            return {"sales": 0.0, "sales_amount": 0.0, "active_count": 0.0, "spu": 0.0}

        def add_totals(totals, metrics):
            totals["sales"] += metrics["sales"]
            totals["sales_amount"] += metrics["sales_amount"]
            totals["spu"] += metrics["spu"]
            totals["active_count"] += round(metrics["active_rate"] * metrics["spu"] / 100) if metrics["spu"] else 0

        def finish_totals(totals):
            return {
                "sales": round(totals["sales"], 2),
                "sales_amount": round(totals["sales_amount"], 2),
                "active_rate": round((totals["active_count"] / totals["spu"] * 100), 2) if totals["spu"] else 0.0,
            }

        def build_item(cat, subject_metrics, include_competitors=True):
            actual_main_metrics = self._statistics_metric_pack(main_df[main_df["__cat3"] == cat])
            comp_rows = []
            comp_totals = {k: 0.0 for k in metric_keys}

            for i, store_name in enumerate(self.store_names):
                sid = str(i)
                sdf = comp_df[(comp_df.get("store_id", "") == sid) & (comp_df["__cat3"] == cat)] if not comp_df.empty else comp_df
                metrics = self._statistics_metric_pack(sdf)
                for key in metric_keys:
                    comp_totals[key] += metrics[key]
                if include_competitors:
                    comp_rows.append({
                        "store_id": sid,
                        "store_name": store_name,
                        "metrics": metrics,
                        "diff": {key: round(subject_metrics[key] - metrics[key], 2) for key in metric_keys},
                        "main_diff": {key: round(subject_metrics[key] - metrics[key], 2) for key in metric_keys},
                    })

            summary = {}
            for key in metric_keys:
                industry_avg = (actual_main_metrics[key] + comp_totals[key]) / store_count if store_count else 0
                avg_diff = subject_metrics[key] - industry_avg
                summary[key] = {
                    "main": round(subject_metrics[key], 2),
                    "industry_avg": round(industry_avg, 2),
                    "avg_diff": round(avg_diff, 2),
                }
            for comp in comp_rows:
                metrics = comp.get("metrics") or {}
                comp["market_diff"] = {
                    key: round(metrics.get(key, 0) - summary[key]["industry_avg"], 2)
                    for key in metric_keys
                }

            return {
                "category": cat,
                "summary": summary,
                "competitors": comp_rows,
            }

        def add_contribution(items, subject_total_sales_amount=None, industry_total_sales_amount=None):
            total_subject_sales_amount = subject_total_sales_amount
            if total_subject_sales_amount is None:
                total_subject_sales_amount = sum(
                    (item.get("summary") or {}).get("sales_amount", {}).get("main", 0)
                    for item in items
                )
            total_industry_sales_amount = industry_total_sales_amount
            if total_industry_sales_amount is None:
                total_industry_sales_amount = sum(
                    (item.get("summary") or {}).get("sales_amount", {}).get("industry_avg", 0)
                    for item in items
                )

            for item in items:
                summary = item.get("summary") or {}
                sales_amount = summary.get("sales_amount") or {}
                category_sales_amount_total = sales_amount.get("main", 0) + sum(
                    ((comp.get("metrics") or {}).get("sales_amount", 0))
                    for comp in item.get("competitors", [])
                )
                subject_contribution = (
                    sales_amount.get("main", 0) / total_subject_sales_amount * 100
                    if total_subject_sales_amount else 0
                )
                average_contribution = (
                    sales_amount.get("industry_avg", 0) / total_industry_sales_amount * 100
                    if total_industry_sales_amount else 0
                )
                summary["category_contribution"] = {
                    "main": round(subject_contribution, 2),
                    "industry_avg": round(average_contribution, 2),
                    "avg_diff": round(subject_contribution - average_contribution, 2),
                }
                for comp in item.get("competitors", []):
                    metrics = comp.get("metrics") or {}
                    comp_contribution = (
                        metrics.get("sales_amount", 0) / category_sales_amount_total * 100
                        if category_sales_amount_total else 0
                    )
                    metrics["category_contribution"] = round(comp_contribution, 2)
                    comp.setdefault("main_diff", {})["category_contribution"] = round(subject_contribution - comp_contribution, 2)
                    comp.setdefault("market_diff", {})["category_contribution"] = round(comp_contribution - average_contribution, 2)

        def sum_subject_sales_for_store(store_id, categories):
            total = 0.0
            for cat in categories:
                sdf = comp_df[(comp_df.get("store_id", "") == store_id) & (comp_df["__cat3"] == cat)] if not comp_df.empty else comp_df
                total += self._statistics_metric_pack(sdf)["sales_amount"]
            return total

        def sum_industry_sales_for_categories(categories):
            total = 0.0
            for cat in categories:
                main_metrics = self._statistics_metric_pack(main_df[main_df["__cat3"] == cat])
                comp_total = 0.0
                for i in range(len(self.store_names)):
                    sid = str(i)
                    sdf = comp_df[(comp_df.get("store_id", "") == sid) & (comp_df["__cat3"] == cat)] if not comp_df.empty else comp_df
                    comp_total += self._statistics_metric_pack(sdf)["sales_amount"]
                total += (main_metrics["sales_amount"] + comp_total) / store_count if store_count else 0.0
            return total

        main_items = []
        main_totals = empty_totals()
        for cat in main_categories:
            main_metrics = self._statistics_metric_pack(main_df[main_df["__cat3"] == cat])
            add_totals(main_totals, main_metrics)
            main_items.append(build_item(cat, main_metrics, include_competitors=True))
        add_contribution(main_items)

        tabs = [{
            "id": "main",
            "label": self.main_store_name or "主店",
            "source_type": "main",
            "source_store_id": "",
            "source_name": self.main_store_name or "主店",
            "items": main_items,
            "totals": finish_totals(main_totals),
        }]

        for i, store_name in enumerate(self.store_names):
            sid = str(i)
            if comp_df.empty:
                store_categories = []
            else:
                sdf = comp_df[comp_df.get("store_id", "") == sid]
                store_categories = list(dict.fromkeys(sdf["__cat3"].tolist()))
            unique_categories = [cat for cat in store_categories if cat and cat not in main_category_set]
            unique_items = []
            unique_totals = empty_totals()
            for cat in unique_categories:
                subject_df = comp_df[(comp_df.get("store_id", "") == sid) & (comp_df["__cat3"] == cat)]
                subject_metrics = self._statistics_metric_pack(subject_df)
                add_totals(unique_totals, subject_metrics)
                unique_items.append(build_item(cat, subject_metrics, include_competitors=False))
            add_contribution(
                unique_items,
                subject_total_sales_amount=sum_subject_sales_for_store(sid, store_categories),
                industry_total_sales_amount=sum_industry_sales_for_categories(store_categories),
            )
            tabs.append({
                "id": f"comp-{sid}",
                "label": f"{store_name}独有",
                "source_type": "competitor_unique",
                "source_store_id": sid,
                "source_name": store_name,
                "items": unique_items,
                "totals": finish_totals(unique_totals),
            })

        return {
            "items": main_items,
            "tabs": tabs,
            "main_totals": finish_totals(main_totals),
            "stores": [{"id": str(i), "name": n} for i, n in enumerate(self.store_names)],
            "main_store": self.main_store_name or "主店",
        }

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

                # 每店对主行最多一条；若历史错误导入产生多条同 main，避免 merge 时一对多把主表行数放大
                if "main_sku_id" in merged_comp.columns and not merged_comp.empty:
                    merged_comp = merged_comp.drop_duplicates(subset=["main_sku_id"], keep="first")
                
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

    def _eliminated_grid_mask(self, df):
        """已标记淘汰的主店商品行。"""
        if df is None or df.empty:
            return pd.Series(False, index=df.index if df is not None else None)
        mask = pd.Series(False, index=df.index)
        if '淘汰标记' in df.columns:
            mask |= df['淘汰标记'].fillna('').astype(str).str.strip() == '1'
        if '是否淘汰' in df.columns:
            mask |= df['是否淘汰'].fillna('').astype(str).str.strip() == '是'
        return mask

    def _fully_eliminated_spu_count(self, df, eliminated_mask):
        """同一 SPU 下的 SKU 全部淘汰时，SPU 才计入淘汰数。"""
        if df is None or df.empty or '商品名称' not in df.columns:
            return 0
        tmp = df[['商品名称']].copy()
        tmp['商品名称'] = tmp['商品名称'].astype(str).str.strip()
        tmp = tmp[(tmp['商品名称'] != '') & (tmp['商品名称'].str.lower() != 'nan')]
        if tmp.empty:
            return 0
        tmp['__eliminated'] = eliminated_mask.reindex(tmp.index).fillna(False).astype(bool)
        return int(tmp.groupby('商品名称')['__eliminated'].all().sum())

    def get_grid_data(self):
        if self.grid_df is None or self.grid_df.empty: return {"items": [], "total": 0}
        # Backward compatibility for old calls, but returning paginated for safety
        return self.get_paginated_grid(page=1, limit=50)

    def _grid_filter_col_mask(self, df, col, needle):
        """Substring match on the main-store grid column selected in the filter popup."""
        needle = str(needle).strip().lower()
        if not needle:
            return pd.Series(True, index=df.index)
        if col in df.columns:
            return df[col].astype(str).str.lower().str.contains(needle, regex=False, na=False)
        return pd.Series(True, index=df.index)

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
            return {
                "items": [], "total": 0, "page": page, "pages": 0,
                "sku_count": 0, "sku_eliminated_count": 0,
                "spu_count": 0, "spu_eliminated_count": 0,
            }

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

        eliminated_mask = self._eliminated_grid_mask(df)
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
            "sku_count": total,
            "sku_eliminated_count": int(eliminated_mask.sum()),
            "spu_count": self._spu_count_from_grid_df(df),
            "spu_eliminated_count": self._fully_eliminated_spu_count(df, eliminated_mask),
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


    def get_main_products_page(self, page=1, limit=50, search=""):
        if not self.active_project_id:
            return {"items": [], "total": 0, "page": page, "limit": limit, "pages": 0}
        page = max(1, int(page))
        like = f"%{search.strip()}%" if search else None
        has_search = like is not None
        if not has_search:
            limit = max(1, min(int(limit), 100))
        offset = (page - 1) * limit
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
                limit_clause = "" if has_search else "LIMIT ? OFFSET ?"
                query_params = tuple(params) if has_search else tuple(params + [limit, offset])
                df = pd.read_sql(
                    f"""
                    SELECT skuId, 商品名称, 规格名称, 主图链接, 活动价, 原价, 销售, 美团类目三级, _row_orig_idx
                    FROM main_products
                    WHERE {where_sql}
                    ORDER BY CAST(COALESCE(NULLIF(销售, ''), '0') AS REAL) DESC, _row_orig_idx ASC
                    {limit_clause}
                    """,
                    conn,
                    params=query_params
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
