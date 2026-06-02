import json
import os
import re
import sys
import threading
import time
from difflib import SequenceMatcher
import pandas as pd
import post_match_engine
from data_mgr_query_unlinked import DataManagerUnlinkedQueryMixin
from utils import clean_text_value

class DataManagerQueryMixin(DataManagerUnlinkedQueryMixin):
    ANALYSIS_SNAPSHOT_VERSION = "2026-05-19.3"
    CATEGORY1_ALIASES = ("美团类目一级", "美团一级类目", "一级类目", "美团一级分类", "一级分类", "美团分类一级")
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

    def _statistics_category1_series(self, df):
        if df is None or df.empty:
            return pd.Series([], dtype=str)
        for col in self.CATEGORY1_ALIASES:
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

    def _statistics_effective_price_series(self, df):
        if df is None or df.empty:
            return pd.Series([], dtype=float)

        primary_col = "活动价" if "活动价" in df.columns else None
        fallback_col = None
        for col in ("美团外卖渠道售价", "原价", "渠道价格", "渠道价"):
            if col in df.columns:
                fallback_col = col
                break

        if primary_col is None and fallback_col is None:
            return pd.Series([0.0] * len(df), index=df.index, dtype=float)

        fallback = (
            df[fallback_col].apply(self._statistics_number)
            if fallback_col
            else pd.Series([0.0] * len(df), index=df.index, dtype=float)
        )
        if primary_col is None:
            return fallback.astype(float)

        primary_raw = df[primary_col].fillna("").map(clean_text_value).astype(str).str.strip()
        has_primary = ~primary_raw.str.lower().isin(("", "nan", "none"))
        primary = df[primary_col].apply(self._statistics_number)
        result = fallback.copy()
        result.loc[has_primary] = primary.loc[has_primary]
        return result.astype(float)

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
        price = self._statistics_effective_price_series(work)
        spu = int(len(work))
        active = int((sales >= 1).sum()) if spu else 0
        return {
            "sales": float(sales.sum()) if not sales.empty else 0.0,
            "sales_amount": float((sales * price).sum()) if not sales.empty else 0.0,
            "spu": spu,
            "active_rate": round((active / spu * 100), 2) if spu else 0.0,
        }

    def _market_category_buckets_path(self):
        rel = os.path.join("data", "market_category_buckets.json")
        candidates = [
            os.path.join(getattr(sys, "_MEIPASS", ""), rel),
            os.path.join(os.path.dirname(os.path.abspath(__file__)), rel),
            os.path.join(getattr(self, "base_dir", ""), rel),
        ]
        for path in candidates:
            if path and os.path.isfile(path):
                return path
        return ""

    def _load_market_category_buckets(self):
        path = self._market_category_buckets_path()
        if not path:
            return {"snack": set(), "department_store": set(), "other": set()}
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return {"snack": set(), "department_store": set(), "other": set()}
        buckets = data.get("buckets") if isinstance(data, dict) else {}
        return {
            "snack": {str(v).strip() for v in buckets.get("snack", []) if str(v).strip()},
            "department_store": {str(v).strip() for v in buckets.get("department_store", []) if str(v).strip()},
            "other": {str(v).strip() for v in buckets.get("other", []) if str(v).strip()},
        }

    def _market_bucket_for_l1(self, l1, buckets):
        name = str(l1 or "").strip()
        if name in buckets.get("snack", set()):
            return "snack"
        if name in buckets.get("department_store", set()):
            return "department_store"
        if name in buckets.get("other", set()):
            return "other"
        return "other"

    def _market_level_for_sales(self, sales_amount):
        value = float(sales_amount or 0)
        if value >= 400000:
            return "夯"
        if value >= 300000:
            return "顶级"
        if value >= 200000:
            return "人上人"
        if value >= 100000:
            return "NPC"
        return "拉完了"

    def _market_prepare_file_df(self, df, file_id, file_name, buckets):
        if df is None or df.empty or "商品名称" not in df.columns:
            return pd.DataFrame(columns=["file_id", "file_name", "name", "category_l1", "bucket", "sales", "price", "sales_amount"])

        work = df.copy()
        work["name"] = work["商品名称"].fillna("").map(clean_text_value).astype(str).str.strip()
        work = work[(work["name"] != "") & (work["name"].str.lower() != "nan")]
        if work.empty:
            return pd.DataFrame(columns=["file_id", "file_name", "name", "category_l1", "bucket", "sales", "price", "sales_amount"])

        work = work.drop_duplicates(subset=["name"], keep="first").copy()
        work["category_l1"] = self._statistics_category1_series(work).reindex(work.index).fillna("").astype(str).str.strip()
        work["sales"] = work["销售"].apply(self._statistics_number) if "销售" in work.columns else 0.0
        work["price"] = self._statistics_effective_price_series(work)
        work["sales_amount"] = work["sales"] * work["price"]
        work["bucket"] = work["category_l1"].map(lambda v: self._market_bucket_for_l1(v, buckets))
        work["file_id"] = file_id
        work["file_name"] = file_name
        return work[["file_id", "file_name", "name", "category_l1", "bucket", "sales", "price", "sales_amount"]]

    def _market_metric_pack(self, sales, sales_amount):
        sales = float(sales or 0)
        sales_amount = float(sales_amount or 0)
        monthly_orders = sales / 4.5 if sales else 0.0
        daily_orders = monthly_orders / 30 if monthly_orders else 0.0
        customer_unit_price = sales_amount / monthly_orders if monthly_orders else 0.0
        gross_profit = sales_amount * 0.22
        return {
            "daily_orders": round(daily_orders, 2),
            "monthly_orders": round(monthly_orders, 2),
            "monthly_sales_amount": round(sales_amount, 2),
            "customer_unit_price": round(customer_unit_price, 2),
            "estimated_gross_profit": round(gross_profit, 2),
        }

    def _snapshot_empty_statistics(self):
        return {"items": [], "tabs": [], "stores": [], "main_store": ""}

    def _snapshot_empty_market_analysis(self):
        return {
            "status": "error",
            "message": "No active project",
            "file_count": 0,
            "top10_categories": [],
            "recommendation": {},
            "metrics": {"average": {}, "top1": {}},
            "metric_diffs": {},
            "mode_options": ["average", "top1"],
        }

    def _snapshot_empty_workbench_summary(self):
        stores = {}
        for i, name in enumerate(getattr(self, "store_names", []) or []):
            stores[str(i)] = {
                "name": name,
                "linked": {"spu_count": 0, "sku_count": 0},
                "unlinked": {"spu_count": 0, "sku_count": 0},
                "total": {"spu_count": 0, "sku_count": 0},
            }
        return {
            "project_id": self.active_project_id,
            "main": {"spu_count": 0, "sku_count": 0},
            "stores": stores,
        }

    def _snapshot_build_state(self):
        if not hasattr(self, "_analysis_snapshot_building"):
            self._analysis_snapshot_building = set()
        if not hasattr(self, "_analysis_snapshot_build_lock"):
            self._analysis_snapshot_build_lock = threading.Lock()
        if not hasattr(self, "_analysis_snapshot_build_reason"):
            self._analysis_snapshot_build_reason = {}
        return self._analysis_snapshot_building, self._analysis_snapshot_build_lock, self._analysis_snapshot_build_reason

    def _project_store_names(self, project_id=None):
        pid = project_id or self.active_project_id
        if not pid:
            return []
        if pid == self.active_project_id and getattr(self, "store_names", None):
            return list(self.store_names)
        with self._db_lock:
            conn = self._get_conn()
            try:
                rows = conn.execute(
                    """
                    SELECT store_name
                    FROM project_files
                    WHERE project_id = ? AND type = 'comp'
                    ORDER BY id ASC
                    """,
                    (pid,),
                ).fetchall()
            finally:
                conn.close()
        return [str(row[0] or "") for row in rows]

    def _read_analysis_snapshot(self, ready_only=True, include_data=True, project_id=None):
        pid = project_id or self.active_project_id
        if not pid:
            return None
        with self._db_lock:
            conn = self._get_conn()
            try:
                if not include_data:
                    if ready_only:
                        row = conn.execute(
                            """
                            SELECT computed_at, version, status, error_message
                            FROM project_analysis_snapshots
                            WHERE project_id = ? AND version = ? AND status = 'ready'
                            """,
                            (pid, self.ANALYSIS_SNAPSHOT_VERSION),
                        ).fetchone()
                    else:
                        row = conn.execute(
                            """
                            SELECT computed_at, version, status, error_message
                            FROM project_analysis_snapshots
                            WHERE project_id = ?
                            """,
                            (pid,),
                        ).fetchone()
                    if not row:
                        return None
                    return {
                        "statistics": {},
                        "market_analysis": {},
                        "workbench_summary": {},
                        "computed_at": row[0] or "",
                        "version": row[1] or "",
                        "status": row[2] or "",
                        "error_message": row[3] or "",
                    }
                if ready_only:
                    row = conn.execute(
                        """
                        SELECT statistics_json, market_analysis_json, workbench_summary_json, computed_at, version, status, error_message
                        FROM project_analysis_snapshots
                        WHERE project_id = ? AND version = ? AND status = 'ready'
                        """,
                        (pid, self.ANALYSIS_SNAPSHOT_VERSION),
                    ).fetchone()
                else:
                    row = conn.execute(
                        """
                        SELECT statistics_json, market_analysis_json, workbench_summary_json, computed_at, version, status, error_message
                        FROM project_analysis_snapshots
                        WHERE project_id = ?
                        """,
                        (pid,),
                    ).fetchone()
            finally:
                conn.close()
        if not row:
            return None
        try:
            return {
                "statistics": json.loads(row[0] or "{}"),
                "market_analysis": json.loads(row[1] or "{}"),
                "workbench_summary": json.loads(row[2] or "{}"),
                "computed_at": row[3] or "",
                "version": row[4] or "",
                "status": row[5] or "",
                "error_message": row[6] or "",
            }
        except Exception:
            return {
                "statistics": self._snapshot_empty_statistics(),
                "market_analysis": self._snapshot_empty_market_analysis(),
                "workbench_summary": self._snapshot_empty_workbench_summary(),
                "computed_at": row[3] or "",
                "version": row[4] or "",
                "status": "error",
                "error_message": "统计快照解析失败，请点击刷新重建",
            }

    def invalidate_analysis_snapshot(self, project_id=None):
        pid = project_id or self.active_project_id
        if not pid:
            return
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    conn.execute(
                        """
                        UPDATE project_analysis_snapshots
                        SET status = 'stale',
                            error_message = ''
                        WHERE project_id = ?
                        """,
                        (pid,),
                    )
            finally:
                conn.close()

    def _snapshot_meta(self, snapshot=None, project_id=None):
        pid = project_id or self.active_project_id
        if not pid:
            return {"status": "missing", "computed_at": "", "version": self.ANALYSIS_SNAPSHOT_VERSION, "message": "No active project"}

        building, build_lock, build_reason = self._snapshot_build_state()
        is_building = False
        reason = ""
        with build_lock:
            is_building = pid in building
            reason = build_reason.get(pid, "")

        snapshot = snapshot if snapshot is not None else self._read_analysis_snapshot(ready_only=False, include_data=False, project_id=pid)
        if not snapshot:
            return {
                "status": "building" if is_building else "missing",
                "computed_at": "",
                "version": self.ANALYSIS_SNAPSHOT_VERSION,
                "message": "正在刷新数据分析" if is_building else "暂无统计快照",
            }

        status = snapshot.get("status") or "missing"
        if snapshot.get("version") != self.ANALYSIS_SNAPSHOT_VERSION and status == "ready":
            status = "stale"
        if is_building and (not snapshot.get("computed_at") or reason == "refresh"):
            status = "building"
        messages = {
            "ready": "统计数据已更新",
            "stale": "统计数据待刷新",
            "building": "正在刷新数据分析",
            "error": snapshot.get("error_message") or "统计刷新失败",
        }
        return {
            "status": status,
            "computed_at": snapshot.get("computed_at") or "",
            "version": snapshot.get("version") or self.ANALYSIS_SNAPSHOT_VERSION,
            "message": messages.get(status, "暂无统计快照"),
        }

    def get_analysis_snapshot_status(self, project_id=None):
        pid = project_id or self.active_project_id
        snapshot = self._read_analysis_snapshot(ready_only=False, include_data=False, project_id=pid)
        return self._snapshot_meta(snapshot, project_id=pid)

    def _attach_snapshot_meta(self, data, snapshot=None, project_id=None):
        if not isinstance(data, dict):
            return data
        meta = self._snapshot_meta(snapshot, project_id=project_id)
        out = dict(data)
        out["snapshot_status"] = meta["status"]
        out["snapshot_computed_at"] = meta["computed_at"]
        out["snapshot_version"] = meta["version"]
        out["snapshot_message"] = meta["message"]
        return out

    def ensure_analysis_snapshot_async(self, force=False):
        pid = self.active_project_id
        if not pid:
            return False
        if not force:
            ready = self._read_analysis_snapshot(ready_only=True, include_data=False)
            if ready:
                return False
        building, build_lock, build_reason = self._snapshot_build_state()
        with build_lock:
            if pid in building:
                return False
            building.add(pid)
            build_reason[pid] = "refresh" if force else "initial"

        def _run():
            try:
                if self.active_project_id != pid:
                    return
                self.rebuild_analysis_snapshot()
            except Exception:
                import traceback
                traceback.print_exc()
            finally:
                with build_lock:
                    building.discard(pid)
                    build_reason.pop(pid, None)

        threading.Thread(target=_run, daemon=True).start()
        return True

    def rebuild_analysis_snapshot(self):
        if not self.active_project_id:
            return {"statistics": self._snapshot_empty_statistics(), "market_analysis": self._snapshot_empty_market_analysis()}

        computed_at = time.strftime("%Y-%m-%d %H:%M:%S")
        try:
            statistics = self._compute_statistics()
            market_analysis = self._compute_market_analysis()
            workbench_summary = self._compute_workbench_summary()
            statistics_json = json.dumps(statistics, ensure_ascii=False, separators=(",", ":"))
            market_json = json.dumps(market_analysis, ensure_ascii=False, separators=(",", ":"))
            workbench_json = json.dumps(workbench_summary, ensure_ascii=False, separators=(",", ":"))
            status = "ready"
            error_message = ""
        except Exception as exc:
            statistics = self._snapshot_empty_statistics()
            market_analysis = self._snapshot_empty_market_analysis()
            workbench_summary = self._snapshot_empty_workbench_summary()
            statistics_json = json.dumps(statistics, ensure_ascii=False, separators=(",", ":"))
            market_json = json.dumps(market_analysis, ensure_ascii=False, separators=(",", ":"))
            workbench_json = json.dumps(workbench_summary, ensure_ascii=False, separators=(",", ":"))
            status = "error"
            error_message = str(exc)

        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    conn.execute(
                        """
                        INSERT INTO project_analysis_snapshots
                            (project_id, statistics_json, market_analysis_json, workbench_summary_json, computed_at, version, status, error_message)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(project_id) DO UPDATE SET
                            statistics_json = excluded.statistics_json,
                            market_analysis_json = excluded.market_analysis_json,
                            workbench_summary_json = excluded.workbench_summary_json,
                            computed_at = excluded.computed_at,
                            version = excluded.version,
                            status = excluded.status,
                            error_message = excluded.error_message
                        """,
                        (
                            self.active_project_id,
                            statistics_json,
                            market_json,
                            workbench_json,
                            computed_at,
                            self.ANALYSIS_SNAPSHOT_VERSION,
                            status,
                            error_message,
                        ),
                    )
            finally:
                conn.close()
        if status != "ready":
            raise RuntimeError(error_message)
        return {"statistics": statistics, "market_analysis": market_analysis, "workbench_summary": workbench_summary}

    def get_statistics(self, refresh=False, sync_missing=False, project_id=None):
        pid = project_id or self.active_project_id
        if not pid:
            return self._attach_snapshot_meta(self._snapshot_empty_statistics(), project_id=pid)
        if pid != self.active_project_id:
            snapshot = self._read_analysis_snapshot(ready_only=not refresh, project_id=pid)
            if snapshot and (not refresh or snapshot.get("status") in ("ready", "stale")):
                return self._attach_snapshot_meta(snapshot.get("statistics") or self._snapshot_empty_statistics(), snapshot, project_id=pid)
            snapshot_meta = self._read_analysis_snapshot(ready_only=False, include_data=False, project_id=pid)
            return self._attach_snapshot_meta(self._snapshot_empty_statistics(), snapshot_meta, project_id=pid)
        if refresh:
            self.ensure_analysis_snapshot_async(force=True)
            snapshot = self._read_analysis_snapshot(ready_only=False, include_data=False)
            if snapshot and snapshot.get("status") in ("ready", "stale"):
                stale_snapshot = self._read_analysis_snapshot(ready_only=False)
                if stale_snapshot and stale_snapshot.get("statistics"):
                    return self._attach_snapshot_meta(stale_snapshot["statistics"], stale_snapshot, project_id=pid)
            return self._attach_snapshot_meta(self._snapshot_empty_statistics(), snapshot, project_id=pid)
        snapshot = self._read_analysis_snapshot(ready_only=True)
        if snapshot:
            return self._attach_snapshot_meta(snapshot["statistics"], snapshot, project_id=pid)
        if sync_missing:
            snapshot_data = self.rebuild_analysis_snapshot()
            snapshot = self._read_analysis_snapshot(ready_only=True)
            return self._attach_snapshot_meta(snapshot_data["statistics"], snapshot, project_id=pid)
        snapshot = self._read_analysis_snapshot(ready_only=False, include_data=False)
        if snapshot and snapshot.get("status") in ("ready", "stale"):
            stale_snapshot = self._read_analysis_snapshot(ready_only=False)
            if stale_snapshot and stale_snapshot.get("statistics"):
                    return self._attach_snapshot_meta(stale_snapshot["statistics"], stale_snapshot, project_id=pid)
        if not snapshot:
            self.ensure_analysis_snapshot_async()
            snapshot = self._read_analysis_snapshot(ready_only=False, include_data=False)
        return self._attach_snapshot_meta(self._snapshot_empty_statistics(), snapshot, project_id=pid)

    def get_market_analysis(self, refresh=False, sync_missing=False, project_id=None):
        pid = project_id or self.active_project_id
        if not pid:
            return self._attach_snapshot_meta(self._snapshot_empty_market_analysis(), project_id=pid)
        if pid != self.active_project_id:
            snapshot = self._read_analysis_snapshot(ready_only=not refresh, project_id=pid)
            if snapshot and (not refresh or snapshot.get("status") in ("ready", "stale")):
                return self._attach_snapshot_meta(snapshot.get("market_analysis") or self._snapshot_empty_market_analysis(), snapshot, project_id=pid)
            snapshot_meta = self._read_analysis_snapshot(ready_only=False, include_data=False, project_id=pid)
            return self._attach_snapshot_meta(self._snapshot_empty_market_analysis(), snapshot_meta, project_id=pid)
        if refresh:
            self.ensure_analysis_snapshot_async(force=True)
            snapshot = self._read_analysis_snapshot(ready_only=False, include_data=False)
            if snapshot and snapshot.get("status") in ("ready", "stale"):
                stale_snapshot = self._read_analysis_snapshot(ready_only=False)
                if stale_snapshot and stale_snapshot.get("market_analysis"):
                    return self._attach_snapshot_meta(stale_snapshot["market_analysis"], stale_snapshot, project_id=pid)
            return self._attach_snapshot_meta(self._snapshot_empty_market_analysis(), snapshot, project_id=pid)
        snapshot = self._read_analysis_snapshot(ready_only=True)
        if snapshot:
            return self._attach_snapshot_meta(snapshot["market_analysis"], snapshot, project_id=pid)
        if sync_missing:
            snapshot_data = self.rebuild_analysis_snapshot()
            snapshot = self._read_analysis_snapshot(ready_only=True)
            return self._attach_snapshot_meta(snapshot_data["market_analysis"], snapshot, project_id=pid)
        snapshot = self._read_analysis_snapshot(ready_only=False, include_data=False)
        if snapshot and snapshot.get("status") in ("ready", "stale"):
            stale_snapshot = self._read_analysis_snapshot(ready_only=False)
            if stale_snapshot and stale_snapshot.get("market_analysis"):
                    return self._attach_snapshot_meta(stale_snapshot["market_analysis"], stale_snapshot, project_id=pid)
        if not snapshot:
            self.ensure_analysis_snapshot_async()
            snapshot = self._read_analysis_snapshot(ready_only=False, include_data=False)
        return self._attach_snapshot_meta(self._snapshot_empty_market_analysis(), snapshot, project_id=pid)

    def get_workbench_summary(self, refresh=False):
        if not self.active_project_id:
            return self._snapshot_empty_workbench_summary()
        snapshot = None if refresh else self._read_analysis_snapshot(ready_only=False)
        if snapshot:
            return snapshot.get("workbench_summary") or self._snapshot_empty_workbench_summary()
        if refresh:
            self.ensure_analysis_snapshot_async(force=True)
        return self._snapshot_empty_workbench_summary()

    def _compute_workbench_summary(self):
        """Project-level counts used by the workbench header; kept out of page queries."""
        if not self.active_project_id:
            return self._snapshot_empty_workbench_summary()

        def _count_pack(row):
            if not row:
                return {"spu_count": 0, "sku_count": 0}
            return {"spu_count": int(row[0] or 0), "sku_count": int(row[1] or 0)}

        with self._db_lock:
            conn = self._get_conn()
            try:
                main = _count_pack(conn.execute(
                    """
                    SELECT
                        COUNT(DISTINCT NULLIF(TRIM(商品名称), '')) AS spu_count,
                        COUNT(*) AS sku_count
                    FROM main_products
                    WHERE project_id = ?
                    """,
                    (self.active_project_id,),
                ).fetchone())

                stores = {}
                for i, name in enumerate(getattr(self, "store_names", []) or []):
                    sid = str(i)
                    total = _count_pack(conn.execute(
                        """
                        SELECT
                            COUNT(DISTINCT NULLIF(TRIM(商品名称), '')) AS spu_count,
                            COUNT(DISTINCT NULLIF(TRIM(skuId), '')) AS sku_count
                        FROM comp_products
                        WHERE project_id = ? AND store_id = ?
                        """,
                        (self.active_project_id, sid),
                    ).fetchone())
                    linked = _count_pack(conn.execute(
                        """
                        SELECT
                            COUNT(DISTINCT NULLIF(TRIM(cp.商品名称), '')) AS spu_count,
                            COUNT(DISTINCT NULLIF(TRIM(pl.comp_sku_id), '')) AS sku_count
                        FROM product_links pl
                        LEFT JOIN comp_products cp
                          ON cp.project_id = pl.project_id
                         AND cp.store_id = pl.store_id
                         AND cp.skuId = pl.comp_sku_id
                        WHERE pl.project_id = ? AND pl.store_id = ?
                        """,
                        (self.active_project_id, sid),
                    ).fetchone())
                    unlinked = _count_pack(conn.execute(
                        """
                        SELECT
                            COUNT(DISTINCT NULLIF(TRIM(cp.商品名称), '')) AS spu_count,
                            COUNT(DISTINCT NULLIF(TRIM(cp.skuId), '')) AS sku_count
                        FROM comp_products cp
                        WHERE cp.project_id = ? AND cp.store_id = ?
                          AND NOT EXISTS (
                              SELECT 1
                              FROM product_links pl
                              WHERE pl.project_id = cp.project_id
                                AND pl.store_id = cp.store_id
                                AND pl.comp_sku_id = cp.skuId
                          )
                        """,
                        (self.active_project_id, sid),
                    ).fetchone())
                    stores[sid] = {
                        "name": name,
                        "linked": linked,
                        "unlinked": unlinked,
                        "total": total,
                    }
            finally:
                conn.close()

        return {
            "project_id": self.active_project_id,
            "main": main,
            "stores": stores,
        }

    def refresh_workbench_summary_snapshot(self):
        if not self.active_project_id:
            return self._snapshot_empty_workbench_summary()
        summary = self._compute_workbench_summary()
        summary_json = json.dumps(summary, ensure_ascii=False, separators=(",", ":"))
        computed_at = time.strftime("%Y-%m-%d %H:%M:%S")
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    conn.execute(
                        """
                        UPDATE project_analysis_snapshots
                        SET workbench_summary_json = ?,
                            computed_at = ?
                        WHERE project_id = ?
                        """,
                        (summary_json, computed_at, self.active_project_id),
                    )
            finally:
                conn.close()
        return summary

    def get_statistics_page(
        self,
        refresh=False,
        tab_id="main",
        page=1,
        limit=20,
        search="",
        sort_key="",
        sort_order="desc",
        project_id=None,
    ):
        data = self.get_statistics(refresh=refresh, project_id=project_id)
        tabs = data.get("tabs") or []
        if not tabs:
            return data
        tab_id = str(tab_id or "main")
        active = next((tab for tab in tabs if str(tab.get("id")) == tab_id), tabs[0])
        items = list(active.get("items") or [])
        q = str(search or "").strip().lower()
        if q:
            items = [item for item in items if q in str(item.get("category") or "").lower()]

        sort_key = str(sort_key or "").strip()
        if sort_key:
            reverse = str(sort_order or "desc").lower() != "asc"

            def _sort_value(item):
                try:
                    return float(((item.get("summary") or {}).get(sort_key) or {}).get("avg_diff") or 0)
                except (TypeError, ValueError):
                    return 0.0

            items = sorted(items, key=lambda item: (_sort_value(item), str(item.get("category") or "")), reverse=reverse)

        total = len(items)
        limit = max(1, min(int(limit or 20), 100))
        pages = max(1, (total + limit - 1) // limit)
        page = min(max(1, int(page or 1)), pages)
        start = (page - 1) * limit
        page_items = items[start:start + limit]

        slim_tabs = []
        main_categories = []
        for tab in tabs:
            tab_items = tab.get("items") or []
            if tab.get("source_type") == "main":
                main_categories = [item.get("category") for item in tab_items if item.get("category")]
            slim = {k: v for k, v in tab.items() if k != "items"}
            slim["item_count"] = len(tab_items)
            slim["items"] = page_items if str(tab.get("id")) == str(active.get("id")) else []
            slim_tabs.append(slim)

        result = {
            **{k: v for k, v in data.items() if k not in ("items", "tabs")},
            "items": page_items if active.get("source_type") == "main" else [],
            "tabs": slim_tabs,
            "active_tab_id": active.get("id"),
            "page": page,
            "limit": limit,
            "total": total,
            "pages": pages,
            "search": search or "",
            "sort_key": sort_key,
            "sort_order": sort_order or "desc",
            "main_categories": main_categories,
        }
        return result

    def _compute_market_analysis(self):
        if not self.active_project_id:
            return self._snapshot_empty_market_analysis()

        with self._db_lock:
            conn = self._get_conn()
            try:
                main_df = self._read_sql(
                    "SELECT * FROM main_products WHERE project_id = ? ORDER BY _row_orig_idx ASC",
                    conn,
                    params=(self.active_project_id,),
                )
                comp_df = self._read_sql(
                    "SELECT * FROM comp_products WHERE project_id = ? ORDER BY store_id ASC, skuId ASC",
                    conn,
                    params=(self.active_project_id,),
                )
            finally:
                conn.close()

        buckets = self._load_market_category_buckets()
        file_frames = []
        if not main_df.empty:
            file_frames.append(self._market_prepare_file_df(main_df, "main", self.main_store_name or "主店", buckets))
        if not comp_df.empty and "store_id" in comp_df.columns:
            comp_df["store_id"] = comp_df["store_id"].astype(str)
            for i, store_name in enumerate(self.store_names):
                sid = str(i)
                sdf = comp_df[comp_df["store_id"] == sid]
                file_frames.append(self._market_prepare_file_df(sdf, f"comp-{sid}", store_name, buckets))

        file_count = len(file_frames)
        if not file_count:
            return {
                "status": "ok",
                "project_id": self.active_project_id,
                "project_name": self.active_project_name,
                "file_count": 0,
                "top10_categories": [],
                "recommendation": {
                    "level": "拉完了",
                    "total_sales_amount": 0,
                    "department_store_ratio": 0,
                    "snack_ratio": 0,
                    "other_ratio": 100,
                    "amounts": {"department_store": 0, "snack": 0, "other": 0},
                },
                "metrics": {"average": self._market_metric_pack(0, 0), "top1": self._market_metric_pack(0, 0)},
                "metric_diffs": {k: 0 for k in self._market_metric_pack(0, 0).keys()},
                "mode_options": ["average", "top1"],
            }

        all_df = pd.concat(file_frames, ignore_index=True)
        valid_category_df = all_df[(all_df["category_l1"] != "") & (all_df["category_l1"].str.lower() != "nan")]
        category_rows = []
        if not valid_category_df.empty:
            grouped = valid_category_df.groupby("category_l1", sort=False)[["sales", "sales_amount"]].sum().reset_index()
            grouped["sales"] = grouped["sales"] / file_count
            grouped["sales_amount"] = grouped["sales_amount"] / file_count
            grouped = grouped.sort_values("sales_amount", ascending=False).head(10)
            category_rows = [
                {
                    "category": row["category_l1"],
                    "order_count": round(float(row["sales"]), 2),
                    "sales_amount": round(float(row["sales_amount"]), 2),
                }
                for _, row in grouped.iterrows()
            ]

        total_sales = float(all_df["sales"].sum())
        total_sales_amount = float(all_df["sales_amount"].sum())
        average_sales = total_sales / file_count
        average_sales_amount = total_sales_amount / file_count
        bucket_amounts = all_df.groupby("bucket")["sales_amount"].sum().to_dict()
        department_amount = float(bucket_amounts.get("department_store", 0)) / file_count
        snack_amount = float(bucket_amounts.get("snack", 0)) / file_count
        other_amount = float(bucket_amounts.get("other", 0)) / file_count
        ratio_base = average_sales_amount
        department_ratio = department_amount / ratio_base * 100 if ratio_base else 0.0
        snack_ratio = snack_amount / ratio_base * 100 if ratio_base else 0.0
        other_ratio = max(0.0, 100.0 - department_ratio - snack_ratio) if ratio_base else 100.0

        per_file = all_df.groupby(["file_id", "file_name"], sort=False)[["sales", "sales_amount"]].sum().reset_index()
        if per_file.empty:
            top1_sales = top1_sales_amount = 0.0
            top1_file = {"file_id": "", "file_name": ""}
        else:
            top1_row = per_file.sort_values("sales_amount", ascending=False).iloc[0]
            top1_sales = float(top1_row["sales"])
            top1_sales_amount = float(top1_row["sales_amount"])
            top1_file = {"file_id": str(top1_row["file_id"]), "file_name": str(top1_row["file_name"])}

        average_metrics = self._market_metric_pack(average_sales, average_sales_amount)
        top1_metrics = self._market_metric_pack(top1_sales, top1_sales_amount)
        metric_diffs = {
            key: int(round(float(average_metrics.get(key, 0)) - float(top1_metrics.get(key, 0))))
            for key in average_metrics.keys()
        }

        return {
            "status": "ok",
            "project_id": self.active_project_id,
            "project_name": self.active_project_name,
            "file_count": file_count,
            "top10_categories": category_rows,
            "recommendation": {
                "level": self._market_level_for_sales(average_sales_amount),
                "total_sales_amount": round(average_sales_amount, 2),
                "department_store_ratio": round(department_ratio, 2),
                "snack_ratio": round(snack_ratio, 2),
                "other_ratio": round(other_ratio, 2),
                "amounts": {
                    "department_store": round(department_amount, 2),
                    "snack": round(snack_amount, 2),
                    "other": round(other_amount, 2),
                },
            },
            "metrics": {
                "average": average_metrics,
                "top1": top1_metrics,
            },
            "metric_diffs": metric_diffs,
            "top1_file": top1_file,
            "mode_options": ["average", "top1"],
            "bucket_reference": {
                "snack": sorted(buckets.get("snack", set())),
                "department_store": sorted(buckets.get("department_store", set())),
                "other": sorted(buckets.get("other", set())),
            },
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
                    df = self._read_sql(
                        "SELECT * FROM comp_products WHERE project_id = ? AND store_id = ?",
                        conn,
                        params=(self.active_project_id, store_id),
                    )
                    source_name = self.store_names[int(store_id)] if store_id.isdigit() and int(store_id) < len(self.store_names) else "竞店"
                else:
                    df = self._read_sql(
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

    def _compute_statistics(self):
        if not self.active_project_id:
            return self._snapshot_empty_statistics()

        with self._db_lock:
            conn = self._get_conn()
            try:
                main_df = self._read_sql(
                    "SELECT * FROM main_products WHERE project_id = ? ORDER BY _row_orig_idx ASC",
                    conn,
                    params=(self.active_project_id,),
                )
                comp_df = self._read_sql(
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
            competitor_total_sales_amount = {}
            for item in items:
                for comp in item.get("competitors", []):
                    sid = str(comp.get("store_id") or "")
                    if not sid:
                        continue
                    competitor_total_sales_amount[sid] = competitor_total_sales_amount.get(sid, 0.0) + (
                        (comp.get("metrics") or {}).get("sales_amount", 0)
                    )

            for item in items:
                summary = item.get("summary") or {}
                sales_amount = summary.get("sales_amount") or {}
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
                    sid = str(comp.get("store_id") or "")
                    comp_total_sales_amount = competitor_total_sales_amount.get(sid, 0.0)
                    comp_contribution = (
                        metrics.get("sales_amount", 0) / comp_total_sales_amount * 100
                        if comp_total_sales_amount else 0
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
            store_totals = empty_totals()
            for cat in store_categories:
                store_df = comp_df[(comp_df.get("store_id", "") == sid) & (comp_df["__cat3"] == cat)]
                add_totals(store_totals, self._statistics_metric_pack(store_df))
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
                "totals": finish_totals(store_totals),
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
                self.main_df = self._read_sql("SELECT * FROM main_products WHERE project_id = ? ORDER BY _row_orig_idx ASC", conn, params=(self.active_project_id,))
                links_df = self._read_sql("SELECT * FROM product_links WHERE project_id = ?", conn, params=(self.active_project_id,))
                comp_df = self._read_sql("SELECT * FROM comp_products WHERE project_id = ?", conn, params=(self.active_project_id,))
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
                st_df.drop(columns=['store_id', 'project_id', 'is_new_add', 'is_ignored'], inplace=True, errors='ignore')
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

    def _linked_store_stats_from_grid_df(self, df):
        """当前关联页筛选结果中，各竞店已关联商品的去重 SPU/SKU 数。"""
        stats = {str(i): {"spu_count": 0, "sku_count": 0} for i in range(len(self.store_names))}
        if df is None or df.empty:
            return stats
        main_skus = (
            df.get("skuId", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .str.strip()
        )
        main_skus = [s for s in main_skus.unique().tolist() if s and s.lower() not in ("nan", "none")]
        if not main_skus:
            return stats
        stat_sets = {sid: {"spu": set(), "sku": set()} for sid in stats}
        with self._db_lock:
            conn = self._get_conn()
            try:
                for start in range(0, len(main_skus), 800):
                    chunk = main_skus[start:start + 800]
                    placeholders = ",".join(["?"] * len(chunk))
                    rows = conn.execute(
                        f"""
                        SELECT pl.store_id, pl.comp_sku_id, cp.商品名称
                        FROM product_links pl
                        LEFT JOIN comp_products cp
                          ON cp.project_id = pl.project_id
                         AND cp.store_id = pl.store_id
                         AND cp.skuId = pl.comp_sku_id
                        WHERE pl.project_id = ?
                          AND pl.main_sku_id IN ({placeholders})
                        """,
                        [self.active_project_id] + chunk,
                    ).fetchall()
                    for store_id, comp_sku, name in rows:
                        sid = str(store_id)
                        if sid not in stat_sets:
                            continue
                        sku = str(comp_sku or "").strip()
                        if sku and sku.lower() not in ("nan", "none"):
                            stat_sets[sid]["sku"].add(sku)
                        nm = str(name or "").strip()
                        if nm and nm.lower() != "nan":
                            stat_sets[sid]["spu"].add(nm)
            finally:
                conn.close()
        return {
            sid: {"spu_count": len(v["spu"]), "sku_count": len(v["sku"])}
            for sid, v in stat_sets.items()
        }

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
            self._reconstruct_from_sqlite()
        if self.grid_df is None or self.grid_df.empty:
            summary = self.get_workbench_summary()
            return {
                "items": [], "total": 0, "page": page, "pages": 0,
                "sku_count": 0, "sku_eliminated_count": 0,
                "spu_count": 0, "spu_eliminated_count": 0,
                "store_stats": {sid: (store.get("linked") or {}) for sid, store in (summary.get("stores") or {}).items()},
            }

        df = self.grid_df.copy()

        # 1. Search Filter
        search_tokens = self._search_tokens(search)
        if search_tokens:
            mask = df.apply(lambda row: self._row_matches_search_tokens(row, search_tokens), axis=1)
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
        summary = self.get_workbench_summary()

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
            "store_stats": {sid: (store.get("linked") or {}) for sid, store in (summary.get("stores") or {}).items()},
        }

    def get_store_products(self, store_id, project_id=None):
        pid = project_id or self.active_project_id
        if not pid:
            return []
        if pid == self.active_project_id and store_id in self.store_dfs and not self.store_dfs[store_id]["df"].empty:
            return self.store_dfs[store_id]["df"].fillna("").to_dict(orient='records')
        with self._db_lock:
            conn = self._get_conn()
            try:
                df = self._read_sql(
                    """
                    SELECT *
                    FROM comp_products
                    WHERE project_id = ? AND store_id = ?
                    ORDER BY skuId ASC
                    """,
                    conn,
                    params=(pid, str(store_id)),
                )
            finally:
                conn.close()
        if not df.empty:
            return df.fillna("").to_dict(orient="records")
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
                    df = self._read_sql(q, conn, params=(self.active_project_id, sid))
                    out[sid] = df.fillna("").to_dict(orient='records')
            finally:
                conn.close()
        return out


    def get_main_products_page(self, page=1, limit=50, search="", project_id=None):
        pid = project_id or self.active_project_id
        if not pid:
            return {"items": [], "total": 0, "page": page, "limit": limit, "pages": 0}
        page = max(1, int(page))
        search_tokens = self._search_tokens(search)
        has_search = bool(search_tokens)
        if not has_search:
            limit = max(1, min(int(limit), 100))
        offset = (page - 1) * limit
        where = ["project_id = ?"]
        params = [pid]
        if search_tokens:
            self._add_search_token_clauses(where, params, ["skuId", "商品名称", "规格名称"], search_tokens)
        where_sql = " AND ".join(where)
        with self._db_lock:
            conn = self._get_conn()
            try:
                total = conn.execute(f"SELECT COUNT(*) FROM main_products WHERE {where_sql}", tuple(params)).fetchone()[0]
                limit_clause = "" if has_search else "LIMIT ? OFFSET ?"
                query_params = tuple(params) if has_search else tuple(params + [limit, offset])
                existing_cols = {
                    str(row[1])
                    for row in conn.execute("PRAGMA table_info(main_products)").fetchall()
                }
                optional_cols = [c for c in ("采购价", "库存") if c in existing_cols]
                select_cols = [
                    "skuId", "商品名称", "规格名称", "主图链接", "活动价", "原价",
                    "销售", "美团类目三级", "_row_orig_idx",
                ] + optional_cols
                df = self._read_sql(
                    f"""
                    SELECT {", ".join(select_cols)}
                    FROM main_products
                    WHERE {where_sql}
                    ORDER BY CAST(COALESCE(NULLIF(销售, ''), '0') AS REAL) DESC, _row_orig_idx ASC
                    {limit_clause}
                    """,
                    conn,
                    params=query_params
                )
                link_cnt = self._read_sql(
                    """
                    SELECT main_sku_id, COUNT(*) AS cnt
                    FROM product_links
                    WHERE project_id = ?
                    GROUP BY main_sku_id
                    """,
                    conn,
                    params=(pid,)
                )
            finally:
                conn.close()
        for optional in ("采购价", "库存"):
            if optional not in df.columns:
                df[optional] = ""
        if not df.empty:
            link_map = {str(r["main_sku_id"]): int(r["cnt"]) for _, r in link_cnt.iterrows()} if not link_cnt.empty else {}
            df["__linked_count"] = df["skuId"].astype(str).map(lambda x: link_map.get(x, 0))
        else:
            df["__linked_count"] = pd.Series(dtype="int64")
        pages = (total + limit - 1) // limit if total else 0
        return {"items": df.fillna("").to_dict(orient="records"), "total": total, "page": page, "limit": limit, "pages": pages}

    def get_main_product_links(self, main_sku_id, project_id=None):
        pid = project_id or self.active_project_id
        if not pid or not main_sku_id:
            return {"items": [], "total": 0}
        with self._db_lock:
            conn = self._get_conn()
            try:
                df = self._read_sql(
                    """
                    SELECT
                        pl.store_id AS __link_store_id,
                        pl.comp_sku_id AS __link_comp_sku_id,
                        pl.similarity AS __link_similarity,
                        pl.match_type AS __link_match_type,
                        pl.is_new_add AS __link_is_new_add,
                        cp.*
                    FROM product_links pl
                    LEFT JOIN comp_products cp
                      ON cp.project_id = pl.project_id
                     AND cp.store_id = pl.store_id
                     AND cp.skuId = pl.comp_sku_id
                    WHERE pl.project_id = ?
                      AND pl.main_sku_id = ?
                    ORDER BY CAST(pl.store_id AS INTEGER) ASC
                    """,
                    conn,
                    params=(pid, str(main_sku_id)),
                )
            finally:
                conn.close()
        if df.empty:
            return {"items": [], "total": 0}
        records = df.fillna("").to_dict(orient="records")
        store_names = self._project_store_names(pid)
        for item in records:
            store_id = str(item.get("__link_store_id", ""))
            try:
                item["__store_name"] = store_names[int(store_id)]
            except (ValueError, IndexError):
                item["__store_name"] = store_id or "竞店"
        return {"items": records, "total": len(records)}

    def get_match_explanation(self, main_sku_id, store_id, project_id=None):
        pid = project_id or self.active_project_id
        if not pid or not main_sku_id or store_id is None:
            return {"status": "error", "message": "缺少项目或商品参数"}
        sid = str(store_id)
        with self._db_lock:
            conn = self._get_conn()
            try:
                main_df = self._read_sql(
                    "SELECT * FROM main_products WHERE project_id = ? AND skuId = ? LIMIT 1",
                    conn,
                    params=(pid, str(main_sku_id)),
                )
                link_df = self._read_sql(
                    """
                    SELECT * FROM product_links
                    WHERE project_id = ? AND main_sku_id = ? AND store_id = ?
                    LIMIT 1
                    """,
                    conn,
                    params=(pid, str(main_sku_id), sid),
                )
                comp_sku = ""
                if not link_df.empty:
                    comp_sku = str(link_df.iloc[0].get("comp_sku_id") or "")
                comp_df = self._read_sql(
                    """
                    SELECT * FROM comp_products
                    WHERE project_id = ? AND store_id = ? AND skuId = ?
                    LIMIT 1
                    """,
                    conn,
                    params=(pid, sid, comp_sku),
                ) if comp_sku else pd.DataFrame()
            finally:
                conn.close()

        if main_df.empty:
            return {"status": "error", "message": "主店商品不存在"}
        if link_df.empty or not comp_sku:
            return {"status": "error", "message": "当前竞店未关联商品"}
        if comp_df.empty:
            return {"status": "error", "message": "竞店关联商品不存在"}

        main = main_df.fillna("").iloc[0].to_dict()
        comp = comp_df.fillna("").iloc[0].to_dict()
        link = link_df.fillna("").iloc[0].to_dict()
        template = self.get_post_match_template_for_project(pid)
        block = post_match_engine.rules_for_item(template, main)
        group = post_match_engine.get_rule_group_for_item(template, main) or {}
        explain = post_match_engine.explain_post_match(main, comp, block)
        weak = post_match_engine.weak_ranking_score(main, comp, block)
        store_names = self._project_store_names(pid)
        try:
            store_name = store_names[int(sid)]
        except (ValueError, IndexError):
            store_name = sid or "竞店"
        return {
            "status": "ok",
            "project_id": pid,
            "store_id": sid,
            "store_name": store_name,
            "rule_group": {
                "id": group.get("id", ""),
                "name": group.get("name", "未命中规则组"),
            },
            "link": {
                "main_sku_id": str(main_sku_id),
                "comp_sku_id": comp_sku,
                "similarity": link.get("similarity", ""),
                "match_type": link.get("match_type", ""),
            },
            "main": main,
            "candidate": comp,
            "post_match": explain,
            "weak_ranking": weak,
            "other_candidates": self._build_match_explain_other_candidates(main, comp_sku, sid, block, project_id=pid),
        }

    def _match_explain_text(self, item):
        if not item:
            return ""
        parts = []
        for col in ("商品名称", "规格名称", "美团类目一级", "美团类目二级", "美团类目三级", "核心品类", "售卖数量", "包装单位", "净含量"):
            val = item.get(col, "")
            if val is None:
                continue
            text = str(val).strip()
            if text and text.lower() not in ("nan", "none", "-"):
                parts.append(text)
        return " ".join(parts)

    def _match_explain_similarity(self, main, candidate):
        a = self._match_explain_text(main)
        b = self._match_explain_text(candidate)
        if not a or not b:
            return 0.0
        return float(SequenceMatcher(None, a, b).ratio())

    def _match_explain_failed_summary(self, explain):
        metric_labels = {
            "core_conflict": "高风险冲突",
            "category_gate": "类目门槛",
            "core": "核心品类",
            "cat3": "三级类目",
            "net": "净含量",
            "sell": "售卖数量",
            "pack": "包装单位",
            "size": "尺寸",
            "multidim_size": "多维尺寸",
            "brand": "品牌",
            "color": "颜色",
            "model": "型号",
            "product_form": "商品形态",
            "key_attributes": "关键属性词",
        }
        failed = [
            m for m in (explain or {}).get("metrics", [])
            if m.get("enabled") and not m.get("passed")
        ]
        if failed:
            return "；".join(
                f"{metric_labels.get(m.get('key'), m.get('key', ''))}: {m.get('reason', '')}".strip(": ")
                for m in failed
                if m.get("reason")
            )
        if (explain or {}).get("accepted"):
            return "后验规则放过；未成为当前结果通常是原始向量/文本候选排序低于已选商品，或未进入当次召回TopK"
        return (explain or {}).get("reason", "")

    def _build_match_explain_other_candidates(self, main, selected_comp_sku, store_id, block, limit=2, project_id=None):
        pid = project_id or self.active_project_id
        if not pid or not main or store_id is None:
            return []
        with self._db_lock:
            conn = self._get_conn()
            try:
                comp_df = self._read_sql(
                    """
                    SELECT * FROM comp_products
                    WHERE project_id = ? AND store_id = ?
                    """,
                    conn,
                    params=(pid, str(store_id)),
                )
            finally:
                conn.close()
        if comp_df.empty:
            return []

        scored_candidates = []
        selected = str(selected_comp_sku or "")
        for cand in comp_df.fillna("").to_dict(orient="records"):
            sku = str(cand.get("skuId") or "")
            if not sku or sku == selected:
                continue
            text_score = self._match_explain_similarity(main, cand)
            scored_candidates.append((text_score, cand))

        scored_candidates.sort(key=lambda row: row[0], reverse=True)

        rows = []
        for text_score, cand in scored_candidates[:80]:
            sku = str(cand.get("skuId") or "")
            explain = post_match_engine.explain_post_match(main, cand, block)
            weak = post_match_engine.weak_ranking_score(main, cand, block)
            accepted = bool((explain or {}).get("accepted"))
            failed_count = sum(
                1 for m in (explain or {}).get("metrics", [])
                if m.get("enabled") and not m.get("passed")
            )
            rows.append({
                "skuId": sku,
                "商品名称": cand.get("商品名称", ""),
                "规格名称": cand.get("规格名称", ""),
                "text_similarity": text_score,
                "weak_bonus": float((weak or {}).get("bonus") or 0.0),
                "post_match": explain,
                "weak_ranking": weak,
                "accepted": accepted,
                "failed_count": failed_count,
                "summary": self._match_explain_failed_summary(explain),
            })

        rows.sort(key=lambda r: (r["text_similarity"] + r["weak_bonus"], r["text_similarity"]), reverse=True)
        rejected = [r for r in rows if not r["accepted"]][:limit]
        accepted = [r for r in rows if r["accepted"]][:max(0, limit - len(rejected))]
        result = rejected + accepted
        result.sort(key=lambda r: (r["text_similarity"] + r["weak_bonus"], r["text_similarity"]), reverse=True)
        return result[:limit]
