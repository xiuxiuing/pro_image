import os
import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook


def make_workbook(path, headers, rows):
    wb = Workbook()
    ws = wb.active
    ws.append(headers)
    for row in rows:
        ws.append(row)
    wb.save(path)


@unittest.skipUnless(os.environ.get("DATABASE_URL"), "DATABASE_URL is required for PostgreSQL smoke tests")
class PostgreSQLSmokeTests(unittest.TestCase):
    def test_empty_database_initializes_core_seed_data(self):
        from data_mgr import DataManager

        with tempfile.TemporaryDirectory() as tmpdir:
            dm = DataManager(tmpdir)
            try:
                with dm._get_conn() as conn:
                    projects = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
                    users = conn.execute("SELECT COUNT(*) FROM user").fetchone()[0]
                    chars = conn.execute("SELECT COUNT(*) FROM characters").fetchone()[0]
                    templates = conn.execute("SELECT name FROM rule_templates ORDER BY id ASC").fetchall()
            finally:
                dm.db.engine.dispose()

        template_names = {row[0] for row in templates}
        self.assertGreaterEqual(projects, 1)
        self.assertGreaterEqual(users, 1)
        self.assertGreaterEqual(chars, 1)
        self.assertIn("生产规则V1", template_names)
        self.assertIn("生产规则V2", template_names)

    def test_project_core_tables_round_trip(self):
        from data_mgr import DataManager
        from online_jobs import JobStore

        with tempfile.TemporaryDirectory() as tmpdir:
            dm = DataManager(tmpdir)
            try:
                pid = dm.create_project(
                    "PG项目",
                    {"path": "/tmp/main.xlsx", "store_name": "主店"},
                    [{"path": "/tmp/comp.xlsx", "store_name": "竞店"}],
                )
                with dm._get_conn() as conn:
                    with conn:
                        conn.execute(
                            "INSERT INTO main_products (project_id, skuId, _row_orig_idx, 商品名称, 规格名称, 销售) VALUES (?, ?, ?, ?, ?, ?)",
                            (pid, "M1", 1, "主商品", "规格A", "10"),
                        )
                        conn.execute(
                            "INSERT INTO comp_products (project_id, store_id, skuId, 商品名称, 规格名称, 销售) VALUES (?, ?, ?, ?, ?, ?)",
                            (pid, "0", "C1", "竞商品", "规格B", "9"),
                        )
                        conn.execute(
                            "INSERT INTO product_links (project_id, main_sku_id, store_id, comp_sku_id, similarity, match_type, is_new_add) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (pid, "M1", "0", "C1", 0.91, "文本匹配", "否"),
                        )

                job_id = JobStore(dm).create_job(pid, "analysis", ["准备", "完成"], {"project_id": pid})
                JobStore(dm).mark_running(job_id)
                JobStore(dm).update_step(job_id, 0, "done")
                JobStore(dm).finish(job_id, "done")

                main_page = dm.get_main_products_page(project_id=pid)
                links = dm.get_main_product_links("M1", project_id=pid)
                progress = JobStore(dm).latest_project_progress(pid)
            finally:
                dm.db.engine.dispose()

        self.assertEqual(main_page["total"], 1)
        self.assertEqual(main_page["items"][0]["skuId"], "M1")
        self.assertEqual(links["total"], 1)
        self.assertEqual(links["items"][0]["__link_comp_sku_id"], "C1")
        self.assertTrue(progress["available"])
        self.assertEqual(progress["job_status"], "done")

    def test_project_source_import_writes_products_and_snapshot(self):
        from data_mgr import DataManager

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            headers = [
                "skuid", "商品名称", "规格", "图片", "条码",
                "一级类目", "二级类目", "三级类目", "活动价", "原价", "销量",
            ]
            main_path = tmp / "main.xlsx"
            comp_path = tmp / "comp.xlsx"
            make_workbook(
                main_path,
                headers,
                [["m1", "可乐", "500ml", "u", "1", "饮品", "饮料", "碳酸饮料", 10, 12, 45]],
            )
            make_workbook(
                comp_path,
                headers,
                [["c1", "橙汁", "1L", "u", "2", "饮品", "饮料", "果汁", 20, 25, 90]],
            )

            dm = DataManager(tmpdir)
            try:
                pid = dm.create_project(
                    "PG导入项目",
                    {"path": str(main_path), "store_name": "主店"},
                    [{"path": str(comp_path), "store_name": "竞店"}],
                )
                dm.import_project_sources(pid)

                with dm._get_conn() as conn:
                    main_count = conn.execute("SELECT COUNT(*) FROM main_products WHERE project_id = ?", (pid,)).fetchone()[0]
                    comp_count = conn.execute("SELECT COUNT(*) FROM comp_products WHERE project_id = ?", (pid,)).fetchone()[0]
                    snapshot = conn.execute(
                        "SELECT status FROM project_analysis_snapshots WHERE project_id = ?",
                        (pid,),
                    ).fetchone()
            finally:
                dm.db.engine.dispose()

        self.assertEqual(main_count, 1)
        self.assertEqual(comp_count, 1)
        self.assertEqual(snapshot[0], "ready")

    def test_project_edit_operations_round_trip(self):
        from data_mgr import DataManager

        with tempfile.TemporaryDirectory() as tmpdir:
            dm = DataManager(tmpdir)
            try:
                pid = dm.create_project(
                    "PG编辑项目",
                    {"path": "/tmp/main-edit.xlsx", "store_name": "主店"},
                    [{"path": "/tmp/comp-edit.xlsx", "store_name": "竞店"}],
                )
                with dm._get_conn() as conn:
                    with conn:
                        conn.execute(
                            "INSERT INTO main_products (project_id, skuId, _row_orig_idx, 商品名称, 规格名称, 销售, 新售价) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (pid, "M2", 1, "主商品2", "规格A", "10", "10"),
                        )
                        conn.execute(
                            "INSERT INTO comp_products (project_id, store_id, skuId, 商品名称, 规格名称, 销售) VALUES (?, ?, ?, ?, ?, ?)",
                            (pid, "0", "C2", "竞商品2", "规格B", "9"),
                        )

                dm.update_cell("M2", {"新售价": "12"}, project_id=pid)
                dm.manual_link("M2", "0", "C2", project_id=pid)
                dm.mark_as_new("0", "C2", "新增该规格", project_id=pid)
                dm.mark_as_ignored("0", "C2", True, project_id=pid)
                dm.unlink_product("M2", "0", project_id=pid)

                with dm._get_conn() as conn:
                    price = conn.execute(
                        "SELECT 新售价 FROM main_products WHERE project_id = ? AND skuId = ?",
                        (pid, "M2"),
                    ).fetchone()[0]
                    comp_flags = conn.execute(
                        "SELECT is_new_add, is_ignored FROM comp_products WHERE project_id = ? AND store_id = ? AND skuId = ?",
                        (pid, "0", "C2"),
                    ).fetchone()
                    links = conn.execute(
                        "SELECT COUNT(*) FROM product_links WHERE project_id = ? AND main_sku_id = ?",
                        (pid, "M2"),
                    ).fetchone()[0]
            finally:
                dm.db.engine.dispose()

        self.assertEqual(price, "12")
        self.assertEqual(comp_flags[0], "否")
        self.assertEqual(comp_flags[1], "是")
        self.assertEqual(links, 0)


if __name__ == "__main__":
    unittest.main()
