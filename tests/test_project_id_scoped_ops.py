import json
import tempfile
import unittest

from data_mgr import DataManager


class ProjectScopedOpsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dm = DataManager(self.tmp.name)
        with self.dm._get_conn() as conn:
            conn.execute("DELETE FROM job_steps")
            conn.execute("DELETE FROM jobs")
            conn.execute("DELETE FROM project_analysis_snapshots")
            conn.execute("DELETE FROM manual_link_corrections")
            conn.execute("DELETE FROM product_links")
            conn.execute("DELETE FROM comp_products")
            conn.execute("DELETE FROM main_products")
            conn.execute("DELETE FROM project_files")
            conn.execute("DELETE FROM projects")
            conn.execute("INSERT INTO projects (id, name, is_active, status) VALUES (1, '项目1', 1, 'ready')")
            conn.execute("SELECT setval(pg_get_serial_sequence('projects', 'id'), 1)")

    def tearDown(self):
        try:
            self.dm.db.engine.dispose()
        finally:
            self.tmp.cleanup()

    def test_update_cell_uses_explicit_project_id(self):
        dm = self.dm
        with dm._get_conn() as conn:
            conn.execute("INSERT INTO projects (id, name, is_active, status) VALUES (2, '项目2', 0, 'ready')")
            conn.execute(
                "INSERT INTO main_products (project_id, skuId, 商品名称, 新售价) VALUES (1, 'SKU-1', '项目1商品', '10')"
            )
            conn.execute(
                "INSERT INTO main_products (project_id, skuId, 商品名称, 新售价) VALUES (2, 'SKU-1', '项目2商品', '20')"
            )

        dm.update_cell("SKU-1", {"新售价": "99"}, project_id=2)

        with dm._get_conn() as conn:
            p1 = conn.execute(
                "SELECT 新售价 FROM main_products WHERE project_id = 1 AND skuId = 'SKU-1'"
            ).fetchone()[0]
            p2 = conn.execute(
                "SELECT 新售价 FROM main_products WHERE project_id = 2 AND skuId = 'SKU-1'"
            ).fetchone()[0]

        self.assertEqual(p1, "10")
        self.assertEqual(p2, "99")

    def test_manual_link_uses_explicit_project_id(self):
        dm = self.dm
        with dm._get_conn() as conn:
            conn.execute("INSERT INTO projects (id, name, is_active, status) VALUES (2, '项目2', 0, 'ready')")
            conn.execute(
                """
                INSERT INTO product_links
                (project_id, main_sku_id, store_id, comp_sku_id, similarity, match_type, is_new_add)
                VALUES (1, 'M1', '0', 'C-old-1', 0.5, '文本匹配', '否')
                """
            )
            conn.execute(
                """
                INSERT INTO product_links
                (project_id, main_sku_id, store_id, comp_sku_id, similarity, match_type, is_new_add)
                VALUES (2, 'M1', '0', 'C-old-2', 0.5, '文本匹配', '否')
                """
            )

        dm.manual_link("M1", "0", "C-new-2", project_id=2)

        with dm._get_conn() as conn:
            p1 = conn.execute(
                "SELECT comp_sku_id FROM product_links WHERE project_id = 1 AND main_sku_id = 'M1'"
            ).fetchone()[0]
            p2 = conn.execute(
                "SELECT comp_sku_id FROM product_links WHERE project_id = 2 AND main_sku_id = 'M1'"
            ).fetchone()[0]

        self.assertEqual(p1, "C-old-1")
        self.assertEqual(p2, "C-new-2")

    def test_read_pages_use_explicit_project_id(self):
        dm = self.dm
        with dm._get_conn() as conn:
            conn.execute("INSERT INTO projects (id, name, is_active, status) VALUES (2, '项目2', 0, 'ready')")
            conn.execute(
                "INSERT INTO main_products (project_id, skuId, _row_orig_idx, 商品名称, 规格名称, 销售) VALUES (1, 'SKU-1', 1, '项目1商品', 'A', '10')"
            )
            conn.execute(
                "INSERT INTO main_products (project_id, skuId, _row_orig_idx, 商品名称, 规格名称, 销售) VALUES (2, 'SKU-2', 1, '项目2商品', 'B', '20')"
            )
            conn.execute(
                "INSERT INTO comp_products (project_id, store_id, skuId, 商品名称, 规格名称, 销售) VALUES (1, '0', 'C-1', '项目1竞品', 'A', '1')"
            )
            conn.execute(
                "INSERT INTO comp_products (project_id, store_id, skuId, 商品名称, 规格名称, 销售) VALUES (2, '0', 'C-2', '项目2竞品', 'B', '2')"
            )

        main_page = dm.get_main_products_page(project_id=2)
        store_rows = dm.get_store_products("0", project_id=2)

        self.assertEqual(main_page["total"], 1)
        self.assertEqual(main_page["items"][0]["skuId"], "SKU-2")
        self.assertEqual(len(store_rows), 1)
        self.assertEqual(store_rows[0]["skuId"], "C-2")

    def test_links_and_snapshot_use_explicit_project_id(self):
        dm = self.dm
        with dm._get_conn() as conn:
            conn.execute("INSERT INTO projects (id, name, is_active, status) VALUES (2, '项目2', 0, 'ready')")
            conn.execute(
                "INSERT INTO project_files (project_id, type, local_path, store_name) VALUES (2, 'comp', '/tmp/store2.xlsx', '项目2竞店')"
            )
            conn.execute(
                "INSERT INTO comp_products (project_id, store_id, skuId, 商品名称, 规格名称, 销售) VALUES (2, '0', 'C-2', '项目2竞品', 'B', '2')"
            )
            conn.execute(
                """
                INSERT INTO product_links
                (project_id, main_sku_id, store_id, comp_sku_id, similarity, match_type, is_new_add)
                VALUES (2, 'M-2', '0', 'C-2', 0.88, '文本匹配', '否')
                """
            )
            conn.execute(
                """
                INSERT INTO project_analysis_snapshots
                (project_id, statistics_json, market_analysis_json, workbench_summary_json, computed_at, version, status, error_message)
                VALUES (?, ?, '{}', '{}', '2026-06-02 10:00:00', ?, 'ready', '')
                """,
                (
                    2,
                    json.dumps({"project_id": 2, "tabs": [{"id": "main", "items": [{"category": "项目2类目"}]}]}),
                    dm.ANALYSIS_SNAPSHOT_VERSION,
                ),
            )

        links = dm.get_main_product_links("M-2", project_id=2)
        stats = dm.get_statistics(project_id=2)

        self.assertEqual(links["total"], 1)
        self.assertEqual(links["items"][0]["__store_name"], "项目2竞店")
        self.assertEqual(stats["project_id"], 2)
        self.assertEqual(stats["tabs"][0]["items"][0]["category"], "项目2类目")

    def test_project_context_restores_state_without_changing_active_project(self):
        dm = self.dm
        with dm._get_conn() as conn:
            conn.execute("UPDATE projects SET is_active = 1 WHERE id = 1")
            conn.execute("INSERT INTO projects (id, name, is_active, status) VALUES (2, '项目2', 0, 'ready')")
            conn.execute(
                "INSERT INTO main_products (project_id, skuId, _row_orig_idx, 商品名称, 规格名称, 销售) VALUES (1, 'SKU-1', 1, '项目1商品', 'A', '10')"
            )
            conn.execute(
                "INSERT INTO main_products (project_id, skuId, _row_orig_idx, 商品名称, 规格名称, 销售) VALUES (2, 'SKU-2', 1, '项目2商品', 'B', '20')"
            )

        self.assertEqual(dm.active_project_id, 1)
        with dm.project_context(2):
            self.assertEqual(dm.active_project_id, 2)
            page = dm.get_paginated_grid()
            self.assertEqual(page["items"][0]["skuId"], "SKU-2")

        self.assertEqual(dm.active_project_id, 1)
        with dm._get_conn() as conn:
            active_rows = conn.execute("SELECT id FROM projects WHERE is_active = 1").fetchall()
        self.assertEqual(active_rows, [(1,)])


if __name__ == "__main__":
    unittest.main()
