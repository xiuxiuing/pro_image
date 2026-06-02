import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook
from sqlalchemy import text

from data_mgr import DataManager
from db_access import Database


def make_workbook(path, headers, rows):
    wb = Workbook()
    ws = wb.active
    ws.append(headers)
    for row in rows:
        ws.append(row)
    wb.save(path)


class MainProductIdentityTests(unittest.TestCase):
    def setUp(self):
        db = Database()
        try:
            with db.engine.begin() as conn:
                conn.execute(text("DROP SCHEMA public CASCADE"))
                conn.execute(text("CREATE SCHEMA public"))
        finally:
            db.close()

    def test_search_tokens_are_deduped_and_limited_to_first_five(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dm = DataManager(tmpdir)

            self.assertEqual(
                dm._search_tokens("护舒宝，考拉安睡裤  L码；2片、夜用 超薄 新老包装随机 L码"),
                ["护舒宝", "考拉安睡裤", "l码", "2片", "夜用"],
            )

    def test_multi_keyword_search_matches_non_contiguous_product_name(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            main_path = tmp / "main.xlsx"
            comp_path = tmp / "comp.xlsx"
            headers = [
                "skuid",
                "商品名称",
                "规格",
                "图片",
                "条码",
                "一级类目",
                "二级类目",
                "三级类目",
                "活动价",
                "美团外卖渠道售价",
                "销量",
            ]
            target_name = "护舒宝 考拉安睡裤夜用裤型卫生巾L码 2片/包 女生超薄防漏安心裤（新老包装随机）"
            make_workbook(
                main_path,
                headers,
                [
                    ["m1", target_name, "2片/包", "", "", "个人洗护", "纸品", "卫生巾", 9.9, 12.9, 10],
                    ["m2", "护舒宝 普通卫生巾M码", "10片", "", "", "个人洗护", "纸品", "卫生巾", 12, 15, 5],
                ],
            )
            make_workbook(
                comp_path,
                headers,
                [["c1", target_name, "2片/包", "", "", "个人洗护", "纸品", "卫生巾", 8.8, 10.9, 8]],
            )

            dm = DataManager(tmpdir)
            pid = dm.create_project(
                "multi search",
                {"path": str(main_path), "store_name": "主店"},
                [{"path": str(comp_path), "store_name": "竞店"}],
            )
            dm.import_project_sources(pid)
            dm.activate_project(pid)

            main_result = dm.get_main_products_page(search="护舒宝 考拉安睡裤 L码", project_id=pid)
            grid_result = dm.get_paginated_grid(search="护舒宝 考拉安睡裤 L码")
            unlinked_result = dm.get_unlinked_pool_page(search="护舒宝 考拉安睡裤 L码")
            missing_result = dm.get_main_products_page(search="护舒宝 考拉安睡裤 M码", project_id=pid)

            self.assertEqual(main_result["total"], 1)
            self.assertEqual(main_result["items"][0]["商品名称"], target_name)
            self.assertEqual(grid_result["total"], 1)
            self.assertEqual(grid_result["items"][0]["商品名称"], target_name)
            self.assertEqual(unlinked_result["total"], 1)
            self.assertEqual(unlinked_result["items"][0]["0商品名称"], target_name)
            self.assertEqual(missing_result["total"], 0)

    def test_main_import_keeps_same_sku_with_different_name_or_spec(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            main_path = tmp / "main.xlsx"
            comp_path = tmp / "comp.xlsx"
            headers = [
                "skuid",
                "商品名称",
                "规格",
                "图片",
                "条码",
                "一级类目",
                "二级类目",
                "三级类目",
                "活动价",
                "美团外卖渠道售价",
                "销量",
            ]
            make_workbook(
                main_path,
                headers,
                [
                    ["1001", "吊带背心", "白色L", "https://img.test/a.webp", "6901", "服饰鞋包", "女装", "女士吊带/背心", 10, 12, 1],
                    ["1001", "吊带背心", "黑色L", "https://img.test/b.webp", "6902", "服饰鞋包", "女装", "女士吊带/背心", 10, 12, 1],
                    ["1001", "吊带背心", "黑色L", "https://img.test/c.webp", "6903", "服饰鞋包", "女装", "女士吊带/背心", 10, 12, 1],
                    ["1002", "矿泉水", "500ml", "https://img.test/d.webp", "6904", "饮品", "饮用水", "天然矿泉水", 3, 5, 2],
                ],
            )
            make_workbook(comp_path, headers, [])

            dm = DataManager(tmpdir)
            pid = dm.create_project(
                "identity",
                {"path": str(main_path), "store_name": "主店"},
                [{"path": str(comp_path), "store_name": "竞店"}],
            )
            dm.import_project_sources(pid)

            with dm._get_conn() as conn:
                rows = conn.execute(
                    """
                    SELECT skuId, 商品名称, 规格名称
                    FROM main_products
                    WHERE project_id = ?
                    ORDER BY _row_orig_idx
                    """,
                    (pid,),
                ).fetchall()

            self.assertEqual(rows, [
                ("1001", "吊带背心", "白色L"),
                ("1001", "吊带背心", "黑色L"),
                ("1002", "矿泉水", "500ml"),
            ])

    def test_comp_import_uses_same_sku_name_spec_identity_as_main(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            main_path = tmp / "main.xlsx"
            comp_path = tmp / "comp.xlsx"
            headers = [
                "skuid",
                "商品名称",
                "规格",
                "图片",
                "条码",
                "一级类目",
                "二级类目",
                "三级类目",
                "活动价",
                "美团外卖渠道售价",
                "销量",
            ]
            make_workbook(main_path, headers, [["m1", "主商品", "1个", "", "", "饮品", "饮用水", "天然矿泉水", 3, 5, 1]])
            make_workbook(
                comp_path,
                headers,
                [
                    ["c1", "吊带背心", "白色L", "https://img.test/a.webp", "6901", "服饰鞋包", "女装", "女士吊带/背心", 10, 12, 1],
                    ["c1", "吊带背心", "黑色L", "https://img.test/b.webp", "6902", "服饰鞋包", "女装", "女士吊带/背心", 10, 12, 1],
                    ["c1", "吊带背心", "黑色L", "https://img.test/c.webp", "6903", "服饰鞋包", "女装", "女士吊带/背心", 10, 12, 1],
                ],
            )

            dm = DataManager(tmpdir)
            pid = dm.create_project(
                "comp identity",
                {"path": str(main_path), "store_name": "主店"},
                [{"path": str(comp_path), "store_name": "竞店"}],
            )
            dm.import_project_sources(pid)

            with dm._get_conn() as conn:
                rows = conn.execute(
                    """
                    SELECT skuId, 商品名称, 规格名称
                    FROM comp_products
                    WHERE project_id = ? AND store_id = '0'
                    ORDER BY skuId, 商品名称, 规格名称
                    """,
                    (pid,),
                ).fetchall()

            self.assertEqual(rows, [
                ("c1", "吊带背心", "白色L"),
                ("c1", "吊带背心", "黑色L"),
            ])

    def test_comp_ignore_status_is_saved_and_clears_new_flag(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            main_path = tmp / "main.xlsx"
            comp_path = tmp / "comp.xlsx"
            headers = [
                "skuid", "商品名称", "规格", "图片", "条码",
                "一级类目", "二级类目", "三级类目", "活动价", "美团外卖渠道售价", "销量",
            ]
            make_workbook(main_path, headers, [["m1", "主商品", "1个", "", "", "饮品", "饮用水", "天然矿泉水", 3, 5, 1]])
            make_workbook(comp_path, headers, [["c1", "竞品", "1个", "", "", "饮品", "饮用水", "天然矿泉水", 3, 5, 1]])

            dm = DataManager(tmpdir)
            pid = dm.create_project(
                "ignore status",
                {"path": str(main_path), "store_name": "主店"},
                [{"path": str(comp_path), "store_name": "竞店"}],
            )
            dm.import_project_sources(pid)
            self.assertTrue(dm.mark_as_new("0", "c1", True, project_id=pid))
            self.assertTrue(dm.mark_as_ignored("0", "c1", True, project_id=pid))

            with dm._get_conn() as conn:
                row = conn.execute(
                    """
                    SELECT is_new_add, is_ignored
                    FROM comp_products
                    WHERE project_id = ? AND store_id = '0' AND skuId = 'c1'
                    """,
                    (pid,),
                ).fetchone()

            self.assertEqual(row, ("否", "是"))

    def test_main_products_schema_uses_identity_index_without_sku_only_primary_key(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dm = DataManager(tmpdir)

            with dm._get_conn() as conn:
                pk_cols = conn.execute(
                    """
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = 'main_products'::regclass AND i.indisprimary
                    ORDER BY array_position(i.indkey, a.attnum)
                    """
                ).fetchall()
                index_row = conn.execute(
                    """
                    SELECT indexdef
                    FROM pg_indexes
                    WHERE tablename = 'main_products'
                      AND indexname = 'idx_main_products_identity'
                    """
                ).fetchone()

            self.assertEqual(pk_cols, [])
            self.assertIsNotNone(index_row)
            self.assertIn('COALESCE("商品名称", \'\'::text)', index_row[0])


if __name__ == "__main__":
    unittest.main()
