import sqlite3
import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook

from data_mgr import DataManager


def make_workbook(path, headers, rows):
    wb = Workbook()
    ws = wb.active
    ws.append(headers)
    for row in rows:
        ws.append(row)
    wb.save(path)


class MainProductIdentityTests(unittest.TestCase):
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

            with sqlite3.connect(tmp / "pro_image.db") as conn:
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

    def test_existing_main_products_table_migrates_away_from_sku_only_primary_key(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "pro_image.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE main_products (
                        project_id INTEGER,
                        skuId TEXT,
                        _row_orig_idx INT,
                        商品名称 TEXT,
                        规格名称 TEXT,
                        PRIMARY KEY(project_id, skuId)
                    )
                    """
                )
                conn.execute(
                    "INSERT INTO main_products (project_id, skuId, _row_orig_idx, 商品名称, 规格名称) VALUES (1, '1001', 0, '旧商品', '白色')"
                )

            DataManager(tmpdir)

            with sqlite3.connect(db_path) as conn:
                sql = conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='main_products'"
                ).fetchone()[0]
                index_sql = conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_main_products_identity'"
                ).fetchone()[0]

            self.assertNotIn("PRIMARY KEY(project_id, skuId)", sql)
            self.assertIn("COALESCE(`商品名称`, '')", index_sql)


if __name__ == "__main__":
    unittest.main()
