import json
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


class MarketAnalysisTests(unittest.TestCase):
    def test_market_category_bucket_reference_keeps_other_under_five_percent(self):
        path = Path(__file__).resolve().parents[1] / "data" / "market_category_buckets.json"
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        buckets = data["buckets"]
        self.assertIn("饮品", buckets["snack"])
        self.assertIn("家居日用", buckets["department_store"])
        self.assertIn("店铺管理", buckets["other"])
        self.assertLessEqual(float(data["sample_distribution"]["other_pct"]), 5.0)

    def test_market_analysis_uses_file_dedupe_l1_top10_and_top1_diff(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
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
                "原价",
                "销量",
            ]
            main_path = tmp / "main.xlsx"
            comp1_path = tmp / "comp1.xlsx"
            comp2_path = tmp / "comp2.xlsx"
            make_workbook(
                main_path,
                headers,
                [
                    ["m1", "可乐", "500ml", "u", "1", "饮品", "饮料", "碳酸饮料", 10, 12, 45],
                    ["m2", "可乐", "500ml重复", "u", "2", "饮品", "饮料", "碳酸饮料", 99, 99, 999],
                    ["m3", "袜子", "1双", "u", "3", "服饰鞋包", "配饰", "袜子", 2, 3, 90],
                    ["m4", "未知品", "1个", "u", "4", "神秘类目", "神秘", "神秘", 1, 2, 45],
                ],
            )
            make_workbook(
                comp1_path,
                headers,
                [
                    ["c11", "橙汁", "1L", "u", "5", "饮品", "饮料", "果汁", 20, 25, 90],
                    ["c12", "收纳盒", "1个", "u", "6", "家居日用", "收纳", "收纳盒", 5, 8, 90],
                ],
            )
            make_workbook(
                comp2_path,
                headers,
                [
                    ["c21", "辣条", "1袋", "u", "7", "休闲食品", "辣条", "面筋制品", 5, 8, 180],
                    ["c22", "矿泉水", "500ml", "u", "8", "饮品", "水", "天然矿泉水", 4, 5, 45],
                ],
            )

            dm = DataManager(tmpdir)
            pid = dm.create_project(
                "market",
                {"path": str(main_path), "store_name": "主店"},
                [
                    {"path": str(comp1_path), "store_name": "竞店1"},
                    {"path": str(comp2_path), "store_name": "竞店2"},
                ],
            )
            dm.import_project_sources(pid)

            with sqlite3.connect(tmp / "pro_image.db") as conn:
                row = conn.execute(
                    """
                    SELECT statistics_json, market_analysis_json, status
                    FROM project_analysis_snapshots
                    WHERE project_id = ?
                    """,
                    (pid,),
                ).fetchone()
            self.assertIsNotNone(row)
            self.assertIn("tabs", json.loads(row[0]))
            self.assertIn("top10_categories", json.loads(row[1]))
            self.assertEqual(row[2], "ready")

            data = dm.get_market_analysis()

        self.assertEqual(data["status"], "ok")
        self.assertEqual(data["file_count"], 3)
        self.assertEqual(data["top10_categories"][0]["category"], "饮品")
        self.assertAlmostEqual(data["top10_categories"][0]["sales_amount"], 810.0)
        self.assertAlmostEqual(data["top10_categories"][0]["order_count"], 60.0)
        self.assertEqual(data["top10_categories"][1]["category"], "休闲食品")
        self.assertEqual(data["recommendation"]["level"], "拉完了")
        self.assertAlmostEqual(data["recommendation"]["total_sales_amount"], 1335.0)
        self.assertAlmostEqual(data["recommendation"]["snack_ratio"], 83.15)
        self.assertAlmostEqual(data["recommendation"]["department_store_ratio"], 15.73)
        self.assertAlmostEqual(data["recommendation"]["other_ratio"], 1.12)
        self.assertEqual(data["top1_file"]["file_name"], "竞店1")
        self.assertAlmostEqual(data["metrics"]["average"]["monthly_orders"], 43.33)
        self.assertAlmostEqual(data["metrics"]["top1"]["monthly_sales_amount"], 2250.0)
        self.assertEqual(data["metric_diffs"]["monthly_sales_amount"], -915)

    def test_deleted_snapshot_is_rebuilt_on_api_read(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            headers = [
                "skuid", "商品名称", "规格", "图片", "条码",
                "一级类目", "二级类目", "三级类目", "活动价", "原价", "销量",
            ]
            main_path = tmp / "main.xlsx"
            comp_path = tmp / "comp.xlsx"
            make_workbook(main_path, headers, [["m1", "可乐", "500ml", "u", "1", "饮品", "饮料", "碳酸饮料", 10, 12, 9]])
            make_workbook(comp_path, headers, [["c1", "袜子", "1双", "u", "2", "服饰鞋包", "配饰", "袜子", 2, 3, 9]])

            dm = DataManager(tmpdir)
            pid = dm.create_project(
                "snapshot",
                {"path": str(main_path), "store_name": "主店"},
                [{"path": str(comp_path), "store_name": "竞店"}],
            )
            dm.import_project_sources(pid)
            with sqlite3.connect(tmp / "pro_image.db") as conn:
                conn.execute("DELETE FROM project_analysis_snapshots WHERE project_id = ?", (pid,))
                conn.commit()

            stats = dm.get_statistics()
            market = dm.get_market_analysis()

            with sqlite3.connect(tmp / "pro_image.db") as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM project_analysis_snapshots WHERE project_id = ? AND status = 'ready'",
                    (pid,),
                ).fetchone()[0]

        self.assertEqual(count, 1)
        self.assertIn("tabs", stats)
        self.assertEqual(market["status"], "ok")

    def test_market_level_boundaries(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dm = DataManager(tmpdir)
            self.assertEqual(dm._market_level_for_sales(400000), "夯")
            self.assertEqual(dm._market_level_for_sales(300000), "顶级")
            self.assertEqual(dm._market_level_for_sales(200000), "人上人")
            self.assertEqual(dm._market_level_for_sales(100000), "NPC")
            self.assertEqual(dm._market_level_for_sales(99999), "拉完了")


if __name__ == "__main__":
    unittest.main()
