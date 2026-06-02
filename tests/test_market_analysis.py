import json
import tempfile
import time
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


class MarketAnalysisTests(unittest.TestCase):
    def setUp(self):
        db = Database()
        try:
            with db.engine.begin() as conn:
                conn.execute(text("DROP SCHEMA public CASCADE"))
                conn.execute(text("CREATE SCHEMA public"))
        finally:
            db.close()

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
            dm.activate_project(pid)

            with dm._get_conn() as conn:
                row = conn.execute(
                    """
                    SELECT statistics_json, market_analysis_json, workbench_summary_json, status
                    FROM project_analysis_snapshots
                    WHERE project_id = ?
                    """,
                    (pid,),
                ).fetchone()
            self.assertIsNotNone(row)
            self.assertIn("tabs", json.loads(row[0]))
            self.assertIn("top10_categories", json.loads(row[1]))
            self.assertEqual(json.loads(row[2])["stores"]["0"]["total"]["sku_count"], 2)
            self.assertEqual(row[3], "ready")

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
    def test_activity_price_falls_back_to_channel_sale_when_blank(self):
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
                "美团外卖渠道售价",
                "销量",
            ]
            main_path = tmp / "main.xlsx"
            comp_path = tmp / "comp.xlsx"
            make_workbook(main_path, headers, [["m1", "可乐", "500ml", "u", "1", "饮品", "饮料", "碳酸饮料", "", 12, 10]])
            make_workbook(comp_path, headers, [["c1", "可乐A", "500ml", "u", "2", "饮品", "饮料", "碳酸饮料", "", 8, 5]])

            dm = DataManager(tmpdir)
            pid = dm.create_project(
                "fallback price",
                {"path": str(main_path), "store_name": "主店"},
                [{"path": str(comp_path), "store_name": "竞店"}],
            )
            dm.import_project_sources(pid)
            dm.activate_project(pid)

            stats = dm.get_statistics()
            market = dm.get_market_analysis()

        self.assertAlmostEqual(stats["tabs"][0]["items"][0]["summary"]["sales_amount"]["main"], 120.0)
        self.assertAlmostEqual(market["top10_categories"][0]["sales_amount"], 80.0)
        self.assertAlmostEqual(market["recommendation"]["total_sales_amount"], 80.0)
        self.assertAlmostEqual(market["metrics"]["average"]["monthly_sales_amount"], 80.0)


    def test_competitor_detail_contribution_uses_each_store_total_sales_amount(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            headers = [
                "skuid", "商品名称", "规格", "图片", "条码",
                "一级类目", "二级类目", "三级类目", "活动价", "原价", "销量",
            ]
            main_path = tmp / "main.xlsx"
            comp_a_path = tmp / "comp_a.xlsx"
            comp_b_path = tmp / "comp_b.xlsx"
            make_workbook(
                main_path,
                headers,
                [
                    ["m1", "主A", "1个", "u", "1", "饮品", "饮料", "类目A", 10, 10, 5],
                    ["m2", "主B", "1个", "u", "2", "饮品", "饮料", "类目B", 10, 10, 5],
                ],
            )
            make_workbook(
                comp_a_path,
                headers,
                [
                    ["a1", "竞A-类目A", "1个", "u", "3", "饮品", "饮料", "类目A", 10, 10, 8],
                    ["a2", "竞A-类目B", "1个", "u", "4", "饮品", "饮料", "类目B", 10, 10, 2],
                ],
            )
            make_workbook(
                comp_b_path,
                headers,
                [
                    ["b1", "竞B-类目A", "1个", "u", "5", "饮品", "饮料", "类目A", 10, 10, 4],
                    ["b2", "竞B-类目B", "1个", "u", "6", "饮品", "饮料", "类目B", 10, 10, 6],
                ],
            )

            dm = DataManager(tmpdir)
            pid = dm.create_project(
                "contribution",
                {"path": str(main_path), "store_name": "主店"},
                [
                    {"path": str(comp_a_path), "store_name": "A"},
                    {"path": str(comp_b_path), "store_name": "B"},
                ],
            )
            dm.import_project_sources(pid)
            dm.activate_project(pid)
            stats = dm.get_statistics()

        main_tab = next(tab for tab in stats["tabs"] if tab["id"] == "main")
        item_a = next(item for item in main_tab["items"] if item["category"] == "类目A")
        comp_a, comp_b = item_a["competitors"]

        self.assertAlmostEqual(comp_a["metrics"]["category_contribution"], 80.0)
        self.assertAlmostEqual(comp_b["metrics"]["category_contribution"], 40.0)
        self.assertAlmostEqual(comp_a["main_diff"]["category_contribution"], -30.0)
        self.assertAlmostEqual(comp_b["main_diff"]["category_contribution"], 10.0)


    def test_competitor_unique_tab_totals_use_full_store_data(self):
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
                [["m1", "主店重合品", "1个", "u", "1", "饮品", "饮料", "重合类目", 10, 10, 1]],
            )
            make_workbook(
                comp_path,
                headers,
                [
                    ["c1", "竞店重合品", "1个", "u", "2", "饮品", "饮料", "重合类目", 10, 10, 10],
                    ["c2", "竞店独有品", "1个", "u", "3", "饮品", "饮料", "独有类目", 10, 10, 5],
                ],
            )

            dm = DataManager(tmpdir)
            pid = dm.create_project(
                "competitor totals",
                {"path": str(main_path), "store_name": "主店"},
                [{"path": str(comp_path), "store_name": "竞店"}],
            )
            dm.import_project_sources(pid)
            dm.activate_project(pid)
            stats = dm.get_statistics()

        comp_tab = next(tab for tab in stats["tabs"] if tab["id"] == "comp-0")
        self.assertEqual([item["category"] for item in comp_tab["items"]], ["独有类目"])
        self.assertAlmostEqual(comp_tab["totals"]["sales"], 15.0)
        self.assertAlmostEqual(comp_tab["totals"]["sales_amount"], 150.0)
        self.assertAlmostEqual(comp_tab["totals"]["active_rate"], 100.0)


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
            dm.activate_project(pid)
            with dm._get_conn() as conn:
                conn.execute("DELETE FROM project_analysis_snapshots WHERE project_id = ?", (pid,))

            stats = dm.get_statistics()
            market = dm.get_market_analysis()
            deadline = time.time() + 3
            count = 0
            while time.time() < deadline:
                with dm._get_conn() as conn:
                    count = conn.execute(
                        "SELECT COUNT(*) FROM project_analysis_snapshots WHERE project_id = ? AND status = 'ready'",
                        (pid,),
                    ).fetchone()[0]
                if count:
                    break
                time.sleep(0.05)

        self.assertEqual(count, 1)
        self.assertIn("tabs", stats)
        self.assertIn(market["snapshot_status"], ("building", "missing", "ready"))

    def test_workbench_summary_is_snapshotted_and_rebuilt_after_link_change(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            headers = [
                "skuid", "商品名称", "规格", "图片", "条码",
                "一级类目", "二级类目", "三级类目", "活动价", "原价", "销量",
            ]
            main_path = tmp / "main.xlsx"
            comp_path = tmp / "comp.xlsx"
            make_workbook(main_path, headers, [["m1", "可乐", "500ml", "u", "1", "饮品", "饮料", "碳酸饮料", 10, 12, 9]])
            make_workbook(comp_path, headers, [["c1", "可乐", "500ml", "u", "2", "饮品", "饮料", "碳酸饮料", 9, 10, 8]])

            dm = DataManager(tmpdir)
            pid = dm.create_project(
                "workbench summary",
                {"path": str(main_path), "store_name": "主店"},
                [{"path": str(comp_path), "store_name": "竞店"}],
            )
            dm.import_project_sources(pid)
            dm.activate_project(pid)

            before = dm.get_workbench_summary()
            self.assertEqual(before["stores"]["0"]["linked"]["sku_count"], 0)
            self.assertEqual(before["stores"]["0"]["unlinked"]["sku_count"], 1)

            dm.manual_link("m1", "0", "c1", project_id=pid)
            with dm._get_conn() as conn:
                row = conn.execute(
                    "SELECT workbench_summary_json FROM project_analysis_snapshots WHERE project_id = ?",
                    (pid,),
                ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(json.loads(row[0])["stores"]["0"]["linked"]["sku_count"], 1)

            after = dm.get_workbench_summary()
            self.assertEqual(after["stores"]["0"]["linked"]["sku_count"], 1)
            self.assertEqual(after["stores"]["0"]["unlinked"]["sku_count"], 0)

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
