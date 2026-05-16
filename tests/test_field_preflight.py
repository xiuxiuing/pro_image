import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook

from data_mgr_base import FIELD_MAPPINGS
from field_registry import detect_field_mapping
import quality_preflight


def make_workbook(path, headers, rows):
    wb = Workbook()
    ws = wb.active
    ws.append(headers)
    for row in rows:
        ws.append(row)
    wb.save(path)


class FieldPreflightTests(unittest.TestCase):
    def test_builtin_aliases_cover_required_user_fields(self):
        headers = [
            "SKUID", "菜单名", "售卖规格", "商品图片", "条码",
            "美团一级分类", "美团二级分类", "美团三级分类",
            "折扣价", "渠道价格", "销量",
        ]
        mapping = detect_field_mapping(headers)

        self.assertEqual(mapping["skuId"]["column"], "SKUID")
        self.assertEqual(mapping["商品名称"]["column"], "菜单名")
        self.assertEqual(mapping["规格名称"]["column"], "售卖规格")
        self.assertEqual(mapping["主图链接"]["column"], "商品图片")
        self.assertEqual(mapping["商品条码"]["column"], "条码")
        self.assertEqual(mapping["美团类目一级"]["column"], "美团一级分类")
        self.assertEqual(mapping["美团类目二级"]["column"], "美团二级分类")
        self.assertEqual(mapping["美团类目三级"]["column"], "美团三级分类")
        self.assertEqual(mapping["活动价"]["column"], "折扣价")
        self.assertEqual(mapping["原价"]["column"], "渠道价格")
        self.assertEqual(mapping["月销量"]["column"], "销量")

    def test_field_mappings_keep_month_sales_stored_as_sales(self):
        self.assertEqual(FIELD_MAPPINGS["月销量"], "销售")
        self.assertEqual(FIELD_MAPPINGS["销量"], "销售")
        self.assertEqual(FIELD_MAPPINGS["美团三级分类"], "美团类目三级")

    def test_preflight_blocks_missing_sku_and_name(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "bad.xlsx"
            make_workbook(path, ["规格", "图片"], [["500ml", "https://img.test/a.webp"]])

            report = quality_preflight.inspect_files([{"key": "main", "label": "主店", "path": str(path)}])

        self.assertEqual(report["level"], "block")
        messages = "\n".join(i["message"] for item in report["items"] for i in item["issues"])
        self.assertIn("skuId", messages)
        self.assertIn("商品名称", messages)

    def test_preflight_uses_user_mapping_for_similar_column(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "ok.xlsx"
            make_workbook(
                path,
                ["商品编码", "商品标题", "规格", "图片", "条码", "一级类目", "二级类目", "三级类目", "活动价", "原价", "销量"],
                [["1001", "依云水", "500ml", "https://img.test/a.webp", "690", "饮品", "水", "矿泉水", 8, 10, 3]],
            )

            report = quality_preflight.inspect_files(
                [{"key": "main", "label": "主店", "path": str(path)}],
                user_mappings={"main": {"skuId": "商品编码", "商品名称": "商品标题"}},
            )

        item = report["items"][0]
        self.assertEqual(item["mapping"]["skuId"]["column"], "商品编码")
        self.assertEqual(item["mapping"]["商品名称"]["column"], "商品标题")
        self.assertNotEqual(report["level"], "block")

    def test_quality_report_summarizes_analysis_metrics(self):
        report = quality_preflight.build_quality_report(
            {"level": "warn", "items": [{"issues": [{"level": "warn", "message": "A字段覆盖率偏低"}]}]},
            {
                "sources": [
                    {
                        "download": {"total": 10, "success": 8},
                        "image_index": {"total": 10, "vectors": 7},
                        "text_index": {"total": 10, "vectors": 10},
                    }
                ],
                "query": {
                    "download": {"total": 5, "success": 5},
                    "image_vectors": {"total": 5, "vectors": 4},
                    "text_vectors": {"total": 5, "vectors": 5},
                },
                "matching": {"sources": [{"matched": 6, "rule_rejected": 3, "vector_candidates": 12}]},
            },
        )

        self.assertEqual(report["summary"]["download_success"], 13)
        self.assertEqual(report["summary"]["download_total"], 15)
        self.assertEqual(report["summary"]["matched"], 6)
        self.assertEqual(report["summary"]["rule_rejected"], 3)
        self.assertIn("A字段覆盖率偏低", report["warnings"])

    def test_enabled_rule_field_low_coverage_requires_confirmation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "rules.xlsx"
            make_workbook(
                path,
                ["SKUID", "商品名称", "规格", "图片", "条码", "一级类目", "二级类目", "三级类目", "活动价", "原价", "销量", "A售卖数量"],
                [["1001", "依云水", "500ml", "https://img.test/a.webp", "690", "饮品", "水", "矿泉水", 8, 10, 3, ""]],
            )

            report = quality_preflight.inspect_files(
                [{"key": "main", "label": "主店", "path": str(path)}],
                rule_template={"rule_groups": [{"metrics": {"sell": {"en": True}}}]},
            )

        self.assertEqual(report["level"], "confirm")
        messages = "\n".join(i["message"] for item in report["items"] for i in item["issues"])
        self.assertIn("当前规则启用字段 A售卖数量", messages)


if __name__ == "__main__":
    unittest.main()
