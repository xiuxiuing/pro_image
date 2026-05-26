import unittest

from extract_info_ai2 import _merge_model_with_rule_fallback
from extract_info_rules import _heuristic_product_info
from extract_info_schema import ProductInfo


class ExtractionSellSpecTests(unittest.TestCase):
    def test_heuristic_prefers_inner_piece_count_over_outer_box(self):
        item = {
            "name": "杜蕾斯 Love大胆爱吧避孕套超薄安全套 3只/盒 安全套套亲密裸入",
            "spec": "3只*1盒",
            "l1": "成人用品",
            "l2": "安全避孕",
            "l3": "避孕套/安全套",
        }

        info = _heuristic_product_info(item)

        self.assertEqual(info.sell_quantity, "3")
        self.assertEqual(info.packaging_unit, "只")

    def test_model_outer_box_quantity_is_corrected_by_rule_fallback(self):
        model_item = ProductInfo(sell_quantity="1", packaging_unit="盒", core_category="避孕套")
        source_item = {
            "name": "杜蕾斯 Love大胆爱吧避孕套超薄安全套 3只/盒 安全套套亲密裸入",
            "spec": "3只*1盒",
            "l1": "成人用品",
            "l2": "安全避孕",
            "l3": "避孕套/安全套",
        }

        merged = _merge_model_with_rule_fallback([model_item], [source_item])

        self.assertEqual(merged[0].sell_quantity, "3")
        self.assertEqual(merged[0].packaging_unit, "只")


if __name__ == "__main__":
    unittest.main()
