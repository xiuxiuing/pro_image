import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook

import utils


class ExcelTextNormalizationTests(unittest.TestCase):
    def test_clean_text_value_removes_common_excel_artifacts(self):
        self.assertEqual("天然矿泉水", utils.clean_text_value("\ufeff\u200b　天然矿泉水\u00a0"))
        self.assertEqual("依云 500ml", utils.clean_text_value(" 依云\u3000500ml "))
        self.assertEqual(24, utils.clean_text_value(24))
        self.assertIsNone(utils.clean_text_value(None))

    def test_excel_reader_cleans_headers_and_string_cells(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "dirty.xlsx"
            wb = Workbook()
            ws = wb.active
            ws.append(["\ufeff美团类目三级\u3000", "商品名称"])
            ws.append(["\u200b天然矿泉水\u00a0", " 依云\u3000500ml "])
            wb.save(path)

            rows = utils.excel_to_list_dict(path)

        self.assertEqual(["美团类目三级", "商品名称"], list(rows[0].keys()))
        self.assertEqual("天然矿泉水", rows[0]["美团类目三级"])
        self.assertEqual("依云 500ml", rows[0]["商品名称"])

    def test_sku_id_extraction_cleans_invisible_characters(self):
        self.assertEqual("5001716604150", utils.get_sku_id({"skuId": "\u200b5001716604150\u00a0"}))


if __name__ == "__main__":
    unittest.main()
