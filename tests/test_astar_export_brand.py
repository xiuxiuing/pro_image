import os
import tempfile
import unittest
from unittest import mock

import pandas as pd

import extract_info_ai2
from extract_info_schema import ProductInfo


class AstarExportBrandTests(unittest.TestCase):
    def test_process_file_ai_writes_brand_column(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "source.xlsx")
            pd.DataFrame(
                [
                    {
                        "skuId": "1",
                        "商品名称": "得力 透明固体胶 8g/个",
                        "规格名称": "8g/个",
                    }
                ]
            ).to_excel(path, index=False)

            result = ProductInfo(
                net_content="8g",
                sell_quantity="1",
                packaging_unit="个",
                brand="得力",
                core_category="固体胶",
                product_form="棒状",
            )
            with mock.patch.object(extract_info_ai2, "extract_batch_ai", return_value=[result]):
                extract_info_ai2.process_file_ai(path, api_key="dummy", batch_size=10)

            out = pd.read_excel(path).fillna("")
            self.assertIn("A品牌", out.columns)
            self.assertEqual(out.loc[0, "A品牌"], "得力")
            self.assertIn("A核心品类", out.columns)
            self.assertEqual(out.loc[0, "A核心品类"], "固体胶")


if __name__ == "__main__":
    unittest.main()
