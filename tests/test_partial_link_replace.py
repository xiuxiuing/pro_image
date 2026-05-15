import sqlite3
import tempfile
import unittest
from pathlib import Path

from data_mgr import DataManager


class PartialLinkReplaceTests(unittest.TestCase):
    def test_category_scoped_replace_trims_main_category_names(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dm = DataManager(tmpdir)
            db_path = Path(tmpdir) / "pro_image.db"

            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "INSERT INTO main_products (project_id, skuId, 美团类目三级) VALUES (?, ?, ?)",
                    (1, "main-1", "天然矿泉水 "),
                )
                conn.execute(
                    """
                    INSERT INTO product_links
                    (project_id, main_sku_id, store_id, comp_sku_id, similarity, match_type, is_new_add)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (1, "main-1", "0", "comp-old", 0.9, "文本匹配", "否"),
                )

            dm.replace_project_links(1, None, categories=["天然矿泉水"])

            with sqlite3.connect(db_path) as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM product_links WHERE project_id = ? AND main_sku_id = ?",
                    (1, "main-1"),
                ).fetchone()[0]

            self.assertEqual(count, 0)


if __name__ == "__main__":
    unittest.main()
