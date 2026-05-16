import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from data_mgr import DataManager


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_RULE_TEMPLATE_NAME = "生产规则V1"
EXPECTED_PRODUCTION_RULE_PATH_COUNT = 1294
EXPECTED_PRODUCTION_RULE_INCREMENT_COUNT = 428
EXPECTED_PRODUCTION_RULE_INCREMENT_GROUP_COUNTS = {
    "A_food_drink": 167,
    "B_care_clean_beauty": 27,
    "C_apparel_size_color": 41,
    "D_daily_kitchen_stationery": 47,
    "E_digital_model": 70,
    "F_sensitive_goods": 76,
}


class BuiltinRuleTemplateTests(unittest.TestCase):
    def _template_row(self, db_path, name=PRODUCTION_RULE_TEMPLATE_NAME):
        with sqlite3.connect(db_path) as conn:
            return conn.execute(
                "SELECT id, name, description, config_json FROM rule_templates WHERE name = ?",
                (name,),
            ).fetchone()

    def test_empty_database_seeds_production_rule_v1_and_uses_it_for_new_projects(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dm = DataManager(tmpdir)
            row = self._template_row(Path(tmpdir) / "pro_image.db")

            self.assertIsNotNone(row)
            config = json.loads(row[3])
            self.assertGreater(len(config.get("rule_groups") or []), 0)
            self.assertGreater(
                len(dm.get_post_match_template_for_project(1).get("rule_groups") or []),
                0,
            )

            main = {"path": str(Path(tmpdir) / "main.xlsx"), "store_name": "主店"}
            comps = [{"path": str(Path(tmpdir) / "comp.xlsx"), "store_name": "竞店"}]
            pid = dm.create_project("新项目", main, comps)

            with sqlite3.connect(Path(tmpdir) / "pro_image.db") as conn:
                project_tid = conn.execute(
                    "SELECT rule_template_id FROM projects WHERE id = ?", (pid,)
                ).fetchone()[0]

            self.assertEqual(project_tid, row[0])

    def test_existing_production_rule_v1_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "pro_image.db"
            custom_config = '{"v":3,"rule_groups":[{"id":"custom","name":"用户自定义","categories":{"paths":[]},"metrics":{}}]}'
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE rule_templates (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        description TEXT DEFAULT '',
                        config_json TEXT NOT NULL DEFAULT '{}',
                        created_at TEXT,
                        updated_at TEXT
                    )
                    """
                )
                conn.execute(
                    "INSERT INTO rule_templates (name, description, config_json) VALUES (?, ?, ?)",
                    (PRODUCTION_RULE_TEMPLATE_NAME, "用户改过的规则", custom_config),
                )

            DataManager(tmpdir)
            row = self._template_row(db_path)

            self.assertEqual(row[2], "用户改过的规则")
            self.assertEqual(row[3], custom_config)

    def test_existing_database_without_production_rule_v1_gets_seeded_without_reordering(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "pro_image.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE rule_templates (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        description TEXT DEFAULT '',
                        config_json TEXT NOT NULL DEFAULT '{}',
                        created_at TEXT,
                        updated_at TEXT
                    )
                    """
                )
                conn.execute(
                    "INSERT INTO rule_templates (name, description, config_json) VALUES (?, ?, ?)",
                    ("老模板", "保留", '{"v":3,"rule_groups":[]}'),
                )

            DataManager(tmpdir)

            with sqlite3.connect(db_path) as conn:
                rows = conn.execute(
                    "SELECT id, name FROM rule_templates ORDER BY id ASC"
                ).fetchall()

            self.assertEqual(rows[0], (1, "老模板"))
            self.assertEqual(rows[1][1], PRODUCTION_RULE_TEMPLATE_NAME)

    def test_packaged_builtin_rule_template_resource_exists_under_data(self):
        path = ROOT / "data" / "default_rule_templates" / "production_rule_v1.json"

        self.assertTrue(path.exists())
        data = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(data["name"], PRODUCTION_RULE_TEMPLATE_NAME)
        self.assertGreater(len(data["config"].get("rule_groups") or []), 0)
        for spec_name in ("ProImage_Windows.spec", "ProImage_macOS.spec"):
            spec = (ROOT / spec_name).read_text(encoding="utf-8")
            self.assertIn("('data', 'data')", spec)

    def test_production_rule_v1_increment_paths_are_merged_and_color_is_disabled(self):
        template_path = ROOT / "data" / "default_rule_templates" / "production_rule_v1.json"
        increment_path = ROOT / "data" / "default_rule_templates" / "production_rule_v1_increment_2026_05.json"

        template = json.loads(template_path.read_text(encoding="utf-8"))
        increment = json.loads(increment_path.read_text(encoding="utf-8"))

        groups = template["config"]["rule_groups"]
        all_paths = [
            (p["l1"], p["l2"], p["l3"])
            for group in groups
            for p in group["categories"]["paths"]
        ]
        increment_paths = {
            (item["l1"], item["l2"], item["l3"])
            for item in increment["items"]
        }

        self.assertEqual(len(all_paths), EXPECTED_PRODUCTION_RULE_PATH_COUNT)
        self.assertEqual(len(set(all_paths)), EXPECTED_PRODUCTION_RULE_PATH_COUNT)
        self.assertEqual(
            increment["increment_path_count"],
            EXPECTED_PRODUCTION_RULE_INCREMENT_COUNT,
        )
        self.assertEqual(
            increment["group_counts"],
            EXPECTED_PRODUCTION_RULE_INCREMENT_GROUP_COUNTS,
        )
        self.assertEqual(len(increment_paths), EXPECTED_PRODUCTION_RULE_INCREMENT_COUNT)
        self.assertTrue(increment_paths.issubset(set(all_paths)))
        self.assertTrue(
            all(not group["metrics"].get("color", {}).get("en") for group in groups)
        )


if __name__ == "__main__":
    unittest.main()
