import ast
import glob
import importlib
import unittest
from pathlib import Path

from packaging_core import BUSINESS_SOURCE_FILES, CORE_NUITKA_MODULES


ROOT = Path(__file__).resolve().parents[1]


def route_rules(patterns):
    rules = set()
    for pattern in patterns:
        for path in glob.glob(str(ROOT / pattern)):
            tree = ast.parse(Path(path).read_text(encoding="utf-8"), filename=path)
            for node in ast.walk(tree):
                if not isinstance(node, ast.FunctionDef):
                    continue
                for deco in node.decorator_list:
                    call = deco if isinstance(deco, ast.Call) else None
                    if not call or not isinstance(call.func, ast.Attribute) or call.func.attr != "route":
                        continue
                    if call.args and isinstance(call.args[0], ast.Constant):
                        rules.add(str(call.args[0].value))
    return rules


class PackagingStructureTests(unittest.TestCase):
    def test_tools_package_manifest_covers_current_runtime_modules(self):
        expected_business = {
            "app.py",
            "auth_manager.py",
            "app_ops.py",
            "app_ops_tasks.py",
            "app_ops_extra.py",
            "app_data.py",
            "app_data_projects.py",
            "app_data_rules.py",
            "app_data_grid.py",
            "data_mgr.py",
            "data_mgr_base.py",
            "data_mgr_import.py",
            "data_mgr_query.py",
            "data_mgr_query_unlinked.py",
            "data_mgr_ops.py",
            "data_mgr_export.py",
            "data_mgr_rule_templates.py",
            "field_registry.py",
            "quality_preflight.py",
            "packaging_core.py",
        }
        expected_core = {
            "license_utils",
            "main_030822",
            "extract_info_ai2",
            "extract_info_schema",
            "extract_info_rules",
            "product_text_extract",
            "post_match_engine",
            "utils",
            "merge_sku_data",
        }

        self.assertTrue(expected_business.issubset(set(BUSINESS_SOURCE_FILES)))
        self.assertTrue(expected_core.issubset(set(CORE_NUITKA_MODULES)))

    def test_nuitka_specs_collect_business_shell_and_exclude_core_modules(self):
        for spec_name in ("ProImage_nuitka_Windows.spec", "ProImage_nuitka_macOS.spec"):
            spec = (ROOT / spec_name).read_text(encoding="utf-8")
            self.assertIn("from packaging_core import CORE_NUITKA_MODULES", spec)
            self.assertIn("excludes=list(CORE_NUITKA_MODULES)", spec)
            self.assertIn("os.path.join(_src, 'data')", spec)
            self.assertIn("os.path.join(_src, 'models')", spec)
            for rel in BUSINESS_SOURCE_FILES:
                module = Path(rel).with_suffix("").name
                if module == "app":
                    continue
                self.assertNotIn(f"'{module}'", spec)
                self.assertNotIn(f'"{module}"', spec)

    def test_ops_routes_stay_on_the_same_urls_after_split(self):
        expected = {
            "/ops-tools",
            "/api/ops/astar-extract",
            "/api/ops/output-generate",
            "/api/ops/tasks/<task_id>/progress",
            "/api/ops/tasks/<task_id>/download",
            "/api/ops/license-key-status",
            "/api/ops/license-generate",
            "/api/ops/package-build",
            "/api/ops/market-analysis-generate",
        }

        self.assertEqual(expected, route_rules(["app_ops.py", "app_ops_*.py"]))

    def test_data_routes_stay_on_the_same_urls_after_split(self):
        expected = {
            "/api/projects",
            "/api/projects/<int:pid>",
            "/api/projects/<int:pid>/activate",
            "/api/projects/<int:pid>/analyze",
            "/api/projects/<int:pid>/preflight",
            "/api/projects/<int:pid>/progress",
            "/api/projects/<int:pid>/quality-report",
            "/api/rule-templates",
            "/api/rule-templates/<int:tid>",
            "/api/rule-category-template",
            "/api/history",
            "/api/projects/switch",
            "/api/project_data",
            "/api/sku_detail/<main_sku_id>",
            "/api/rule-categories/parse",
            "/api/rule-categories/default",
            "/api/rule-categories/bucket-tags",
            "/api/config",
            "/api/grid_data",
            "/api/statistics",
            "/api/statistics/snapshot-status",
            "/api/market-analysis",
            "/api/statistics/products",
            "/api/statistics/export",
            "/api/store_products/<store_id>",
            "/api/unlinked_items",
            "/api/main_products",
            "/api/main_products/<path:main_sku_id>/links",
            "/api/eliminate",
            "/api/toggle_handled",
            "/api/toggle_ref",
            "/api/toggle_add",
            "/api/toggle_ignore",
            "/api/price_match",
            "/api/clear_price_match",
            "/api/manual_link",
            "/api/unlink",
            "/api/update_cell",
            "/img/<path:filename>",
            "/api/export",
            "/api/export_new",
        }

        self.assertEqual(expected, route_rules(["app_data.py", "app_data_*.py"]))

    def test_data_blueprints_initialize_on_current_flask(self):
        import app_data

        app_data = importlib.reload(app_data)
        noop = lambda *args, **kwargs: None
        app_data.init_data(
            object(),
            noop,
            noop,
            noop,
            noop,
            lambda _pid: {},
            noop,
            lambda filename, fallback: filename or fallback,
            "",
            "",
            "",
            "",
            "",
        )

        names = [bp.name for bp in app_data.get_data_blueprints()]
        self.assertIn(names, (["data"], ["data_projects", "data_rules", "data_grid"]))


if __name__ == "__main__":
    unittest.main()
