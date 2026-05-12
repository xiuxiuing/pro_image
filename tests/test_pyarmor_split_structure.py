import ast
import glob
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAX_PYARMOR_SOURCE_BYTES = 25_000


def pyarmor_files():
    tree = ast.parse((ROOT / "app_ops.py").read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "_OPS_PYARMOR_FILES":
                    return ast.literal_eval(node.value)
    raise AssertionError("_OPS_PYARMOR_FILES not found")


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


class PyArmorSplitStructureTests(unittest.TestCase):
    def test_pyarmor_file_manifest_covers_runtime_modules(self):
        required = {
            "app.py",
            "app_ops.py",
            "app_ops_extra.py",
            "app_data.py",
            "data_mgr.py",
            "data_mgr_base.py",
            "data_mgr_import.py",
            "data_mgr_query.py",
            "data_mgr_ops.py",
            "data_mgr_export.py",
            "data_mgr_rule_templates.py",
            "license_utils.py",
            "main_030822.py",
            "extract_info_ai2.py",
            "product_text_extract.py",
            "post_match_engine.py",
            "utils.py",
            "merge_sku_data.py",
        }

        self.assertTrue(required.issubset(set(pyarmor_files())))

    def test_pyarmor_source_files_are_small_enough_for_free_tier(self):
        too_large = []
        for rel in pyarmor_files():
            path = ROOT / rel
            self.assertTrue(path.exists(), f"{rel} is listed for PyArmor but does not exist")
            size = len(path.read_bytes())
            if size > MAX_PYARMOR_SOURCE_BYTES:
                too_large.append((rel, size))

        self.assertEqual([], too_large)

    def test_pyinstaller_specs_hiddenimports_cover_split_modules(self):
        failures = {}
        for spec_name in ("ProImage_Windows.spec", "ProImage_macOS.spec"):
            spec = (ROOT / spec_name).read_text(encoding="utf-8")
            missing = []
            for rel in pyarmor_files():
                module = Path(rel).with_suffix("").name
                if module == "app":
                    continue
                if f"'{module}'" not in spec and f'"{module}"' not in spec:
                    missing.append(module)
            if missing:
                failures[spec_name] = missing

        self.assertEqual({}, failures)

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
        }

        self.assertEqual(expected, route_rules(["app_ops.py", "app_ops_*.py"]))

    def test_data_routes_stay_on_the_same_urls_after_split(self):
        expected = {
            "/api/projects",
            "/api/projects/<int:pid>",
            "/api/projects/<int:pid>/activate",
            "/api/projects/<int:pid>/progress",
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
            "/api/store_products/<store_id>",
            "/api/unlinked_items",
            "/api/main_products",
            "/api/eliminate",
            "/api/toggle_handled",
            "/api/toggle_ref",
            "/api/toggle_add",
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


if __name__ == "__main__":
    unittest.main()
