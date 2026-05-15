import ast
import tempfile
import unittest
from pathlib import Path

from app_ops_extra import _compiled_module_glob
from packaging_core import BUSINESS_SOURCE_FILES, CORE_NUITKA_MODULES, RESOURCE_DIRS


ROOT = Path(__file__).resolve().parents[1]


class NuitkaCorePackagingTests(unittest.TestCase):
    def test_core_and_business_packaging_files_exist(self):
        for mod in CORE_NUITKA_MODULES:
            self.assertTrue((ROOT / f"{mod}.py").exists(), mod)
        for rel in BUSINESS_SOURCE_FILES:
            self.assertTrue((ROOT / rel).exists(), rel)
        for rel in RESOURCE_DIRS:
            self.assertTrue((ROOT / rel).is_dir(), rel)

    def test_nuitka_specs_exclude_only_core_modules(self):
        for spec_name in ("ProImage_nuitka_macOS.spec", "ProImage_nuitka_Windows.spec"):
            text = (ROOT / spec_name).read_text(encoding="utf-8")
            self.assertIn("from packaging_core import CORE_NUITKA_MODULES", text)
            self.assertIn("excludes=list(CORE_NUITKA_MODULES)", text)
            self.assertNotIn("'data_mgr'", text)
            self.assertNotIn("'app_ops'", text)

    def test_ops_tools_package_steps_use_nuitka_core_flow(self):
        tree = ast.parse((ROOT / "app_ops_extra.py").read_text(encoding="utf-8"))
        steps = None
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "steps":
                        try:
                            steps = ast.literal_eval(node.value)
                        except Exception:
                            pass
        self.assertEqual(
            ["检查环境", "Nuitka 编译核心", "准备打包目录", "PyInstaller 打包", "验证产物", "压缩产物"],
            steps,
        )

    def test_ops_tools_copy_mentions_nuitka_and_not_pyarmor(self):
        text = (ROOT / "templates" / "ops_tools.html").read_text(encoding="utf-8")
        self.assertIn("Nuitka 编译核心算法模块", text)
        self.assertIn("ProImage_nuitka_*", text)
        self.assertNotIn("PyArmor gen -O dist/obfuscated", text)

    def test_default_rule_template_resource_is_packaged_data(self):
        self.assertTrue(
            (ROOT / "data" / "default_rule_templates" / "production_rule_v1.json").exists()
        )

    def test_compiled_module_glob_is_platform_specific(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "main_030822.cpython-312-darwin.so").write_text("")
            (root / "main_030822.cp312-win_amd64.pyd").write_text("")

            self.assertEqual(
                [str(root / "main_030822.cp312-win_amd64.pyd")],
                _compiled_module_glob(str(root), "main_030822", "windows"),
            )
            self.assertEqual(
                [str(root / "main_030822.cpython-312-darwin.so")],
                _compiled_module_glob(str(root), "main_030822", "macos"),
            )


if __name__ == "__main__":
    unittest.main()
