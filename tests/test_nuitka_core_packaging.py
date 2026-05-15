import ast
import tempfile
import unittest
from pathlib import Path

from app_ops_extra import _compiled_module_glob, _missing_model_resources, _verify_nuitka_artifact
from packaging_core import BUSINESS_SOURCE_FILES, CORE_NUITKA_MODULES, REQUIRED_MODEL_FILES, RESOURCE_DIRS


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
            self.assertIn("os.path.join(_src, 'models')", text)
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

    def test_model_resource_requirements_are_checked(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            missing = _missing_model_resources(str(root))
            self.assertIn("models/dinov2-base/preprocessor_config.json", missing)

            for parts in REQUIRED_MODEL_FILES:
                path = root / "models" / Path(*parts)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("{}", encoding="utf-8")

            self.assertEqual([], _missing_model_resources(str(root)))

    def test_artifact_verification_requires_model_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            artifact = Path(tmpdir) / "ProImage_AI"
            (artifact / "_internal" / "templates").mkdir(parents=True)
            (artifact / "_internal" / "static").mkdir(parents=True)
            rule = artifact / "_internal" / "data" / "default_rule_templates" / "production_rule_v1.json"
            rule.parent.mkdir(parents=True)
            rule.write_text("{}", encoding="utf-8")
            for mod in CORE_NUITKA_MODULES:
                (artifact / "_internal" / f"{mod}.cp312-win_amd64.pyd").write_text("")

            with self.assertRaisesRegex(RuntimeError, "models"):
                _verify_nuitka_artifact("windows", str(artifact))

            for parts in REQUIRED_MODEL_FILES:
                path = artifact / "_internal" / "models" / Path(*parts)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("{}", encoding="utf-8")

            self.assertIn("验证通过", _verify_nuitka_artifact("windows", str(artifact)))


if __name__ == "__main__":
    unittest.main()
