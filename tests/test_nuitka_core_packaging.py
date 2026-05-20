import ast
import tempfile
import unittest
from pathlib import Path

from app_ops_extra import _missing_model_resources, _verify_nuitka_artifact
from packaging_core import (
    BUSINESS_SOURCE_FILES,
    BUILD_DEPENDENCY_MODULES,
    CORE_NUITKA_MODULES,
    REQUIRED_MODEL_FILES,
    RESOURCE_DIRS,
    cleanup_nuitka_module_intermediates,
    cleanup_packaging_intermediates,
    compiled_module_glob,
    disk_free_bytes,
    missing_build_modules,
    purge_stale_nuitka_modules,
    requirements_build_path,
    require_disk_space_for_zip,
    select_compiled_module,
)


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

    def test_requirements_build_lists_nuitka_and_pyinstaller(self):
        req = (ROOT / "requirements-build.txt").read_text(encoding="utf-8")
        self.assertIn("nuitka", req)
        self.assertIn("pyinstaller", req)
        self.assertTrue((ROOT / "requirements-build.txt").is_file())

    def test_disk_free_bytes_accepts_not_yet_created_zip_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = str(Path(tmpdir) / "out" / "package.zip")
            self.assertGreater(disk_free_bytes(zip_path), 0)

    def test_missing_build_modules_reports_uninstalled_only(self):
        missing = missing_build_modules()
        for import_name, _pip_name in BUILD_DEPENDENCY_MODULES:
            if import_name in missing:
                with self.assertRaises(ImportError):
                    __import__(import_name)

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
                compiled_module_glob(str(root), "main_030822", "windows"),
            )
            self.assertEqual(
                [str(root / "main_030822.cpython-312-darwin.so")],
                compiled_module_glob(str(root), "main_030822", "macos"),
            )

    def test_compiled_module_glob_prefers_current_abi_when_multiple_exist(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "utils.cp312-win_amd64.pyd").write_text("")
            (root / "utils.cp314-win_amd64.pyd").write_text("")

            self.assertEqual(
                [str(root / "utils.cp312-win_amd64.pyd")],
                compiled_module_glob(str(root), "utils", "windows"),
            )
            removed = purge_stale_nuitka_modules(str(root))
            self.assertEqual(1, removed)
            self.assertFalse((root / "utils.cp314-win_amd64.pyd").exists())
            self.assertTrue((root / "utils.cp312-win_amd64.pyd").exists())

    def test_cleanup_packaging_intermediates_removes_workspace(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "_build_src" / "app.py").parent.mkdir(parents=True)
            (root / "_build_src" / "app.py").write_text("print('x')", encoding="utf-8")
            (root / "build" / "ProImage_nuitka_Windows").mkdir(parents=True)
            (root / "build" / "ProImage_nuitka_Windows" / "warn.txt").write_text("", encoding="utf-8")
            modules = root / "nuitka_modules"
            modules.mkdir()
            (modules / "utils.build").mkdir()
            (modules / "utils.pyi").write_text("", encoding="utf-8")
            (modules / "utils.cp312-win_amd64.pyd").write_text("", encoding="utf-8")
            artifact = root / "dist" / "ProImage_AI"
            artifact.mkdir(parents=True)
            (artifact / "ProImage_AI.exe").write_text("", encoding="utf-8")

            summary = cleanup_packaging_intermediates(
                str(root), "ProImage_nuitka_Windows.spec", artifact_path=str(artifact)
            )
            self.assertFalse((root / "_build_src").exists())
            self.assertFalse((root / "build").exists())
            self.assertFalse(modules.exists())
            self.assertFalse(artifact.exists())
            joined = "".join(summary)
            self.assertIn("_build_src", joined)
            self.assertIn("nuitka_modules", joined)
            self.assertIn("ProImage_AI", joined)

    def test_select_compiled_module_returns_none_for_wrong_abi_only(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            wrong = root / "utils.cp314-win_amd64.pyd"
            wrong.write_text("")
            self.assertIsNone(select_compiled_module([str(wrong)], "windows"))

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
