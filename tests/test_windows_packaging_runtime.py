import ast
import builtins
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
APP_PATH = ROOT / "app.py"


def load_app_function(name, extra_globals=None):
    tree = ast.parse(APP_PATH.read_text(encoding="utf-8"), filename=str(APP_PATH))
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            module = ast.Module(body=[node], type_ignores=[])
            ast.fix_missing_locations(module)
            namespace = dict(extra_globals or {})
            exec(compile(module, str(APP_PATH), "exec"), namespace)
            return namespace[name], namespace
    raise AssertionError(f"{name} not found in app.py")


class WindowsPackagingRuntimeTests(unittest.TestCase):
    def test_windows_single_instance_lock_does_not_require_fcntl(self):
        calls = []

        def fake_locking(_fileno, mode, nbytes):
            calls.append((mode, nbytes))

        fake_msvcrt = types.SimpleNamespace(LK_NBLCK=1, locking=fake_locking)
        fake_sys = types.SimpleNamespace(frozen=True, platform="win32")

        original_import = builtins.__import__

        def guarded_import(name, *args, **kwargs):
            if name == "fcntl":
                raise AssertionError("Windows lock path must not import fcntl")
            return original_import(name, *args, **kwargs)

        with tempfile.TemporaryDirectory() as tmpdir:
            fn, namespace = load_app_function(
                "_acquire_single_instance_lock",
                {
                    "os": os,
                    "sys": fake_sys,
                    "data_root": tmpdir,
                    "_single_instance_lock_fh": None,
                },
            )
            with mock.patch.dict(sys.modules, {"msvcrt": fake_msvcrt}):
                with mock.patch("builtins.__import__", side_effect=guarded_import):
                    self.assertTrue(fn())

            self.assertEqual(calls, [(fake_msvcrt.LK_NBLCK, 1)])
            namespace["_single_instance_lock_fh"].close()

    def test_frozen_app_schedules_browser_open(self):
        opened = []
        timers = []

        class FakeTimer:
            def __init__(self, delay, callback):
                self.delay = delay
                self.callback = callback
                self.daemon = False
                timers.append(self)

            def start(self):
                self.callback()

        fake_threading = types.SimpleNamespace(Timer=FakeTimer)
        fake_webbrowser = types.SimpleNamespace(open=lambda url: opened.append(url))
        fake_sys = types.SimpleNamespace(frozen=True)

        fn, _ = load_app_function(
            "_schedule_open_browser",
            {
                "sys": fake_sys,
                "threading": fake_threading,
                "webbrowser": fake_webbrowser,
            },
        )

        fn(5088)

        self.assertEqual(len(timers), 1)
        self.assertEqual(timers[0].delay, 1.5)
        self.assertTrue(timers[0].daemon)
        self.assertEqual(opened, ["http://127.0.0.1:5088"])

    def test_main_block_schedules_browser_open_before_serving(self):
        tree = ast.parse(APP_PATH.read_text(encoding="utf-8"), filename=str(APP_PATH))
        main_blocks = [
            node for node in tree.body
            if isinstance(node, ast.If) and ast.unparse(node.test) == "__name__ == '__main__'"
        ]

        self.assertEqual(len(main_blocks), 1)
        body = [ast.unparse(stmt) for stmt in main_blocks[0].body]
        self.assertIn("_schedule_open_browser(port)", body)

    def test_ops_tools_package_build_uses_nuitka_windows_spec(self):
        text = (ROOT / "app_ops_extra.py").read_text(encoding="utf-8")

        self.assertIn('"ProImage_nuitka_Windows.spec"', text)
        self.assertIn('target not in ("macos", "windows")', text)


if __name__ == "__main__":
    unittest.main()
