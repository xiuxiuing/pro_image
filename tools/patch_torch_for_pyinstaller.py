#!/usr/bin/env python3
"""已合并至 patch_pyinstaller_site_packages.py，保留此入口以兼容旧说明。"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from patch_pyinstaller_site_packages import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
