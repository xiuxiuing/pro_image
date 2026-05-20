# -*- coding: utf-8 -*-
"""Shared packaging configuration for the protected-core build."""

import glob
import importlib
import os
import shutil
import subprocess
import sys

# Windows 打包 ZIP 默认输出目录（C 盘满时避免压缩失败）
DEFAULT_PACKAGE_ZIP_DIR_WINDOWS = r"H:\ProImage_packages"

BUILD_DEPENDENCY_MODULES = (
    ("nuitka", "nuitka"),
    ("PyInstaller", "pyinstaller"),
    ("ordered_set", "ordered-set"),
)

CORE_NUITKA_MODULES = (
    "main_030822",
    "post_match_engine",
    "product_text_extract",
    "extract_info_ai2",
    "extract_info_rules",
    "extract_info_schema",
    "license_utils",
    "utils",
    "merge_sku_data",
)

BUSINESS_SOURCE_FILES = (
    "app.py",
    "app_ops.py",
    "app_ops_extra.py",
    "app_ops_tasks.py",
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
)

RESOURCE_DIRS = ("templates", "static", "data")

REQUIRED_MODEL_FILES = (
    ("dinov2-base", "preprocessor_config.json"),
    ("dinov2-base", "config.json"),
    ("bge-base-zh-v1.5", "tokenizer_config.json"),
    ("bge-base-zh-v1.5", "config.json"),
)


def core_source_files():
    return tuple(f"{name}.py" for name in CORE_NUITKA_MODULES)


def nuitka_abi_markers():
    """Return filename substrings that identify the current interpreter's Nuitka ABI."""
    major, minor = sys.version_info.major, sys.version_info.minor
    return (
        f"cp{major}{minor}-",
        f"cpython-{major}{minor}-",
    )


def nuitka_module_suffixes(target):
    """Return acceptable compiled-module filename suffixes for the active Python."""
    major, minor = sys.version_info.major, sys.version_info.minor
    if target == "windows":
        return (f".cp{major}{minor}-win_amd64.pyd",)
    return (
        f".cpython-{major}{minor}-darwin.so",
        f".cp{major}{minor}-darwin.so",
    )


def select_compiled_module(paths, target):
    """Pick the compiled artifact that matches the interpreter running the packager."""
    if not paths:
        return None
    for suffix in nuitka_module_suffixes(target):
        matched = sorted(p for p in paths if p.endswith(suffix))
        if matched:
            return matched[-1]
    return None


def purge_stale_nuitka_modules(modules_dir):
    """Remove core-module builds compiled for a different Python version."""
    if not os.path.isdir(modules_dir):
        return 0
    markers = nuitka_abi_markers()
    removed = 0
    for fn in os.listdir(modules_dir):
        path = os.path.join(modules_dir, fn)
        if not os.path.isfile(path):
            continue
        if not any(fn.startswith(f"{mod}.") for mod in CORE_NUITKA_MODULES):
            continue
        if not (fn.endswith(".pyd") or fn.endswith(".so")):
            continue
        if any(marker in fn for marker in markers):
            continue
        os.remove(path)
        removed += 1
    return removed


def compiled_module_glob(root, module, target):
    suffixes = (".pyd",) if target == "windows" else (".so",)
    matches = []
    for suffix in suffixes:
        matches.extend(glob.glob(os.path.join(root, f"{module}*{suffix}")))
    selected = select_compiled_module(sorted(matches), target)
    return [selected] if selected else []


def requirements_build_path(root):
    return os.path.join(root, "requirements-build.txt")


def missing_build_modules():
    """Return import names for build tools that are not installed."""
    missing = []
    for import_name, _pip_name in BUILD_DEPENDENCY_MODULES:
        try:
            importlib.import_module(import_name)
        except ImportError:
            missing.append(import_name)
    return missing


def ensure_build_dependencies(root):
    """
    Install requirements-build.txt when Nuitka/PyInstaller are missing.
    Returns pip package names that were installed.
    """
    missing = missing_build_modules()
    if not missing:
        return []
    req_file = requirements_build_path(root)
    if not os.path.isfile(req_file):
        raise RuntimeError(
            f"缺少打包依赖 {', '.join(missing)}，且未找到 {req_file}。"
            f"请执行：{sys.executable} -m pip install -r requirements-build.txt"
        )
    proc = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-r", req_file],
        cwd=root,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout or "").strip()[-800:]
        raise RuntimeError(
            f"自动安装打包依赖失败（{', '.join(missing)}）。"
            f"请手动执行：{sys.executable} -m pip install -r requirements-build.txt\n{tail}"
        )
    still_missing = missing_build_modules()
    if still_missing:
        raise RuntimeError(
            f"安装后仍缺少模块：{', '.join(still_missing)}。"
            f"请确认使用同一 Python：{sys.executable}"
        )
    return [pip_name for import_name, pip_name in BUILD_DEPENDENCY_MODULES if import_name in missing]


def resolve_package_zip_dir(task_dir):
    """ZIP 输出目录：环境变量 > Windows 默认 H 盘 > 任务目录。"""
    env = (os.environ.get("PROIMAGE_PACKAGE_ZIP_DIR") or "").strip()
    if env:
        return env
    if os.name == "nt" and os.path.exists("H:\\"):
        return DEFAULT_PACKAGE_ZIP_DIR_WINDOWS
    return task_dir


def disk_free_bytes(path):
    """Free space on the volume containing *path* (works if *path* does not exist yet)."""
    check = os.path.abspath(path)
    while not os.path.exists(check):
        parent = os.path.dirname(check)
        if not parent or parent == check:
            break
        check = parent
    return shutil.disk_usage(check).free


def estimate_zip_workspace_bytes(artifact_path):
    total = 0
    if os.path.isdir(artifact_path):
        for dirpath, _dirnames, filenames in os.walk(artifact_path):
            for name in filenames:
                try:
                    total += os.path.getsize(os.path.join(dirpath, name))
                except OSError:
                    pass
    elif os.path.isfile(artifact_path):
        try:
            total = os.path.getsize(artifact_path)
        except OSError:
            total = 0
    return max(int(total * 1.15) + (512 * 1024 * 1024), 1024 * 1024 * 1024)


def require_disk_space_for_zip(artifact_path, zip_path):
    needed = estimate_zip_workspace_bytes(artifact_path)
    for check_path in (zip_path, artifact_path):
        free = disk_free_bytes(check_path)
        if free < needed:
            free_gb = free / (1024 ** 3)
            need_gb = needed / (1024 ** 3)
            drive = os.path.splitdrive(os.path.abspath(check_path))[0] or check_path
            raise OSError(
                28,
                (
                    f"磁盘空间不足：{drive} 剩余 {free_gb:.1f} GB，压缩至少需要约 {need_gb:.1f} GB。"
                    f"请先清理 _build_src、build、dist 旧产物或 uploads；"
                    f"Windows 默认 ZIP 目录为 {DEFAULT_PACKAGE_ZIP_DIR_WINDOWS}；"
                    f"也可设置环境变量 PROIMAGE_PACKAGE_ZIP_DIR。"
                ),
            )


def cleanup_pre_zip_workspace(root):
    """Remove large intermediates before ZIP so compression has headroom."""
    summary = []
    build_src = os.path.join(root, "_build_src")
    if os.path.isdir(build_src):
        shutil.rmtree(build_src)
        summary.append("_build_src")
    build_root = os.path.join(root, "build")
    if os.path.isdir(build_root):
        shutil.rmtree(build_root)
        summary.append("build")
    removed = purge_stale_nuitka_modules(os.path.join(root, "nuitka_modules"))
    if removed:
        summary.append(f"nuitka 缓存({removed})")
    return summary


def cleanup_nuitka_module_intermediates(modules_dir):
    """Remove Nuitka build folders and side files; keep compiled .pyd/.so artifacts."""
    if not os.path.isdir(modules_dir):
        return 0
    removed = 0
    for name in os.listdir(modules_dir):
        path = os.path.join(modules_dir, name)
        if name.endswith(".build") and os.path.isdir(path):
            shutil.rmtree(path)
            removed += 1
        elif name.endswith(".pyi") and os.path.isfile(path):
            os.remove(path)
            removed += 1
        elif name.endswith(".dist-info") and os.path.isdir(path):
            shutil.rmtree(path)
            removed += 1
    return removed


def cleanup_packaging_intermediates(root, spec_name, artifact_path=None):
    """Delete packaging workspace after ZIP is created; only the task ZIP is kept."""
    summary = list(cleanup_pre_zip_workspace(root))
    modules_dir = os.path.join(root, "nuitka_modules")
    if os.path.isdir(modules_dir):
        shutil.rmtree(modules_dir)
        summary.append("nuitka_modules")
    if artifact_path and os.path.exists(artifact_path):
        if os.path.isdir(artifact_path):
            shutil.rmtree(artifact_path)
        else:
            os.remove(artifact_path)
        summary.append(os.path.basename(artifact_path.rstrip("/\\")))
    return summary
