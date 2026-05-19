# -*- coding: utf-8 -*-
"""Shared packaging configuration for the protected-core build."""

import glob
import os
import shutil
import sys

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
    summary = []
    build_src = os.path.join(root, "_build_src")
    if os.path.isdir(build_src):
        shutil.rmtree(build_src)
        summary.append("_build_src")
    build_root = os.path.join(root, "build")
    if os.path.isdir(build_root):
        shutil.rmtree(build_root)
        summary.append("build")
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
