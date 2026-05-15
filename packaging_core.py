# -*- coding: utf-8 -*-
"""Shared packaging configuration for the protected-core build."""

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
