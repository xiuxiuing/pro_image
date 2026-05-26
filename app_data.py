from flask import Blueprint, request, jsonify, send_file, send_from_directory
import os
import time
import json
import threading
import traceback
from openpyxl import load_workbook

# These will be initialized by app.py
dm = None
_init_progress = None
_init_import_progress = None
_update_step = None
_schedule_clear_progress = None
_validate_upload = None
_safe_upload_filename = None
_get_analysis_progress_data = None
_template = None
_static = None
data_root = None
DEFAULT_RULE_CATEGORIES_XLSX = None
CATEGORY_L1_BUCKET_TAGS_JSON = None

data_bp = Blueprint('data', __name__)

def init_data(dm_obj, init_prog_fn, init_import_prog_fn, update_step_fn, clear_prog_fn, progress_data_fn, validate_fn, safe_name_fn, template_path, static_path, dat_root, default_xlsx, bucket_json):
    global dm, _init_progress, _init_import_progress, _update_step, _schedule_clear_progress, _get_analysis_progress_data, _validate_upload, _safe_upload_filename, _template, _static, data_root, DEFAULT_RULE_CATEGORIES_XLSX, CATEGORY_L1_BUCKET_TAGS_JSON
    dm = dm_obj
    _init_progress = init_prog_fn
    _init_import_progress = init_import_prog_fn
    _update_step = update_step_fn
    _schedule_clear_progress = clear_prog_fn
    _get_analysis_progress_data = progress_data_fn
    _validate_upload = validate_fn
    _safe_upload_filename = safe_name_fn
    _template = template_path
    _static = static_path
    data_root = dat_root
    DEFAULT_RULE_CATEGORIES_XLSX = default_xlsx
    CATEGORY_L1_BUCKET_TAGS_JSON = bucket_json
    _register_split_blueprints()

_routes_registered = False

def _register_split_blueprints():
    global _routes_registered
    import app_data_projects
    import app_data_rules
    import app_data_grid
    import app_data_match_agent
    ctx = {
        "dm": dm,
        "init_progress": _init_progress,
        "init_import_progress": _init_import_progress,
        "update_step": _update_step,
        "schedule_clear_progress": _schedule_clear_progress,
        "get_analysis_progress_data": _get_analysis_progress_data,
        "validate_upload": _validate_upload,
        "safe_upload_filename": _safe_upload_filename,
        "data_root": data_root,
        "default_rule_categories_xlsx": DEFAULT_RULE_CATEGORIES_XLSX,
        "category_l1_bucket_tags_json": CATEGORY_L1_BUCKET_TAGS_JSON,
    }
    app_data_projects.init_projects(ctx)
    app_data_rules.init_rules(ctx)
    app_data_grid.init_grid(ctx)
    app_data_match_agent.init_match_agent(ctx)
    if not _routes_registered:
        data_bp.register_blueprint(app_data_projects.projects_bp)
        data_bp.register_blueprint(app_data_rules.rules_bp)
        data_bp.register_blueprint(app_data_grid.grid_bp)
        data_bp.register_blueprint(app_data_match_agent.match_agent_bp)
        _routes_registered = True
