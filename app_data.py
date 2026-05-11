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
_update_step = None
_schedule_clear_progress = None
_validate_upload = None
_safe_upload_filename = None
_template = None
_static = None
data_root = None
DEFAULT_RULE_CATEGORIES_XLSX = None
CATEGORY_L1_BUCKET_TAGS_JSON = None

data_bp = Blueprint('data', __name__)

def init_data(dm_obj, init_prog_fn, update_step_fn, clear_prog_fn, validate_fn, safe_name_fn, template_path, static_path, dat_root, default_xlsx, bucket_json):
    global dm, _init_progress, _update_step, _schedule_clear_progress, _validate_upload, _safe_upload_filename, _template, _static, data_root, DEFAULT_RULE_CATEGORIES_XLSX, CATEGORY_L1_BUCKET_TAGS_JSON
    dm = dm_obj
    _init_progress = init_prog_fn
    _update_step = update_step_fn
    _schedule_clear_progress = clear_prog_fn
    _validate_upload = validate_fn
    _safe_upload_filename = safe_name_fn
    _template = template_path
    _static = static_path
    data_root = dat_root
    DEFAULT_RULE_CATEGORIES_XLSX = default_xlsx
    CATEGORY_L1_BUCKET_TAGS_JSON = bucket_json

def _norm_cell(v):
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() in ("nan", "none", "null") else s

def _build_category_tree(rows):
    l1_aliases = ("美团一级类目", "一级类目", "美团类目一级", "美团1级类目")
    l2_aliases = ("美团二级类目", "二级类目", "美团类目二级", "美团2级类目")
    l3_aliases = ("美团三级类目", "三级类目", "美团类目三级", "美团3级类目")

    def first_val(item, aliases):
        for key in aliases:
            if key in item:
                v = _norm_cell(item.get(key))
                if v:
                    return v
        return ""

    l1_map = {}
    for row in rows:
        l3 = first_val(row, l3_aliases)
        if not l3:
            continue
        l1 = first_val(row, l1_aliases) or "未分类一级类目"
        l2 = first_val(row, l2_aliases) or "未分类二级类目"
        l1_entry = l1_map.setdefault(l1, {"name": l1, "children_map": {}, "l3_count": 0})
        l2_entry = l1_entry["children_map"].setdefault(l2, {"name": l2, "children": [], "seen": set()})
        if l3 not in l2_entry["seen"]:
            l2_entry["children"].append({"name": l3})
            l2_entry["seen"].add(l3)
            l1_entry["l3_count"] += 1

    items = []
    for l1_name in sorted(l1_map.keys()):
        l1_entry = l1_map[l1_name]
        children = []
        for l2_name in sorted(l1_entry["children_map"].keys()):
            l2_entry = l1_entry["children_map"][l2_name]
            children.append({
                "name": l2_name,
                "l3_count": len(l2_entry["children"]),
                "children": sorted(l2_entry["children"], key=lambda x: x["name"]),
            })
        items.append({
            "name": l1_name,
            "l3_count": l1_entry["l3_count"],
            "children": children,
        })

    return {
        "items": items,
        "l1_count": len(items),
        "l3_count": sum(item["l3_count"] for item in items),
    }

def _workbook_to_rows(wb):
    ws = wb.active
    rows = ws.iter_rows(values_only=True)
    headers = next(rows, None)
    if not headers:
        return []
    out = []
    for row in rows:
        if all(cell is None for cell in row):
            continue
        out.append(dict(zip(headers, row)))
    return out

def _excel_file_to_rows(file_storage):
    wb = load_workbook(file_storage, data_only=True)
    return _workbook_to_rows(wb)

def _excel_path_to_rows(path: str):
    wb = load_workbook(path, data_only=True)
    return _workbook_to_rows(wb)


@data_bp.route('/api/projects', methods=['GET', 'POST'])
def handle_projects():
    if request.method == 'POST':
        name = request.form.get('name')
        if not name: return jsonify({"status": "error", "message": "Project name is required"}), 400

        match_config_json = (request.form.get('match_config_json') or "").strip()
        raw_rt = (request.form.get("rule_template_id") or "").strip()
        try:
            rule_template_id = int(raw_rt) if raw_rt else None
        except ValueError:
            rule_template_id = None

        main_file = request.files.get('main_file')
        comp_files = request.files.getlist('comp_files')
        
        if not main_file or not main_file.filename:
            return jsonify({"status": "error", "message": "Main store file is required"}), 400
        
        valid_comp_files = [f for f in comp_files if f.filename]
        if not valid_comp_files:
            return jsonify({"status": "error", "message": "At least one competitor store file is required"}), 400

        err = _validate_upload(main_file, "主店文件")
        if err: return jsonify({"status": "error", "message": err}), 400
        for f in valid_comp_files:
            err = _validate_upload(f, f"竞店文件 ({f.filename})")
            if err: return jsonify({"status": "error", "message": err}), 400
        result_file = request.files.get('result_file')
        if result_file and result_file.filename:
            err = _validate_upload(result_file, "结果文件")
            if err: return jsonify({"status": "error", "message": err}), 400

        import time, os, shutil
        # Temporary PID for directory naming
        temp_pid = int(time.time())
        proj_dir = os.path.join(data_root, "uploads", f"project_{temp_pid}")
        sources_dir = os.path.join(proj_dir, "sources")
        os.makedirs(sources_dir, exist_ok=True)
        
        # Save files with role prefixes
        main_saved_name = "main__" + _safe_upload_filename(main_file.filename, "main.xlsx")
        main_path = os.path.join(sources_dir, main_saved_name)
        main_file.save(main_path)
        main_store_name = main_file.filename.replace(".xlsx", "").replace(".xls", "")
        
        # Save Competitor Store Files
        comp_infos = []
        comp_paths = []
        for idx, f in enumerate(valid_comp_files):
            comp_saved_name = f"comp_{idx}__" + _safe_upload_filename(f.filename, f"comp_{idx}.xlsx")
            path = os.path.join(sources_dir, comp_saved_name)
            f.save(path)
            comp_paths.append(path)
            comp_infos.append({"path": path, "store_name": f.filename.replace(".xlsx", "").replace(".xls", "")})
        
        manual_result_path = None
        if result_file and result_file.filename:
            outputs_dir = os.path.join(proj_dir, "outputs")
            os.makedirs(outputs_dir, exist_ok=True)
            manual_result_path = os.path.join(outputs_dir, result_file.filename)
            result_file.save(manual_result_path)

        is_manual = bool(manual_result_path)
        pid = dm.create_project(
            name,
            {"path": main_path, "store_name": main_store_name},
            comp_infos,
            status='ready' if is_manual else 'analyzing',
            match_config_json=match_config_json,
            rule_template_id=rule_template_id,
        )

        # Rename temp directory to real PID
        real_proj_dir = os.path.join(data_root, "uploads", f"project_{pid}")
        if os.path.exists(real_proj_dir): shutil.rmtree(real_proj_dir)
        os.rename(proj_dir, real_proj_dir)

        with dm._db_lock:
            with dm._get_conn() as conn:
                conn.execute("UPDATE project_files SET local_path = REPLACE(local_path, ?, ?) WHERE project_id = ?",
                            (f"project_{temp_pid}", f"project_{pid}", pid))

        dirs = dm._ensure_project_dirs(pid)
        final_main_path = main_path.replace(f"project_{temp_pid}", f"project_{pid}")
        final_comp_paths = [p.replace(f"project_{temp_pid}", f"project_{pid}") for p in comp_paths]
        final_manual_result_path = manual_result_path.replace(f"project_{temp_pid}", f"project_{pid}") if manual_result_path else None

        if is_manual:
            output_file = os.path.join(dirs["outputs"], f"output_{pid}.xlsx")
            shutil.copy(final_manual_result_path, output_file)
            dm.activate_project(pid)
            return jsonify({"status": "success", "project_id": pid})

        # Async path
        use_ai = request.form.get('use_ai') == 'on'
        api_key = request.form.get('api_key')
        ai_model_name = (request.form.get('ai_model_name') or "").strip()
        kimi_api_key = (request.form.get('kimi_api_key') or "").strip()
        kimi_model_name = (request.form.get('kimi_model_name') or "").strip()

        main_name = os.path.basename(final_main_path).replace('.xlsx','').replace('.xls','')
        comp_names = [os.path.basename(p).replace('.xlsx','').replace('.xls','') for p in final_comp_paths]

        def _run_analysis_bg():
            _t0 = time.time()
            import extract_info_ai2, main_030822
            has_ai = bool(use_ai and api_key)
            prog = _init_progress(pid, has_ai, main_name, comp_names)
            ai_file_count = (1 + len(comp_names)) if has_ai else 0
            try:
                if has_ai:
                    all_ai_paths = [final_main_path] + final_comp_paths
                    _ai_gap = int(os.environ.get("PROIMAGE_AI_INTER_FILE_SLEEP_SEC", "8") or "8")
                    for fi, fp in enumerate(all_ai_paths):
                        _update_step(pid, fi, "running")
                        def _ai_cb(batch, total, _fi=fi):
                            _update_step(pid, _fi, "running", f"batch {batch}/{total}")
                        extract_info_ai2.process_file_ai(
                            fp, api_key, progress_cb=_ai_cb,
                            model_name=ai_model_name, fallback_api_key=kimi_api_key or None, fallback_model=kimi_model_name or None
                        )
                        _update_step(pid, fi, "done")
                        if fi + 1 < len(all_ai_paths) and _ai_gap > 0:
                            time.sleep(_ai_gap)

                analysis_base = ai_file_count
                def _analysis_cb(event, idx=0, detail=""):
                    if event == "source_start":
                        _update_step(pid, analysis_base + idx, "running", detail)
                    elif event == "source_done":
                        _update_step(pid, analysis_base + idx, "done")
                    elif event == "query_start":
                        _update_step(pid, len(prog["steps"]) - 1, "running", detail)
                    elif event == "query_progress":
                        _update_step(pid, len(prog["steps"]) - 1, "running", detail)

                _pmt = dm.get_post_match_template_for_project(pid)
                main_030822.run_analysis(
                    final_main_path, final_comp_paths,
                    output_name=str(pid), output_dir=dirs["outputs"],
                    progress_cb=_analysis_cb, match_config=match_config_json, post_match_template=_pmt
                )
                _update_step(pid, len(prog["steps"]) - 1, "done", "分析完成")
                dm.update_project_status(pid, 'ready')
            except BaseException as e:
                traceback.print_exc()
                try: dm.update_project_status(pid, 'failed')
                except: pass
            finally:
                _schedule_clear_progress(pid)

        threading.Thread(target=_run_analysis_bg, daemon=True).start()
        return jsonify({"status": "success", "project_id": pid})

    return jsonify(dm.list_projects())

    return jsonify(dm.list_projects())

@data_bp.route('/api/projects/<int:pid>', methods=['DELETE'])
def delete_project(pid):
    dm.delete_project(pid)
    return jsonify({"status": "success"})

@data_bp.route('/api/projects/<int:pid>/activate', methods=['POST'])
def activate_project(pid):
    projects = dm.list_projects()
    proj = next((p for p in projects if p['id'] == pid), None)
    if not proj:
        return jsonify({"status": "error", "message": "项目不存在"}), 404
    if proj.get('status') == 'analyzing':
        return jsonify({"status": "error", "message": "该项目正在分析中，请等待完成"}), 400
    if proj.get('status') == 'failed':
        return jsonify({"status": "error", "message": "该项目分析失败，请删除后重新创建"}), 400
    dm.activate_project(pid)
    return jsonify({"status": "success"})

@data_bp.route('/api/projects/<int:pid>/progress')
def get_analysis_progress_route(pid):
    # This calls back to the progress management in app.py or wherever it's passed from
    from app import get_analysis_progress_data
    return jsonify(get_analysis_progress_data(pid))

@data_bp.route("/api/rule-templates", methods=["GET", "POST"])
def api_rule_templates():
    if request.method == "GET":
        return jsonify({"status": "ok", "items": dm.list_rule_templates()})
    d = request.get_json(silent=True) or {}
    name = (d.get("name") or "").strip()
    if not name:
        return jsonify({"status": "error", "message": "模板名称不能为空"}), 400
    desc = (d.get("description") or "").strip()
    config = d.get("config")
    if not isinstance(config, dict):
        return jsonify({"status": "error", "message": "config 须为对象"}), 400
    rid = dm.create_rule_template(name, desc, config)
    return jsonify({"status": "ok", "id": rid})

@data_bp.route("/api/rule-templates/<int:tid>", methods=["GET", "PUT", "DELETE"])
def api_rule_template_one(tid):
    if request.method == "GET":
        t = dm.get_rule_template(tid)
        if not t:
            return jsonify({"status": "error", "message": "不存在"}), 404
        return jsonify({"status": "ok", "item": t})
    if request.method == "DELETE":
        ok, err = dm.delete_rule_template(tid)
        if not ok:
            return jsonify({"status": "error", "message": err or "删除失败"}), 400
        return jsonify({"status": "ok"})
    d = request.get_json(silent=True) or {}
    name = (d.get("name") or "").strip()
    if not name:
        return jsonify({"status": "error", "message": "模板名称不能为空"}), 400
    desc = (d.get("description") or "").strip()
    config = d.get("config")
    if not isinstance(config, dict):
        return jsonify({"status": "error", "message": "config 须为对象"}), 400
    if not dm.update_rule_template(tid, name, desc, config):
        return jsonify({"status": "error", "message": "更新失败"}), 400
    return jsonify({"status": "ok"})

@data_bp.route("/api/rule-category-template")
def api_rule_category_template():
    from flask import send_file
    import io
    from openpyxl import Workbook
    if os.path.isfile(DEFAULT_RULE_CATEGORIES_XLSX):
        return send_file(
            DEFAULT_RULE_CATEGORIES_XLSX,
            as_attachment=True,
            download_name="类目配置模板.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    wb = Workbook()
    ws = wb.active
    ws.title = "类目模板"
    ws.append(["美团一级类目", "美团二级类目", "美团三级类目"])
    ws.append(["饮料", "碳酸饮料", "可乐"])
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return send_file(
        buf,
        as_attachment=True,
        download_name="类目配置模板.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

@data_bp.route('/api/history')
def get_history():
    return jsonify(dm.get_history())

@data_bp.route('/api/projects/switch', methods=['POST'])
def switch_project():
    name = request.json.get('name')
    if dm.load_project(name):
        return jsonify({"status": "success", "name": name})
    return jsonify({"status": "error", "message": "Failed to load project"}), 400

@data_bp.route('/api/project_data')
def get_project_data():
    if not dm.active_project_name:
        return jsonify({"status": "error", "message": "No active project"}), 400
    
    page = int(request.args.get('page', 1))
    page_size = int(request.args.get('page_size', 50))
    search = request.args.get('search', '')
    category = request.args.get('category', '')
    status_filter = request.args.get('status', 'all')
    
    data = dm.get_active_project_data(
        page=page, 
        page_size=page_size, 
        search=search, 
        category=category,
        status_filter=status_filter
    )
    return jsonify(data)

@data_bp.route('/api/sku_detail/<main_sku_id>')
def get_sku_detail(main_sku_id):
    detail = dm.get_sku_detail(main_sku_id)
    if detail:
        return jsonify(detail)
    return jsonify({"status": "error", "message": "SKU not found"}), 404

@data_bp.route("/api/rule-categories/parse", methods=["POST"])
def api_rule_categories_parse():
    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify({"status": "error", "message": "请上传类目文件"}), 400
    err = _validate_upload(f, "类目文件")
    if err:
        return jsonify({"status": "error", "message": err}), 400
    try:
        rows = _excel_file_to_rows(f)
        tree = _build_category_tree(rows)
        if not tree["items"]:
            return jsonify({"status": "error", "message": "未读取到有效的三级类目，请检查模板列名与内容"}), 400
        return jsonify({"status": "ok", "tree": tree})
    except Exception as e:
        return jsonify({"status": "error", "message": f"解析失败：{e}"}), 400

@data_bp.route("/api/rule-categories/default", methods=["GET"])
def api_rule_categories_default():
    if not os.path.isfile(DEFAULT_RULE_CATEGORIES_XLSX):
        return jsonify({"status": "error", "message": "未配置默认类目文件"}), 404
    try:
        rows = _excel_path_to_rows(DEFAULT_RULE_CATEGORIES_XLSX)
        tree = _build_category_tree(rows)
        if not tree["items"]:
            return jsonify({"status": "error", "message": "默认类目文件无有效三级类目"}), 500
        return jsonify({"status": "ok", "tree": tree})
    except Exception as e:
        return jsonify({"status": "error", "message": f"解析失败：{e}"}), 500

@data_bp.route("/api/rule-categories/bucket-tags", methods=["GET"])
def api_rule_categories_bucket_tags():
    if not os.path.isfile(CATEGORY_L1_BUCKET_TAGS_JSON):
        return jsonify({"status": "ok", "version": 0, "tags": []})
    try:
        with open(CATEGORY_L1_BUCKET_TAGS_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
        tags = data.get("tags") if isinstance(data, dict) else []
        if not isinstance(tags, list):
            tags = []
        return jsonify({"status": "ok", "version": int(data.get("version", 1) if isinstance(data, dict) else 1), "tags": tags})
    except Exception as e:
        return jsonify({"status": "error", "message": f"读取标签配置失败：{e}"}), 500

@data_bp.route("/api/config", methods=['GET'])
def get_config():
    return jsonify({
        "main_store": dm.main_store_name, "target_file": dm.target_file, "output_file": dm.output_file,
        "source_files": dm.source_files, "stores": [{"id": str(i), "name": n, "path": dm.source_files[i]} for i, n in enumerate(dm.store_names)]
    })

@data_bp.route('/api/grid_data')
def get_grid_data():
    page = request.args.get('page', 1, type=int)
    limit = request.args.get('limit', 50, type=int)
    search = request.args.get('search', "")
    mode = request.args.get('mode', "all")
    filters_json = request.args.get('filters', "{}")
    sort_field = request.args.get('sort_field', "")
    sort_order = request.args.get('sort_order', "desc")
    negative_sales = request.args.get('negative_sales', "0") == "1"
    return jsonify(dm.get_paginated_grid(
        page=page, limit=limit, search=search, mode=mode,
        filters_json=filters_json, sort_field=sort_field, sort_order=sort_order,
        negative_sales_only=negative_sales,
    ))

@data_bp.route('/api/store_products/<store_id>')
def get_store_products(store_id):
    return jsonify(dm.get_store_products(store_id))

@data_bp.route('/api/unlinked_items')
def get_unlinked_items():
    page = request.args.get('page', 1, type=int)
    limit = request.args.get('limit', 30, type=int)
    search = request.args.get('search', "")
    category3 = request.args.get('category3', "")
    sort_store_id = request.args.get('sort_store_id', "")
    sort_order = request.args.get('sort_order', "desc")
    filters_json = request.args.get('filters', "{}")
    negative_sales = request.args.get('negative_sales', "0") == "1"
    return jsonify(dm.get_unlinked_pool_page(
        page=page, limit=limit, search=search, category3=category3,
        sort_store_id=sort_store_id, sort_order=sort_order,
        filters_json=filters_json, negative_sales_only=negative_sales,
    ))

@data_bp.route('/api/main_products')
def get_main_products():
    page = request.args.get('page', 1, type=int)
    limit = request.args.get('limit', 50, type=int)
    search = request.args.get('search', "")
    return jsonify(dm.get_main_products_page(page=page, limit=limit, search=search))

@data_bp.route('/api/eliminate', methods=['POST'])
def eliminate():
    d = request.json
    main_sku_id = d.get('main_sku_id')
    if not main_sku_id:
        return jsonify({"status": "error", "message": "Missing main_sku_id"}), 400
    dm.eliminate_product(main_sku_id, d.get('status', 1))
    return jsonify({"status": "success"})

@data_bp.route('/api/toggle_handled', methods=['POST'])
def toggle_handled():
    d = request.json
    sku_id = d.get('main_sku_id')
    if not sku_id:
        return jsonify({"status": "error", "message": "Missing main_sku_id"}), 400
    dm.toggle_handled(sku_id, d.get('handled', True))
    return jsonify({"status": "success"})

@data_bp.route('/api/toggle_ref', methods=['POST'])
def toggle_ref():
    d = request.json
    sku_id = d.get('main_sku_id')
    field = d.get('field')
    store_id = d.get('store_id', '')
    if not sku_id or field not in ('name', 'image'):
        return jsonify({"status": "error", "message": "Missing params"}), 400
    dm.set_ref(sku_id, field, store_id)
    return jsonify({"status": "success"})

@data_bp.route('/api/toggle_add', methods=['POST'])
def toggle_add():
    d = request.json
    store_id = d.get('store_id')
    comp_sku_id = d.get('sku_id')
    if store_id is None or not comp_sku_id:
        return jsonify({"status": "error", "message": "Missing store_id or sku_id"}), 400
    ok = dm.mark_as_new(store_id, comp_sku_id, d.get('is_new', True))
    if not ok:
        return jsonify({"status": "error", "message": "未找到可标记的商品"}), 400
    return jsonify({"status": "success"})

@data_bp.route('/api/price_match', methods=['POST'])
def price_match():
    d = request.json
    main_sku_id = d.get('main_sku_id')
    store_id = d.get('store_id')
    if not main_sku_id or store_id is None:
        return jsonify({"status": "error", "message": "Missing params"}), 400
    result = dm.price_match(main_sku_id, store_id)
    if not result:
        return jsonify({"status": "error", "message": "未找到可跟价的商品"}), 400
    return jsonify({"status": "success", **result})

@data_bp.route('/api/clear_price_match', methods=['POST'])
def clear_price_match():
    d = request.json
    main_sku_id = d.get('main_sku_id')
    if not main_sku_id:
        return jsonify({"status": "error", "message": "Missing main_sku_id"}), 400
    dm.clear_price_match(main_sku_id)
    return jsonify({"status": "success"})

@data_bp.route('/api/manual_link', methods=['POST'])
def manual_link():
    d = request.json
    main_sku_id = d.get('main_sku_id')
    store_id = d.get('store_id')
    comp_sku_id = d.get('comp_sku_id')
    if not main_sku_id or store_id is None or not comp_sku_id:
        return jsonify({"status": "error", "message": "Missing params"}), 400
    dm.manual_link(main_sku_id, store_id, comp_sku_id)
    return jsonify({"status": "success"})

@data_bp.route('/api/unlink', methods=['POST'])
def unlink():
    d = request.json
    main_sku_id = d.get('main_sku_id')
    store_id = d.get('store_id')
    if not main_sku_id or store_id is None:
        return jsonify({"status": "error", "message": "Missing params"}), 400
    dm.unlink_product(main_sku_id, store_id)
    return jsonify({"status": "success"})

@data_bp.route('/api/update_cell', methods=['POST'])
def update_cell():
    d = request.json
    main_sku_id = d.get('main_sku_id')
    if not main_sku_id:
        return jsonify({"status": "error", "message": "Missing main_sku_id"}), 400
    dm.update_cell(main_sku_id, {d.get('column'): d.get('value')})
    return jsonify({"status": "success"})

@data_bp.route('/img/<path:filename>')
def serve_img(filename):
    return send_from_directory(os.path.join(data_root, "img"), filename)

@data_bp.route('/api/export')
def export_data():
    p = dm.save_separate_exports()
    resp = send_file(p, as_attachment=True)
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"; resp.headers["Pragma"] = "no-cache"; resp.headers["Expires"] = "0"
    return resp

@data_bp.route('/api/export_new')
def export_new_data():
    p = dm.export_new_items()
    resp = send_file(p, as_attachment=True)
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"; resp.headers["Pragma"] = "no-cache"; resp.headers["Expires"] = "0"
    return resp
