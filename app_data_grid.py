import json
import os
import tempfile
from flask import Blueprint, request, jsonify, send_file, send_from_directory
from openpyxl import Workbook

grid_bp = Blueprint('data_grid', __name__)


def init_grid(context):
    global dm, data_root
    dm = context["dm"]
    data_root = context["data_root"]


def _request_project_id(default_active=True):
    data = request.get_json(silent=True) if request.method in ("POST", "PUT", "PATCH", "DELETE") else None
    raw = None
    if isinstance(data, dict):
        raw = data.get("project_id")
    if raw in (None, ""):
        raw = request.args.get("project_id")
    if raw in (None, "") and default_active:
        raw = dm.active_project_id
    try:
        return int(raw) if raw not in (None, "") else None
    except (TypeError, ValueError):
        return None


@grid_bp.route('/api/history')
def get_history():
    return jsonify(dm.get_history())

@grid_bp.route('/api/projects/switch', methods=['POST'])
def switch_project():
    name = request.json.get('name')
    if dm.load_project(name):
        return jsonify({"status": "success", "name": name})
    return jsonify({"status": "error", "message": "Failed to load project"}), 400

@grid_bp.route('/api/project_data')
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

@grid_bp.route('/api/sku_detail/<main_sku_id>')
def get_sku_detail(main_sku_id):
    detail = dm.get_sku_detail(main_sku_id)
    if detail:
        return jsonify(detail)
    return jsonify({"status": "error", "message": "SKU not found"}), 404

@grid_bp.route("/api/rule-categories/parse", methods=["POST"])
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

@grid_bp.route("/api/rule-categories/default", methods=["GET"])
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

@grid_bp.route("/api/rule-categories/bucket-tags", methods=["GET"])
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

@grid_bp.route("/api/config", methods=['GET'])
def get_config():
    project_id = _request_project_id()
    has_links = False
    if project_id:
        with dm._db_lock:
            with dm._get_conn() as conn:
                row = conn.execute(
                    "SELECT 1 FROM product_links WHERE project_id = ? LIMIT 1",
                    (project_id,),
                ).fetchone()
                has_links = bool(row)
    main_store_name = dm.main_store_name
    target_file = dm.target_file
    output_file = dm.output_file
    source_files = dm.source_files
    store_names = dm.store_names
    if project_id and project_id != dm.active_project_id:
        dirs = dm._get_project_dirs(project_id)
        output_file = os.path.join(dirs["outputs"], f"output_{project_id}.xlsx")
        source_files = []
        store_names = []
        main_store_name = ""
        target_file = ""
        with dm._db_lock:
            with dm._get_conn() as conn:
                rows = conn.execute(
                    "SELECT type, local_path, store_name FROM project_files WHERE project_id = ? ORDER BY id ASC",
                    (project_id,),
                ).fetchall()
        for f_type, path, store_name in rows:
            if f_type == "main":
                target_file = path
                main_store_name = store_name
            elif f_type == "comp":
                source_files.append(path)
                store_names.append(store_name)
    return jsonify({
        "project_id": project_id,
        "has_links": has_links,
        "main_store": main_store_name, "target_file": target_file, "output_file": output_file,
        "source_files": source_files, "stores": [{"id": str(i), "name": n, "path": source_files[i]} for i, n in enumerate(store_names)]
    })

@grid_bp.route('/api/grid_data')
def get_grid_data():
    project_id = _request_project_id()
    page = request.args.get('page', 1, type=int)
    limit = request.args.get('limit', 50, type=int)
    search = request.args.get('search', "")
    mode = request.args.get('mode', "all")
    filters_json = request.args.get('filters', "{}")
    sort_field = request.args.get('sort_field', "")
    sort_order = request.args.get('sort_order', "desc")
    negative_sales = request.args.get('negative_sales', "0") == "1"
    with dm.project_context(project_id):
        return jsonify(dm.get_paginated_grid(
            page=page, limit=limit, search=search, mode=mode,
            filters_json=filters_json, sort_field=sort_field, sort_order=sort_order,
            negative_sales_only=negative_sales,
        ))

@grid_bp.route('/api/statistics')
def get_statistics():
    project_id = _request_project_id()
    paged = request.args.get("paged", "0") == "1"
    refresh = request.args.get("refresh", "0") == "1"
    if paged:
        return jsonify(dm.get_statistics_page(
            refresh=refresh,
            tab_id=request.args.get("tab", "main"),
            page=request.args.get("page", 1, type=int),
            limit=request.args.get("limit", 20, type=int),
            search=request.args.get("search", ""),
            sort_key=request.args.get("sort_key", ""),
            sort_order=request.args.get("sort_order", "desc"),
            project_id=project_id,
        ))
    return jsonify(dm.get_statistics(refresh=refresh, project_id=project_id))

@grid_bp.route('/api/statistics/snapshot-status')
def get_statistics_snapshot_status():
    project_id = _request_project_id()
    return jsonify({"status": "ok", **dm.get_analysis_snapshot_status(project_id=project_id)})

@grid_bp.route('/api/market-analysis')
def get_market_analysis():
    project_id = _request_project_id()
    return jsonify(dm.get_market_analysis(refresh=request.args.get("refresh", "0") == "1", project_id=project_id))

@grid_bp.route('/api/statistics/products')
def get_statistics_products():
    project_id = _request_project_id()
    with dm.project_context(project_id):
        return jsonify(dm.get_statistics_products(
            request.args.get("category", ""),
            request.args.get("source_type", "main"),
            request.args.get("store_id", ""),
        ))

@grid_bp.route('/api/statistics/export')
def export_statistics():
    project_id = _request_project_id()
    data = dm.get_statistics(sync_missing=True, project_id=project_id)

    def whole(value):
        try:
            return int(round(float(value or 0)))
        except (TypeError, ValueError):
            return 0

    wb = Workbook()
    ws = wb.active
    ws.title = "数据分析"
    ws.append([
        "类目来源", "三级分类", "行类型", "店铺",
        "销售量（单）-数据", "销售量（单）-对比值", "销售量（单）-差值",
        "销售额（元）-数据", "销售额（元）-对比值", "销售额（元）-差值",
        "SPU数量-数据", "SPU数量-对比值", "SPU数量-差值",
        "动销效率(%)-数据", "动销效率(%)-对比值", "动销效率(%)-差值",
        "类目贡献(%)-数据", "类目贡献(%)-对比值", "类目贡献(%)-差值",
    ])

    metric_order = ["sales", "sales_amount", "spu", "active_rate", "category_contribution"]
    tabs = data.get("tabs") or [{
        "source_type": "main",
        "source_name": data.get("main_store") or "主店",
        "items": data.get("items", []),
    }]
    for tab in tabs:
        source_name = tab.get("source_name") or ("主店" if tab.get("source_type") == "main" else tab.get("label", ""))
        source_label = "主店" if tab.get("source_type") == "main" else source_name
        for item in tab.get("items", []):
            category = item.get("category", "")
            summary = item.get("summary") or {}
            row = [source_label, category, "主表汇总", source_name]
            for key in metric_order:
                block = summary.get(key) or {}
                row.extend([whole(block.get("main", 0)), whole(block.get("industry_avg", 0)), whole(block.get("avg_diff", 0))])
            ws.append(row)

            if tab.get("source_type") != "main":
                continue
            for comp in item.get("competitors", []):
                metrics = comp.get("metrics") or {}
                main_diff = comp.get("main_diff") or comp.get("diff") or {}
                market_diff = comp.get("market_diff") or {}
                row = [source_label, category, "竞店明细", comp.get("store_name", "")]
                for key in metric_order:
                    row.extend([whole(metrics.get(key, 0)), whole(main_diff.get(key, 0)), whole(market_diff.get(key, 0))])
                ws.append(row)

    for col in ws.columns:
        width = max((len(str(cell.value or "")) for cell in col), default=10)
        ws.column_dimensions[col[0].column_letter].width = min(max(width + 2, 10), 28)

    out_dir = tempfile.gettempdir()
    out_path = os.path.join(out_dir, f"statistics_{project_id or dm.active_project_id or 'project'}.xlsx")
    wb.save(out_path)
    return send_file(out_path, as_attachment=True, download_name="数据分析导出.xlsx")

@grid_bp.route('/api/store_products/<store_id>')
def get_store_products(store_id):
    project_id = _request_project_id()
    return jsonify(dm.get_store_products(store_id, project_id=project_id))

@grid_bp.route('/api/unlinked_items')
def get_unlinked_items():
    project_id = _request_project_id()
    page = request.args.get('page', 1, type=int)
    limit = request.args.get('limit', 30, type=int)
    search = request.args.get('search', "")
    category3 = request.args.get('category3', "")
    sort_store_id = request.args.get('sort_store_id', "")
    sort_order = request.args.get('sort_order', "desc")
    filters_json = request.args.get('filters', "{}")
    negative_sales = request.args.get('negative_sales', "0") == "1"
    with dm.project_context(project_id):
        return jsonify(dm.get_unlinked_pool_page(
            page=page, limit=limit, search=search, category3=category3,
            sort_store_id=sort_store_id, sort_order=sort_order,
            filters_json=filters_json, negative_sales_only=negative_sales,
        ))

@grid_bp.route('/api/main_products')
def get_main_products():
    project_id = _request_project_id()
    page = request.args.get('page', 1, type=int)
    limit = request.args.get('limit', 50, type=int)
    search = request.args.get('search', "")
    return jsonify(dm.get_main_products_page(page=page, limit=limit, search=search, project_id=project_id))

@grid_bp.route('/api/main_products/<path:main_sku_id>/links')
def get_main_product_links(main_sku_id):
    project_id = _request_project_id()
    return jsonify(dm.get_main_product_links(main_sku_id, project_id=project_id))

@grid_bp.route('/api/main_products/<path:main_sku_id>/match-explain/<store_id>')
def get_main_product_match_explain(main_sku_id, store_id):
    project_id = _request_project_id()
    return jsonify(dm.get_match_explanation(main_sku_id, store_id, project_id=project_id))

@grid_bp.route('/api/eliminate', methods=['POST'])
def eliminate():
    d = request.json
    project_id = _request_project_id()
    main_sku_id = d.get('main_sku_id')
    if not main_sku_id:
        return jsonify({"status": "error", "message": "Missing main_sku_id"}), 400
    dm.eliminate_product(main_sku_id, d.get('status', 1), project_id=project_id)
    return jsonify({"status": "success"})

@grid_bp.route('/api/toggle_handled', methods=['POST'])
def toggle_handled():
    d = request.json
    project_id = _request_project_id()
    sku_id = d.get('main_sku_id')
    if not sku_id:
        return jsonify({"status": "error", "message": "Missing main_sku_id"}), 400
    dm.toggle_handled(sku_id, d.get('handled', True), project_id=project_id)
    return jsonify({"status": "success"})

@grid_bp.route('/api/toggle_ref', methods=['POST'])
def toggle_ref():
    d = request.json
    project_id = _request_project_id()
    sku_id = d.get('main_sku_id')
    field = d.get('field')
    store_id = d.get('store_id', '')
    if not sku_id or field not in ('name', 'image'):
        return jsonify({"status": "error", "message": "Missing params"}), 400
    dm.set_ref(sku_id, field, store_id, project_id=project_id)
    return jsonify({"status": "success"})

@grid_bp.route('/api/toggle_add', methods=['POST'])
def toggle_add():
    d = request.json
    project_id = _request_project_id()
    store_id = d.get('store_id')
    comp_sku_id = d.get('sku_id')
    if store_id is None or not comp_sku_id:
        return jsonify({"status": "error", "message": "Missing store_id or sku_id"}), 400
    ok = dm.mark_as_new(store_id, comp_sku_id, d.get('is_new', True), project_id=project_id)
    if not ok:
        return jsonify({"status": "error", "message": "未找到可标记的商品"}), 400
    return jsonify({"status": "success"})

@grid_bp.route('/api/toggle_ignore', methods=['POST'])
def toggle_ignore():
    d = request.json
    project_id = _request_project_id()
    store_id = d.get('store_id')
    comp_sku_id = d.get('sku_id')
    if store_id is None or not comp_sku_id:
        return jsonify({"status": "error", "message": "Missing store_id or sku_id"}), 400
    ok = dm.mark_as_ignored(store_id, comp_sku_id, d.get('is_ignored', True), project_id=project_id)
    if not ok:
        return jsonify({"status": "error", "message": "未找到可标记的商品"}), 400
    return jsonify({"status": "success"})

@grid_bp.route('/api/price_match', methods=['POST'])
def price_match():
    d = request.json
    project_id = _request_project_id()
    main_sku_id = d.get('main_sku_id')
    store_id = d.get('store_id')
    if not main_sku_id or store_id is None:
        return jsonify({"status": "error", "message": "Missing params"}), 400
    match_act = d.get('match_act', True)
    match_orig = d.get('match_orig', True)
    result = dm.price_match(main_sku_id, store_id, match_act=match_act, match_orig=match_orig, project_id=project_id)
    if not result:
        return jsonify({"status": "error", "message": "未找到可跟价的商品"}), 400
    return jsonify({"status": "success", **result})

@grid_bp.route('/api/clear_price_match', methods=['POST'])
def clear_price_match():
    d = request.json
    project_id = _request_project_id()
    main_sku_id = d.get('main_sku_id')
    if not main_sku_id:
        return jsonify({"status": "error", "message": "Missing main_sku_id"}), 400
    dm.clear_price_match(main_sku_id, project_id=project_id)
    return jsonify({"status": "success"})

@grid_bp.route('/api/manual_link', methods=['POST'])
def manual_link():
    d = request.json
    project_id = _request_project_id()
    main_sku_id = d.get('main_sku_id')
    store_id = d.get('store_id')
    comp_sku_id = d.get('comp_sku_id')
    if not main_sku_id or store_id is None or not comp_sku_id:
        return jsonify({"status": "error", "message": "Missing params"}), 400
    dm.manual_link(main_sku_id, store_id, comp_sku_id, project_id=project_id)
    return jsonify({"status": "success"})

@grid_bp.route('/api/unlink', methods=['POST'])
def unlink():
    d = request.json
    project_id = _request_project_id()
    main_sku_id = d.get('main_sku_id')
    store_id = d.get('store_id')
    if not main_sku_id or store_id is None:
        return jsonify({"status": "error", "message": "Missing params"}), 400
    dm.unlink_product(main_sku_id, store_id, project_id=project_id)
    return jsonify({"status": "success"})

@grid_bp.route('/api/update_cell', methods=['POST'])
def update_cell():
    d = request.json
    project_id = _request_project_id()
    main_sku_id = d.get('main_sku_id')
    if not main_sku_id:
        return jsonify({"status": "error", "message": "Missing main_sku_id"}), 400
    dm.update_cell(main_sku_id, {d.get('column'): d.get('value')}, project_id=project_id)
    return jsonify({"status": "success"})

@grid_bp.route('/img/<path:filename>')
def serve_img(filename):
    return send_from_directory(os.path.join(data_root, "img"), filename)

@grid_bp.route('/api/export')
def export_data():
    project_id = _request_project_id()
    with dm.project_context(project_id):
        p = dm.save_separate_exports()
    resp = send_file(p, as_attachment=True)
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"; resp.headers["Pragma"] = "no-cache"; resp.headers["Expires"] = "0"
    return resp

@grid_bp.route('/api/export_new')
def export_new_data():
    project_id = _request_project_id()
    with dm.project_context(project_id):
        p = dm.export_new_items()
    resp = send_file(p, as_attachment=True)
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"; resp.headers["Pragma"] = "no-cache"; resp.headers["Expires"] = "0"
    return resp

@grid_bp.route('/api/export_corrections')
def export_corrections():
    project_id = _request_project_id()
    with dm.project_context(project_id):
        p = dm.export_manual_corrections()
    resp = send_file(p, as_attachment=True)
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"; resp.headers["Pragma"] = "no-cache"; resp.headers["Expires"] = "0"
    return resp
