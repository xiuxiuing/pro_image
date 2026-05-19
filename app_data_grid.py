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
    has_links = False
    if dm.active_project_id:
        with dm._db_lock:
            with dm._get_conn() as conn:
                row = conn.execute(
                    "SELECT 1 FROM product_links WHERE project_id = ? LIMIT 1",
                    (dm.active_project_id,),
                ).fetchone()
                has_links = bool(row)
    return jsonify({
        "project_id": dm.active_project_id,
        "has_links": has_links,
        "main_store": dm.main_store_name, "target_file": dm.target_file, "output_file": dm.output_file,
        "source_files": dm.source_files, "stores": [{"id": str(i), "name": n, "path": dm.source_files[i]} for i, n in enumerate(dm.store_names)]
    })

@grid_bp.route('/api/grid_data')
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

@grid_bp.route('/api/statistics')
def get_statistics():
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
        ))
    return jsonify(dm.get_statistics(refresh=refresh))

@grid_bp.route('/api/market-analysis')
def get_market_analysis():
    return jsonify(dm.get_market_analysis(refresh=request.args.get("refresh", "0") == "1"))

@grid_bp.route('/api/statistics/products')
def get_statistics_products():
    return jsonify(dm.get_statistics_products(
        request.args.get("category", ""),
        request.args.get("source_type", "main"),
        request.args.get("store_id", ""),
    ))

@grid_bp.route('/api/statistics/export')
def export_statistics():
    data = dm.get_statistics()

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
    out_path = os.path.join(out_dir, f"statistics_{dm.active_project_id or 'project'}.xlsx")
    wb.save(out_path)
    return send_file(out_path, as_attachment=True, download_name="数据分析导出.xlsx")

@grid_bp.route('/api/store_products/<store_id>')
def get_store_products(store_id):
    return jsonify(dm.get_store_products(store_id))

@grid_bp.route('/api/unlinked_items')
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

@grid_bp.route('/api/main_products')
def get_main_products():
    page = request.args.get('page', 1, type=int)
    limit = request.args.get('limit', 50, type=int)
    search = request.args.get('search', "")
    return jsonify(dm.get_main_products_page(page=page, limit=limit, search=search))

@grid_bp.route('/api/main_products/<path:main_sku_id>/links')
def get_main_product_links(main_sku_id):
    return jsonify(dm.get_main_product_links(main_sku_id))

@grid_bp.route('/api/eliminate', methods=['POST'])
def eliminate():
    d = request.json
    main_sku_id = d.get('main_sku_id')
    if not main_sku_id:
        return jsonify({"status": "error", "message": "Missing main_sku_id"}), 400
    dm.eliminate_product(main_sku_id, d.get('status', 1))
    return jsonify({"status": "success"})

@grid_bp.route('/api/toggle_handled', methods=['POST'])
def toggle_handled():
    d = request.json
    sku_id = d.get('main_sku_id')
    if not sku_id:
        return jsonify({"status": "error", "message": "Missing main_sku_id"}), 400
    dm.toggle_handled(sku_id, d.get('handled', True))
    return jsonify({"status": "success"})

@grid_bp.route('/api/toggle_ref', methods=['POST'])
def toggle_ref():
    d = request.json
    sku_id = d.get('main_sku_id')
    field = d.get('field')
    store_id = d.get('store_id', '')
    if not sku_id or field not in ('name', 'image'):
        return jsonify({"status": "error", "message": "Missing params"}), 400
    dm.set_ref(sku_id, field, store_id)
    return jsonify({"status": "success"})

@grid_bp.route('/api/toggle_add', methods=['POST'])
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

@grid_bp.route('/api/toggle_ignore', methods=['POST'])
def toggle_ignore():
    d = request.json
    store_id = d.get('store_id')
    comp_sku_id = d.get('sku_id')
    if store_id is None or not comp_sku_id:
        return jsonify({"status": "error", "message": "Missing store_id or sku_id"}), 400
    ok = dm.mark_as_ignored(store_id, comp_sku_id, d.get('is_ignored', True))
    if not ok:
        return jsonify({"status": "error", "message": "未找到可标记的商品"}), 400
    return jsonify({"status": "success"})

@grid_bp.route('/api/price_match', methods=['POST'])
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

@grid_bp.route('/api/clear_price_match', methods=['POST'])
def clear_price_match():
    d = request.json
    main_sku_id = d.get('main_sku_id')
    if not main_sku_id:
        return jsonify({"status": "error", "message": "Missing main_sku_id"}), 400
    dm.clear_price_match(main_sku_id)
    return jsonify({"status": "success"})

@grid_bp.route('/api/manual_link', methods=['POST'])
def manual_link():
    d = request.json
    main_sku_id = d.get('main_sku_id')
    store_id = d.get('store_id')
    comp_sku_id = d.get('comp_sku_id')
    if not main_sku_id or store_id is None or not comp_sku_id:
        return jsonify({"status": "error", "message": "Missing params"}), 400
    dm.manual_link(main_sku_id, store_id, comp_sku_id)
    return jsonify({"status": "success"})

@grid_bp.route('/api/unlink', methods=['POST'])
def unlink():
    d = request.json
    main_sku_id = d.get('main_sku_id')
    store_id = d.get('store_id')
    if not main_sku_id or store_id is None:
        return jsonify({"status": "error", "message": "Missing params"}), 400
    dm.unlink_product(main_sku_id, store_id)
    return jsonify({"status": "success"})

@grid_bp.route('/api/update_cell', methods=['POST'])
def update_cell():
    d = request.json
    main_sku_id = d.get('main_sku_id')
    if not main_sku_id:
        return jsonify({"status": "error", "message": "Missing main_sku_id"}), 400
    dm.update_cell(main_sku_id, {d.get('column'): d.get('value')})
    return jsonify({"status": "success"})

@grid_bp.route('/img/<path:filename>')
def serve_img(filename):
    return send_from_directory(os.path.join(data_root, "img"), filename)

@grid_bp.route('/api/export')
def export_data():
    p = dm.save_separate_exports()
    resp = send_file(p, as_attachment=True)
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"; resp.headers["Pragma"] = "no-cache"; resp.headers["Expires"] = "0"
    return resp

@grid_bp.route('/api/export_new')
def export_new_data():
    p = dm.export_new_items()
    resp = send_file(p, as_attachment=True)
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"; resp.headers["Pragma"] = "no-cache"; resp.headers["Expires"] = "0"
    return resp
