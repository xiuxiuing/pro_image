import os
import time
import threading
import traceback
import shutil
import json
from flask import Blueprint, request, jsonify
import quality_preflight
import pandas as pd
from PIL import Image, ImageDraw, ImageFont

# Initialized by app_ops.init_ops
extract_info_ai2 = None
main_030822 = None

tasks_bp = Blueprint('ops_tasks', __name__)


def init_ops_tasks(context):
    global extract_info_ai2, main_030822, _ops_dm
    global _ops_license_error_response, _ops_collect_astar_source_files, _ops_validate_source_uploads
    global _ops_file_label, _ops_create_task, _ops_task_dir, _ops_save_file, _ops_copy_to_dir
    global _ops_validate_astar_input_columns, _ops_update_step, _ops_set_task, _ops_zip_files
    global _ops_get_task, _ops_fail_task, _ops_rule_template_from_request
    global _ops_public_astar_file_choices, _ops_get_astar_choice, _ops_validate_excel_uploads
    global _ops_now, _ops_validate_upload
    extract_info_ai2 = context["extract_info_ai2"]
    main_030822 = context["main_030822"]
    _ops_license_error_response = context["license_error_response"]
    _ops_collect_astar_source_files = context["collect_astar_source_files"]
    _ops_validate_source_uploads = context["validate_source_uploads"]
    _ops_file_label = context["file_label"]
    _ops_create_task = context["create_task"]
    _ops_task_dir = context["task_dir"]
    _ops_save_file = context["save_file"]
    _ops_copy_to_dir = context["copy_to_dir"]
    _ops_validate_astar_input_columns = context["validate_astar_input_columns"]
    _ops_update_step = context["update_step"]
    _ops_set_task = context["set_task"]
    _ops_zip_files = context["zip_files"]
    _ops_get_task = context["get_task"]
    _ops_fail_task = context["fail_task"]
    _ops_rule_template_from_request = context["rule_template_from_request"]
    _ops_public_astar_file_choices = context["public_astar_file_choices"]
    _ops_get_astar_choice = context["get_astar_choice"]
    _ops_validate_excel_uploads = context["validate_excel_uploads"]
    _ops_now = context["now"]
    _ops_validate_upload = context["validate_upload"]
    _ops_dm = context["dm"]


def _ops_market_font(size, bold=False):
    candidates = [
        ("/System/Library/Fonts/PingFang.ttc", 0),
        ("/System/Library/Fonts/STHeiti Light.ttc", 0),
        ("/System/Library/Fonts/Supplemental/Arial Unicode.ttf", 0),
        ("/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc", 0),
    ]
    for path, index in candidates:
        if not os.path.isfile(path):
            continue
        try:
            return ImageFont.truetype(path, size=size, index=index)
        except Exception:
            continue
    return ImageFont.load_default()


def _ops_market_truncate(text, limit=14):
    text = str(text or "").strip()
    if len(text) <= limit:
        return text or "-"
    return text[: max(1, limit - 1)] + "…"


def _ops_market_draw_text(draw, xy, text, font, fill, anchor=None):
    kwargs = {"font": font, "fill": fill}
    if anchor:
        kwargs["anchor"] = anchor
    draw.text(xy, str(text), **kwargs)


def _ops_market_compute_from_files(items):
    buckets = _ops_dm._load_market_category_buckets()
    file_frames = []
    for idx, item in enumerate(items):
        src_path = item["path"]
        normalized_path = os.path.join(os.path.dirname(src_path), f"market_normalized_{idx + 1}.xlsx")
        try:
            read_path = quality_preflight.normalize_file_for_analysis(src_path, normalized_path, {})
        except Exception:
            read_path = src_path
        df = pd.read_excel(read_path)
        file_frames.append(
            _ops_dm._market_prepare_file_df(
                df,
                f"upload-{idx}",
                item.get("original_name") or f"文件{idx + 1}",
                buckets,
            )
        )

    file_frames = [df for df in file_frames if df is not None and not df.empty]
    file_count = len(file_frames)
    if not file_count:
        return {
            "status": "error",
            "message": "未从上传文件中解析出有效商圈样本数据",
            "file_count": 0,
            "top10_categories": [],
            "recommendation": {},
            "metrics": {"average": {}, "top1": {}},
            "metric_diffs": {},
            "mode_options": ["average", "top1"],
        }

    all_df = pd.concat(file_frames, ignore_index=True)
    valid_category_df = all_df[(all_df["category_l1"] != "") & (all_df["category_l1"].str.lower() != "nan")]
    category_rows = []
    if not valid_category_df.empty:
        grouped = valid_category_df.groupby("category_l1", sort=False)[["sales", "sales_amount"]].sum().reset_index()
        grouped["sales"] = grouped["sales"] / file_count
        grouped["sales_amount"] = grouped["sales_amount"] / file_count
        grouped = grouped.sort_values("sales_amount", ascending=False).head(10)
        category_rows = [
            {
                "category": row["category_l1"],
                "order_count": round(float(row["sales"]), 2),
                "sales_amount": round(float(row["sales_amount"]), 2),
            }
            for _, row in grouped.iterrows()
        ]

    total_sales = float(all_df["sales"].sum())
    total_sales_amount = float(all_df["sales_amount"].sum())
    average_sales = total_sales / file_count
    average_sales_amount = total_sales_amount / file_count
    bucket_amounts = all_df.groupby("bucket")["sales_amount"].sum().to_dict()
    department_amount = float(bucket_amounts.get("department_store", 0)) / file_count
    snack_amount = float(bucket_amounts.get("snack", 0)) / file_count
    other_amount = float(bucket_amounts.get("other", 0)) / file_count
    ratio_base = average_sales_amount
    department_ratio = department_amount / ratio_base * 100 if ratio_base else 0.0
    snack_ratio = snack_amount / ratio_base * 100 if ratio_base else 0.0
    other_ratio = max(0.0, 100.0 - department_ratio - snack_ratio) if ratio_base else 100.0

    per_file = all_df.groupby(["file_id", "file_name"], sort=False)[["sales", "sales_amount"]].sum().reset_index()
    if per_file.empty:
        top1_sales = top1_sales_amount = 0.0
        top1_file = {"file_id": "", "file_name": ""}
    else:
        top1_row = per_file.sort_values("sales_amount", ascending=False).iloc[0]
        top1_sales = float(top1_row["sales"])
        top1_sales_amount = float(top1_row["sales_amount"])
        top1_file = {"file_id": str(top1_row["file_id"]), "file_name": str(top1_row["file_name"])}

    average_metrics = _ops_dm._market_metric_pack(average_sales, average_sales_amount)
    top1_metrics = _ops_dm._market_metric_pack(top1_sales, top1_sales_amount)
    metric_diffs = {
        key: int(round(float(average_metrics.get(key, 0)) - float(top1_metrics.get(key, 0))))
        for key in average_metrics.keys()
    }

    return {
        "status": "ok",
        "file_count": file_count,
        "top10_categories": category_rows,
        "recommendation": {
            "level": _ops_dm._market_level_for_sales(average_sales_amount),
            "total_sales_amount": round(average_sales_amount, 2),
            "department_store_ratio": round(department_ratio, 2),
            "snack_ratio": round(snack_ratio, 2),
            "other_ratio": round(other_ratio, 2),
            "amounts": {
                "department_store": round(department_amount, 2),
                "snack": round(snack_amount, 2),
                "other": round(other_amount, 2),
            },
        },
        "metrics": {
            "average": average_metrics,
            "top1": top1_metrics,
        },
        "metric_diffs": metric_diffs,
        "top1_file": top1_file,
        "mode_options": ["average", "top1"],
    }


def _ops_market_fmt_int(value):
    return f"{int(round(float(value or 0))):,}"


def _ops_market_fmt_money(value):
    return f"{int(round(float(value or 0))):,}"


def _ops_market_fmt_decimal(value):
    number = float(value or 0)
    if abs(number) >= 1000:
        return f"{int(round(number)):,}"
    text = f"{number:.2f}"
    return text[:-3] if text.endswith(".00") else text


def _ops_market_render_png(data, out_path):
    width, height = 1600, 960
    bg = "#BBD2C6"
    panel = "#FDF9F1"
    image = Image.new("RGB", (width, height), bg)
    draw = ImageDraw.Draw(image)
    title_font = _ops_market_font(38, bold=True)
    sub_font = _ops_market_font(20)
    panel_title_font = _ops_market_font(26, bold=True)
    value_font = _ops_market_font(34, bold=True)
    label_font = _ops_market_font(18)
    small_font = _ops_market_font(16)
    tiny_font = _ops_market_font(14)

    def panel_box(x1, y1, x2, y2, radius=24):
        draw.rounded_rectangle((x1, y1, x2, y2), radius=radius, fill=panel)

    _ops_market_draw_text(draw, (58, 36), "商圈分析", title_font, "#0f172a")
    _ops_market_draw_text(draw, (58, 84), f"商圈样本汇总 · 共 {data.get('file_count', 0)} 份文件", sub_font, "#64748b")

    left = (40, 130, 980, 575)
    right = (1000, 130, 1560, 575)
    bottom = (40, 600, 1560, 920)
    panel_box(*left)
    panel_box(*right)
    panel_box(*bottom)

    _ops_market_draw_text(draw, (74, 162), "top10类目", panel_title_font, "#333333")
    _ops_market_draw_text(draw, (1034, 162), "商圈建议", panel_title_font, "#333333")
    _ops_market_draw_text(draw, (74, 632), "关键指标", panel_title_font, "#333333")

    top_rows = (data.get("top10_categories") or [])[:10]
    chart_x = 106
    chart_y = 224
    chart_w = 790
    chart_h = 250
    baseline = chart_y + chart_h
    axis_color = "#E5E7EB"
    max_value = max(
        [float(item.get("order_count") or 0) for item in top_rows]
        + [float(item.get("sales_amount") or 0) for item in top_rows]
        + [1.0]
    )
    for line_idx in range(6):
        y = baseline - int(chart_h * line_idx / 5)
        draw.line((chart_x, y, chart_x + chart_w, y), fill=axis_color, width=1)
        label = _ops_market_fmt_decimal(max_value * line_idx / 5)
        _ops_market_draw_text(draw, (chart_x - 10, y), label, tiny_font, "#94a3b8", anchor="rm")

    if top_rows:
        group_w = chart_w / len(top_rows)
        bar_w = min(22, max(10, int(group_w * 0.22)))
        for idx, item in enumerate(top_rows):
            center_x = chart_x + group_w * idx + group_w / 2
            sales_value = float(item.get("order_count") or 0)
            amount_value = float(item.get("sales_amount") or 0)
            sales_h = max(2, int((sales_value / max_value) * chart_h)) if sales_value else 0
            amount_h = max(2, int((amount_value / max_value) * chart_h)) if amount_value else 0
            x1 = int(center_x - bar_w - 2)
            x2 = int(center_x + 2)
            if sales_h:
                draw.rounded_rectangle((x1, baseline - sales_h, x1 + bar_w, baseline), radius=4, fill="#DE861E")
            if amount_h:
                draw.rounded_rectangle((x2, baseline - amount_h, x2 + bar_w, baseline), radius=4, fill="#A0C6B8")
            category = _ops_market_truncate(item.get("category"), 5)
            _ops_market_draw_text(draw, (center_x, baseline + 18), category, tiny_font, "#64748b", anchor="mm")
    else:
        _ops_market_draw_text(draw, (chart_x + chart_w / 2, chart_y + chart_h / 2), "暂无有效类目数据", label_font, "#94a3b8", anchor="mm")

    legend_y = 528
    legend_x = 380
    draw.rounded_rectangle((legend_x, legend_y, legend_x + 14, legend_y + 14), radius=3, fill="#DE861E")
    _ops_market_draw_text(draw, (legend_x + 24, legend_y + 2), "商品销量", tiny_font, "#64748b")
    draw.rounded_rectangle((legend_x + 120, legend_y, legend_x + 134, legend_y + 14), radius=3, fill="#A0C6B8")
    _ops_market_draw_text(draw, (legend_x + 144, legend_y + 2), "销售额", tiny_font, "#64748b")

    rec = data.get("recommendation") or {}
    center = (1280, 325)
    outer_r = 132
    inner_r = 82
    ratios = [
        ("百货占比", float(rec.get("department_store_ratio") or 0), "#DE861E"),
        ("休食占比", float(rec.get("snack_ratio") or 0), "#F1D4A7"),
        ("其他占比", float(rec.get("other_ratio") or 0), "#A0C6B8"),
    ]
    start = -90.0
    for _, value, color in ratios:
        sweep = 360.0 * (value / 100.0 if value else 0.0)
        if sweep > 0:
            draw.pieslice(
                (center[0] - outer_r, center[1] - outer_r, center[0] + outer_r, center[1] + outer_r),
                start=start,
                end=start + sweep,
                fill=color,
            )
            start += sweep
    draw.ellipse((center[0] - inner_r, center[1] - inner_r, center[0] + inner_r, center[1] + inner_r), fill=panel)
    _ops_market_draw_text(draw, (center[0], center[1] - 12), rec.get("level") or "拉完了", value_font, "#333333", anchor="mm")
    _ops_market_draw_text(draw, (center[0], center[1] + 30), "商圈建议", small_font, "#64748b", anchor="mm")
    legend_y = 500
    legend_x = 1050
    for idx, (label, value, color) in enumerate(ratios):
        x = legend_x + idx * 160
        draw.rounded_rectangle((x, legend_y, x + 14, legend_y + 14), radius=3, fill=color)
        _ops_market_draw_text(draw, (x + 24, legend_y + 2), f"{label} {_ops_market_fmt_decimal(value)}%", tiny_font, "#64748b")

    cards = [
        ("日均单量(估算)", data.get("metrics", {}).get("average", {}).get("daily_orders", 0), data.get("metric_diffs", {}).get("daily_orders", 0), "#F3CD95"),
        ("月总单量", data.get("metrics", {}).get("average", {}).get("monthly_orders", 0), data.get("metric_diffs", {}).get("monthly_orders", 0), "#B8D6C9"),
        ("月销售额", data.get("metrics", {}).get("average", {}).get("monthly_sales_amount", 0), data.get("metric_diffs", {}).get("monthly_sales_amount", 0), "#B6BB6D"),
        ("客单价(估算)", data.get("metrics", {}).get("average", {}).get("customer_unit_price", 0), data.get("metric_diffs", {}).get("customer_unit_price", 0), "#F2A9B8"),
        ("预计毛利(估算)", data.get("metrics", {}).get("average", {}).get("estimated_gross_profit", 0), data.get("metric_diffs", {}).get("estimated_gross_profit", 0), "#E7D9AF"),
    ]
    card_w = 278
    gap = 20
    start_x = 74
    card_y = 690
    for idx, (label, value, diff, color) in enumerate(cards):
        x1 = start_x + idx * (card_w + gap)
        x2 = x1 + card_w
        draw.rounded_rectangle((x1, card_y, x2, card_y + 180), radius=18, fill=color)
        _ops_market_draw_text(draw, (x1 + 22, card_y + 28), _ops_market_fmt_decimal(value), value_font, "#333333")
        _ops_market_draw_text(draw, (x1 + 22, card_y + 82), label, label_font, "#555555")
        pill_w = 104
        draw.rounded_rectangle((x1 + 22, card_y + 120, x1 + 22 + pill_w, card_y + 148), radius=14, fill="#FFFFFF")
        _ops_market_draw_text(draw, (x1 + 34, card_y + 127), f"对比 {int(diff):+d}", tiny_font, "#64748b")

    image.save(out_path, format="PNG")


@tasks_bp.route('/api/ops/market-analysis-generate', methods=['POST'])
def api_ops_market_analysis_generate():
    license_err = _ops_license_error_response()
    if license_err:
        return license_err

    source_files = [f for f in request.files.getlist('source_files') if f and f.filename]
    err = _ops_validate_source_uploads(source_files)
    if err:
        return jsonify({"status": "error", "message": err}), 400

    task = _ops_create_task(
        "market_analysis",
        ["准备文件", "解析样本", "聚合指标", "生成看板", "导出图片"],
        "商圈分析排队中",
    )
    task_id = task["task_id"]
    task_dir = _ops_task_dir(task_id)
    sources_dir = os.path.join(task_dir, "sources")
    os.makedirs(sources_dir, exist_ok=True)
    saved_sources = [_ops_save_file(f, sources_dir, "market", i) for i, f in enumerate(source_files)]

    def _run_market_bg():
        _ops_set_task(task_id, status="running", started_at=_ops_now(), message="商圈分析生成中")
        try:
            _ops_update_step(task_id, 0, "running", f"已接收 {len(saved_sources)} 个文件")
            _ops_update_step(task_id, 0, "done", "完成")

            _ops_update_step(task_id, 1, "running", "读取 Excel 样本")
            for item in saved_sources:
                pd.read_excel(item["path"], nrows=3)
            _ops_update_step(task_id, 1, "done", "完成")

            _ops_update_step(task_id, 2, "running", "计算商圈汇总")
            market_data = _ops_market_compute_from_files(saved_sources)
            if market_data.get("status") != "ok":
                raise RuntimeError(market_data.get("message") or "商圈分析聚合失败")
            _ops_update_step(task_id, 2, "done", f"{market_data.get('file_count', 0)} 份样本")

            _ops_update_step(task_id, 3, "running", "绘制 PNG 看板")
            result_name = f"商圈分析看板_{time.strftime('%Y%m%d_%H%M%S')}.png"
            result_path = os.path.join(task_dir, result_name)
            _ops_market_render_png(market_data, result_path)
            _ops_update_step(task_id, 3, "done", "完成")

            _ops_update_step(task_id, 4, "running", "准备下载结果")
            _ops_update_step(task_id, 4, "done", "完成")
            _ops_set_task(
                task_id,
                status="done",
                ended_at=_ops_now(),
                message="商圈分析图片已生成",
                result_path=result_path,
                result_kind="market_analysis_png",
                download_name=result_name,
            )
        except BaseException as e:
            traceback.print_exc()
            running_idx = next((i for i, s in enumerate((_ops_get_task(task_id) or {}).get("steps", [])) if s["status"] == "running"), 0)
            _ops_update_step(task_id, running_idx, "failed", str(e))
            _ops_fail_task(task_id, e)

    threading.Thread(target=_run_market_bg, daemon=True).start()
    return jsonify({"status": "ok", "task_id": task_id})


@tasks_bp.route('/api/ops/astar-extract', methods=['POST'])
def api_ops_astar_extract():
    license_err = _ops_license_error_response()
    if license_err:
        return license_err
    source_files = _ops_collect_astar_source_files()
    err = _ops_validate_source_uploads(source_files)
    if err:
        return jsonify({"status": "error", "message": err}), 400

    api_key = (request.form.get("api_key") or "").strip()
    if not api_key:
        return jsonify({"status": "error", "message": "请填写 Gemini API Key"}), 400
    ai_model_name = (request.form.get("ai_model_name") or "").strip()
    kimi_api_key = (request.form.get("kimi_api_key") or "").strip()
    kimi_model_name = (request.form.get("kimi_model_name") or "").strip()

    labels = [_ops_file_label(f, f"文件{i+1}") for i, f in enumerate(source_files)]
    task = _ops_create_task("astar", [f"A* 提取 {label}" for label in labels], "A* 提取排队中")
    task_id = task["task_id"]
    task_dir = _ops_task_dir(task_id)
    sources_dir = os.path.join(task_dir, "sources")
    astar_dir = os.path.join(task_dir, "astar")
    os.makedirs(sources_dir, exist_ok=True)
    os.makedirs(astar_dir, exist_ok=True)

    saved_sources = [_ops_save_file(f, sources_dir, "source", i) for i, f in enumerate(source_files)]
    astar_files = []
    for i, item in enumerate(saved_sources):
        astar_item = _ops_copy_to_dir(item["path"], astar_dir, "source", i, item["original_name"])
        astar_item["index"] = i
        astar_files.append(astar_item)

    def _run_astar_bg():
        _ops_set_task(task_id, status="running", started_at=_ops_now(), message="A* 提取中")
        try:
            for idx, item in enumerate(astar_files):
                _ops_update_step(task_id, idx, "running", "检查表头")
                _ops_validate_astar_input_columns(item["path"])

                def _ai_cb(batch, total, _idx=idx):
                    _ops_update_step(task_id, _idx, "running", f"batch {batch}/{total}")

                extract_info_ai2.process_file_ai(
                    item["path"],
                    api_key,
                    progress_cb=_ai_cb,
                    model_name=ai_model_name,
                    fallback_api_key=kimi_api_key or None,
                    fallback_model=kimi_model_name or None,
                )
                _ops_update_step(task_id, idx, "done", "完成")

            zip_name = f"A星提取结果_{time.strftime('%Y%m%d_%H%M%S')}.zip"
            zip_path = os.path.join(task_dir, zip_name)
            zip_items = [{"path": item["path"], "arcname": item["original_name"]} for item in astar_files]
            _ops_zip_files(zip_items, zip_path)
            _ops_set_task(
                task_id,
                status="done",
                ended_at=_ops_now(),
                message="A* 提取完成",
                result_path=zip_path,
                result_kind="astar_zip",
                download_name=zip_name,
                astar_files=astar_files,
                astar_main=astar_files[0] if astar_files else None,
                astar_comps=astar_files[1:] if len(astar_files) > 1 else [],
            )
        except BaseException as e:
            traceback.print_exc()
            _ops_update_step(task_id, next((i for i, s in enumerate(_ops_get_task(task_id).get("steps", [])) if s["status"] == "running"), 0), "failed", str(e))
            _ops_fail_task(task_id, e)

    threading.Thread(target=_run_astar_bg, daemon=True).start()
    return jsonify({"status": "ok", "task_id": task_id})

@tasks_bp.route('/api/ops/output-generate', methods=['POST'])
def api_ops_output_generate():
    license_err = _ops_license_error_response()
    if license_err:
        return license_err
    astar_task_id = (request.form.get("astar_task_id") or "").strip()
    use_astar_task = (request.form.get("use_astar_task") or "").strip() == "1"
    main_source = (request.form.get("main_source") or "").strip() or "local"
    comp_source = (request.form.get("comp_source") or "").strip() or "local"
    main_file = request.files.get('main_file')
    comp_files = [f for f in request.files.getlist('comp_files') if f and f.filename]
    preflight_confirmed = (request.form.get("preflight_confirmed") or "").strip() == "1"
    try:
        column_mappings = json.loads(request.form.get("column_mappings_json") or "{}")
    except json.JSONDecodeError:
        column_mappings = {}

    try:
        rule_template = _ops_rule_template_from_request(request.form.get("rule_template_id"))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400

    from_task = None
    legacy_task_mode = use_astar_task and not request.form.get("main_source") and not request.form.get("comp_source")
    needs_astar_task = main_source == "task" or comp_source == "task" or (use_astar_task and astar_task_id)
    if needs_astar_task and astar_task_id:
        from_task = _ops_get_task(astar_task_id)
        if not from_task or from_task.get("status") != "done":
            return jsonify({"status": "error", "message": "上一步 A* 任务不存在或尚未完成"}), 400
        if legacy_task_mode and (not from_task.get("astar_main") or not from_task.get("astar_comps")):
            return jsonify({"status": "error", "message": "上一步任务没有可用的 A* 文件"}), 400
        if not legacy_task_mode and not _ops_public_astar_file_choices(from_task):
            return jsonify({"status": "error", "message": "上一步任务没有可用的 A* 文件"}), 400
    elif main_source == "task" or comp_source == "task":
        return jsonify({"status": "error", "message": "请先选择可用的上一步 A* 任务"}), 400

    if legacy_task_mode:
        main_input = {"source": "task", "item": from_task["astar_main"]}
        comp_inputs = [{"source": "task", "item": item} for item in (from_task.get("astar_comps") or [])]
    else:
        if main_source == "task":
            try:
                main_input = {"source": "task", "item": _ops_get_astar_choice(from_task, request.form.get("astar_main_index"))}
            except ValueError as e:
                return jsonify({"status": "error", "message": str(e)}), 400
        else:
            if not main_file or not main_file.filename:
                return jsonify({"status": "error", "message": "请上传主店 A* 文件"}), 400
            err = _ops_validate_upload(main_file, "主店 A* 文件")
            if err:
                return jsonify({"status": "error", "message": err}), 400
            main_input = {"source": "upload", "file": main_file}

        if comp_source == "task":
            comp_indexes = request.form.getlist("astar_comp_indexes")
            if not comp_indexes:
                return jsonify({"status": "error", "message": "请选择至少一个竞店 A* 文件"}), 400
            comp_inputs = []
            seen = set()
            try:
                main_task_idx = int(main_input["item"].get("index")) if main_input["source"] == "task" else None
                for raw_idx in comp_indexes:
                    item = _ops_get_astar_choice(from_task, raw_idx)
                    idx = int(item.get("index"))
                    if idx == main_task_idx:
                        return jsonify({"status": "error", "message": "竞店 A* 文件不能与主店相同"}), 400
                    if idx in seen:
                        continue
                    seen.add(idx)
                    comp_inputs.append({"source": "task", "item": item})
            except ValueError as e:
                return jsonify({"status": "error", "message": str(e)}), 400
        else:
            valid_comp_files = [f for f in comp_files if f and f.filename]
            if not valid_comp_files:
                return jsonify({"status": "error", "message": "请至少上传一个竞店 A* 文件"}), 400
            for f in valid_comp_files:
                err = _ops_validate_upload(f, f"竞店 A* 文件 ({f.filename})")
                if err:
                    return jsonify({"status": "error", "message": err}), 400
            comp_inputs = [{"source": "upload", "file": f} for f in valid_comp_files]

    if not comp_inputs:
        return jsonify({"status": "error", "message": "请至少提供一个竞店 A* 文件"}), 400

    if not use_astar_task and main_source == "local" and comp_source == "local":
        err = _ops_validate_excel_uploads(main_file, comp_files)
        if err:
            return jsonify({"status": "error", "message": err}), 400

    task = _ops_create_task(
        "output",
        ["准备文件"] + [f"向量分析 竞店{i+1}" for i in range(len(comp_inputs))] + ["查询匹配主店"],
        "Output 生成排队中",
    )
    task_id = task["task_id"]
    task_dir = _ops_task_dir(task_id)
    sources_dir = os.path.join(task_dir, "sources")
    outputs_dir = os.path.join(task_dir, "outputs")
    os.makedirs(sources_dir, exist_ok=True)
    os.makedirs(outputs_dir, exist_ok=True)

    if main_input["source"] == "task":
        src_main = main_input["item"]
        saved_main = _ops_copy_to_dir(src_main["path"], sources_dir, "main", 0, src_main.get("original_name"))
    else:
        saved_main = _ops_save_file(main_input["file"], sources_dir, "main", 0)

    saved_comps = []
    for i, comp_input in enumerate(comp_inputs):
        if comp_input["source"] == "task":
            item = comp_input["item"]
            saved_comps.append(_ops_copy_to_dir(item["path"], sources_dir, "comp", i, item.get("original_name")))
        else:
            saved_comps.append(_ops_save_file(comp_input["file"], sources_dir, "comp", i))

    preflight_files = [{"key": "main", "label": "主店 A* 文件", "path": saved_main["path"]}] + [
        {"key": f"comp_{idx}", "label": f"竞店{idx+1} A* 文件", "path": item["path"]}
        for idx, item in enumerate(saved_comps)
    ]
    preflight = quality_preflight.inspect_files(
        preflight_files,
        user_mappings=column_mappings,
        rule_template=rule_template.get("config"),
    )
    if preflight["level"] == "block":
        return jsonify({"status": "error", "message": "预检未通过，请修正字段后再生成 Output", "preflight": preflight}), 400
    if preflight.get("requires_confirmation") and not preflight_confirmed:
        return jsonify({"status": "needs_confirmation", "message": "预检发现字段风险，请确认后继续", "preflight": preflight}), 409

    if from_task:
        _ops_set_task(task_id, source_task_id=astar_task_id)

    def _run_output_bg():
        _ops_set_task(task_id, status="running", started_at=_ops_now(), message="Output 生成中")
        try:
            _ops_update_step(task_id, 0, "running", "检查文件")
            normalized_dir = os.path.join(task_dir, "normalized")
            saved_main["path"] = quality_preflight.normalize_file_for_analysis(
                saved_main["path"],
                os.path.join(normalized_dir, "main_normalized.xlsx"),
                (column_mappings or {}).get("main") or {},
            )
            for idx, item in enumerate(saved_comps):
                item["path"] = quality_preflight.normalize_file_for_analysis(
                    item["path"],
                    os.path.join(normalized_dir, f"comp_{idx}_normalized.xlsx"),
                    (column_mappings or {}).get(f"comp_{idx}") or {},
                )
            _ops_validate_astar_input_columns(saved_main["path"])
            for item in saved_comps:
                _ops_validate_astar_input_columns(item["path"])
            _ops_update_step(task_id, 0, "done", "完成")

            analysis_base = 1

            def _analysis_cb(event, idx=0, detail=""):
                if event == "source_start":
                    _ops_update_step(task_id, analysis_base + idx, "running", detail)
                elif event == "source_done":
                    _ops_update_step(task_id, analysis_base + idx, "done", "完成")
                elif event == "query_start":
                    _ops_update_step(task_id, len(_ops_get_task(task_id).get("steps", [])) - 1, "running", detail)
                elif event == "query_progress":
                    _ops_update_step(task_id, len(_ops_get_task(task_id).get("steps", [])) - 1, "running", detail)

            out_name = f"ops_{task_id}"
            analysis_metrics = {}
            out_path = main_030822.run_analysis(
                saved_main["path"],
                [item["path"] for item in saved_comps],
                output_name=out_name,
                output_dir=outputs_dir,
                progress_cb=_analysis_cb,
                match_config=None,
                post_match_template=rule_template.get("config"),
                analysis_metrics=analysis_metrics,
            )
            final_name = f"output_{rule_template.get('name') or '规则模板'}_{time.strftime('%Y%m%d_%H%M%S')}.xlsx"
            final_path = os.path.join(outputs_dir, final_name)
            if os.path.abspath(out_path) != os.path.abspath(final_path):
                shutil.move(out_path, final_path)
            report = quality_preflight.build_quality_report(
                preflight,
                analysis_metrics,
                {"task_id": task_id, "rule_template": rule_template.get("name") or ""},
            )
            report_path = os.path.join(outputs_dir, "quality_report.json")
            quality_preflight.save_quality_report(report, report_path)
            _ops_update_step(task_id, len(_ops_get_task(task_id).get("steps", [])) - 1, "done", "分析完成")
            _ops_set_task(
                task_id,
                status="done",
                ended_at=_ops_now(),
                message="Output 生成完成",
                result_path=final_path,
                result_kind="output_xlsx",
                download_name=final_name,
                quality_report_path=report_path,
                quality_summary=report.get("summary") or {},
            )
        except BaseException as e:
            traceback.print_exc()
            running_idx = next((i for i, s in enumerate((_ops_get_task(task_id) or {}).get("steps", [])) if s["status"] == "running"), 0)
            _ops_update_step(task_id, running_idx, "failed", str(e))
            _ops_fail_task(task_id, e)

    threading.Thread(target=_run_output_bg, daemon=True).start()
    return jsonify({"status": "ok", "task_id": task_id})
