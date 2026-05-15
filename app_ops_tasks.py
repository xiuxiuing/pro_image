import os
import time
import threading
import traceback
import shutil
from flask import Blueprint, request, jsonify

# Initialized by app_ops.init_ops
extract_info_ai2 = None
main_030822 = None

tasks_bp = Blueprint('ops_tasks', __name__)


def init_ops_tasks(context):
    global extract_info_ai2, main_030822
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

    if from_task:
        _ops_set_task(task_id, source_task_id=astar_task_id)

    def _run_output_bg():
        _ops_set_task(task_id, status="running", started_at=_ops_now(), message="Output 生成中")
        try:
            _ops_update_step(task_id, 0, "running", "检查文件")
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
            out_path = main_030822.run_analysis(
                saved_main["path"],
                [item["path"] for item in saved_comps],
                output_name=out_name,
                output_dir=outputs_dir,
                progress_cb=_analysis_cb,
                match_config=None,
                post_match_template=rule_template.get("config"),
            )
            final_name = f"output_{rule_template.get('name') or '规则模板'}_{time.strftime('%Y%m%d_%H%M%S')}.xlsx"
            final_path = os.path.join(outputs_dir, final_name)
            if os.path.abspath(out_path) != os.path.abspath(final_path):
                shutil.move(out_path, final_path)
            _ops_update_step(task_id, len(_ops_get_task(task_id).get("steps", [])) - 1, "done", "分析完成")
            _ops_set_task(
                task_id,
                status="done",
                ended_at=_ops_now(),
                message="Output 生成完成",
                result_path=final_path,
                result_kind="output_xlsx",
                download_name=final_name,
            )
        except BaseException as e:
            traceback.print_exc()
            running_idx = next((i for i, s in enumerate((_ops_get_task(task_id) or {}).get("steps", [])) if s["status"] == "running"), 0)
            _ops_update_step(task_id, running_idx, "failed", str(e))
            _ops_fail_task(task_id, e)

    threading.Thread(target=_run_output_bg, daemon=True).start()
    return jsonify({"status": "ok", "task_id": task_id})
