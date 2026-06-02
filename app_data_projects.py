import os
import time
import threading
import traceback
import shutil
import json
from flask import Blueprint, request, jsonify
import utils
import quality_preflight
from online_jobs import JobStore
from project_analysis_runner import ProgressFns, run_auto_analysis, run_manual_import

projects_bp = Blueprint('data_projects', __name__)
_active_analysis_pids = set()
_active_analysis_lock = threading.Lock()


def _celery_enabled():
    return os.environ.get("PROIMAGE_USE_CELERY", "").strip().lower() in ("1", "true", "yes", "on")


def _progress_fns():
    return ProgressFns(_init_progress, _init_import_progress, _update_step, _schedule_clear_progress)


def init_projects(context):
    global dm, _init_progress, _init_import_progress, _update_step, _schedule_clear_progress
    global _get_analysis_progress_data, _validate_upload, _safe_upload_filename, data_root
    dm = context["dm"]
    _init_progress = context["init_progress"]
    _init_import_progress = context["init_import_progress"]
    _update_step = context["update_step"]
    _schedule_clear_progress = context["schedule_clear_progress"]
    _get_analysis_progress_data = context["get_analysis_progress_data"]
    _validate_upload = context["validate_upload"]
    _safe_upload_filename = context["safe_upload_filename"]
    data_root = context["data_root"]


@projects_bp.route('/api/projects', methods=['GET', 'POST'])
def handle_projects():
    if request.method == 'POST':
        name = request.form.get('name')
        if not name: return jsonify({"status": "error", "message": "Project name is required"}), 400

        main_file = request.files.get('main_file')
        comp_files = request.files.getlist('comp_files')
        
        if not main_file or not main_file.filename:
            return jsonify({"status": "error", "message": "Main store file is required"}), 400
        
        valid_comp_files = [f for f in comp_files if f.filename]
        if not valid_comp_files:
            return jsonify({"status": "error", "message": "At least one competitor store file is required"}), 400

        raw_rt = (request.form.get("rule_template_id") or "").strip()
        try:
            rule_template_id = int(raw_rt) if raw_rt else None
        except ValueError:
            rule_template_id = None
        if rule_template_id and not dm.get_rule_template(rule_template_id):
            return jsonify({"status": "error", "message": "规则模板不存在"}), 400

        err = _validate_upload(main_file, "主店文件")
        if err: return jsonify({"status": "error", "message": err}), 400
        for f in valid_comp_files:
            err = _validate_upload(f, f"竞店文件 ({f.filename})")
            if err: return jsonify({"status": "error", "message": err}), 400
        import time, os, shutil
        # Temporary PID for directory naming
        temp_pid = int(time.time())
        dirs = dm.storage.ensure_project_dirs(temp_pid)
        proj_dir = dirs["root"]
        sources_dir = dirs["sources"]
        
        # Save files with role prefixes
        main_saved_name = "main__" + _safe_upload_filename(main_file.filename, "main.xlsx")
        main_path = os.path.join(sources_dir, main_saved_name)
        main_file.save(main_path)
        main_store_name = main_file.filename.replace(".xlsx", "").replace(".xls", "")
        
        # Save Competitor Store Files
        comp_infos = []
        for idx, f in enumerate(valid_comp_files):
            comp_saved_name = f"comp_{idx}__" + _safe_upload_filename(f.filename, f"comp_{idx}.xlsx")
            path = os.path.join(sources_dir, comp_saved_name)
            f.save(path)
            comp_infos.append({"path": path, "store_name": f.filename.replace(".xlsx", "").replace(".xls", "")})
        
        pid = dm.create_project(
            name,
            {"path": main_path, "store_name": main_store_name},
            comp_infos,
            status='creating',
            match_config_json="",
            rule_template_id=rule_template_id,
        )

        # Rename temp directory to real PID
        real_proj_dir = dm.storage.project_root(pid)
        if os.path.exists(real_proj_dir): shutil.rmtree(real_proj_dir)
        os.rename(proj_dir, real_proj_dir)

        with dm._db_lock:
            with dm._get_conn() as conn:
                conn.execute("UPDATE project_files SET local_path = REPLACE(local_path, ?, ?) WHERE project_id = ?",
                            (f"project_{temp_pid}", f"project_{pid}", pid))

        dm.storage.ensure_project_dirs(pid)

        import_labels = ["读取上传文件", "写入业务数据", "刷新分析快照", "完成"]
        import_job_id = JobStore(dm).create_job(
            pid,
            "source_import",
            import_labels,
            {"project_id": pid, "mode": "create_project"},
        )

        def _import_sources_bg(project_id, job_id):
            store = JobStore(dm)
            try:
                store.mark_running(job_id)
                store.update_step(job_id, 0, "running", "准备导入")
                dm.import_project_sources(project_id)
                store.update_step(job_id, 0, "done")
                store.update_step(job_id, 1, "done", "数据已写入")
                store.update_step(job_id, 2, "done", "快照已刷新")
                dm.update_project_status(project_id, 'ready')
                store.update_step(job_id, 3, "done")
                store.finish(job_id, "done")
            except BaseException:
                traceback.print_exc()
                try:
                    dm.update_project_status(project_id, 'failed')
                except Exception:
                    pass
                try:
                    store.finish(job_id, "failed", "导入失败")
                except Exception:
                    pass

        if _celery_enabled():
            try:
                from pro_image_tasks import import_project_sources_task
                import_project_sources_task.delay(pid, import_job_id)
            except Exception:
                traceback.print_exc()
                threading.Thread(target=_import_sources_bg, args=(pid, import_job_id), daemon=True).start()
        else:
            threading.Thread(target=_import_sources_bg, args=(pid, import_job_id), daemon=True).start()
        return jsonify({"status": "success", "project_id": pid, "ready": False})

    return jsonify(dm.list_projects())

@projects_bp.route('/api/projects/<int:pid>', methods=['DELETE'])
def delete_project(pid):
    dm.delete_project(pid)
    return jsonify({"status": "success"})

@projects_bp.route('/api/projects/<int:pid>/activate', methods=['POST'])
def activate_project(pid):
    projects = dm.list_projects()
    proj = next((p for p in projects if p['id'] == pid), None)
    if not proj:
        return jsonify({"status": "error", "message": "项目不存在"}), 404
    if proj.get('status') == 'analyzing':
        return jsonify({"status": "error", "message": "该项目正在分析中，请等待完成"}), 400
    if JobStore(dm).project_has_active_job(pid, ["source_import", "analysis", "manual_import"]):
        return jsonify({"status": "error", "message": "该项目已有任务正在排队或执行，请等待完成"}), 400
    if proj.get('status') == 'creating':
        return jsonify({"status": "error", "message": "该项目正在创建中，请等待完成"}), 400
    if proj.get('status') == 'failed':
        return jsonify({"status": "error", "message": "该项目分析失败，请删除后重新创建"}), 400
    dm.activate_project(pid, skip_load=True)
    return jsonify({"status": "success"})

@projects_bp.route('/api/projects/<int:pid>/preflight', methods=['POST'])
def preflight_project(pid):
    projects = dm.list_projects()
    proj = next((p for p in projects if p['id'] == pid), None)
    if not proj:
        return jsonify({"status": "error", "message": "项目不存在"}), 404
    try:
        column_mappings = json.loads(request.form.get("column_mappings_json") or "{}")
    except json.JSONDecodeError:
        column_mappings = {}
    raw_rt = (request.form.get("rule_template_id") or "").strip()
    try:
        rule_template_id = int(raw_rt) if raw_rt else None
    except ValueError:
        rule_template_id = None
    selected_rule_template = dm.get_rule_template(rule_template_id) if rule_template_id else None
    dm.activate_project(pid, skip_load=True)
    main_path = dm.target_file
    comp_paths = list(dm.source_files or [])
    if not main_path or not os.path.exists(main_path):
        return jsonify({"status": "error", "message": "主店源文件不存在，请重新创建项目"}), 400
    if not comp_paths:
        return jsonify({"status": "error", "message": "竞店源文件不存在，请重新创建项目"}), 400
    files = [{"key": "main", "label": "主店文件", "path": main_path}] + [
        {"key": f"comp_{i}", "label": f"竞店{i+1}", "path": p}
        for i, p in enumerate(comp_paths)
    ]
    return jsonify(quality_preflight.inspect_files(
        files,
        user_mappings=column_mappings,
        rule_template=(selected_rule_template or {}).get("config"),
    ))

@projects_bp.route('/api/projects/<int:pid>/analyze', methods=['POST'])
def analyze_project(pid):
    projects = dm.list_projects()
    proj = next((p for p in projects if p['id'] == pid), None)
    if not proj:
        return jsonify({"status": "error", "message": "项目不存在"}), 404
    if proj.get('status') == 'analyzing':
        return jsonify({"status": "error", "message": "该项目正在分析中，请等待完成"}), 400
    if JobStore(dm).project_has_active_job(pid, ["source_import", "analysis", "manual_import"]):
        return jsonify({"status": "error", "message": "该项目已有任务正在排队或执行，请等待完成"}), 400

    mode = (request.form.get("mode") or "auto").strip()
    match_config_json = (request.form.get('match_config_json') or "").strip()
    raw_selected_categories = (request.form.get("selected_categories") or "").strip()
    try:
        selected_categories = json.loads(raw_selected_categories) if raw_selected_categories else []
    except json.JSONDecodeError:
        selected_categories = []
    selected_categories = [str(utils.clean_text_value(c)).strip() for c in selected_categories if utils.clean_text_value(c)]
    raw_rt = (request.form.get("rule_template_id") or "").strip()
    preflight_confirmed = (request.form.get("preflight_confirmed") or "").strip() == "1"
    try:
        column_mappings = json.loads(request.form.get("column_mappings_json") or "{}")
    except json.JSONDecodeError:
        column_mappings = {}
    try:
        rule_template_id = int(raw_rt) if raw_rt else None
    except ValueError:
        rule_template_id = None
    selected_rule_template = dm.get_rule_template(rule_template_id) if rule_template_id else None

    dm.activate_project(pid, skip_load=True)
    dirs = dm._ensure_project_dirs(pid)
    main_path = dm.target_file
    comp_paths = list(dm.source_files or [])
    if not main_path or not os.path.exists(main_path):
        return jsonify({"status": "error", "message": "主店源文件不存在，请重新创建项目"}), 400
    if not comp_paths:
        return jsonify({"status": "error", "message": "竞店源文件不存在，请重新创建项目"}), 400

    preflight_files = [{"key": "main", "label": "主店文件", "path": main_path}] + [
        {"key": f"comp_{i}", "label": f"竞店{i+1}", "path": p}
        for i, p in enumerate(comp_paths)
    ]
    preflight = quality_preflight.inspect_files(
        preflight_files,
        user_mappings=column_mappings,
        rule_template=(selected_rule_template or {}).get("config"),
    )
    if preflight["level"] == "block":
        return jsonify({"status": "error", "message": "预检未通过，请修正字段后再分析", "preflight": preflight}), 400
    if preflight.get("requires_confirmation") and not preflight_confirmed:
        return jsonify({"status": "needs_confirmation", "message": "预检发现字段风险，请确认后继续", "preflight": preflight}), 409

    with dm._db_lock:
        with dm._get_conn() as conn:
            conn.execute(
                "UPDATE projects SET match_config = ?, rule_template_id = ? WHERE id = ?",
                (match_config_json or "", rule_template_id, pid),
            )
            rows = conn.execute(
                """
                SELECT DISTINCT trim(COALESCE(美团类目三级, '')) FROM main_products
                WHERE project_id = ? AND trim(COALESCE(美团类目三级, '')) != ''
                """,
                (pid,),
            ).fetchall()
            all_categories = [str(utils.clean_text_value(r[0])).strip() for r in rows if utils.clean_text_value(r[0])]

    selected_set = set(selected_categories)
    all_set = set(all_categories)
    partial_categories = sorted(selected_set) if selected_set and selected_set != all_set else []

    if mode == "manual":
        result_file = request.files.get('result_file')
        if not result_file or not result_file.filename:
            return jsonify({"status": "error", "message": "请上传关联结果文件"}), 400
        err = _validate_upload(result_file, "关联结果文件")
        if err:
            return jsonify({"status": "error", "message": err}), 400
        run_stamp = int(time.time())
        if partial_categories:
            output_file = os.path.join(dirs["outputs"], f"manual_partial_output_{pid}_{run_stamp}.xlsx")
        else:
            output_file = os.path.join(dirs["outputs"], f"manual_output_{pid}_{run_stamp}.xlsx")
        result_file.save(output_file)

        with _active_analysis_lock:
            if pid in _active_analysis_pids:
                return jsonify({"status": "error", "message": "该项目正在分析中，请等待完成"}), 400
            _active_analysis_pids.add(pid)

        try:
            with dm._db_lock:
                with dm._get_conn() as conn:
                    conn.execute(
                        "UPDATE projects SET status = 'analyzing', analysis_started_at = ? WHERE id = ?",
                        (time.strftime('%Y-%m-%d %H:%M:%S'), pid),
                    )
        except Exception:
            with _active_analysis_lock:
                _active_analysis_pids.discard(pid)
            raise

        payload = {
            "project_id": pid,
            "output_file": output_file,
            "partial_categories": list(partial_categories),
        }
        if _celery_enabled():
            payload["job_id"] = JobStore(dm).create_job(
                pid,
                "manual_import",
                ["保存关联文件", "解析关联结果", "写入关联数据", "完成"],
                {"project_id": pid, "mode": "manual"},
                status="queued",
            )
            with _active_analysis_lock:
                _active_analysis_pids.discard(pid)
            try:
                from pro_image_tasks import run_manual_import_task
                run_manual_import_task.delay(payload)
            except Exception:
                traceback.print_exc()
                with _active_analysis_lock:
                    _active_analysis_pids.add(pid)

                def _run_manual_import_bg():
                    try:
                        run_manual_import(dm, payload, _progress_fns())
                    finally:
                        with _active_analysis_lock:
                            _active_analysis_pids.discard(pid)

                threading.Thread(target=_run_manual_import_bg, daemon=True).start()
        else:
            def _run_manual_import_bg():
                try:
                    run_manual_import(dm, payload, _progress_fns())
                finally:
                    with _active_analysis_lock:
                        _active_analysis_pids.discard(pid)

            threading.Thread(target=_run_manual_import_bg, daemon=True).start()
        return jsonify({"status": "success", "project_id": pid, "ready": False})

    use_ai = request.form.get('use_ai') == 'on'
    api_key = (request.form.get('api_key') or "").strip()
    ai_model_name = (request.form.get('ai_model_name') or "").strip()
    ai_provider = (request.form.get('ai_provider') or "").strip().lower()
    if not ai_provider:
        if ai_model_name.lower().startswith("deepseek") or api_key.lower().startswith("sk-"):
            ai_provider = "deepseek"
        else:
            ai_provider = "gemini"
    kimi_api_key = (request.form.get('kimi_api_key') or "").strip()
    kimi_model_name = (request.form.get('kimi_model_name') or "").strip()
    fallback_provider = (request.form.get('fallback_provider') or "").strip().lower()
    if not fallback_provider:
        fallback_provider = "kimi" if kimi_model_name.lower().startswith("kimi") else "deepseek"

    main_name = os.path.basename(main_path).replace('.xlsx','').replace('.xls','')
    comp_names = [os.path.basename(p).replace('.xlsx','').replace('.xls','') for p in comp_paths]

    with _active_analysis_lock:
        if pid in _active_analysis_pids:
            return jsonify({"status": "error", "message": "该项目正在分析中，请等待完成"}), 400
        _active_analysis_pids.add(pid)

    try:
        with dm._db_lock:
            with dm._get_conn() as conn:
                conn.execute(
                    "UPDATE projects SET status = 'analyzing', analysis_started_at = ? WHERE id = ?",
                    (time.strftime('%Y-%m-%d %H:%M:%S'), pid),
                )
    except Exception:
        with _active_analysis_lock:
            _active_analysis_pids.discard(pid)
        raise

    payload = {
        "project_id": pid,
        "dirs": dirs,
        "main_path": main_path,
        "comp_paths": comp_paths,
        "main_name": main_name,
        "comp_names": comp_names,
        "preflight": preflight,
        "partial_categories": list(partial_categories),
        "column_mappings": column_mappings,
        "match_config_json": match_config_json,
        "use_ai": use_ai,
        "api_key": api_key,
        "ai_model_name": ai_model_name,
        "ai_provider": ai_provider,
        "kimi_api_key": kimi_api_key,
        "kimi_model_name": kimi_model_name,
        "fallback_provider": fallback_provider,
    }
    if _celery_enabled():
        step_labels = []
        if use_ai and api_key:
            step_labels.append(f"AI提取 {main_name}")
            for cn in comp_names:
                step_labels.append(f"AI提取 {cn}")
        for cn in comp_names:
            step_labels.append(f"AI分析 {cn}")
        step_labels.append(f"AI匹配 {main_name}")
        payload["job_id"] = JobStore(dm).create_job(
            pid,
            "analysis",
            step_labels,
            {"project_id": pid, "mode": "auto"},
            status="queued",
        )
        with _active_analysis_lock:
            _active_analysis_pids.discard(pid)
        try:
            from pro_image_tasks import run_auto_analysis_task
            run_auto_analysis_task.delay(payload)
        except Exception:
            traceback.print_exc()
            with _active_analysis_lock:
                _active_analysis_pids.add(pid)

            def _run_analysis_bg():
                try:
                    run_auto_analysis(dm, payload, _progress_fns())
                finally:
                    with _active_analysis_lock:
                        _active_analysis_pids.discard(pid)

            threading.Thread(target=_run_analysis_bg, daemon=True).start()
    else:
        def _run_analysis_bg():
            try:
                run_auto_analysis(dm, payload, _progress_fns())
            finally:
                with _active_analysis_lock:
                    _active_analysis_pids.discard(pid)

        threading.Thread(target=_run_analysis_bg, daemon=True).start()
    return jsonify({"status": "success", "project_id": pid, "ready": False})

@projects_bp.route('/api/projects/<int:pid>/progress')
def get_analysis_progress_route(pid):
    return jsonify(_get_analysis_progress_data(pid))

@projects_bp.route('/api/projects/<int:pid>/quality-report')
def get_project_quality_report(pid):
    dirs = dm._get_project_dirs(pid)
    path = os.path.join(dirs["outputs"], "quality_report_latest.json")
    if not os.path.isfile(path):
        return jsonify({"status": "error", "message": "暂无质量报告"}), 404
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return jsonify({"status": "ok", "report": data})
