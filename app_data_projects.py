import os
import time
import threading
import traceback
import shutil
from flask import Blueprint, request, jsonify

projects_bp = Blueprint('data_projects', __name__)


def init_projects(context):
    global dm, _init_progress, _update_step, _schedule_clear_progress
    global _validate_upload, _safe_upload_filename, data_root
    dm = context["dm"]
    _init_progress = context["init_progress"]
    _update_step = context["update_step"]
    _schedule_clear_progress = context["schedule_clear_progress"]
    _validate_upload = context["validate_upload"]
    _safe_upload_filename = context["safe_upload_filename"]
    data_root = context["data_root"]


@projects_bp.route('/api/projects', methods=['GET', 'POST'])
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
    if proj.get('status') == 'failed':
        return jsonify({"status": "error", "message": "该项目分析失败，请删除后重新创建"}), 400
    dm.activate_project(pid)
    return jsonify({"status": "success"})

@projects_bp.route('/api/projects/<int:pid>/progress')
def get_analysis_progress_route(pid):
    # This calls back to the progress management in app.py or wherever it's passed from
    from app import get_analysis_progress_data
    return jsonify(get_analysis_progress_data(pid))
