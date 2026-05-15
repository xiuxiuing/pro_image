import os
import time
import threading
import traceback
import shutil
import json
from flask import Blueprint, request, jsonify

projects_bp = Blueprint('data_projects', __name__)
_active_analysis_pids = set()
_active_analysis_lock = threading.Lock()


def init_projects(context):
    global dm, _init_progress, _update_step, _schedule_clear_progress
    global _get_analysis_progress_data, _validate_upload, _safe_upload_filename, data_root
    dm = context["dm"]
    _init_progress = context["init_progress"]
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

        err = _validate_upload(main_file, "主店文件")
        if err: return jsonify({"status": "error", "message": err}), 400
        for f in valid_comp_files:
            err = _validate_upload(f, f"竞店文件 ({f.filename})")
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
        for idx, f in enumerate(valid_comp_files):
            comp_saved_name = f"comp_{idx}__" + _safe_upload_filename(f.filename, f"comp_{idx}.xlsx")
            path = os.path.join(sources_dir, comp_saved_name)
            f.save(path)
            comp_infos.append({"path": path, "store_name": f.filename.replace(".xlsx", "").replace(".xls", "")})
        
        pid = dm.create_project(
            name,
            {"path": main_path, "store_name": main_store_name},
            comp_infos,
            status='ready',
            match_config_json="",
            rule_template_id=None,
        )

        # Rename temp directory to real PID
        real_proj_dir = os.path.join(data_root, "uploads", f"project_{pid}")
        if os.path.exists(real_proj_dir): shutil.rmtree(real_proj_dir)
        os.rename(proj_dir, real_proj_dir)

        with dm._db_lock:
            with dm._get_conn() as conn:
                conn.execute("UPDATE project_files SET local_path = REPLACE(local_path, ?, ?) WHERE project_id = ?",
                            (f"project_{temp_pid}", f"project_{pid}", pid))

        dm._ensure_project_dirs(pid)
        dm.import_project_sources(pid)
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

@projects_bp.route('/api/projects/<int:pid>/analyze', methods=['POST'])
def analyze_project(pid):
    projects = dm.list_projects()
    proj = next((p for p in projects if p['id'] == pid), None)
    if not proj:
        return jsonify({"status": "error", "message": "项目不存在"}), 404
    if proj.get('status') == 'analyzing':
        return jsonify({"status": "error", "message": "该项目正在分析中，请等待完成"}), 400

    mode = (request.form.get("mode") or "auto").strip()
    match_config_json = (request.form.get('match_config_json') or "").strip()
    raw_selected_categories = (request.form.get("selected_categories") or "").strip()
    try:
        selected_categories = json.loads(raw_selected_categories) if raw_selected_categories else []
    except json.JSONDecodeError:
        selected_categories = []
    selected_categories = [str(c).strip() for c in selected_categories if str(c).strip()]
    raw_rt = (request.form.get("rule_template_id") or "").strip()
    try:
        rule_template_id = int(raw_rt) if raw_rt else None
    except ValueError:
        rule_template_id = None

    dm.activate_project(pid, skip_load=True)
    dirs = dm._ensure_project_dirs(pid)
    main_path = dm.target_file
    comp_paths = list(dm.source_files or [])
    if not main_path or not os.path.exists(main_path):
        return jsonify({"status": "error", "message": "主店源文件不存在，请重新创建项目"}), 400
    if not comp_paths:
        return jsonify({"status": "error", "message": "竞店源文件不存在，请重新创建项目"}), 400

    with dm._db_lock:
        with dm._get_conn() as conn:
            conn.execute(
                "UPDATE projects SET match_config = ?, rule_template_id = ? WHERE id = ?",
                (match_config_json or "", rule_template_id, pid),
            )
            rows = conn.execute(
                """
                SELECT DISTINCT 美团类目三级 FROM main_products
                WHERE project_id = ? AND 美团类目三级 IS NOT NULL AND 美团类目三级 != ''
                """,
                (pid,),
            ).fetchall()
            all_categories = [str(r[0]).strip() for r in rows if str(r[0]).strip()]

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
        if partial_categories:
            output_file = os.path.join(dirs["outputs"], f"partial_output_{pid}_{int(time.time())}.xlsx")
            result_file.save(output_file)
            links_df = dm.parse_links_from_output(pid, output_file)
            dm.replace_project_links(pid, links_df, categories=partial_categories)
        else:
            output_file = os.path.join(dirs["outputs"], f"output_{pid}.xlsx")
            result_file.save(output_file)
            links_df = dm.parse_links_from_output(pid, output_file)
            dm.replace_project_links(pid, links_df)
        dm.update_project_status(pid, 'ready')
        return jsonify({"status": "success", "project_id": pid, "ready": True})

    use_ai = request.form.get('use_ai') == 'on'
    api_key = request.form.get('api_key')
    ai_model_name = (request.form.get('ai_model_name') or "").strip()
    kimi_api_key = (request.form.get('kimi_api_key') or "").strip()
    kimi_model_name = (request.form.get('kimi_model_name') or "").strip()

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

    def _filtered_source_files():
        if not partial_categories:
            return main_path, comp_paths
        import pandas as pd
        import utils
        from data_mgr_base import FIELD_MAPPINGS

        cache_dir = os.path.join(dirs["cache"], f"partial_analysis_{int(time.time())}")
        os.makedirs(cache_dir, exist_ok=True)
        selected = set(partial_categories)

        def _filter_file(path, name):
            rows = utils.excel_to_list_dict(path)
            df = pd.DataFrame(rows)
            if df.empty:
                out = os.path.join(cache_dir, name)
                df.to_excel(out, index=False)
                return out
            df = dm._apply_mappings(df, FIELD_MAPPINGS)
            if "美团类目三级" not in df.columns:
                df = df.iloc[0:0].copy()
            else:
                cat = df["美团类目三级"].fillna("").astype(str).str.strip()
                df = df[cat.isin(selected)].copy()
            out = os.path.join(cache_dir, name)
            df.to_excel(out, index=False)
            return out

        filtered_main = _filter_file(main_path, "main_partial.xlsx")
        filtered_comps = [_filter_file(p, f"comp_{idx}_partial.xlsx") for idx, p in enumerate(comp_paths)]
        return filtered_main, filtered_comps

    def _run_analysis_bg():
        import extract_info_ai2, main_030822
        has_ai = bool(use_ai and api_key)
        prog = _init_progress(pid, has_ai, main_name, comp_names)
        ai_file_count = (1 + len(comp_names)) if has_ai else 0
        try:
            analysis_main_path, analysis_comp_paths = _filtered_source_files()
            if has_ai:
                all_ai_paths = [analysis_main_path] + analysis_comp_paths
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
            output_name = f"partial_{pid}_{int(time.time())}" if partial_categories else str(pid)
            main_030822.run_analysis(
                analysis_main_path, analysis_comp_paths,
                output_name=output_name, output_dir=dirs["outputs"],
                progress_cb=_analysis_cb, match_config=match_config_json, post_match_template=_pmt
            )
            _update_step(pid, len(prog["steps"]) - 1, "done", "分析完成")
            if partial_categories:
                partial_output = os.path.join(dirs["outputs"], f"output_{output_name}.xlsx")
                links_df = dm.parse_links_from_output(pid, partial_output)
                dm.replace_project_links(pid, links_df, categories=partial_categories)
            else:
                full_output = os.path.join(dirs["outputs"], f"output_{pid}.xlsx")
                links_df = dm.parse_links_from_output(pid, full_output)
                dm.replace_project_links(pid, links_df)
            dm.update_project_status(pid, 'ready')
            try:
                dm.activate_project(pid)
            except Exception:
                traceback.print_exc()
        except BaseException:
            traceback.print_exc()
            try:
                dm.update_project_status(pid, 'failed')
            except Exception:
                pass
        finally:
            with _active_analysis_lock:
                _active_analysis_pids.discard(pid)
            _schedule_clear_progress(pid)

    threading.Thread(target=_run_analysis_bg, daemon=True).start()
    return jsonify({"status": "success", "project_id": pid, "ready": False})

@projects_bp.route('/api/projects/<int:pid>/progress')
def get_analysis_progress_route(pid):
    return jsonify(_get_analysis_progress_data(pid))
