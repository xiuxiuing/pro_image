from flask import Flask, render_template, request, jsonify, send_file, send_from_directory
from data_mgr import DataManager
from license_utils import LicenseManager
import os
import sys
import signal
import faulthandler
import shutil
import time
import threading
import traceback

_single_instance_lock_fh = None

if hasattr(signal, 'SIGUSR1'):
    faulthandler.register(signal.SIGUSR1, all_threads=True, chain=False)


def _resolve_app_paths():
    """
    PyInstaller 打包后：只读资源在 sys._MEIPASS；数据库/上传/缓存必须写在 exe 旁可写目录，
    否则写入 _MEIPASS 会失败或无法持久化。
    """
    if getattr(sys, 'frozen', False):
        resource_root = getattr(sys, '_MEIPASS', os.path.dirname(sys.executable))
        exe_dir = os.path.dirname(sys.executable)
        # macOS .app：sys.executable 通常在 .../ProImage_AI.app/Contents/MacOS/
        # 交付时更希望把可写数据放在 .app 同级目录：<folder>/ProImage_data
        bundle_dir = None
        try:
            p = exe_dir
            if p.endswith(os.path.join('Contents', 'MacOS')):
                bundle_dir = os.path.dirname(os.path.dirname(p))  # .../ProImage_AI.app
        except Exception:
            bundle_dir = None

        external_data_root = None
        if bundle_dir:
            external_data_root = os.path.join(os.path.dirname(bundle_dir), 'ProImage_data')

        candidate_roots = [r for r in [external_data_root, os.path.join(exe_dir, 'ProImage_data')] if r]
        data_root = None
        for r in candidate_roots:
            try:
                os.makedirs(r, exist_ok=True)
                data_root = r
                break
            except Exception:
                continue
        if not data_root:
            data_root = os.path.join(exe_dir, 'ProImage_data')
            os.makedirs(data_root, exist_ok=True)
        os.makedirs(data_root, exist_ok=True)
        os.makedirs(os.path.join(data_root, 'uploads'), exist_ok=True)
        os.makedirs(os.path.join(data_root, 'img'), exist_ok=True)
    else:
        resource_root = os.path.dirname(os.path.abspath(__file__))
        data_root = resource_root
    return resource_root, data_root


resource_root, data_root = _resolve_app_paths()
# 冻结版：分析线程里相对路径 img/、query_img/ 与 DataManager 使用同一根目录
if getattr(sys, 'frozen', False):
    os.chdir(data_root)


def _acquire_single_instance_lock():
    """
    防止 macOS 启动很慢时被多次双击，导致同时启动多个实例。
    仅在 frozen（.app）模式启用。
    """
    global _single_instance_lock_fh
    if not getattr(sys, 'frozen', False):
        return True

    try:
        import fcntl  # macOS/Linux
    except Exception:
        return True

    lock_path = os.path.join(data_root, "ProImage_AI.lock")
    try:
        fh = open(lock_path, "w")
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        fh.write(str(os.getpid()))
        fh.flush()
        _single_instance_lock_fh = fh
        return True
    except Exception:
        try:
            fh.close()
        except Exception:
            pass
        return False


if not _acquire_single_instance_lock():
    raise SystemExit(0)

_template = os.path.join(resource_root, 'templates')
_static = os.path.join(resource_root, 'static')
if os.path.isdir(_static):
    app = Flask(__name__, template_folder=_template, static_folder=_static, static_url_path='/static')
else:
    app = Flask(__name__, template_folder=_template)

app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB
dm = DataManager(data_root)

# 放在单实例锁之后，避免多次双击时重复触发重依赖初始化
import extract_info_ai2  # noqa: E402
import main_030822  # noqa: E402

# ── Analysis progress tracking ──
_analysis_progress = {}
_progress_lock = threading.Lock()

def _init_progress(pid, use_ai, main_name, comp_names):
    steps = []
    if use_ai:
        steps.append({"label": f"AI提取 {main_name}", "status": "pending", "detail": ""})
        for cn in comp_names:
            steps.append({"label": f"AI提取 {cn}", "status": "pending", "detail": ""})
    for cn in comp_names:
        steps.append({"label": f"向量分析 {cn}", "status": "pending", "detail": ""})
    steps.append({"label": f"查询匹配 {main_name}", "status": "pending", "detail": ""})
    prog = {"started_at": time.time(), "steps": steps}
    with _progress_lock:
        _analysis_progress[pid] = prog
    return prog

def _update_step(pid, step_idx, status, detail=""):
    with _progress_lock:
        prog = _analysis_progress.get(pid)
        if not prog or step_idx >= len(prog["steps"]):
            return
        step = prog["steps"][step_idx]
        step["status"] = status
        step["detail"] = detail
        if status == "running" and not step.get("started_at"):
            step["started_at"] = time.time()
        if status == "done" and not step.get("ended_at"):
            step["ended_at"] = time.time()

def _clear_progress(pid):
    with _progress_lock:
        _analysis_progress.pop(pid, None)

MAX_FILE_SIZE = 80 * 1024 * 1024  # 80MB per file
ALLOWED_EXTENSIONS = {'.xlsx', '.xls'}

def _validate_upload(file_storage, label):
    """Validate file extension and size. Returns error message or None."""
    ext = os.path.splitext(file_storage.filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        return f"{label}：不支持的文件格式 ({ext})，仅支持 .xlsx / .xls"
    file_storage.seek(0, 2)
    size = file_storage.tell()
    file_storage.seek(0)
    if size > MAX_FILE_SIZE:
        return f"{label}：文件过大 ({size // 1024 // 1024}MB)，上限 {MAX_FILE_SIZE // 1024 // 1024}MB"
    return None

# --- License Check Logic ---
LICENSE_FILE = os.path.join(data_root, "license.dat")
CURRENT_HWID = LicenseManager.get_hwid()

def check_license():
    if not os.path.exists(LICENSE_FILE): return False, "License file missing"
    with open(LICENSE_FILE, "r") as f: content = f.read().strip()
    return LicenseManager.verify_license(content, CURRENT_HWID)

def get_license_details():
    if not os.path.exists(LICENSE_FILE):
        return {
            "valid": False,
            "message": "License file missing",
            "expires": None,
            "days_remaining": None,
        }
    with open(LICENSE_FILE, "r") as f:
        content = f.read().strip()
    return LicenseManager.verify_license_detailed(content, CURRENT_HWID)

@app.errorhandler(413)
def request_entity_too_large(e):
    return jsonify({"status": "error", "message": "上传文件总大小超过 100MB 限制"}), 413

@app.route('/api/license_info')
def get_license_info():
    d = get_license_details()
    return jsonify({
        "hwid": CURRENT_HWID,
        "is_valid": d["valid"],
        "message": d["message"],
        "expires": d.get("expires"),
        "days_remaining": d.get("days_remaining"),
    })

@app.route('/')
def projects_page():
    is_valid, _ = check_license()
    if not is_valid: return render_template('activate.html', hwid=CURRENT_HWID)
    return render_template('projects.html')

@app.route('/dashboard')
def index():
    is_valid, _ = check_license()
    if not is_valid: return render_template('activate.html', hwid=CURRENT_HWID)
    return render_template('index.html', active_project=dm.active_project_name)

@app.route('/api/projects', methods=['GET', 'POST'])
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
        result_file = request.files.get('result_file')
        if result_file and result_file.filename:
            err = _validate_upload(result_file, "结果文件")
            if err: return jsonify({"status": "error", "message": err}), 400

        # Temporary PID for directory naming
        temp_pid = int(time.time())
        proj_dir = os.path.join(data_root, "uploads", f"project_{temp_pid}")
        sources_dir = os.path.join(proj_dir, "sources")
        os.makedirs(sources_dir, exist_ok=True)
        
        # Save Main Store File
        main_path = os.path.join(sources_dir, main_file.filename)
        main_file.save(main_path)
        main_store_name = main_file.filename.replace(".xlsx", "").replace(".xls", "")
        
        # Save Competitor Store Files
        comp_infos = []
        comp_paths = []
        for f in valid_comp_files:
            path = os.path.join(sources_dir, f.filename)
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
        pid = dm.create_project(name, {"path": main_path, "store_name": main_store_name},
                                comp_infos, status='ready' if is_manual else 'analyzing')

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
            # Sync path: copy result → activate → import → redirect to dashboard
            output_file = os.path.join(dirs["outputs"], f"output_{pid}.xlsx")
            shutil.copy(final_manual_result_path, output_file)
            dm.activate_project(pid)
            return jsonify({"status": "success", "project_id": pid})

        # Async path: return immediately, run analysis in background thread
        use_ai = request.form.get('use_ai') == 'on'
        api_key = request.form.get('api_key')
        ai_model_name = (request.form.get('ai_model_name') or "").strip()

        main_name = os.path.basename(final_main_path).replace('.xlsx','').replace('.xls','')
        comp_names = [os.path.basename(p).replace('.xlsx','').replace('.xls','') for p in final_comp_paths]

        def _run_analysis_bg():
            _t0 = time.time()
            print(f"[BG] Project {pid} thread started at {time.strftime('%H:%M:%S')}", flush=True)
            has_ai = bool(use_ai and api_key)
            prog = _init_progress(pid, has_ai, main_name, comp_names)
            ai_file_count = (1 + len(comp_names)) if has_ai else 0
            try:
                if has_ai:
                    all_ai_paths = [final_main_path] + final_comp_paths
                    for fi, fp in enumerate(all_ai_paths):
                        _update_step(pid, fi, "running")
                        def _ai_cb(batch, total, _fi=fi):
                            _update_step(pid, _fi, "running", f"batch {batch}/{total}")
                        extract_info_ai2.process_file_ai(fp, api_key, progress_cb=_ai_cb, model_name=ai_model_name)
                        _update_step(pid, fi, "done")
                    print(f"[BG] Project {pid} AI extraction done in {time.time()-_t0:.1f}s", flush=True)

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

                print(f"[BG] Project {pid} starting run_analysis...", flush=True)
                main_030822.run_analysis(
                    final_main_path, final_comp_paths,
                    output_name=str(pid), output_dir=dirs["outputs"],
                    progress_cb=_analysis_cb
                )
                _update_step(pid, len(prog["steps"]) - 1, "done")
                dm.update_project_status(pid, 'ready')
                print(f"[BG] Project {pid} analysis complete in {time.time()-_t0:.1f}s", flush=True)
            except BaseException as e:
                traceback.print_exc()
                try:
                    dm.update_project_status(pid, 'failed')
                except Exception:
                    pass
                print(f"[BG] Project {pid} FAILED ({type(e).__name__}: {e}) after {time.time()-_t0:.1f}s", flush=True)
            finally:
                _clear_progress(pid)

        threading.Thread(target=_run_analysis_bg, daemon=True).start()
        return jsonify({"status": "success", "project_id": pid})
        
    return jsonify(dm.list_projects())

@app.route('/api/projects/<int:pid>/activate', methods=['POST'])
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

@app.route('/api/projects/<int:pid>', methods=['DELETE'])
def delete_project(pid):
    dm.delete_project(pid)
    return jsonify({"status": "success"})

@app.route('/api/config', methods=['GET'])
def get_config():
    return jsonify({
        "main_store": dm.main_store_name, "target_file": dm.target_file, "output_file": dm.output_file,
        "source_files": dm.source_files, "stores": [{"id": str(i), "name": n, "path": dm.source_files[i]} for i, n in enumerate(dm.store_names)]
    })

@app.route('/api/grid_data')
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

@app.route('/api/store_products/<store_id>')
def get_store_products(store_id):
    return jsonify(dm.get_store_products(store_id))


@app.route('/api/unlinked_items')
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

@app.route('/api/main_products')
def get_main_products():
    page = request.args.get('page', 1, type=int)
    limit = request.args.get('limit', 50, type=int)
    search = request.args.get('search', "")
    return jsonify(dm.get_main_products_page(page=page, limit=limit, search=search))

@app.route('/api/eliminate', methods=['POST'])
def eliminate():
    d = request.json
    main_sku_id = d.get('main_sku_id')
    if not main_sku_id:
        return jsonify({"status": "error", "message": "Missing main_sku_id"}), 400
    dm.eliminate_product(main_sku_id, d.get('status', 1))
    return jsonify({"status": "success"})

@app.route('/api/toggle_handled', methods=['POST'])
def toggle_handled():
    d = request.json
    sku_id = d.get('main_sku_id')
    if not sku_id:
        return jsonify({"status": "error", "message": "Missing main_sku_id"}), 400
    dm.toggle_handled(sku_id, d.get('handled', True))
    return jsonify({"status": "success"})

@app.route('/api/toggle_ref', methods=['POST'])
def toggle_ref():
    d = request.json
    sku_id = d.get('main_sku_id')
    field = d.get('field')  # 'name' or 'image'
    store_id = d.get('store_id', '')
    if not sku_id or field not in ('name', 'image'):
        return jsonify({"status": "error", "message": "Missing params"}), 400
    dm.set_ref(sku_id, field, store_id)
    return jsonify({"status": "success"})

@app.route('/api/toggle_add', methods=['POST'])
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

@app.route('/api/price_match', methods=['POST'])
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

@app.route('/api/clear_price_match', methods=['POST'])
def clear_price_match():
    d = request.json
    main_sku_id = d.get('main_sku_id')
    if not main_sku_id:
        return jsonify({"status": "error", "message": "Missing main_sku_id"}), 400
    dm.clear_price_match(main_sku_id)
    return jsonify({"status": "success"})

@app.route('/api/manual_link', methods=['POST'])
def manual_link():
    d = request.json
    main_sku_id = d.get('main_sku_id')
    store_id = d.get('store_id')
    comp_sku_id = d.get('comp_sku_id')
    if not main_sku_id or store_id is None or not comp_sku_id:
        return jsonify({"status": "error", "message": "Missing params"}), 400
    dm.manual_link(main_sku_id, store_id, comp_sku_id)
    return jsonify({"status": "success"})

@app.route('/api/unlink', methods=['POST'])
def unlink():
    d = request.json
    main_sku_id = d.get('main_sku_id')
    store_id = d.get('store_id')
    if not main_sku_id or store_id is None:
        return jsonify({"status": "error", "message": "Missing params"}), 400
    dm.unlink_product(main_sku_id, store_id)
    return jsonify({"status": "success"})

@app.route('/api/update_cell', methods=['POST'])
def update_cell():
    d = request.json
    main_sku_id = d.get('main_sku_id')
    if not main_sku_id:
        return jsonify({"status": "error", "message": "Missing main_sku_id"}), 400
    dm.update_cell(main_sku_id, {d.get('column'): d.get('value')})
    return jsonify({"status": "success"})

@app.route('/img/<path:filename>')
def serve_img(filename):
    return send_from_directory(os.path.join(data_root, "img"), filename)


@app.route('/api/export')
def export_data():
    p = dm.save_separate_exports()
    resp = send_file(p, as_attachment=True)
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"; resp.headers["Pragma"] = "no-cache"; resp.headers["Expires"] = "0"
    return resp

@app.route('/api/export_new')
def export_new_data():
    p = dm.export_new_items()
    resp = send_file(p, as_attachment=True)
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"; resp.headers["Pragma"] = "no-cache"; resp.headers["Expires"] = "0"
    return resp

@app.route('/api/projects/<int:pid>/progress')
def get_analysis_progress(pid):
    with _progress_lock:
        prog = _analysis_progress.get(pid)
    if not prog:
        return jsonify({"available": False})
    steps = prog["steps"]
    elapsed = time.time() - prog["started_at"]
    done_count = sum(1 for s in steps if s["status"] == "done")
    total = len(steps)
    pct = int(done_count / total * 100) if total else 0
    running_idx = next((i for i, s in enumerate(steps) if s["status"] == "running"), -1)
    if running_idx >= 0:
        pct = int((done_count + 0.5) / total * 100)
    done_durations = [s["ended_at"] - s["started_at"] for s in steps
                      if s.get("started_at") and s.get("ended_at")]
    avg_step = (sum(done_durations) / len(done_durations)) if done_durations else 0
    remaining_steps = total - done_count - (1 if running_idx >= 0 else 0)
    running_elapsed = (time.time() - steps[running_idx]["started_at"]) if running_idx >= 0 and steps[running_idx].get("started_at") else 0
    est_remaining = max(0, avg_step - running_elapsed) + remaining_steps * avg_step if avg_step > 0 else 0
    out_steps = []
    for s in steps:
        item = {"label": s["label"], "status": s["status"], "detail": s.get("detail", "")}
        if s.get("started_at") and s.get("ended_at"):
            item["duration_s"] = round(s["ended_at"] - s["started_at"], 1)
        elif s.get("started_at"):
            item["running_s"] = round(time.time() - s["started_at"], 1)
        out_steps.append(item)
    return jsonify({
        "available": True, "elapsed_s": round(elapsed, 1),
        "pct": pct, "estimated_remaining_s": round(est_remaining, 1),
        "done_count": done_count, "total_steps": total,
        "steps": out_steps,
    })

@app.route('/api/debug/threads')
def debug_threads():
    import io
    buf = io.StringIO()
    buf.write(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    buf.write(f"Active threads: {threading.active_count()}\n\n")
    frames = sys._current_frames()
    for tid, frame in frames.items():
        tname = "unknown"
        for t in threading.enumerate():
            if t.ident == tid:
                tname = t.name
                break
        buf.write(f"--- Thread {tid} ({tname}) ---\n")
        traceback.print_stack(frame, file=buf)
        buf.write("\n")
    return buf.getvalue(), 200, {'Content-Type': 'text/plain; charset=utf-8'}

if __name__ == '__main__':
    app.run(debug=False, port=5001)
