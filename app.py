import os
import sys

# macOS：FAISS 与 PyTorch 同时链接不同 OpenMP/BLAS 时易 SIGSEGV；需在 numpy/torch 初始化前收紧线程
if sys.platform == "darwin":
    for _k, _v in (
        ("OMP_NUM_THREADS", "1"),
        ("MKL_NUM_THREADS", "1"),
        ("VECLIB_MAXIMUM_THREADS", "1"),
        ("NUMEXPR_MAX_THREADS", "1"),
        ("OPENBLAS_NUM_THREADS", "1"),
    ):
        os.environ.setdefault(_k, _v)

from flask import Flask, render_template, request, jsonify, send_file
from data_mgr import DataManager
from license_utils import LicenseManager
import signal
import faulthandler
import shutil
import time
import threading
import traceback
import platform
import webbrowser
from werkzeug.utils import secure_filename

_single_instance_lock_fh = None

if hasattr(signal, 'SIGUSR1'):
    faulthandler.register(signal.SIGUSR1, all_threads=True, chain=False)

def _resolve_app_paths():
    if getattr(sys, 'frozen', False):
        resource_root = getattr(sys, '_MEIPASS', os.path.dirname(sys.executable))
        exe_dir = os.path.dirname(sys.executable)
        bundle_dir = None
        try:
            p = exe_dir
            if p.endswith(os.path.join('Contents', 'MacOS')):
                bundle_dir = os.path.dirname(os.path.dirname(p))
        except Exception: pass
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
            except Exception: continue
        if not data_root:
            data_root = os.path.join(exe_dir, 'ProImage_data')
            os.makedirs(data_root, exist_ok=True)
        os.makedirs(os.path.join(data_root, 'uploads'), exist_ok=True)
        os.makedirs(os.path.join(data_root, 'img'), exist_ok=True)
    else:
        resource_root = os.path.dirname(os.path.abspath(__file__))
        data_root = resource_root
    return resource_root, data_root

resource_root, data_root = _resolve_app_paths()
DEFAULT_RULE_CATEGORIES_XLSX = os.path.join(resource_root, "data", "default_meituan_categories.xlsx")
CATEGORY_L1_BUCKET_TAGS_JSON = os.path.join(resource_root, "data", "category_l1_bucket_tags.json")

if getattr(sys, 'frozen', False):
    os.chdir(data_root)

def _acquire_single_instance_lock():
    global _single_instance_lock_fh
    if not getattr(sys, 'frozen', False): return True
    fh = None
    try:
        lock_path = os.path.join(data_root, "ProImage_AI.lock")
        fh = open(lock_path, "w")
        if sys.platform.startswith("win"):
            import msvcrt
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        fh.write(str(os.getpid()))
        fh.flush()
        _single_instance_lock_fh = fh
        return True
    except Exception:
        if fh:
            try: fh.close()
            except Exception: pass
        return False

if not _acquire_single_instance_lock(): raise SystemExit(0)

_template = os.path.join(resource_root, 'templates')
_static = os.path.join(resource_root, 'static')
if os.path.isdir(_static):
    app = Flask(__name__, template_folder=_template, static_folder=_static, static_url_path='/static')
else:
    app = Flask(__name__, template_folder=_template)

app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB
dm = DataManager(data_root)

class _LazyModule:
    def __init__(self, module_name):
        self._module_name = module_name
        self._module = None
        self._lock = threading.Lock()

    def _load(self):
        if self._module is None:
            with self._lock:
                if self._module is None:
                    import importlib
                    self._module = importlib.import_module(self._module_name)
        return self._module

    def __getattr__(self, name):
        return getattr(self._load(), name)


extract_info_ai2 = _LazyModule("extract_info_ai2")
main_030822 = _LazyModule("main_030822")

_analysis_progress = {}
_progress_lock = threading.Lock()

def _init_progress(pid, use_ai, main_name, comp_names):
    steps = []
    if use_ai:
        steps.append({"label": f"AI提取 {main_name}", "status": "pending", "detail": ""})
        for cn in comp_names: steps.append({"label": f"AI提取 {cn}", "status": "pending", "detail": ""})
    for cn in comp_names: steps.append({"label": f"向量分析 {cn}", "status": "pending", "detail": ""})
    steps.append({"label": f"查询匹配 {main_name}", "status": "pending", "detail": ""})
    prog = {"started_at": time.time(), "steps": steps}
    with _progress_lock: _analysis_progress[pid] = prog
    return prog

def _update_step(pid, step_idx, status, detail=""):
    with _progress_lock:
        prog = _analysis_progress.get(pid)
        if not prog or step_idx >= len(prog["steps"]): return
        step = prog["steps"][step_idx]
        step["status"] = status
        step["detail"] = detail
        if status == "running" and not step.get("started_at"): step["started_at"] = time.time()
        if status == "done" and not step.get("ended_at"): step["ended_at"] = time.time()

def _clear_progress(pid):
    with _progress_lock: _analysis_progress.pop(pid, None)

def _schedule_clear_progress(pid):
    threading.Timer(5.0, lambda p=pid: _clear_progress(p)).start()

def get_analysis_progress_data(pid):
    with _progress_lock:
        prog = _analysis_progress.get(pid)
    if not prog: return {"available": False}
    steps = prog["steps"]
    elapsed = time.time() - prog["started_at"]
    done_count = sum(1 for s in steps if s["status"] == "done")
    total = len(steps)
    pct = int(done_count / total * 100) if total else 0
    running_idx = next((i for i, s in enumerate(steps) if s["status"] == "running"), -1)
    if running_idx >= 0: pct = int((done_count + 0.5) / total * 100)
    done_durations = [s["ended_at"] - s["started_at"] for s in steps if s.get("started_at") and s.get("ended_at")]
    avg_step = (sum(done_durations) / len(done_durations)) if done_durations else 0
    remaining_steps = total - done_count - (1 if running_idx >= 0 else 0)
    running_elapsed = (time.time() - steps[running_idx]["started_at"]) if running_idx >= 0 and steps[running_idx].get("started_at") else 0
    est_remaining = max(0, avg_step - running_elapsed) + remaining_steps * avg_step if avg_step > 0 else 0
    out_steps = []
    for s in steps:
        item = {"label": s["label"], "status": s["status"], "detail": s.get("detail", "")}
        if s.get("started_at") and s.get("ended_at"): item["duration_s"] = round(s["ended_at"] - s["started_at"], 1)
        elif s.get("started_at"): item["running_s"] = round(time.time() - s["started_at"], 1)
        out_steps.append(item)
    return {
        "available": True, "elapsed_s": round(elapsed, 1), "pct": pct, "estimated_remaining_s": round(est_remaining, 1),
        "done_count": done_count, "total_steps": total, "steps": out_steps
    }

MAX_FILE_SIZE = 80 * 1024 * 1024
ALLOWED_EXTENSIONS = {'.xlsx', '.xls'}

def _validate_upload(file_storage, label):
    ext = os.path.splitext(file_storage.filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS: return f"{label}：不支持的文件格式 ({ext})"
    file_storage.seek(0, 2); size = file_storage.tell(); file_storage.seek(0)
    if size > MAX_FILE_SIZE: return f"{label}：文件过大 ({size // 1048576}MB)"
    return None

def _safe_upload_filename(filename, fallback):
    base = secure_filename(filename or "")
    orig_ext = os.path.splitext(filename or "")[1].lower()
    if not base: base = fallback
    elif orig_ext and not os.path.splitext(base)[1]: base = base + orig_ext
    return base

# --- License Logic ---
LICENSE_FILE = os.path.join(data_root, "license.dat")
CURRENT_HWID = LicenseManager.get_hwid()

def check_license():
    if not os.path.exists(LICENSE_FILE): return False, "License file missing"
    with open(LICENSE_FILE, "r") as f: content = f.read().strip()
    return LicenseManager.verify_license(content, CURRENT_HWID)

def get_license_details():
    if not os.path.exists(LICENSE_FILE): return {"valid": False, "message": "License missing"}
    with open(LICENSE_FILE, "r") as f: content = f.read().strip()
    return LicenseManager.verify_license_detailed(content, CURRENT_HWID)

# --- Blueprints ---
import app_ops, app_data
app_ops.init_ops(app, dm, resource_root, data_root, check_license, CURRENT_HWID, extract_info_ai2, main_030822, _validate_upload, _safe_upload_filename)
app.register_blueprint(app_ops.ops_bp)
app_data.init_data(dm, _init_progress, _update_step, _schedule_clear_progress, get_analysis_progress_data, _validate_upload, _safe_upload_filename, _template, _static, data_root, DEFAULT_RULE_CATEGORIES_XLSX, CATEGORY_L1_BUCKET_TAGS_JSON)
app.register_blueprint(app_data.data_bp)

@app.errorhandler(413)
def request_entity_too_large(e):
    return jsonify({"status": "error", "message": "上传文件总大小超过 100MB 限制"}), 413

@app.route('/api/license_info')
def get_license_info():
    d = get_license_details()
    return jsonify({
        "hwid": CURRENT_HWID, 
        "is_valid": d.get("valid"), 
        "message": d.get("message"), 
        "expires": d.get("expires"),
        "days_remaining": d.get("days_remaining")
    })

@app.route('/')
def projects_page():
    is_valid, _ = check_license()
    if not is_valid: return render_template('activate.html', hwid=CURRENT_HWID)
    return render_template('projects.html')

@app.route('/activate', methods=['POST'])
def activate_license():
    f = request.files.get('license')
    if not f: return jsonify({"status": "error", "message": "No file uploaded"}), 400
    content = f.read().decode('utf-8', errors='ignore').strip()
    is_valid, msg = LicenseManager.verify_license(content, CURRENT_HWID)
    if is_valid:
        with open(LICENSE_FILE, 'w') as lf: lf.write(content)
        return jsonify({"status": "success"})
    return jsonify({"status": "error", "message": msg or "Invalid license"}), 400

@app.route('/dashboard')
def index():
    is_valid, _ = check_license()
    if not is_valid: return render_template('activate.html', hwid=CURRENT_HWID)
    return render_template('index.html', active_project=dm.active_project_name)

@app.route('/market-analysis')
def market_analysis_page():
    is_valid, _ = check_license()
    if not is_valid: return render_template('activate.html', hwid=CURRENT_HWID)
    return render_template('market_analysis.html', active_project=dm.active_project_name)

@app.route("/match-rules")
def match_rules_page():
    is_valid, _ = check_license()
    if not is_valid: return render_template("activate.html", hwid=CURRENT_HWID)
    return render_template("match_rules.html")

@app.route("/match-rules/new")
def match_rule_new_page():
    is_valid, _ = check_license()
    if not is_valid: return render_template("activate.html", hwid=CURRENT_HWID)
    return render_template("match_rule_edit.html", template_id=0, template_name="")

@app.route("/match-rules/<int:template_id>")
def match_rule_edit_page(template_id):
    is_valid, _ = check_license()
    if not is_valid: return render_template("activate.html", hwid=CURRENT_HWID)
    return render_template("match_rule_edit.html", template_id=template_id, template_name="")

def _schedule_open_browser(port):
    if not getattr(sys, 'frozen', False): return
    url = f"http://127.0.0.1:{port}"
    def _open_browser():
        try:
            webbrowser.open(url)
        except Exception:
            pass
    timer = threading.Timer(1.5, _open_browser)
    timer.daemon = True
    timer.start()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5001))
    print(f"Starting server on port {port}...")
    _schedule_open_browser(port)
    app.run(host='0.0.0.0', port=port, debug=False)
