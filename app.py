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

from flask import Flask, render_template, request, jsonify, send_file, redirect, session, url_for
from data_mgr import DataManager
from license_utils import LicenseManager
from online_jobs import JobStore
import signal
import faulthandler
import shutil
import time
import threading
import traceback
import platform
import webbrowser
import re
import uuid
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
job_store = JobStore(dm)
app.secret_key = 'PiPaiSmartOpsSecretKey2026_Secure'

def rc4_crypt(key: bytes, data: bytes) -> bytes:
    x = 0
    box = list(range(256))
    for i in range(256):
        x = (x + box[i] + key[i % len(key)]) % 256
        box[i], box[x] = box[x], box[i]
    x = 0
    y = 0
    out = []
    for char in data:
        x = (x + 1) % 256
        y = (y + box[x]) % 256
        box[x], box[y] = box[y], box[x]
        out.append(char ^ box[(box[x] + box[y]) % 256])
    return bytes(out)

def rc4_decrypt_str(ciphertext_b64: str, key_str: str) -> str:
    import base64
    try:
        ciphertext = base64.b64decode(ciphertext_b64.encode('utf-8'))
        key = key_str.encode('utf-8')
        decrypted = rc4_crypt(key, ciphertext)
        return decrypted.decode('utf-8', errors='ignore')
    except Exception as e:
        print(f"Decryption error: {e}")
        return ""

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
_progress_job_ids = {}
_progress_lock = threading.Lock()

def _init_progress(pid, use_ai, main_name, comp_names, job_id=None):
    steps = []
    if use_ai:
        steps.append({"label": f"AI提取 {main_name}", "status": "pending", "detail": ""})
        for cn in comp_names: steps.append({"label": f"AI提取 {cn}", "status": "pending", "detail": ""})
    for cn in comp_names: steps.append({"label": f"AI分析 {cn}", "status": "pending", "detail": ""})
    steps.append({"label": f"AI匹配 {main_name}", "status": "pending", "detail": ""})
    prog = {"started_at": time.time(), "steps": steps}
    with _progress_lock: _analysis_progress[pid] = prog
    try:
        if job_id:
            job_store.mark_running(job_id)
        else:
            job_id = job_store.create_job(
                pid,
                "analysis",
                [s["label"] for s in steps],
                {"source": "legacy_progress"},
                status="running",
            )
        with _progress_lock:
            _progress_job_ids[pid] = job_id
    except Exception:
        pass
    return prog

def _init_import_progress(pid, labels, job_id=None):
    steps = [{"label": str(lbl), "status": "pending", "detail": ""} for lbl in labels]
    prog = {"started_at": time.time(), "steps": steps}
    with _progress_lock:
        _analysis_progress[pid] = prog
    try:
        if job_id:
            job_store.mark_running(job_id)
        else:
            job_id = job_store.create_job(
                pid,
                "manual_import",
                [s["label"] for s in steps],
                {"source": "legacy_progress"},
                status="running",
            )
        with _progress_lock:
            _progress_job_ids[pid] = job_id
    except Exception:
        pass
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
    with _progress_lock:
        job_id = _progress_job_ids.get(pid)
    if job_id:
        try:
            job_store.update_step(job_id, step_idx, status, detail)
        except Exception:
            pass

def _clear_progress(pid):
    with _progress_lock:
        _analysis_progress.pop(pid, None)
        _progress_job_ids.pop(pid, None)

def _schedule_clear_progress(pid):
    with _progress_lock:
        job_id = _progress_job_ids.get(pid)
    if job_id:
        try:
            with dm._db_lock:
                conn = dm._get_conn()
                try:
                    row = conn.execute("SELECT status FROM projects WHERE id = ?", (pid,)).fetchone()
                    project_status = row[0] if row else ""
                finally:
                    conn.close()
            job_store.finish(job_id, "failed" if project_status == "failed" else "succeeded")
        except Exception:
            pass
    threading.Timer(5.0, lambda p=pid: _clear_progress(p)).start()

def get_analysis_progress_data(pid):
    try:
        persisted = job_store.latest_project_progress(pid)
        if persisted.get("available"):
            return persisted
    except Exception:
        pass
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
import app_ops, app_data, api_v1
app_ops.init_ops(app, dm, resource_root, data_root, check_license, CURRENT_HWID, extract_info_ai2, main_030822, _validate_upload, _safe_upload_filename)
app.register_blueprint(app_ops.ops_bp)
app_data.init_data(dm, _init_progress, _init_import_progress, _update_step, _schedule_clear_progress, get_analysis_progress_data, _validate_upload, _safe_upload_filename, _template, _static, data_root, DEFAULT_RULE_CATEGORIES_XLSX, CATEGORY_L1_BUCKET_TAGS_JSON)
app.register_blueprint(app_data.data_bp)
api_v1.init_api_v1({"dm": dm, "job_store": job_store})
app.register_blueprint(api_v1.api_v1_bp)

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

@app.before_request
def require_login():
    if request.path.startswith('/static'):
        return
    if request.path in ['/login', '/api/license_info', '/activate']:
        return
    allowed_routes = ['login', 'activate_license', 'get_license_info']
    if request.endpoint in allowed_routes:
        return
        
    if 'userid' not in session:
        if request.path.startswith('/api/'):
            if request.path.startswith('/api/v1/'):
                return jsonify({
                    "status": "error",
                    "data": None,
                    "error": {
                        "code": "unauthorized",
                        "message": "未登录，请先登录",
                    },
                    "meta": {},
                }), 401
            return jsonify({"status": "error", "message": "未登录，请先登录", "unauthorized": True}), 401
        return redirect('/login')
        
    # Verify user still exists in the database to handle database resets cleanly
    userid = session['userid']
    conn = dm._get_conn()
    user_exists = False
    try:
        row = conn.execute("SELECT 1 FROM \"user\" WHERE userid = ?", (userid,)).fetchone()
        if row:
            user_exists = True
    except Exception:
        pass
    finally:
        conn.close()
        
    if not user_exists:
        session.clear()
        if request.path.startswith('/api/'):
            if request.path.startswith('/api/v1/'):
                return jsonify({
                    "status": "error",
                    "data": None,
                    "error": {
                        "code": "unauthorized",
                        "message": "登录已失效，请重新登录",
                    },
                    "meta": {},
                }), 401
            return jsonify({"status": "error", "message": "登录已失效，请重新登录", "unauthorized": True}), 401
        return redirect('/login')
        
    # Backend Role Permissions Check
    protected_paths = [
        '/', 
        '/config-management/permissions', 
        '/config-management/rules', 
        '/user-management/users', 
        '/user-management/roles', 
        '/data-management/operations', 
        '/data-management/stores', 
        '/market-analysis', 
        '/pi-agent',
        '/ops-tools'
    ]
    if request.path in protected_paths:
        if session.get('username') == 'admin' or session.get('role_name') == '超级管理员':
            return
            
        role_id = session.get('role_id')
        if not role_id:
            if request.path.startswith('/api/'):
                return jsonify({"status": "error", "message": "权限拒绝"}), 403
            return "无权访问该页面，请联系管理员分配权限。", 403
            
        conn = dm._get_conn()
        row = None
        try:
            row = conn.execute("SELECT permissions FROM characters WHERE characterid = ?", (role_id,)).fetchone()
        finally:
            conn.close()
            
        perms = []
        if row:
            import json
            try: perms = json.loads(row[0])
            except Exception: pass
            
        if request.path not in perms:
            if request.path.startswith('/api/'):
                return jsonify({"status": "error", "message": "权限拒绝"}), 403
            
            # Auto-redirect to their first allowed page if they have one
            if perms:
                for p in protected_paths:
                    if p in perms:
                        return redirect(p)
            return "无权访问该页面，请联系管理员分配权限。", 403

@app.context_processor
def inject_permissions():
    userid = session.get('userid')
    if not userid:
        return {"user_permissions": [], "is_super_admin": False}
        
    if session.get('username') == 'admin' or session.get('role_name') == '超级管理员':
        return {"user_permissions": [], "is_super_admin": True}
        
    role_id = session.get('role_id')
    if not role_id:
        return {"user_permissions": [], "is_super_admin": False}
        
    conn = dm._get_conn()
    row = None
    try:
        row = conn.execute("SELECT permissions FROM characters WHERE characterid = ?", (role_id,)).fetchone()
    finally:
        conn.close()
        
    if not row:
        return {"user_permissions": [], "is_super_admin": False}
        
    import json
    try:
        perms = json.loads(row[0])
    except Exception:
        perms = []
        
    return {"user_permissions": perms, "is_super_admin": False}

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        data = request.get_json() or request.form
        account = data.get('account', '').strip()
        password_encrypted = data.get('password', '').strip()
        
        if not account or not password_encrypted:
            return jsonify({"status": "error", "message": "用户名和密码不能为空"}), 400
            
        password = rc4_decrypt_str(password_encrypted, 'PiPaiSmartOpsKey2026')
        
        fail_count = session.get('login_fail_count', 0)
        
        conn = dm._get_conn()
        user_row = None
        try:
            user_row = conn.execute(
                "SELECT userid, username, phone, password, status, role_id, role_name, avatar FROM \"user\" WHERE (username = ? OR phone = ?) AND status = 1",
                (account, account)
            ).fetchone()
        finally:
            conn.close()
            
        if not user_row:
            session['login_fail_count'] = fail_count + 1
            current_fails = session['login_fail_count']
            if current_fails >= 3:
                return jsonify({
                    "status": "error", 
                    "message": "密码输错已达3次，请联系管理员重置！", 
                    "fail_count": current_fails
                }), 400
            return jsonify({
                "status": "error", 
                "message": f"用户名或密码错误（已输错 {current_fails} 次）", 
                "fail_count": current_fails
            }), 400
            
        db_userid, db_username, db_phone, db_password, db_status, db_role_id, db_role_name, db_avatar = user_row
        
        if db_password != password:
            session['login_fail_count'] = fail_count + 1
            current_fails = session['login_fail_count']
            if current_fails >= 3:
                return jsonify({
                    "status": "error", 
                    "message": "密码输错已达3次，请联系管理员重置！", 
                    "fail_count": current_fails
                }), 400
            return jsonify({
                "status": "error", 
                "message": f"用户名或密码错误（已输错 {current_fails} 次）", 
                "fail_count": current_fails
            }), 400
            
        session['login_fail_count'] = 0
        session['userid'] = db_userid
        session['username'] = db_username
        session['role_id'] = db_role_id
        session['role_name'] = db_role_name
        session['avatar'] = db_avatar or ''
        
        return jsonify({"status": "success", "message": "登录成功"})
        
    if 'userid' in session:
        return redirect('/')
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect('/login')

@app.route('/api/profile', methods=['GET'])
def get_profile():
    userid = session.get('userid')
    if not userid:
        return jsonify({"status": "error", "message": "未登录"}), 401
    
    conn = dm._get_conn()
    row = None
    try:
        row = conn.execute(
            "SELECT username, phone, email, role_name, avatar FROM \"user\" WHERE userid = ?",
            (userid,)
        ).fetchone()
    finally:
        conn.close()
        
    if not row:
        return jsonify({"status": "error", "message": "用户不存在"}), 404
        
    username, phone, email, role_name, avatar = row
    return jsonify({
        "status": "success",
        "data": {
            "username": username,
            "phone": phone,
            "email": email,
            "role_name": role_name,
            "avatar": avatar or ""
        }
    })

@app.route('/api/profile/avatar', methods=['POST'])
def upload_avatar():
    userid = session.get('userid')
    if not userid:
        return jsonify({"status": "error", "message": "未登录"}), 401
        
    f = request.files.get('avatar')
    if not f:
        return jsonify({"status": "error", "message": "未检测到上传的图片文件"}), 400
        
    ext = os.path.splitext(f.filename)[1].lower()
    if ext not in ['.png', '.jpg', '.jpeg', '.gif']:
        return jsonify({"status": "error", "message": "不支持的文件格式，仅支持 png, jpg, jpeg, gif 图片"}), 400
        
    f.seek(0, 2)
    size = f.tell()
    f.seek(0)
    if size > 2 * 1024 * 1024:
        return jsonify({"status": "error", "message": "上传失败：图片大小不能超过 2MB"}), 400
        
    save_dir = os.path.join(resource_root, 'static', 'uploads', 'avatars')
    os.makedirs(save_dir, exist_ok=True)
    
    filename = f"{userid}_{int(time.time())}{ext}"
    dest_path = os.path.join(save_dir, filename)
    f.save(dest_path)
    
    avatar_url = f"/static/uploads/avatars/{filename}"
    
    conn = dm._get_conn()
    try:
        with conn:
            conn.execute("UPDATE \"user\" SET avatar = ? WHERE userid = ?", (avatar_url, userid))
    finally:
        conn.close()
        
    session['avatar'] = avatar_url
    
    return jsonify({"status": "success", "avatar": avatar_url})

@app.route('/api/profile/reset-password', methods=['POST'])
def reset_profile_password():
    userid = session.get('userid')
    if not userid:
        return jsonify({"status": "error", "message": "未登录"}), 401
        
    conn = dm._get_conn()
    row = None
    try:
        row = conn.execute("SELECT phone FROM \"user\" WHERE userid = ?", (userid,)).fetchone()
    finally:
        conn.close()
        
    if not row:
        return jsonify({"status": "error", "message": "用户不存在"}), 404
        
    phone = row[0].strip()
    if not phone or len(phone) < 6:
        return jsonify({"status": "error", "message": "密码重置失败：用户绑定的手机号码无效"}), 400
        
    new_pwd = phone[-6:]
    
    conn = dm._get_conn()
    try:
        with conn:
            conn.execute("UPDATE \"user\" SET password = ? WHERE userid = ?", (new_pwd, userid))
    finally:
        conn.close()
        
    return jsonify({"status": "success", "message": f"密码已成功重置为手机号后6位：{new_pwd}"})

@app.route('/api/profile/change-password', methods=['POST'])
def change_profile_password():
    userid = session.get('userid')
    if not userid:
        return jsonify({"status": "error", "message": "未登录"}), 401
        
    data = request.get_json() or request.form
    old_password_encrypted = data.get('old_password', '').strip()
    new_password_encrypted = data.get('new_password', '').strip()
    
    if not old_password_encrypted or not new_password_encrypted:
        return jsonify({"status": "error", "message": "旧密码和新密码不能为空"}), 400
        
    old_password = rc4_decrypt_str(old_password_encrypted, 'PiPaiSmartOpsKey2026')
    new_password = rc4_decrypt_str(new_password_encrypted, 'PiPaiSmartOpsKey2026')
    
    if not old_password or not new_password:
        return jsonify({"status": "error", "message": "密码安全解密失败，请重试"}), 400
        
    conn = dm._get_conn()
    row = None
    try:
        row = conn.execute("SELECT password FROM \"user\" WHERE userid = ?", (userid,)).fetchone()
    finally:
        conn.close()
        
    if not row:
        return jsonify({"status": "error", "message": "用户不存在"}), 404
        
    db_password = row[0]
    if db_password != old_password:
        return jsonify({"status": "error", "message": "原密码输入错误，请重新输入"}), 400
        
    if new_password == old_password:
        return jsonify({"status": "error", "message": "新密码不能与旧密码相同！"}), 400
        
    conn = dm._get_conn()
    try:
        with conn:
            conn.execute("UPDATE \"user\" SET password = ? WHERE userid = ?", (new_password, userid))
    finally:
        conn.close()
        
    return jsonify({"status": "success", "message": "密码修改成功，请牢记您的新密码！"})

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

@app.route('/market-analysis/raw')
def market_analysis_raw_page():
    is_valid, _ = check_license()
    if not is_valid: return render_template('activate.html', hwid=CURRENT_HWID)
    return render_template('market_analysis_raw.html', active_project=dm.active_project_name)

@app.route("/match-agent")
def match_agent_page():
    is_valid, _ = check_license()
    if not is_valid: return render_template("activate.html", hwid=CURRENT_HWID)
    return render_template("match_agent.html")

@app.route("/pi-agent")
def pi_agent_page():
    is_valid, _ = check_license()
    if not is_valid: return render_template("activate.html", hwid=CURRENT_HWID)
    return render_template("pi_agent.html")

@app.route("/match-rules")
def match_rules_page():
    is_valid, _ = check_license()
    if not is_valid: return render_template("activate.html", hwid=CURRENT_HWID)
    return render_template("match_rules.html")

@app.route("/config-management")
def config_management_page():
    is_valid, _ = check_license()
    if not is_valid: return render_template("activate.html", hwid=CURRENT_HWID)
    return redirect("/config-management/permissions")

@app.route("/config-management/permissions")
def permission_config_page():
    is_valid, _ = check_license()
    if not is_valid: return render_template("activate.html", hwid=CURRENT_HWID)
    return render_template("permission_config.html")

@app.route("/config-management/rules")
def rule_config_page():
    is_valid, _ = check_license()
    if not is_valid: return render_template("activate.html", hwid=CURRENT_HWID)
    return render_template("match_rules.html")

@app.route("/user-management")
def user_management_page():
    is_valid, _ = check_license()
    if not is_valid: return render_template("activate.html", hwid=CURRENT_HWID)
    return redirect("/user-management/users")

@app.route("/user-management/users")
def user_list_page():
    is_valid, _ = check_license()
    if not is_valid: return render_template("activate.html", hwid=CURRENT_HWID)
    return render_template("user_list.html")

@app.route("/user-management/roles")
def role_list_page():
    is_valid, _ = check_license()
    if not is_valid: return render_template("activate.html", hwid=CURRENT_HWID)
    return render_template("role_list.html")

PHONE_RE = re.compile(r"^\d{11}$")
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def _json_error(message, status=400):
    return jsonify({"status": "error", "message": message}), status

def _get_role(conn, role_id):
    if not role_id:
        return None
    # Check in characters table first
    row = conn.execute(
        "SELECT characterid, name FROM characters WHERE characterid = ? AND status = 1",
        (str(role_id),),
    ).fetchone()
    if row:
        return row
    # Fallback to roles table if role_id is numeric
    try:
        rid = int(role_id)
        row = conn.execute(
            "SELECT id, name FROM roles WHERE id = ? AND status = 1",
            (rid,),
        ).fetchone()
        return row
    except (TypeError, ValueError):
        pass
    return None

def _validate_user_payload(payload, require_all=True):
    username = (payload.get("username") or "").strip()
    phone = (payload.get("phone") or "").strip()
    email = (payload.get("email") or "").strip()
    brand = (payload.get("brand") or "").strip()
    role_id = payload.get("role_id")
    if require_all and not username:
        return None, "用户名不能为空"
    if require_all and not phone:
        return None, "手机号不能为空"
    if phone and not PHONE_RE.match(phone):
        return None, "手机号必须是 11 位数字"
    if email and not EMAIL_RE.match(email):
        return None, "邮箱格式不正确"
    if require_all and not role_id:
        return None, "请选择所属角色"
    return {
        "username": username,
        "phone": phone,
        "email": email,
        "brand": brand,
        "role_id": role_id,
    }, None

@app.route("/api/user-roles")
def api_user_roles():
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            rows = conn.execute(
                "SELECT characterid, name FROM characters WHERE status = 1 ORDER BY created_at DESC, characterid ASC"
            ).fetchall()
        finally:
            conn.close()
    return jsonify({
        "status": "success",
        "roles": [{"id": row[0], "name": row[1]} for row in rows],
    })

@app.route("/api/users", methods=["GET"])
def api_users_list():
    page = max(1, request.args.get("page", default=1, type=int) or 1)
    page_size = request.args.get("page_size", default=10, type=int) or 10
    page_size = min(max(1, page_size), 100)
    status_filter = (request.args.get("status") or "").strip()
    keyword = (request.args.get("keyword") or "").strip()
    where = []
    params = []
    if status_filter in {"1", "2"}:
        where.append("status = ?")
        params.append(int(status_filter))
    if keyword:
        like = f"%{keyword}%"
        where.append("(username LIKE ? OR phone LIKE ? OR email LIKE ? OR brand LIKE ? OR role_name LIKE ?)")
        params.extend([like, like, like, like, like])
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    offset = (page - 1) * page_size
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            total = conn.execute(f"SELECT COUNT(*) FROM \"user\" {where_sql}", params).fetchone()[0]
            rows = conn.execute(
                f"""
                SELECT userid, username, phone, email, brand, status, created_at, role_id, role_name
                FROM "user"
                {where_sql}
                ORDER BY created_at DESC, userid DESC
                LIMIT ? OFFSET ?
                """,
                params + [page_size, offset],
            ).fetchall()
        finally:
            conn.close()
    users = [
        {
            "userid": row[0],
            "username": row[1],
            "phone": row[2],
            "email": row[3] or "",
            "brand": row[4] or "",
            "status": int(row[5] or 1),
            "created_at": row[6] or "",
            "role_id": row[7],
            "role_name": row[8] or "",
        }
        for row in rows
    ]
    return jsonify({
        "status": "success",
        "users": users,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total": total,
            "pages": (total + page_size - 1) // page_size if total else 1,
        },
    })

@app.route("/api/users", methods=["POST"])
def api_users_create():
    payload = request.get_json(silent=True) or {}
    data, err = _validate_user_payload(payload)
    if err:
        return _json_error(err)
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            role = _get_role(conn, data["role_id"])
            if not role:
                return _json_error("所属角色不存在")
            exists = conn.execute("SELECT userid FROM \"user\" WHERE phone = ?", (data["phone"],)).fetchone()
            if exists:
                return _json_error("手机号已存在")
            now = time.strftime("%Y-%m-%d %H:%M:%S")
            userid = uuid.uuid4().hex
            password = data["phone"][-6:]
            with conn:
                conn.execute(
                    """
                    INSERT INTO "user"
                    (userid, username, phone, email, password, brand, status, created_at, role_id, role_name)
                    VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
                    """,
                    (
                        userid,
                        data["username"],
                        data["phone"],
                        data["email"],
                        password,
                        data["brand"],
                        now,
                        role[0],
                        role[1],
                    ),
                )
                dm.sync_character_userids(conn)
        finally:
            conn.close()
    return jsonify({"status": "success", "message": "用户创建成功", "userid": userid})

@app.route("/api/users/<userid>", methods=["PUT"])
def api_users_update(userid):
    payload = request.get_json(silent=True) or {}
    data, err = _validate_user_payload(payload)
    if err:
        return _json_error(err)
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            role = _get_role(conn, data["role_id"])
            if not role:
                return _json_error("所属角色不存在")
            current = conn.execute("SELECT userid FROM \"user\" WHERE userid = ?", (userid,)).fetchone()
            if not current:
                return _json_error("用户不存在", 404)
            exists = conn.execute(
                "SELECT userid FROM \"user\" WHERE phone = ? AND userid <> ?",
                (data["phone"], userid),
            ).fetchone()
            if exists:
                return _json_error("手机号已存在")
            with conn:
                conn.execute(
                    """
                    UPDATE "user"
                    SET username = ?, phone = ?, email = ?, password = ?, brand = ?, role_id = ?, role_name = ?
                    WHERE userid = ?
                    """,
                    (
                        data["username"],
                        data["phone"],
                        data["email"],
                        data["phone"][-6:],
                        data["brand"],
                        role[0],
                        role[1],
                        userid,
                    ),
                )
                dm.sync_character_userids(conn)
        finally:
            conn.close()
    return jsonify({"status": "success", "message": "用户更新成功"})

@app.route("/api/users/<userid>/freeze", methods=["POST"])
def api_users_freeze(userid):
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            current = conn.execute("SELECT userid FROM \"user\" WHERE userid = ?", (userid,)).fetchone()
            if not current:
                return _json_error("用户不存在", 404)
            with conn:
                conn.execute("UPDATE \"user\" SET status = 2 WHERE userid = ?", (userid,))
        finally:
            conn.close()
    return jsonify({"status": "success", "message": "用户已冻结"})

@app.route("/api/users/<userid>/unfreeze", methods=["POST"])
def api_users_unfreeze(userid):
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            current = conn.execute("SELECT userid FROM \"user\" WHERE userid = ?", (userid,)).fetchone()
            if not current:
                return _json_error("用户不存在", 404)
            with conn:
                conn.execute("UPDATE \"user\" SET status = 1 WHERE userid = ?", (userid,))
        finally:
            conn.close()
    return jsonify({"status": "success", "message": "用户已解冻"})

# --- Characters API endpoints ---

@app.route("/api/characters", methods=["GET"])
def api_characters_list():
    import json
    page = max(1, request.args.get("page", default=1, type=int) or 1)
    page_size = request.args.get("page_size", default=10, type=int) or 10
    page_size = min(max(1, page_size), 100)
    status_filter = (request.args.get("status") or "").strip()
    keyword = (request.args.get("keyword") or "").strip()
    
    where = []
    params = []
    if status_filter in {"1", "2"}:
        where.append("status = ?")
        params.append(int(status_filter))
    if keyword:
        like = f"%{keyword}%"
        where.append("(name LIKE ? OR description LIKE ?)")
        params.extend([like, like])
    
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    offset = (page - 1) * page_size
    
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            total = conn.execute(f"SELECT COUNT(*) FROM characters {where_sql}", params).fetchone()[0]
            rows = conn.execute(
                f"""
                SELECT characterid, name, description, permissions, status, created_at
                FROM characters
                {where_sql}
                ORDER BY created_at DESC, characterid DESC
                LIMIT ? OFFSET ?
                """,
                params + [page_size, offset]
            ).fetchall()
            
            characters = []
            for row in rows:
                char_id, name, desc, perms_json, status, created_at = row
                try:
                    perms = json.loads(perms_json) if perms_json else []
                except Exception:
                    perms = []
                
                # Fetch dynamically from user table to ensure accuracy
                user_rows = conn.execute("SELECT userid, username FROM \"user\" WHERE role_id = ?", (char_id,)).fetchall()
                userids = [ur[0] for ur in user_rows]
                usernames = [ur[1] for ur in user_rows]
                
                characters.append({
                    "characterid": char_id,
                    "name": name,
                    "description": desc or "",
                    "permissions": perms,
                    "userids": userids,
                    "usernames": usernames,
                    "user_count": len(userids),
                    "status": int(status or 1),
                    "created_at": created_at or ""
                })
        finally:
            conn.close()
            
    return jsonify({
        "status": "success",
        "characters": characters,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total": total,
            "pages": (total + page_size - 1) // page_size if total else 1,
        }
    })

@app.route("/api/characters", methods=["POST"])
def api_characters_create():
    import json
    payload = request.get_json(silent=True) or {}
    name = (payload.get("name") or "").strip()
    description = (payload.get("description") or "").strip()
    permissions = payload.get("permissions") or []
    
    if not name:
        return _json_error("角色名不能为空")
    
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            exists = conn.execute("SELECT characterid FROM characters WHERE name = ?", (name,)).fetchone()
            if exists:
                return _json_error("角色名已存在")
            
            characterid = uuid.uuid4().hex
            now = time.strftime("%Y-%m-%d %H:%M:%S")
            perms_json = json.dumps(permissions, ensure_ascii=False)
            
            with conn:
                conn.execute(
                    """
                    INSERT INTO characters (characterid, name, description, permissions, status, created_at)
                    VALUES (?, ?, ?, ?, 1, ?)
                    """,
                    (characterid, name, description, perms_json, now)
                )
        finally:
            conn.close()
            
    return jsonify({"status": "success", "message": "角色创建成功", "characterid": characterid})

@app.route("/api/characters/<characterid>", methods=["PUT"])
def api_characters_update(characterid):
    import json
    payload = request.get_json(silent=True) or {}
    name = (payload.get("name") or "").strip()
    description = (payload.get("description") or "").strip()
    permissions = payload.get("permissions") or []
    
    if not name:
        return _json_error("角色名不能为空")
    
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            current = conn.execute("SELECT characterid, name FROM characters WHERE characterid = ?", (characterid,)).fetchone()
            if not current:
                return _json_error("角色不存在", 404)
            
            exists = conn.execute("SELECT characterid FROM characters WHERE name = ? AND characterid <> ?", (name, characterid)).fetchone()
            if exists:
                return _json_error("角色名已存在")
            
            perms_json = json.dumps(permissions, ensure_ascii=False)
            
            with conn:
                conn.execute(
                    """
                    UPDATE characters
                    SET name = ?, description = ?, permissions = ?
                    WHERE characterid = ?
                    """,
                    (name, description, perms_json, characterid)
                )
                if current[1] != name:
                    conn.execute("UPDATE \"user\" SET role_name = ? WHERE role_id = ?", (name, characterid))
        finally:
            conn.close()
            
    return jsonify({"status": "success", "message": "角色更新成功"})

@app.route("/api/characters/<characterid>/freeze", methods=["POST"])
def api_characters_freeze(characterid):
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            current = conn.execute("SELECT characterid FROM characters WHERE characterid = ?", (characterid,)).fetchone()
            if not current:
                return _json_error("角色不存在", 404)
            with conn:
                conn.execute("UPDATE characters SET status = 2 WHERE characterid = ?", (characterid,))
        finally:
            conn.close()
    return jsonify({"status": "success", "message": "角色已冻结"})

@app.route("/api/characters/<characterid>/unfreeze", methods=["POST"])
def api_characters_unfreeze(characterid):
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            current = conn.execute("SELECT characterid FROM characters WHERE characterid = ?", (characterid,)).fetchone()
            if not current:
                return _json_error("角色不存在", 404)
            with conn:
                conn.execute("UPDATE characters SET status = 1 WHERE characterid = ?", (characterid,))
        finally:
            conn.close()
    return jsonify({"status": "success", "message": "角色已解冻"})

@app.route("/data-management")
def data_management_page():
    is_valid, _ = check_license()
    if not is_valid: return render_template("activate.html", hwid=CURRENT_HWID)
    return redirect("/data-management/operations")

@app.route("/data-management/operations")
def operation_data_page():
    is_valid, _ = check_license()
    if not is_valid: return render_template("activate.html", hwid=CURRENT_HWID)
    return render_template("data_management.html", page_title="运营数据", active_data_child="operations")

@app.route("/data-management/stores")
def store_data_page():
    is_valid, _ = check_license()
    if not is_valid: return render_template("activate.html", hwid=CURRENT_HWID)
    return render_template("data_management.html", page_title="门店数据", active_data_child="stores")

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
