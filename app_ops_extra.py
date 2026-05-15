import os
import sys
import shutil
import time
import threading
import traceback
import json
import platform
import datetime
import glob
from flask import Blueprint, request, jsonify
from packaging_core import BUSINESS_SOURCE_FILES, CORE_NUITKA_MODULES, REQUIRED_MODEL_FILES, RESOURCE_DIRS

# These will be initialized by app_ops.py or app.py
ops_context = {}

extra_bp = Blueprint('ops_extra', __name__)

def init_ops_extra(context):
    global ops_context
    ops_context = context

def _copytree_fresh(src, dst):
    if os.path.exists(dst):
        shutil.rmtree(dst)
    shutil.copytree(src, dst)

def _compiled_module_glob(root, module, target):
    suffixes = (".pyd",) if target == "windows" else (".so",)
    patterns = [os.path.join(root, f"{module}*{suffix}") for suffix in suffixes]
    matches = []
    for pat in patterns:
        matches.extend(glob.glob(pat))
    return sorted(matches)

def _missing_model_resources(root):
    missing = []
    models_root = os.path.join(root, "models")
    for parts in REQUIRED_MODEL_FILES:
        rel = os.path.join("models", *parts)
        if not os.path.isfile(os.path.join(models_root, *parts)):
            missing.append(rel)
    return missing

def _ensure_required_models(root):
    missing = _missing_model_resources(root)
    if missing:
        raise RuntimeError("缺少本地模型资源：" + ", ".join(missing))

def _remove_tree_if_exists(path, label):
    if not os.path.exists(path):
        return
    try:
        shutil.rmtree(path)
    except PermissionError as exc:
        raise RuntimeError(f"{label} 正被其他程序占用，请关闭已运行的打包程序或资源管理器预览后重试：{path}") from exc

def _prepare_nuitka_build_src(root, target):
    build_src = os.path.join(root, "_build_src")
    if os.path.isdir(build_src):
        shutil.rmtree(build_src)
    os.makedirs(build_src, exist_ok=True)
    for rel in BUSINESS_SOURCE_FILES:
        src = os.path.join(root, rel)
        if not os.path.isfile(src):
            raise RuntimeError(f"缺少业务壳源码：{rel}")
        shutil.copy2(src, os.path.join(build_src, rel))
    for name in RESOURCE_DIRS:
        src = os.path.join(root, name)
        if not os.path.isdir(src):
            raise RuntimeError(f"缺少资源目录：{name}")
        _copytree_fresh(src, os.path.join(build_src, name))
    _ensure_required_models(root)
    _copytree_fresh(os.path.join(root, "models"), os.path.join(build_src, "models"))
    modules_dir = os.path.join(root, "nuitka_modules")
    for mod in CORE_NUITKA_MODULES:
        matches = _compiled_module_glob(modules_dir, mod, target)
        if not matches:
            raise RuntimeError(f"缺少核心编译产物：{mod}")
        shutil.copy2(matches[-1], build_src)
    return build_src

def _verify_nuitka_artifact(target, artifact):
    if not os.path.exists(artifact):
        raise RuntimeError(f"打包产物不存在：{artifact}")
    found = {}
    leaked = []
    own_core_py = {
        f"{m}.py" for m in CORE_NUITKA_MODULES
    } | {
        f"Contents/Resources/{m}.py" for m in CORE_NUITKA_MODULES
    } | {
        f"Contents/Frameworks/{m}.py" for m in CORE_NUITKA_MODULES
    }
    artifact_paths = set()
    for dirpath, _, filenames in os.walk(artifact):
        artifact_paths.add(os.path.relpath(dirpath, artifact).replace("\\", "/"))
        for fn in filenames:
            stem = fn.split(".")[0]
            full = os.path.join(dirpath, fn)
            artifact_paths.add(os.path.relpath(full, artifact).replace("\\", "/"))
            if stem in CORE_NUITKA_MODULES and fn.endswith((".pyd", ".so")):
                found[stem] = full
            rel = os.path.relpath(full, artifact).replace("\\", "/")
            if rel in own_core_py:
                leaked.append(full)
    missing = [m for m in CORE_NUITKA_MODULES if m not in found]
    if missing:
        raise RuntimeError("核心编译模块未进入产物：" + ", ".join(missing))
    if leaked:
        raise RuntimeError("核心源码泄露到产物：" + ", ".join(leaked[:5]))
    required = [
        os.path.join("templates"),
        os.path.join("static"),
        os.path.join("data", "default_rule_templates", "production_rule_v1.json"),
    ]
    required.extend(os.path.join("models", *parts) for parts in REQUIRED_MODEL_FILES)
    for rel in required:
        needle = rel.replace("\\", "/")
        if not any(p == needle or p.endswith("/" + needle) for p in artifact_paths):
            raise RuntimeError(f"产物缺少资源：{rel}")
    return f"验证通过：{len(found)} 个核心模块已编译进入 {target} 产物"

@extra_bp.route('/api/ops/license-key-status')
def api_ops_license_key_status():
    license_err = ops_context["license_error_response"]()
    if license_err: return license_err
    status = ops_context["default_private_key_status"]()
    return jsonify({"status": "ok", **status})

@extra_bp.route('/api/ops/license-generate', methods=['POST'])
def api_ops_license_generate():
    license_err = ops_context["license_error_response"]()
    if license_err: return license_err
    raw_hwids = (request.form.get("hwids") or "").strip()
    hwids = [x.strip().upper() for x in raw_hwids.replace(",", "\n").replace("，", "\n").splitlines() if x.strip()]
    if not hwids:
        return jsonify({"status": "error", "message": "请填写至少一个 HWID"}), 400
    try:
        days = int((request.form.get("days") or "30").strip())
    except ValueError:
        return jsonify({"status": "error", "message": "有效天数必须是数字"}), 400
    if days <= 0 or days > 3650:
        return jsonify({"status": "error", "message": "有效天数需在 1 到 3650 之间"}), 400

    task = ops_context["create_task"]("license", ["生成 license.dat"], "授权文件生成中")
    task_id = task["task_id"]
    task_dir = ops_context["task_dir"](task_id)
    out_path = os.path.join(task_dir, "license.dat")
    uploaded_key = request.files.get("private_key")
    try:
        ops_context["set_task"](task_id, status="running", started_at=ops_context["now"]())
        ops_context["update_step"](task_id, 0, "running", "签名授权")
        expires = ops_context["create_license_file"](hwids, days, out_path, uploaded_key=uploaded_key)
        ops_context["update_step"](task_id, 0, "done", f"到期日 {expires}")
        ops_context["set_task"](
            task_id,
            status="done",
            ended_at=ops_context["now"](),
            message=f"license.dat 已生成，到期日 {expires}",
            result_path=out_path,
            result_kind="license_dat",
            download_name="license.dat",
        )
    except BaseException as e:
        traceback.print_exc()
        ops_context["update_step"](task_id, 0, "failed", str(e))
        ops_context["fail_task"](task_id, e)
    return jsonify({"status": "ok", "task_id": task_id})

@extra_bp.route('/api/ops/package-build', methods=['POST'])
def api_ops_package_build():
    license_err = ops_context["license_error_response"]()
    if license_err: return license_err
    target = (request.form.get("target") or "").strip().lower()
    if target not in ("macos", "windows"):
        return jsonify({"status": "error", "message": "请选择 macOS 或 Windows"}), 400

    steps = ["检查环境", "Nuitka 编译核心", "准备打包目录", "PyInstaller 打包", "验证产物", "压缩产物"]
    task = ops_context["create_task"]("package", steps, f"{target} 打包排队中")
    task_id = task["task_id"]

    def _run_package_bg():
        ops_context["set_task"](task_id, status="running", started_at=ops_context["now"](), message=f"{target} 打包中")
        try:
            system = platform.system()
            ops_context["update_step"](task_id, 0, "running", f"当前系统 {system}")
            if target == "macos":
                if system != "Darwin":
                    raise RuntimeError("macOS .app 需要在 macOS 打包机上执行")
                patch_script = os.path.join(ops_context["resource_root"], "tools", "patch_pyinstaller_site_packages.py")
                if os.path.isfile(patch_script):
                    ops_context["run_command"](task_id, 0, [sys.executable, patch_script], ops_context["resource_root"])
                else:
                    ops_context["update_step"](task_id, 0, "done", "未找到 patch 脚本，跳过")
                spec = "ProImage_nuitka_macOS.spec"
                artifact = os.path.join(ops_context["resource_root"], "dist", "ProImage_AI.app")
                zip_name = f"ProImage_AI_macOS_{time.strftime('%Y%m%d_%H%M%S')}.zip"
            else:
                if system != "Windows":
                    raise RuntimeError("Windows 程序需要在 Windows 打包机上执行")
                ops_context["update_step"](task_id, 0, "done", "Windows 环境")
                spec = "ProImage_nuitka_Windows.spec"
                artifact = os.path.join(ops_context["resource_root"], "dist", "ProImage_AI")
                zip_name = f"ProImage_Windows_{time.strftime('%Y%m%d_%H%M%S')}.zip"

            ops_context["run_command"](task_id, 0, [sys.executable, "-m", "nuitka", "--version"], ops_context["resource_root"])
            ops_context["run_command"](task_id, 0, [sys.executable, "-m", "PyInstaller", "--version"], ops_context["resource_root"])
            if _missing_model_resources(ops_context["resource_root"]):
                ops_context["update_step"](task_id, 0, "running", "缺少本地模型，自动下载 models/")
                ops_context["run_command"](task_id, 0, [sys.executable, "download_models.py"], ops_context["resource_root"])
            _ensure_required_models(ops_context["resource_root"])
            ops_context["update_step"](task_id, 0, "done", f"平台={system} Python={sys.version.split()[0]}")

            modules_dir = os.path.join(ops_context["resource_root"], "nuitka_modules")
            os.makedirs(modules_dir, exist_ok=True)
            for mod in CORE_NUITKA_MODULES:
                src = f"{mod}.py"
                if not os.path.isfile(os.path.join(ops_context["resource_root"], src)):
                    raise RuntimeError(f"缺少核心源码：{src}")
                ops_context["run_command"](
                    task_id,
                    1,
                    [sys.executable, "-m", "nuitka", "--module", "--output-dir=nuitka_modules", src],
                    ops_context["resource_root"],
                )
            ops_context["update_step"](task_id, 1, "done", f"完成，已编译 {len(CORE_NUITKA_MODULES)} 个核心模块")

            ops_context["update_step"](task_id, 2, "running", "复制业务壳、资源和核心编译产物")
            build_src = _prepare_nuitka_build_src(ops_context["resource_root"], target)
            ops_context["update_step"](task_id, 2, "done", build_src)

            _remove_tree_if_exists(artifact, "旧打包产物")
            _remove_tree_if_exists(os.path.join(ops_context["resource_root"], "build", os.path.splitext(spec)[0]), "旧构建缓存")
            ops_context["run_command"](task_id, 3, [sys.executable, "-m", "PyInstaller", "-y", spec], ops_context["resource_root"])
            if target == "macos":
                ops_context["run_command"](task_id, 3, ["xattr", "-cr", artifact], ops_context["resource_root"])
                ops_context["run_command"](task_id, 3, ["codesign", "--force", "--deep", "--sign", "-", artifact], ops_context["resource_root"])

            ops_context["update_step"](task_id, 4, "running", "检查核心模块和资源")
            verify_msg = _verify_nuitka_artifact(target, artifact)
            ops_context["update_step"](task_id, 4, "done", verify_msg)
            
            task_dir = ops_context["task_dir"](task_id)
            zip_path = os.path.join(task_dir, zip_name)
            ops_context["update_step"](task_id, 5, "running", "压缩产物")
            ops_context["zip_path"](artifact, zip_path)
            ops_context["update_step"](task_id, 5, "done", "完成")
            ops_context["set_task"](
                task_id,
                status="done",
                ended_at=ops_context["now"](),
                message="打包完成",
                result_path=zip_path,
                result_kind="package_zip",
                download_name=zip_name,
            )
        except BaseException as e:
            traceback.print_exc()
            task_info = ops_context["get_task"](task_id)
            running_idx = next((i for i, s in enumerate(task_info.get("steps", [])) if s["status"] == "running"), 0)
            ops_context["update_step"](task_id, running_idx, "failed", str(e))
            ops_context["fail_task"](task_id, e)

    threading.Thread(target=_run_package_bg, daemon=True).start()
    return jsonify({"status": "ok", "task_id": task_id})
