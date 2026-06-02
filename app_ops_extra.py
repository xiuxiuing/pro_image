import os
import shutil
import time
import threading
import traceback
import json
from flask import Blueprint, request, jsonify
from packaging_core import (
    BUSINESS_SOURCE_FILES,
    CORE_NUITKA_MODULES,
    DEFAULT_RULE_TEMPLATE_FILES,
    REQUIRED_MODEL_FILES,
    RESOURCE_DIRS,
    cleanup_packaging_intermediates,
    cleanup_pre_zip_workspace,
    compiled_module_glob,
    ensure_build_dependencies,
    nuitka_abi_markers,
    purge_stale_nuitka_modules,
    require_disk_space_for_zip,
    resolve_package_zip_dir,
)

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
        matches = compiled_module_glob(modules_dir, mod, target)
        if not matches:
            raise RuntimeError(
                f"缺少与当前 Python 匹配的核心编译产物：{mod}（需要包含 {', '.join(nuitka_abi_markers())}）"
            )
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
                if not any(marker in fn for marker in nuitka_abi_markers()):
                    raise RuntimeError(
                        f"产物中的核心模块 ABI 不匹配当前 Python：{fn}（需要 {', '.join(nuitka_abi_markers())}）"
                    )
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
    ]
    required.extend(os.path.join(*parts) for parts in DEFAULT_RULE_TEMPLATE_FILES)
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
