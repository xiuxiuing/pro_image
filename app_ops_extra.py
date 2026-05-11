import os
import sys
import shutil
import time
import threading
import traceback
import json
import platform
import datetime
from flask import Blueprint, request, jsonify

# These will be initialized by app_ops.py or app.py
ops_context = {}

extra_bp = Blueprint('ops_extra', __name__)

def init_ops_extra(context):
    global ops_context
    ops_context = context

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

    task = ops_context["create_task"]("package", ["检查环境", "PyArmor 混淆", "PyInstaller 打包", "压缩产物"], f"{target} 打包排队中")
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
                spec = "ProImage_macOS.spec"
                artifact = os.path.join(ops_context["resource_root"], "dist", "ProImage_AI.app")
                zip_name = f"ProImage_AI_macOS_{time.strftime('%Y%m%d_%H%M%S')}.zip"
            else:
                if system != "Windows":
                    raise RuntimeError("Windows 程序需要在 Windows 打包机上执行")
                ops_context["update_step"](task_id, 0, "done", "Windows 环境")
                spec = "ProImage_Windows.spec"
                artifact = os.path.join(ops_context["resource_root"], "dist", "ProImage")
                zip_name = f"ProImage_Windows_{time.strftime('%Y%m%d_%H%M%S')}.zip"

            obf_dir = os.path.join(ops_context["resource_root"], "dist", "obfuscated")
            if os.path.isdir(obf_dir):
                shutil.rmtree(obf_dir)
            
            pyarmor_cmd = ops_context["pyarmor_command"]() + ["gen", "-O", os.path.join("dist", "obfuscated")] + ops_context["pyarmor_files"]
            try:
                ops_context["run_command"](task_id, 1, pyarmor_cmd, ops_context["resource_root"])
                ops_context["verify_pyarmor_output"](obf_dir)
                ops_context["update_step"](task_id, 1, "done", f"完成，已生成 {len(ops_context['pyarmor_files'])} 个混淆文件")
            except BaseException as e:
                if os.path.isdir(obf_dir):
                    shutil.rmtree(obf_dir)
                detail = str(e).splitlines()[-1] if str(e).splitlines() else str(e)
                ops_context["update_step"](task_id, 1, "done", f"混淆失败，已清理并改用源码模式：{detail[:180]}")

            ops_context["run_command"](task_id, 2, [sys.executable, "-m", "PyInstaller", "-y", spec], ops_context["resource_root"])
            if not os.path.exists(artifact):
                raise RuntimeError(f"打包产物不存在：{artifact}")
            if target == "macos":
                ops_context["run_command"](task_id, 2, ["xattr", "-cr", artifact], ops_context["resource_root"])
                ops_context["run_command"](task_id, 2, ["codesign", "--force", "--deep", "--sign", "-", artifact], ops_context["resource_root"])
            
            task_dir = ops_context["task_dir"](task_id)
            zip_path = os.path.join(task_dir, zip_name)
            ops_context["update_step"](task_id, 3, "running", "压缩产物")
            ops_context["zip_path"](artifact, zip_path)
            ops_context["update_step"](task_id, 3, "done", "完成")
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
