import os
from flask import Blueprint, jsonify, request, send_file

match_agent_bp = Blueprint("match_agent", __name__)

dm = None
_validate_upload = None


def init_match_agent(context):
    global dm, _validate_upload
    dm = context["dm"]
    _validate_upload = context["validate_upload"]


@match_agent_bp.route("/api/match-agent/stores", methods=["GET"])
def api_match_agent_stores():
    project_id = request.args.get("project_id", type=int) or dm.active_project_id
    return jsonify({"status": "ok", **dm.list_match_agent_project_stores(project_id=project_id)})


@match_agent_bp.route("/api/match-agent/cases", methods=["GET", "POST"])
def api_match_agent_cases():
    if request.method == "GET":
        project_id = request.args.get("project_id", type=int) or dm.active_project_id
        limit = request.args.get("limit", 200, type=int)
        return jsonify({"status": "ok", **dm.list_match_feedback_cases(project_id=project_id, limit=limit)})
    data = request.get_json(silent=True) or {}
    ok, msg, cid = dm.create_match_feedback_case(data)
    if not ok:
        return jsonify({"status": "error", "message": msg}), 400
    return jsonify({"status": "ok", "id": cid})


@match_agent_bp.route("/api/match-agent/cases/import", methods=["POST"])
def api_match_agent_cases_import():
    file = request.files.get("file")
    if not file or not file.filename:
        return jsonify({"status": "error", "message": "请上传反馈样本Excel"}), 400
    err = _validate_upload(file, "反馈样本Excel") if _validate_upload else None
    if err:
        return jsonify({"status": "error", "message": err}), 400
    project_id = request.form.get("project_id", type=int) or dm.active_project_id
    result = dm.import_match_feedback_cases(file, project_id=project_id)
    code = 200 if result.get("status") == "ok" else 400
    return jsonify(result), code


@match_agent_bp.route("/api/match-agent/runs", methods=["GET", "POST"])
def api_match_agent_runs():
    try:
        if request.method == "GET":
            project_id = request.args.get("project_id", type=int) or dm.active_project_id
            return jsonify({"status": "ok", **dm.list_match_agent_runs(project_id=project_id)})
        data = request.get_json(silent=True) or {}
        try:
            temperature = float(data.get("temperature") or 0.2)
        except (TypeError, ValueError):
            return jsonify({"status": "error", "message": "温度必须是数字"}), 400
        result = dm.run_match_agent(
            provider=data.get("provider") or "gemini",
            model_name=data.get("model_name") or "",
            temperature=temperature,
            project_id=data.get("project_id") or dm.active_project_id,
            api_key=data.get("api_key") or "",
        )
        code = 200 if result.get("status") == "ok" else 400
        return jsonify(result), code
    except Exception as e:
        return jsonify({"status": "error", "message": f"Agent 运行失败：{e}"}), 500


@match_agent_bp.route("/api/match-agent/quick-run", methods=["POST"])
def api_match_agent_quick_run():
    try:
        data = request.get_json(silent=True) or {}
        try:
            temperature = float(data.get("temperature") or 0.2)
        except (TypeError, ValueError):
            return jsonify({"status": "error", "message": "温度必须是数字"}), 400
        result = dm.quick_run_match_agent(
            data,
            provider=data.get("provider") or "gemini",
            model_name=data.get("model_name") or "",
            temperature=temperature,
            api_key=data.get("api_key") or "",
        )
        code = 200 if result.get("status") == "ok" else 400
        return jsonify(result), code
    except Exception as e:
        return jsonify({"status": "error", "message": f"快速优化失败：{e}"}), 500


@match_agent_bp.route("/api/match-agent/runs/<int:run_id>", methods=["GET"])
def api_match_agent_run(run_id):
    item = dm.get_match_agent_run(run_id)
    if not item:
        return jsonify({"status": "error", "message": "运行记录不存在"}), 404
    return jsonify({"status": "ok", "run": item})


@match_agent_bp.route("/api/match-agent/runs/<int:run_id>/report", methods=["GET"])
def api_match_agent_report(run_id):
    path = dm.get_match_agent_report_path(run_id)
    if not path:
        return jsonify({"status": "error", "message": "报告不存在"}), 404
    return send_file(
        path,
        as_attachment=True,
        download_name=os.path.basename(path),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@match_agent_bp.route("/api/match-agent/runs/<int:run_id>/publish-rule-template", methods=["POST"])
def api_match_agent_publish_rule_template(run_id):
    ok, msg, tid = dm.publish_match_agent_rule_template(run_id)
    if not ok:
        return jsonify({"status": "error", "message": msg}), 400
    return jsonify({"status": "ok", "template_id": tid})


@match_agent_bp.route("/api/match-agent/runs/<int:run_id>/apply", methods=["POST"])
def api_match_agent_apply(run_id):
    ok, msg, tid = dm.apply_match_agent_run(run_id)
    if not ok:
        return jsonify({"status": "error", "message": msg}), 400
    return jsonify({"status": "ok", "template_id": tid})


@match_agent_bp.route("/api/match-agent/runs/<int:run_id>/apply-to-v2", methods=["POST"])
def api_match_agent_apply_to_v2(run_id):
    data = request.get_json(silent=True) or {}
    ok, msg, result = dm.apply_match_agent_run_to_v2(run_id, bind_project=bool(data.get("bind_project")))
    if not ok:
        return jsonify({"status": "error", "message": msg}), 400
    return jsonify({"status": "ok", **(result or {})})
