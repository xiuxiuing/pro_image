import json
import re
import time
import uuid

from flask import Blueprint, g, jsonify, request, session

import app_data_projects
import app_data_grid
import app_data_rules
import app_data_match_agent
from online_jobs import JobStore


api_v1_bp = Blueprint("api_v1", __name__, url_prefix="/api/v1")
dm = None
job_store = None
PHONE_RE = re.compile(r"^\d{11}$")
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PROJECT_MEMBER_ROLES = {"owner", "editor", "viewer"}
PROJECT_ROLE_RANK = {"viewer": 1, "editor": 2, "owner": 3}


def init_api_v1(context):
    global dm, job_store
    dm = context["dm"]
    job_store = context.get("job_store") or JobStore(dm)


def ok(data=None, meta=None, status_code=200):
    return jsonify({
        "status": "ok",
        "data": data if data is not None else {},
        "error": None,
        "meta": meta or {},
    }), status_code


def fail(message, code="bad_request", status_code=400, meta=None, data=None):
    return jsonify({
        "status": "error",
        "data": data,
        "error": {
            "code": code,
            "message": message,
        },
        "meta": meta or {},
    }), status_code


def legacy_response_to_v1(result, success_code="ok"):
    status_code = 200
    response = result
    if isinstance(result, tuple):
        response = result[0]
        if len(result) > 1 and isinstance(result[1], int):
            status_code = result[1]
    payload = response.get_json(silent=True) if hasattr(response, "get_json") else None
    if not isinstance(payload, dict):
        if status_code >= 400:
            return fail("请求失败", "legacy_error", status_code)
        return ok({})

    legacy_status = str(payload.get("status") or "").lower()
    if status_code < 400 and legacy_status not in ("error", "failed"):
        data = dict(payload)
        data.pop("status", None)
        if legacy_status and legacy_status not in ("success", "ok"):
            data["legacy_status"] = legacy_status
        return ok(data, status_code=status_code)

    message = payload.get("message") or payload.get("error") or "请求失败"
    if status_code < 400:
        status_code = 400
    code = "bad_request"
    if status_code == 404:
        code = "not_found"
    elif status_code == 401:
        code = "unauthorized"
    elif status_code == 403:
        code = "forbidden"
    elif status_code == 409 or legacy_status == "needs_confirmation":
        code = "needs_confirmation" if legacy_status == "needs_confirmation" else "conflict"
    data = dict(payload)
    data.pop("status", None)
    data.pop("message", None)
    return fail(message, code, status_code, data=data or None)


def audited_legacy(result, action, project_id=None, target_type="", target_id="", detail=None):
    converted = legacy_response_to_v1(result)
    response, status_code = converted if isinstance(converted, tuple) else (converted, 200)
    payload = response.get_json(silent=True) if hasattr(response, "get_json") else None
    if status_code < 400 and isinstance(payload, dict) and payload.get("status") == "ok":
        try:
            audit(
                action,
                project_id=project_id if project_id is not None else request_project_id(default_active=True),
                target_type=target_type,
                target_id=target_id,
                detail=detail or {},
            )
        except Exception:
            pass
    return converted


def parse_pagination(default_limit=50, max_limit=200):
    try:
        page = max(1, int(request.args.get("page", 1)))
    except (TypeError, ValueError):
        page = 1
    try:
        limit = max(1, min(int(request.args.get("limit", default_limit)), max_limit))
    except (TypeError, ValueError):
        limit = default_limit
    return page, limit


def paginate_items(items, page, limit):
    total = len(items)
    start = (page - 1) * limit
    end = start + limit
    pages = (total + limit - 1) // limit if total else 0
    return items[start:end], {
        "page": page,
        "limit": limit,
        "total": total,
        "pages": pages,
    }


def current_user_payload():
    if hasattr(g, "api_v1_user"):
        return g.api_v1_user
    userid = session.get("userid")
    if not userid:
        return None
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            row = conn.execute(
                """
                SELECT userid, username, phone, email, role_id, role_name, avatar, status
                FROM "user"
                WHERE userid = ?
                """,
                (userid,),
            ).fetchone()
        finally:
            conn.close()
    if not row:
        g.api_v1_user = None
        return None
    is_super_admin = session.get("username") == "admin" or session.get("role_name") == "超级管理员"
    g.api_v1_user = {
        "userid": row[0],
        "username": row[1],
        "phone": row[2] or "",
        "email": row[3] or "",
        "role_id": row[4] or "",
        "role_name": row[5] or "",
        "avatar": row[6] or "",
        "status": row[7],
        "is_super_admin": bool(is_super_admin),
    }
    return g.api_v1_user


def is_super_admin(user=None):
    user = user if user is not None else current_user_payload()
    return bool(user and user.get("is_super_admin"))


def require_super_admin():
    if not is_super_admin():
        return fail("权限拒绝", "forbidden", 403)
    return None


def request_project_id(default_active=False):
    raw = None
    if request.view_args:
        raw = request.view_args.get("project_id")
    if raw in (None, ""):
        data = request.get_json(silent=True) if request.method in ("POST", "PUT", "PATCH", "DELETE") else None
        if isinstance(data, dict):
            raw = data.get("project_id")
    if raw in (None, ""):
        raw = request.args.get("project_id")
    if raw in (None, "") and default_active:
        raw = dm.active_project_id
    try:
        return int(raw) if raw not in (None, "") else None
    except (TypeError, ValueError):
        return None


def project_role(project_id, user_id=None):
    user = current_user_payload()
    uid = user_id or (user or {}).get("userid")
    if not project_id or not uid:
        return ""
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            row = conn.execute(
                "SELECT role FROM project_members WHERE project_id = ? AND user_id = ?",
                (int(project_id), uid),
            ).fetchone()
        finally:
            conn.close()
    return row[0] if row else ""


def require_project_role(project_id, min_role="viewer"):
    if is_super_admin():
        return None
    if not project_id:
        return fail("缺少 project_id", "bad_request", 400)
    actual = project_role(project_id)
    if PROJECT_ROLE_RANK.get(actual, 0) < PROJECT_ROLE_RANK[min_role]:
        return fail("项目权限不足", "forbidden", 403)
    return None


def accessible_project_ids():
    if is_super_admin():
        return None
    user = current_user_payload()
    if not user:
        return set()
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            rows = conn.execute(
                "SELECT project_id FROM project_members WHERE user_id = ?",
                (user["userid"],),
            ).fetchall()
        finally:
            conn.close()
    return {int(row[0]) for row in rows}


@api_v1_bp.before_request
def api_v1_permissions():
    user = current_user_payload()
    if not user:
        return None

    endpoint = (request.endpoint or "").split(".")[-1]
    if endpoint in {
        "users_list", "users_create", "users_update", "users_freeze", "users_unfreeze",
        "roles_list", "roles_create", "roles_update", "roles_freeze", "roles_unfreeze",
        "audit_logs_list",
    }:
        denied = require_super_admin()
        if denied:
            return denied

    if endpoint in {"project_members_list"}:
        denied = require_project_role(request_project_id(), "viewer")
        if denied:
            return denied
    if endpoint in {"project_members_add", "project_members_remove"}:
        denied = require_project_role(request_project_id(), "owner")
        if denied:
            return denied

    if endpoint in {
        "projects_get", "projects_latest_job", "jobs_project_latest",
        "products_grid", "products_main", "products_store", "products_unlinked",
        "products_main_links", "products_match_explain",
        "statistics_get", "statistics_snapshot_status", "statistics_products",
        "market_analysis_get", "match_agent_stores", "match_agent_cases", "match_agent_runs",
    }:
        denied = require_project_role(request_project_id(default_active=True), "viewer")
        if denied:
            return denied

    if endpoint in {
        "projects_activate", "projects_preflight", "projects_analyze",
        "products_eliminate", "products_handled", "products_ref", "products_update_cell",
        "links_new_flag", "links_ignore", "links_price_match", "links_clear_price_match",
        "links_manual", "links_unlink", "match_agent_cases_import", "match_agent_quick_run",
        "match_agent_publish_rule_template", "match_agent_apply", "match_agent_apply_to_v2",
    }:
        denied = require_project_role(request_project_id(default_active=True), "editor")
        if denied:
            return denied

    if endpoint in {"projects_create"}:
        denied = require_super_admin()
        if denied:
            return denied

    if endpoint in {
        "rule_templates", "rule_template_one", "rule_categories_parse",
        "rule_categories_default", "rule_categories_bucket_tags",
    } and request.method not in ("GET",):
        denied = require_super_admin()
        if denied:
            return denied

    return None


def users_payload():
    page, limit = parse_pagination(default_limit=10, max_limit=100)
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
    offset = (page - 1) * limit
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            total = conn.execute(f"SELECT COUNT(*) FROM \"user\" {where_sql}", params).fetchone()[0]
            rows = conn.execute(
                f"""
                SELECT userid, username, phone, email, brand, status, created_at, role_id, role_name, avatar
                FROM "user"
                {where_sql}
                ORDER BY created_at DESC, userid DESC
                LIMIT ? OFFSET ?
                """,
                params + [limit, offset],
            ).fetchall()
        finally:
            conn.close()
    items = [
        {
            "userid": row[0],
            "username": row[1],
            "phone": row[2],
            "email": row[3] or "",
            "brand": row[4] or "",
            "status": int(row[5] or 1),
            "created_at": row[6] or "",
            "role_id": row[7] or "",
            "role_name": row[8] or "",
            "avatar": row[9] or "",
        }
        for row in rows
    ]
    pages = (total + limit - 1) // limit if total else 0
    return {"items": items}, {"page": page, "limit": limit, "total": total, "pages": pages}


def roles_payload():
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            rows = conn.execute(
                """
                SELECT characterid, name, description, permissions, status, created_at, userids
                FROM characters
                ORDER BY created_at DESC, characterid ASC
                """
            ).fetchall()
        finally:
            conn.close()
    items = [
        {
            "id": row[0],
            "name": row[1],
            "description": row[2] or "",
            "permissions": row[3] or "[]",
            "status": int(row[4] or 1),
            "created_at": row[5] or "",
            "userids": row[6] or "[]",
        }
        for row in rows
    ]
    return {"items": items}


def get_role(conn, role_id):
    if not role_id:
        return None
    return conn.execute(
        "SELECT characterid, name FROM characters WHERE characterid = ? AND status = 1",
        (str(role_id),),
    ).fetchone()


def validate_user_payload(payload, require_all=True):
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


def audit(action, project_id=None, target_type="", target_id="", detail=None, conn=None):
    user = current_user_payload() or {}
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    payload = (
        int(project_id) if project_id not in (None, "") else None,
        user.get("userid") or "",
        user.get("username") or "",
        action,
        target_type or "",
        str(target_id or ""),
        json.dumps(detail or {}, ensure_ascii=False, separators=(",", ":")),
        ts,
    )

    def write(c):
        c.execute(
            """
            INSERT INTO audit_logs
                (project_id, user_id, username, action, target_type, target_id, detail_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )

    if conn is not None:
        write(conn)
        return
    with dm._db_lock:
        c = dm._get_conn()
        try:
            with c:
                write(c)
        finally:
            c.close()


def project_exists(project_id):
    return any(int(p["id"]) == int(project_id) for p in dm.list_projects())


def project_detail(project_id):
    projects = dm.list_projects()
    project = next((p for p in projects if int(p["id"]) == int(project_id)), None)
    if not project:
        return None
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            rows = conn.execute(
                """
                SELECT id, type, local_path, store_name
                FROM project_files
                WHERE project_id = ?
                ORDER BY id ASC
                """,
                (int(project_id),),
            ).fetchall()
        finally:
            conn.close()
    project["files"] = [
        {
            "id": row[0],
            "type": row[1],
            "local_path": row[2],
            "store_name": row[3] or "",
        }
        for row in rows
    ]
    project["latest_job"] = job_store.latest_project_progress(project_id)
    return project


@api_v1_bp.route("/auth/me", methods=["GET"])
def auth_me():
    user = current_user_payload()
    if not user:
        return fail("未登录，请先登录", "unauthorized", 401)
    return ok(user)


@api_v1_bp.route("/users", methods=["GET"])
def users_list():
    data, meta = users_payload()
    return ok(data, meta=meta)


@api_v1_bp.route("/users", methods=["POST"])
def users_create():
    payload = request.get_json(silent=True) or {}
    data, err = validate_user_payload(payload)
    if err:
        return fail(err)
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            role = get_role(conn, data["role_id"])
            if not role:
                return fail("所属角色不存在")
            exists = conn.execute("SELECT userid FROM \"user\" WHERE phone = ?", (data["phone"],)).fetchone()
            if exists:
                return fail("手机号已存在")
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
                audit("user.create", target_type="user", target_id=userid, detail={"username": data["username"]}, conn=conn)
        finally:
            conn.close()
    return ok({"userid": userid, "message": "用户创建成功"}, status_code=201)


@api_v1_bp.route("/users/<userid>", methods=["PUT"])
def users_update(userid):
    payload = request.get_json(silent=True) or {}
    data, err = validate_user_payload(payload)
    if err:
        return fail(err)
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            role = get_role(conn, data["role_id"])
            if not role:
                return fail("所属角色不存在")
            current = conn.execute("SELECT userid FROM \"user\" WHERE userid = ?", (userid,)).fetchone()
            if not current:
                return fail("用户不存在", "not_found", 404)
            exists = conn.execute(
                "SELECT userid FROM \"user\" WHERE phone = ? AND userid <> ?",
                (data["phone"], userid),
            ).fetchone()
            if exists:
                return fail("手机号已存在")
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
                audit("user.update", target_type="user", target_id=userid, detail={"username": data["username"]}, conn=conn)
        finally:
            conn.close()
    return ok({"message": "用户更新成功"})


@api_v1_bp.route("/users/<userid>/freeze", methods=["POST"])
def users_freeze(userid):
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            current = conn.execute("SELECT userid FROM \"user\" WHERE userid = ?", (userid,)).fetchone()
            if not current:
                return fail("用户不存在", "not_found", 404)
            with conn:
                conn.execute("UPDATE \"user\" SET status = 2 WHERE userid = ?", (userid,))
                audit("user.freeze", target_type="user", target_id=userid, conn=conn)
        finally:
            conn.close()
    return ok({"message": "用户已冻结"})


@api_v1_bp.route("/users/<userid>/unfreeze", methods=["POST"])
def users_unfreeze(userid):
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            current = conn.execute("SELECT userid FROM \"user\" WHERE userid = ?", (userid,)).fetchone()
            if not current:
                return fail("用户不存在", "not_found", 404)
            with conn:
                conn.execute("UPDATE \"user\" SET status = 1 WHERE userid = ?", (userid,))
                audit("user.unfreeze", target_type="user", target_id=userid, conn=conn)
        finally:
            conn.close()
    return ok({"message": "用户已解冻"})


@api_v1_bp.route("/roles", methods=["GET"])
def roles_list():
    return ok(roles_payload())


@api_v1_bp.route("/roles", methods=["POST"])
def roles_create():
    payload = request.get_json(silent=True) or {}
    name = (payload.get("name") or "").strip()
    description = (payload.get("description") or "").strip()
    permissions = payload.get("permissions") or []
    if not name:
        return fail("角色名不能为空")
    characterid = uuid.uuid4().hex
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            exists = conn.execute("SELECT characterid FROM characters WHERE name = ?", (name,)).fetchone()
            if exists:
                return fail("角色名已存在")
            with conn:
                conn.execute(
                    """
                    INSERT INTO characters (characterid, name, description, permissions, status, created_at)
                    VALUES (?, ?, ?, ?, 1, ?)
                    """,
                    (characterid, name, description, json.dumps(permissions, ensure_ascii=False), now),
                )
                audit("role.create", target_type="role", target_id=characterid, detail={"name": name}, conn=conn)
        finally:
            conn.close()
    return ok({"id": characterid, "message": "角色创建成功"}, status_code=201)


@api_v1_bp.route("/roles/<role_id>", methods=["PUT"])
def roles_update(role_id):
    payload = request.get_json(silent=True) or {}
    name = (payload.get("name") or "").strip()
    description = (payload.get("description") or "").strip()
    permissions = payload.get("permissions") or []
    if not name:
        return fail("角色名不能为空")
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            current = conn.execute("SELECT characterid, name FROM characters WHERE characterid = ?", (role_id,)).fetchone()
            if not current:
                return fail("角色不存在", "not_found", 404)
            exists = conn.execute("SELECT characterid FROM characters WHERE name = ? AND characterid <> ?", (name, role_id)).fetchone()
            if exists:
                return fail("角色名已存在")
            with conn:
                conn.execute(
                    """
                    UPDATE characters
                    SET name = ?, description = ?, permissions = ?
                    WHERE characterid = ?
                    """,
                    (name, description, json.dumps(permissions, ensure_ascii=False), role_id),
                )
                if current[1] != name:
                    conn.execute("UPDATE \"user\" SET role_name = ? WHERE role_id = ?", (name, role_id))
                audit("role.update", target_type="role", target_id=role_id, detail={"name": name}, conn=conn)
        finally:
            conn.close()
    return ok({"message": "角色更新成功"})


@api_v1_bp.route("/roles/<role_id>/freeze", methods=["POST"])
def roles_freeze(role_id):
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            current = conn.execute("SELECT characterid FROM characters WHERE characterid = ?", (role_id,)).fetchone()
            if not current:
                return fail("角色不存在", "not_found", 404)
            with conn:
                conn.execute("UPDATE characters SET status = 2 WHERE characterid = ?", (role_id,))
                audit("role.freeze", target_type="role", target_id=role_id, conn=conn)
        finally:
            conn.close()
    return ok({"message": "角色已冻结"})


@api_v1_bp.route("/roles/<role_id>/unfreeze", methods=["POST"])
def roles_unfreeze(role_id):
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            current = conn.execute("SELECT characterid FROM characters WHERE characterid = ?", (role_id,)).fetchone()
            if not current:
                return fail("角色不存在", "not_found", 404)
            with conn:
                conn.execute("UPDATE characters SET status = 1 WHERE characterid = ?", (role_id,))
                audit("role.unfreeze", target_type="role", target_id=role_id, conn=conn)
        finally:
            conn.close()
    return ok({"message": "角色已解冻"})


@api_v1_bp.route("/audit-logs", methods=["GET"])
def audit_logs_list():
    page, limit = parse_pagination(default_limit=20, max_limit=100)
    project_id = request.args.get("project_id")
    action = (request.args.get("action") or "").strip()
    where = []
    params = []
    if project_id not in (None, ""):
        try:
            where.append("project_id = ?")
            params.append(int(project_id))
        except (TypeError, ValueError):
            return fail("project_id 无效")
    if action:
        where.append("action = ?")
        params.append(action)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    offset = (page - 1) * limit
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            total = conn.execute(f"SELECT COUNT(*) FROM audit_logs {where_sql}", params).fetchone()[0]
            rows = conn.execute(
                f"""
                SELECT id, project_id, user_id, username, action, target_type, target_id, detail_json, created_at
                FROM audit_logs
                {where_sql}
                ORDER BY created_at DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                params + [limit, offset],
            ).fetchall()
        finally:
            conn.close()
    items = []
    for row in rows:
        try:
            detail = json.loads(row[7] or "{}")
        except Exception:
            detail = {}
        items.append({
            "id": row[0],
            "project_id": row[1],
            "user_id": row[2] or "",
            "username": row[3] or "",
            "action": row[4],
            "target_type": row[5] or "",
            "target_id": row[6] or "",
            "detail": detail,
            "created_at": row[8],
        })
    pages = (total + limit - 1) // limit if total else 0
    return ok({"items": items}, meta={"page": page, "limit": limit, "total": total, "pages": pages})


@api_v1_bp.route("/projects", methods=["GET"])
def projects_list():
    page, limit = parse_pagination()
    items = dm.list_projects()
    allowed_ids = accessible_project_ids()
    if allowed_ids is not None:
        items = [p for p in items if int(p.get("id") or 0) in allowed_ids]
    status_filter = (request.args.get("status") or "").strip()
    if status_filter:
        items = [p for p in items if str(p.get("status") or "") == status_filter]
    paged, meta = paginate_items(items, page, limit)
    return ok({"items": paged}, meta=meta)


@api_v1_bp.route("/projects", methods=["POST"])
def projects_create():
    return legacy_response_to_v1(app_data_projects.handle_projects(), success_code="created")


@api_v1_bp.route("/projects/<int:project_id>", methods=["GET"])
def projects_get(project_id):
    project = project_detail(project_id)
    if not project:
        return fail("项目不存在", "not_found", 404)
    return ok(project)


@api_v1_bp.route("/projects/<int:project_id>/members", methods=["GET"])
def project_members_list(project_id):
    if not project_exists(project_id):
        return fail("项目不存在", "not_found", 404)
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            rows = conn.execute(
                """
                SELECT pm.id, pm.project_id, pm.user_id, COALESCE(u.username, ''), pm.role, pm.created_at
                FROM project_members pm
                LEFT JOIN "user" u ON u.userid = pm.user_id
                WHERE pm.project_id = ?
                ORDER BY pm.created_at DESC, pm.id DESC
                """,
                (project_id,),
            ).fetchall()
        finally:
            conn.close()
    return ok({"items": [
        {
            "id": row[0],
            "project_id": row[1],
            "user_id": row[2],
            "username": row[3],
            "role": row[4],
            "created_at": row[5],
        }
        for row in rows
    ]})


@api_v1_bp.route("/projects/<int:project_id>/members", methods=["POST"])
def project_members_add(project_id):
    if not project_exists(project_id):
        return fail("项目不存在", "not_found", 404)
    payload = request.get_json(silent=True) or {}
    user_id = (payload.get("user_id") or "").strip()
    role = (payload.get("role") or "viewer").strip()
    if not user_id:
        return fail("请选择用户")
    if role not in PROJECT_MEMBER_ROLES:
        return fail("项目角色无效")
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            user = conn.execute("SELECT userid FROM \"user\" WHERE userid = ?", (user_id,)).fetchone()
            if not user:
                return fail("用户不存在", "not_found", 404)
            with conn:
                conn.execute(
                    """
                    INSERT INTO project_members (project_id, user_id, role, created_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (project_id, user_id)
                    DO UPDATE SET role = EXCLUDED.role
                    """,
                    (project_id, user_id, role, now),
                )
                audit(
                    "project_member.upsert",
                    project_id=project_id,
                    target_type="project_member",
                    target_id=user_id,
                    detail={"role": role},
                    conn=conn,
                )
        finally:
            conn.close()
    return ok({"project_id": project_id, "user_id": user_id, "role": role}, status_code=201)


@api_v1_bp.route("/projects/<int:project_id>/members/<user_id>", methods=["DELETE"])
def project_members_remove(project_id, user_id):
    if not project_exists(project_id):
        return fail("项目不存在", "not_found", 404)
    with dm._db_lock:
        conn = dm._get_conn()
        try:
            with conn:
                cur = conn.execute(
                    "DELETE FROM project_members WHERE project_id = ? AND user_id = ?",
                    (project_id, user_id),
                )
                if cur.rowcount == 0:
                    return fail("项目成员不存在", "not_found", 404)
                audit(
                    "project_member.remove",
                    project_id=project_id,
                    target_type="project_member",
                    target_id=user_id,
                    conn=conn,
                )
        finally:
            conn.close()
    return ok({"project_id": project_id, "user_id": user_id})


@api_v1_bp.route("/projects/<int:project_id>/activate", methods=["POST"])
def projects_activate(project_id):
    project = project_detail(project_id)
    if not project:
        return fail("项目不存在", "not_found", 404)
    if project.get("status") == "analyzing":
        return fail("该项目正在分析中，请等待完成", "project_busy", 409)
    if project.get("status") == "creating":
        return fail("该项目正在创建中，请等待完成", "project_busy", 409)
    if project.get("status") == "failed":
        return fail("该项目分析失败，请删除后重新创建", "project_failed", 409)
    if job_store.project_has_active_job(project_id, ["source_import", "analysis", "manual_import"]):
        return fail("该项目已有任务正在排队或执行，请等待完成", "project_busy", 409)
    dm.activate_project(project_id, skip_load=True)
    return ok({"project_id": project_id, "active": True})


@api_v1_bp.route("/projects/<int:project_id>/preflight", methods=["POST"])
def projects_preflight(project_id):
    return legacy_response_to_v1(app_data_projects.preflight_project(project_id))


@api_v1_bp.route("/projects/<int:project_id>/analysis", methods=["POST"])
def projects_analyze(project_id):
    return legacy_response_to_v1(app_data_projects.analyze_project(project_id))


@api_v1_bp.route("/projects/<int:project_id>/jobs/latest", methods=["GET"])
def projects_latest_job(project_id):
    project = project_detail(project_id)
    if not project:
        return fail("项目不存在", "not_found", 404)
    return ok(job_store.latest_project_progress(project_id))


@api_v1_bp.route("/jobs/project/<int:project_id>/latest", methods=["GET"])
def jobs_project_latest(project_id):
    return projects_latest_job(project_id)


@api_v1_bp.route("/products/grid", methods=["GET"])
def products_grid():
    return legacy_response_to_v1(app_data_grid.get_grid_data())


@api_v1_bp.route("/products/main", methods=["GET"])
def products_main():
    return legacy_response_to_v1(app_data_grid.get_main_products())


@api_v1_bp.route("/products/stores/<store_id>", methods=["GET"])
def products_store(store_id):
    return legacy_response_to_v1(app_data_grid.get_store_products(store_id))


@api_v1_bp.route("/products/unlinked", methods=["GET"])
def products_unlinked():
    return legacy_response_to_v1(app_data_grid.get_unlinked_items())


@api_v1_bp.route("/products/main/<path:main_sku_id>/links", methods=["GET"])
def products_main_links(main_sku_id):
    return legacy_response_to_v1(app_data_grid.get_main_product_links(main_sku_id))


@api_v1_bp.route("/products/main/<path:main_sku_id>/match-explain/<store_id>", methods=["GET"])
def products_match_explain(main_sku_id, store_id):
    return legacy_response_to_v1(app_data_grid.get_main_product_match_explain(main_sku_id, store_id))


@api_v1_bp.route("/statistics", methods=["GET"])
def statistics_get():
    return legacy_response_to_v1(app_data_grid.get_statistics())


@api_v1_bp.route("/statistics/snapshot-status", methods=["GET"])
def statistics_snapshot_status():
    return legacy_response_to_v1(app_data_grid.get_statistics_snapshot_status())


@api_v1_bp.route("/statistics/products", methods=["GET"])
def statistics_products():
    return legacy_response_to_v1(app_data_grid.get_statistics_products())


@api_v1_bp.route("/market-analysis", methods=["GET"])
def market_analysis_get():
    return legacy_response_to_v1(app_data_grid.get_market_analysis())


@api_v1_bp.route("/products/eliminate", methods=["POST"])
def products_eliminate():
    data = request.get_json(silent=True) or {}
    return audited_legacy(
        app_data_grid.eliminate(),
        "product.eliminate",
        target_type="main_product",
        target_id=data.get("main_sku_id"),
        detail={"status": data.get("status")},
    )


@api_v1_bp.route("/products/handled", methods=["POST"])
def products_handled():
    data = request.get_json(silent=True) or {}
    return audited_legacy(
        app_data_grid.toggle_handled(),
        "product.handled",
        target_type="main_product",
        target_id=data.get("main_sku_id"),
        detail={"handled": data.get("handled")},
    )


@api_v1_bp.route("/products/ref", methods=["POST"])
def products_ref():
    data = request.get_json(silent=True) or {}
    return audited_legacy(
        app_data_grid.toggle_ref(),
        "product.ref",
        target_type="main_product",
        target_id=data.get("main_sku_id"),
        detail={"field": data.get("field"), "store_id": data.get("store_id")},
    )


@api_v1_bp.route("/products/cell", methods=["PATCH", "POST"])
def products_update_cell():
    data = request.get_json(silent=True) or {}
    return audited_legacy(
        app_data_grid.update_cell(),
        "product.cell_update",
        target_type="main_product",
        target_id=data.get("main_sku_id"),
        detail={"column": data.get("column")},
    )


@api_v1_bp.route("/links/new-flag", methods=["POST"])
def links_new_flag():
    data = request.get_json(silent=True) or {}
    return audited_legacy(
        app_data_grid.toggle_add(),
        "link.new_flag",
        target_type="comp_product",
        target_id=data.get("sku_id"),
        detail={"store_id": data.get("store_id"), "is_new": data.get("is_new")},
    )


@api_v1_bp.route("/links/ignore", methods=["POST"])
def links_ignore():
    data = request.get_json(silent=True) or {}
    return audited_legacy(
        app_data_grid.toggle_ignore(),
        "link.ignore",
        target_type="comp_product",
        target_id=data.get("sku_id"),
        detail={"store_id": data.get("store_id"), "is_ignored": data.get("is_ignored")},
    )


@api_v1_bp.route("/links/price-match", methods=["POST"])
def links_price_match():
    data = request.get_json(silent=True) or {}
    return audited_legacy(
        app_data_grid.price_match(),
        "link.price_match",
        target_type="main_product",
        target_id=data.get("main_sku_id"),
        detail={"store_id": data.get("store_id")},
    )


@api_v1_bp.route("/links/price-match/clear", methods=["POST"])
def links_clear_price_match():
    data = request.get_json(silent=True) or {}
    return audited_legacy(
        app_data_grid.clear_price_match(),
        "link.clear_price_match",
        target_type="main_product",
        target_id=data.get("main_sku_id"),
    )


@api_v1_bp.route("/links/manual", methods=["POST"])
def links_manual():
    data = request.get_json(silent=True) or {}
    return audited_legacy(
        app_data_grid.manual_link(),
        "link.manual",
        target_type="main_product",
        target_id=data.get("main_sku_id"),
        detail={"store_id": data.get("store_id"), "comp_sku_id": data.get("comp_sku_id")},
    )


@api_v1_bp.route("/links/unlink", methods=["POST"])
def links_unlink():
    data = request.get_json(silent=True) or {}
    return audited_legacy(
        app_data_grid.unlink(),
        "link.unlink",
        target_type="main_product",
        target_id=data.get("main_sku_id"),
        detail={"store_id": data.get("store_id")},
    )


@api_v1_bp.route("/rule-templates", methods=["GET", "POST"])
def rule_templates():
    result = app_data_rules.api_rule_templates()
    if request.method == "POST":
        return audited_legacy(result, "rule_template.create", target_type="rule_template")
    return legacy_response_to_v1(result)


@api_v1_bp.route("/rule-templates/<int:template_id>", methods=["GET", "PUT", "DELETE"])
def rule_template_one(template_id):
    result = app_data_rules.api_rule_template_one(template_id)
    if request.method == "PUT":
        return audited_legacy(result, "rule_template.update", target_type="rule_template", target_id=template_id)
    if request.method == "DELETE":
        return audited_legacy(result, "rule_template.delete", target_type="rule_template", target_id=template_id)
    return legacy_response_to_v1(result)


@api_v1_bp.route("/rule-categories/parse", methods=["POST"])
def rule_categories_parse():
    return legacy_response_to_v1(app_data_rules.api_rule_categories_parse())


@api_v1_bp.route("/rule-categories/default", methods=["GET"])
def rule_categories_default():
    return legacy_response_to_v1(app_data_rules.api_rule_categories_default())


@api_v1_bp.route("/rule-categories/bucket-tags", methods=["GET"])
def rule_categories_bucket_tags():
    return legacy_response_to_v1(app_data_rules.api_rule_categories_bucket_tags())


@api_v1_bp.route("/match-agent/stores", methods=["GET"])
def match_agent_stores():
    return legacy_response_to_v1(app_data_match_agent.api_match_agent_stores())


@api_v1_bp.route("/match-agent/cases", methods=["GET", "POST"])
def match_agent_cases():
    result = app_data_match_agent.api_match_agent_cases()
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        return audited_legacy(
            result,
            "match_agent.case.create",
            project_id=data.get("project_id") or dm.active_project_id,
            target_type="match_feedback_case",
        )
    return legacy_response_to_v1(result)


@api_v1_bp.route("/match-agent/cases/import", methods=["POST"])
def match_agent_cases_import():
    return audited_legacy(
        app_data_match_agent.api_match_agent_cases_import(),
        "match_agent.case.import",
        project_id=request.form.get("project_id") or dm.active_project_id,
        target_type="match_feedback_case",
    )


@api_v1_bp.route("/match-agent/runs", methods=["GET", "POST"])
def match_agent_runs():
    result = app_data_match_agent.api_match_agent_runs()
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        return audited_legacy(
            result,
            "match_agent.run",
            project_id=data.get("project_id") or dm.active_project_id,
            target_type="match_agent_run",
        )
    return legacy_response_to_v1(result)


@api_v1_bp.route("/match-agent/quick-run", methods=["POST"])
def match_agent_quick_run():
    data = request.get_json(silent=True) or {}
    return audited_legacy(
        app_data_match_agent.api_match_agent_quick_run(),
        "match_agent.quick_run",
        project_id=data.get("project_id") or dm.active_project_id,
        target_type="match_agent_run",
    )


@api_v1_bp.route("/match-agent/runs/<int:run_id>", methods=["GET"])
def match_agent_run(run_id):
    return legacy_response_to_v1(app_data_match_agent.api_match_agent_run(run_id))


@api_v1_bp.route("/match-agent/runs/<int:run_id>/publish-rule-template", methods=["POST"])
def match_agent_publish_rule_template(run_id):
    return audited_legacy(
        app_data_match_agent.api_match_agent_publish_rule_template(run_id),
        "match_agent.publish_rule_template",
        target_type="match_agent_run",
        target_id=run_id,
    )


@api_v1_bp.route("/match-agent/runs/<int:run_id>/apply", methods=["POST"])
def match_agent_apply(run_id):
    return audited_legacy(
        app_data_match_agent.api_match_agent_apply(run_id),
        "match_agent.apply",
        target_type="match_agent_run",
        target_id=run_id,
    )


@api_v1_bp.route("/match-agent/runs/<int:run_id>/apply-to-v2", methods=["POST"])
def match_agent_apply_to_v2(run_id):
    return audited_legacy(
        app_data_match_agent.api_match_agent_apply_to_v2(run_id),
        "match_agent.apply_to_v2",
        target_type="match_agent_run",
        target_id=run_id,
    )
