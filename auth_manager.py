import re
import sqlite3
import time

from werkzeug.security import check_password_hash, generate_password_hash


DEFAULT_ADMIN_PHONES = ("13517296019", "17557283001")
DEFAULT_ADMIN_PHONE = DEFAULT_ADMIN_PHONES[0]
ROLE_ADMIN = "admin"
ROLE_MANAGER = "manager"
ROLE_MEMBER = "member"

ROLE_LABELS = {
    ROLE_ADMIN: "Admin",
    ROLE_MANAGER: "管理员",
    ROLE_MEMBER: "成员",
}
VALID_ROLES = tuple(ROLE_LABELS.keys())
MANAGE_ROLES = {ROLE_ADMIN, ROLE_MANAGER}

_PHONE_RE = re.compile(r"^1\d{10}$")


class AuthError(Exception):
    def __init__(self, code, message, status_code=400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


class AuthManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self._init_db()

    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _init_db(self):
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS auth_users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    phone TEXT NOT NULL UNIQUE,
                    role TEXT NOT NULL DEFAULT 'member',
                    password_hash TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    created_by INTEGER,
                    last_login_at TEXT,
                    is_active INTEGER NOT NULL DEFAULT 1
                )
                """
            )
            self._ensure_columns(conn)
            now = self._now()
            for phone in DEFAULT_ADMIN_PHONES:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO auth_users
                        (phone, role, password_hash, created_at, updated_at, is_active)
                    VALUES (?, ?, '', ?, ?, 1)
                    """,
                    (phone, ROLE_ADMIN, now, now),
                )
                conn.execute(
                    """
                    UPDATE auth_users
                    SET role = ?, is_active = 1, updated_at = ?
                    WHERE phone = ? AND role != ?
                    """,
                    (ROLE_ADMIN, now, phone, ROLE_ADMIN),
                )

    def _ensure_columns(self, conn):
        columns = {row[1] for row in conn.execute("PRAGMA table_info(auth_users)").fetchall()}
        additions = {
            "role": "ALTER TABLE auth_users ADD COLUMN role TEXT NOT NULL DEFAULT 'member'",
            "password_hash": "ALTER TABLE auth_users ADD COLUMN password_hash TEXT NOT NULL DEFAULT ''",
            "created_at": "ALTER TABLE auth_users ADD COLUMN created_at TEXT NOT NULL DEFAULT ''",
            "updated_at": "ALTER TABLE auth_users ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''",
            "created_by": "ALTER TABLE auth_users ADD COLUMN created_by INTEGER",
            "last_login_at": "ALTER TABLE auth_users ADD COLUMN last_login_at TEXT",
            "is_active": "ALTER TABLE auth_users ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1",
        }
        for name, sql in additions.items():
            if name not in columns:
                conn.execute(sql)

    def _now(self):
        return time.strftime("%Y-%m-%d %H:%M:%S")

    def _normalize_phone(self, phone):
        normalized = re.sub(r"\D", "", str(phone or ""))
        if not _PHONE_RE.match(normalized):
            raise AuthError("invalid_phone", "请输入有效的 11 位手机号")
        return normalized

    def _validate_password(self, password):
        if len(str(password or "")) < 6:
            raise AuthError("invalid_password", "密码至少需要 6 位")
        return str(password)

    def _validate_role(self, role):
        role = str(role or ROLE_MEMBER).strip().lower()
        if role not in VALID_ROLES:
            raise AuthError("invalid_role", "请选择有效权限")
        return role

    def _public_user(self, row):
        if row is None:
            return None
        role = row["role"] if row["role"] in ROLE_LABELS else ROLE_MEMBER
        return {
            "id": row["id"],
            "phone": row["phone"],
            "role": role,
            "role_label": ROLE_LABELS[role],
            "has_password": bool(row["password_hash"]),
            "created_at": row["created_at"] or "",
            "updated_at": row["updated_at"] or "",
            "created_by": row["created_by"],
            "last_login_at": row["last_login_at"] or "",
            "is_active": bool(row["is_active"]),
            "can_manage_users": role in MANAGE_ROLES,
        }

    def _user_role(self, user):
        if not user:
            return ""
        if isinstance(user, dict):
            return user.get("role", "")
        return str(user)

    def can_manage_users(self, user):
        return self._user_role(user) in MANAGE_ROLES

    def can_assign_role(self, actor, role):
        actor_role = self._user_role(actor)
        role = self._validate_role(role)
        if actor_role == ROLE_ADMIN:
            return True
        return actor_role == ROLE_MANAGER and role == ROLE_MEMBER

    def role_choices_for_actor(self, actor):
        if self._user_role(actor) == ROLE_ADMIN:
            roles = VALID_ROLES
        else:
            roles = (ROLE_MEMBER,)
        return [{"value": role, "label": ROLE_LABELS[role]} for role in roles]

    def get_user_by_phone(self, phone):
        phone = self._normalize_phone(phone)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM auth_users WHERE phone = ? LIMIT 1",
                (phone,),
            ).fetchone()
        return self._public_user(row)

    def get_user_by_id(self, user_id):
        try:
            uid = int(user_id)
        except (TypeError, ValueError):
            return None
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM auth_users WHERE id = ? LIMIT 1",
                (uid,),
            ).fetchone()
        return self._public_user(row)

    def list_users(self):
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM auth_users
                WHERE is_active = 1
                ORDER BY
                    CASE role WHEN 'admin' THEN 0 WHEN 'manager' THEN 1 ELSE 2 END,
                    id ASC
                """
            ).fetchall()
        return [self._public_user(row) for row in rows]

    def register(self, phone, password):
        phone = self._normalize_phone(phone)
        password = self._validate_password(password)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM auth_users WHERE phone = ? AND is_active = 1 LIMIT 1",
                (phone,),
            ).fetchone()
            if row is None:
                raise AuthError("phone_not_allowed", "手机号未开通，请联系管理员添加")
            if row["password_hash"]:
                raise AuthError("already_registered", "该手机号已注册，请直接登录")
            now = self._now()
            conn.execute(
                "UPDATE auth_users SET password_hash = ?, updated_at = ? WHERE id = ?",
                (generate_password_hash(password), now, row["id"]),
            )
            row = conn.execute("SELECT * FROM auth_users WHERE id = ?", (row["id"],)).fetchone()
        return self._public_user(row)

    def authenticate(self, phone, password):
        phone = self._normalize_phone(phone)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM auth_users WHERE phone = ? AND is_active = 1 LIMIT 1",
                (phone,),
            ).fetchone()
            if row is None:
                raise AuthError("invalid_credentials", "手机号或密码错误", 401)
            if not row["password_hash"]:
                raise AuthError("not_registered", "该手机号尚未注册，请先注册", 401)
            if not check_password_hash(row["password_hash"], str(password or "")):
                raise AuthError("invalid_credentials", "手机号或密码错误", 401)
            now = self._now()
            conn.execute("UPDATE auth_users SET last_login_at = ? WHERE id = ?", (now, row["id"]))
            row = conn.execute("SELECT * FROM auth_users WHERE id = ?", (row["id"],)).fetchone()
        return self._public_user(row)

    def add_allowed_user(self, actor, phone, role=ROLE_MEMBER):
        if not self.can_manage_users(actor):
            raise AuthError("permission_denied", "当前账号无权添加用户", 403)
        role = self._validate_role(role)
        if not self.can_assign_role(actor, role):
            raise AuthError("permission_denied", "当前账号无权分配该权限", 403)
        phone = self._normalize_phone(phone)
        actor_id = actor.get("id") if isinstance(actor, dict) else None
        now = self._now()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM auth_users WHERE phone = ? LIMIT 1",
                (phone,),
            ).fetchone()
            if row:
                existing_role = row["role"] if row["role"] in ROLE_LABELS else ROLE_MEMBER
                if phone in DEFAULT_ADMIN_PHONES and role != ROLE_ADMIN:
                    raise AuthError("permission_denied", "默认 Admin 账号不能调整为其他权限", 403)
                if self._user_role(actor) != ROLE_ADMIN and existing_role != ROLE_MEMBER:
                    raise AuthError("permission_denied", "当前账号无权调整该用户权限", 403)
                if existing_role != role and not self.can_assign_role(actor, role):
                    raise AuthError("permission_denied", "当前账号无权调整该用户权限", 403)
                conn.execute(
                    """
                    UPDATE auth_users
                    SET role = ?, is_active = 1, updated_at = ?
                    WHERE id = ?
                    """,
                    (role, now, row["id"]),
                )
                row = conn.execute("SELECT * FROM auth_users WHERE id = ?", (row["id"],)).fetchone()
            else:
                conn.execute(
                    """
                    INSERT INTO auth_users
                        (phone, role, password_hash, created_at, updated_at, created_by, is_active)
                    VALUES (?, ?, '', ?, ?, ?, 1)
                    """,
                    (phone, role, now, now, actor_id),
                )
                row = conn.execute(
                    "SELECT * FROM auth_users WHERE phone = ? LIMIT 1",
                    (phone,),
                ).fetchone()
        return self._public_user(row)
