# -*- coding: utf-8 -*-
import json
import time
import sqlite3
from typing import List, Optional, Tuple, Any, Dict

import post_match_engine as _pme

class DataManagerRuleTemplateMixin:
    def list_rule_templates(self) -> List[Dict[str, Any]]:
        with self._db_lock:
            conn = self._get_conn()
            try:
                cur = conn.execute(
                    "SELECT id, name, description, config_json, created_at FROM rule_templates ORDER BY id ASC"
                )
                items = []
                for r in cur.fetchall():
                    cfg = _pme.template_from_db_json(r[3] or "{}")
                    summary = _pme.summarize_template(cfg)
                    items.append({
                        "id": r[0],
                        "name": r[1],
                        "description": r[2] or "",
                        "config_json": r[3] or "{}",
                        "created_at": r[4] or "",
                        "group_count": summary["group_count"],
                        "category3_count": summary["category3_count"],
                        "enabled_metric_total": summary["enabled_metric_total"],
                    })
                return items
            finally:
                conn.close()

    def get_rule_template(self, template_id: int) -> Optional[Dict[str, Any]]:
        with self._db_lock:
            conn = self._get_conn()
            try:
                cur = conn.execute(
                    "SELECT id, name, description, config_json, created_at, updated_at FROM rule_templates WHERE id = ?",
                    (template_id,),
                )
                r = cur.fetchone()
                if not r:
                    return None
                return {
                    "id": r[0],
                    "name": r[1],
                    "description": r[2] or "",
                    "config_json": r[3] or "{}",
                    "created_at": r[4] or "",
                    "updated_at": r[5] or "",
                    "config": _pme.template_from_db_json(r[3] or "{}"),
                }
            finally:
                conn.close()

    def get_post_match_template_for_project(self, project_id: int) -> Dict[str, Any]:
        """供 run_analysis：dict 经 normalize 使用。"""
        tid = None
        with self._db_lock:
            conn = self._get_conn()
            try:
                cur = conn.execute(
                    "SELECT rule_template_id FROM projects WHERE id = ?", (project_id,)
                )
                row = cur.fetchone()
                if row and row[0] is not None:
                    tid = int(row[0])
            finally:
                conn.close()
        with self._db_lock:
            conn = self._get_conn()
            try:
                if not tid:
                    r0 = conn.execute("SELECT id FROM rule_templates ORDER BY id LIMIT 1").fetchone()
                    if r0:
                        tid = int(r0[0])
            finally:
                conn.close()
        if not tid:
            return _pme.get_builtin_default_template()
        t = self.get_rule_template(tid)
        if not t:
            return _pme.get_builtin_default_template()
        try:
            return _pme.template_from_db_json(t.get("config_json") or "")
        except Exception:
            return _pme.get_builtin_default_template()

    def create_rule_template(self, name: str, description: str, config: Dict[str, Any]) -> int:
        normalized = _pme.normalize_template(config)
        cfg = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    cur = conn.execute(
                        "INSERT INTO rule_templates (name, description, config_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                        ((name or "").strip() or "未命名", description or "", cfg, now, now),
                    )
                    return int(cur.lastrowid)
            finally:
                conn.close()

    def update_rule_template(self, template_id: int, name: str, description: str, config: Dict[str, Any]) -> bool:
        cfg = json.dumps(_pme.normalize_template(config), ensure_ascii=False, separators=(",", ":"))
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    cur = conn.execute(
                        "UPDATE rule_templates SET name=?, description=?, config_json=?, updated_at=? WHERE id=?",
                        ((name or "").strip() or "未命名", description or "", cfg, now, template_id),
                    )
                    return cur.rowcount > 0
            finally:
                conn.close()

    def delete_rule_template(self, template_id: int) -> Tuple[bool, Optional[str]]:
        with self._db_lock:
            conn = self._get_conn()
            try:
                n = conn.execute(
                    "SELECT COUNT(*) FROM projects WHERE rule_template_id = ?", (template_id,)
                ).fetchone()[0]
                if n > 0:
                    return False, "有项目正在使用该规则，无法删除"
                with conn:
                    cur = conn.execute("DELETE FROM rule_templates WHERE id = ?", (template_id,))
                    if cur.rowcount == 0:
                        return False, "规则不存在"
                return True, None
            finally:
                conn.close()
