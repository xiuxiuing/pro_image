# -*- coding: utf-8 -*-
import difflib
import json
import os
import re
import shutil
import time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

import post_match_engine
import match_agent_report


def _now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _norm(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() in ("nan", "none", "null") else s


def _json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def _json_loads(raw: str, default: Any) -> Any:
    try:
        data = json.loads(raw or "")
        return data if data is not None else default
    except Exception:
        return default


def _get(row: Optional[dict], key: str) -> str:
    return _norm((row or {}).get(key))


_A_FIELDS = [
    "A核心品类", "A单件净含量", "A售卖数量", "A包装单位", "A尺寸", "A多维尺寸",
    "A品牌", "A型号", "A商品形态", "A关键属性词", "A颜色",
]
_DEFAULT_GEMINI_MODEL = "gemini-2.5-flash"
_DEFAULT_DEEPSEEK_MODEL = "deepseek-v4-flash"
_DEEPSEEK_BASE_URL = "https://api.deepseek.com"
_PRODUCTION_RULE_V2_NAME = "生产规则V2"
_RULE_KEYS = (
    "core_conflict", "core", "cat3", "net", "sell", "pack", "size", "multidim_size",
    "product_form", "key_attributes", "color", "brand", "model",
)
_MODEL_NETWORK_ERROR_MARKERS = (
    "server disconnected without sending a response",
    "connection error",
    "connection reset",
    "connection refused",
    "remoteprotocolerror",
    "read timed out",
    "timeout",
    "timed out",
    "temporarily unavailable",
)


class DataManagerMatchAgentMixin:
    def list_match_agent_project_stores(self, project_id: Optional[int] = None) -> Dict[str, Any]:
        pid = int(project_id or self.active_project_id or 0)
        if not pid:
            return {"items": []}
        with self._db_lock:
            conn = self._get_conn()
            try:
                rows = conn.execute(
                    """
                    SELECT store_name
                    FROM project_files
                    WHERE project_id = ? AND type = 'comp'
                    ORDER BY id ASC
                    """,
                    (pid,),
                ).fetchall()
                items = [{"store_id": str(i), "store_name": _norm(row[0]) or f"竞店{i}"} for i, row in enumerate(rows)]
                if not items:
                    rows = conn.execute(
                        """
                        SELECT DISTINCT store_id
                        FROM comp_products
                        WHERE project_id = ?
                        ORDER BY CAST(store_id AS INTEGER), store_id
                        """,
                        (pid,),
                    ).fetchall()
                    items = [{"store_id": _norm(row[0]), "store_name": _norm(row[0]) or "竞店"} for row in rows]
                return {"items": items}
            finally:
                conn.close()

    def _match_agent_store_name(self, store_id: str, project_id: Optional[int] = None) -> str:
        sid = str(store_id or "")
        stores = self.list_match_agent_project_stores(project_id).get("items") or []
        for item in stores:
            if str(item.get("store_id")) == sid:
                return item.get("store_name") or sid or "竞店"
        return sid or "竞店"

    def _match_agent_current_link(self, conn, project_id: int, main_sku_id: str, store_id: str) -> Dict[str, Any]:
        row = conn.execute(
            """
            SELECT comp_sku_id, similarity, match_type
            FROM product_links
            WHERE project_id = ? AND main_sku_id = ? AND store_id = ?
            LIMIT 1
            """,
            (project_id, str(main_sku_id), str(store_id)),
        ).fetchone()
        if not row:
            return {"comp_sku_id": "", "similarity": "", "match_type": ""}
        return {"comp_sku_id": _norm(row[0]), "similarity": row[1], "match_type": _norm(row[2])}

    def _match_agent_get_main(self, conn, project_id: int, main_sku_id: str) -> Optional[dict]:
        df = pd.read_sql(
            "SELECT * FROM main_products WHERE project_id = ? AND skuId = ? LIMIT 1",
            conn,
            params=(project_id, str(main_sku_id)),
        )
        return None if df.empty else df.iloc[0].fillna("").to_dict()

    def _match_agent_get_comp(self, conn, project_id: int, store_id: str, comp_sku_id: str) -> Optional[dict]:
        df = pd.read_sql(
            "SELECT * FROM comp_products WHERE project_id = ? AND store_id = ? AND skuId = ? LIMIT 1",
            conn,
            params=(project_id, str(store_id), str(comp_sku_id)),
        )
        return None if df.empty else df.iloc[0].fillna("").to_dict()

    def _match_agent_text_rank(self, conn, project_id: int, store_id: str, main_item: dict, correct_sku: str) -> Tuple[int, float, List[Dict[str, Any]]]:
        df = pd.read_sql(
            "SELECT skuId, 商品名称, 规格名称, 美团类目三级 FROM comp_products WHERE project_id = ? AND store_id = ?",
            conn,
            params=(project_id, str(store_id)),
        )
        if df.empty:
            return 0, 0.0, []
        query = f"{_get(main_item, '美团类目三级')} {_get(main_item, '商品名称')} {_get(main_item, '规格名称')}"
        rows = []
        for _, r in df.fillna("").iterrows():
            text = f"{_norm(r.get('美团类目三级'))} {_norm(r.get('商品名称'))} {_norm(r.get('规格名称'))}"
            score = difflib.SequenceMatcher(None, query, text).ratio()
            rows.append({"skuId": _norm(r.get("skuId")), "score": round(score, 4), "name": _norm(r.get("商品名称"))})
        rows.sort(key=lambda x: x["score"], reverse=True)
        for idx, item in enumerate(rows, 1):
            if item["skuId"] == str(correct_sku):
                return idx, item["score"], rows[:10]
        return 0, 0.0, rows[:10]

    def _match_agent_a_field_diff(self, main: Optional[dict], wrong: Optional[dict], correct: Optional[dict]) -> List[Dict[str, Any]]:
        rows = []
        for field in _A_FIELDS:
            mv = _get(main, field)
            wv = _get(wrong, field)
            cv = _get(correct, field)
            if not mv and not cv:
                status = "主店与正确SKU均缺失"
            elif not mv:
                status = "主店缺失"
            elif not cv:
                status = "正确SKU缺失"
            elif mv == cv:
                status = "主店与正确SKU一致"
            else:
                status = "主店与正确SKU不同"
            rows.append({
                "field": field,
                "main": mv,
                "wrong": wv,
                "correct": cv,
                "main_vs_wrong": "same" if mv and mv == wv else ("missing" if not mv or not wv else "different"),
                "main_vs_correct": "same" if mv and mv == cv else ("missing" if not mv or not cv else "different"),
                "status": status,
            })
        return rows

    def _match_agent_text_vector_diff(self, main: Optional[dict], wrong: Optional[dict], correct: Optional[dict], project_id: int) -> Dict[str, Any]:
        def fallback_text(item: Optional[dict]) -> str:
            return f"[CAT1]={_get(item, '美团类目一级')}\n{_get(item, '规格名称')}, {_get(item, '商品名称')}".strip()

        out = {
            "status": "unavailable",
            "main_text": fallback_text(main),
            "wrong_text": fallback_text(wrong),
            "correct_text": fallback_text(correct),
            "main_wrong_score": None,
            "main_correct_score": None,
            "delta_correct_minus_wrong": None,
            "error": "",
        }
        if not main or not correct:
            out["error"] = "缺少主店或正确SKU商品数据"
            return out
        try:
            import numpy as np
            import main_030822

            match_cfg = {}
            with self._db_lock:
                conn2 = self._get_conn()
                try:
                    row = conn2.execute("SELECT COALESCE(match_config, '') FROM projects WHERE id = ?", (project_id,)).fetchone()
                finally:
                    conn2.close()
            if row and row[0]:
                try:
                    match_cfg = json.loads(row[0]) if isinstance(row[0], str) else {}
                except Exception:
                    match_cfg = {}

            out["main_text"] = main_030822._build_segmented_text(main, match_cfg)
            out["wrong_text"] = main_030822._build_segmented_text(wrong, match_cfg) if wrong else ""
            out["correct_text"] = main_030822._build_segmented_text(correct, match_cfg)
            vecs = main_030822.texts_to_embeddings([out["main_text"], out["wrong_text"], out["correct_text"]], batch_size=3)
            mv, wv, cv = vecs[0], vecs[1], vecs[2]
            if mv is None or cv is None:
                out["error"] = "BGE 文本向量生成失败"
                return out
            main_correct = float(np.dot(mv, cv))
            main_wrong = float(np.dot(mv, wv)) if wv is not None else None
            out.update({
                "status": "ok",
                "main_wrong_score": round(main_wrong, 6) if main_wrong is not None else None,
                "main_correct_score": round(main_correct, 6),
                "delta_correct_minus_wrong": round(main_correct - main_wrong, 6) if main_wrong is not None else None,
            })
            return out
        except Exception as e:
            out["error"] = f"{type(e).__name__}: {e}"
            return out

    def _match_agent_rule_diagnostics(self, template: dict, main: Optional[dict], wrong: Optional[dict], correct: Optional[dict]) -> Dict[str, Any]:
        block = post_match_engine.rules_for_item(template, main or {})
        group = post_match_engine.get_rule_group_for_item(template, main or {}) or {}
        return {
            "rule_group": group.get("name", ""),
            "wrong": post_match_engine.explain_post_match(main or {}, wrong or {}, block) if wrong else {"accepted": False, "reason": "错误SKU不存在", "metrics": []},
            "correct": post_match_engine.explain_post_match(main or {}, correct or {}, block) if correct else {"accepted": False, "reason": "正确SKU不存在", "metrics": []},
        }

    def _match_agent_blocked_metrics(self, explain: Dict[str, Any]) -> List[str]:
        return [
            str(m.get("key"))
            for m in (explain or {}).get("metrics") or []
            if m.get("enabled") and not m.get("passed")
        ]

    def _match_agent_trigger(self, diagnostic: Dict[str, Any]) -> Dict[str, Any]:
        vector = diagnostic.get("vector_diff") or {}
        text_correct = vector.get("main_correct_score")
        text_wrong = vector.get("main_wrong_score")
        image = diagnostic.get("image_vector_diff") or {}
        image_correct = image.get("main_correct_score")
        image_wrong = image.get("main_wrong_score")

        text_better = text_correct is not None and text_wrong is not None and float(text_correct) > float(text_wrong)
        image_better = image_correct is not None and image_wrong is not None and float(image_correct) > float(image_wrong)
        rule_diag = diagnostic.get("rule_diagnostics") or {}
        correct_explain = rule_diag.get("correct") or {}
        correct_blocked = correct_explain.get("accepted") is False
        blocked_metrics = self._match_agent_blocked_metrics(correct_explain)

        vector_better = bool(text_better or image_better)
        ok = bool(vector_better and correct_blocked)
        wrong_explain = (rule_diag.get("wrong") or {})
        wrong_blocked = wrong_explain.get("accepted") is False
        if ok:
            reason = "正确SKU向量分更高且被规则模板拦截，建议进入规则模板优化评估"
            core_reason = reason
        elif vector_better:
            reason = "正确SKU向量分更高，但未被当前规则模板拦截，本次不建议只通过规则模板优化"
            core_reason = "非规则拦截原因：正确SKU向量分高于错误SKU，但规则模板未拦截正确SKU；请排查图片向量、候选召回/TopK、历史或人工关联、导入结果覆盖等因素"
        elif correct_blocked:
            reason = "正确SKU被规则模板拦截，但向量分未高于错误SKU，本次不建议自动优化规则模板"
            core_reason = "弱向量证据：正确SKU被规则模板拦截，但正确SKU向量分未高于错误SKU"
        else:
            reason = "正确SKU向量分未高于错误SKU，且未被规则模板拦截，本次不建议修改规则模板"
            core_reason = "非规则模板问题：正确SKU向量分未高于错误SKU，且规则模板未拦截正确SKU"
        return {
            "triggered": ok,
            "reason": reason,
            "core_reason": core_reason,
            "text_correct_better": text_better,
            "image_correct_better": image_better,
            "correct_rule_blocked": correct_blocked,
            "wrong_rule_blocked": wrong_blocked,
            "blocked_metrics": blocked_metrics,
        }

    def _match_agent_diagnosis_summary(self, diagnostic: Dict[str, Any]) -> Tuple[str, str]:
        trigger = diagnostic.get("trigger") or {}
        vector = diagnostic.get("vector_diff") or {}
        rank = int(diagnostic.get("candidate_rank") or 0)
        correct_rule_blocked = bool(trigger.get("correct_rule_blocked"))
        blocked_metrics = trigger.get("blocked_metrics") or []
        text_correct = vector.get("main_correct_score")
        text_wrong = vector.get("main_wrong_score")
        try:
            vector_better = text_correct is not None and text_wrong is not None and float(text_correct) > float(text_wrong)
        except (TypeError, ValueError):
            vector_better = False

        if correct_rule_blocked:
            detail = "、".join(blocked_metrics) if blocked_metrics else "规则指标"
            return (
                "三级类目规则过滤",
                f"正确SKU进入候选后被后验规则拦截（拦截维度：{detail}），因此原结果未选中；放宽/调整规则后才可能匹配上。",
            )
        if rank == 0:
            return (
                "向量召回/TopK未命中",
                "正确SKU未进入当前文本候选TopK，说明不是三级类目规则过滤导致；原流程在向量召回/候选阶段就没有把它作为可选项。",
            )
        if vector_better:
            return (
                "候选排序/融合问题",
                f"正确SKU文本分高于错误SKU且未被规则拦截，但原结果仍未选中；更可能是图片向量、图文融合排序、历史/人工关联或导入覆盖影响，文本候选排名第 {rank}。",
            )
        return (
            "向量排序证据不足",
            f"正确SKU未被规则拦截，但文本分未高于错误SKU；原流程更可能在向量排序/融合阶段选择了错误SKU，文本候选排名第 {rank or '未进入TopK'}。",
        )

    def _match_agent_diagnose_case(self, conn, case: dict, template: dict) -> Dict[str, Any]:
        project_id = int(case.get("project_id") or self.active_project_id)
        main_sku = _norm(case.get("main_sku_id"))
        store_id = _norm(case.get("store_id"))
        correct_sku = _norm(case.get("correct_comp_sku_id"))
        main = self._match_agent_get_main(conn, project_id, main_sku)
        correct = self._match_agent_get_comp(conn, project_id, store_id, correct_sku)
        current_sku = _norm(case.get("current_comp_sku_id"))
        current = self._match_agent_get_comp(conn, project_id, store_id, current_sku) if current_sku else None
        rank, score, top10 = self._match_agent_text_rank(conn, project_id, store_id, main or {}, correct_sku) if main else (0, 0.0, [])
        category3 = _get(main, "美团类目三级") or _get(correct, "美团类目三级")
        reason = "当前无匹配" if not current_sku else "当前匹配与反馈正确 SKU 不一致"
        if current and correct and _get(current, "美团类目三级") != _get(correct, "美团类目三级"):
            reason = "当前匹配三级类目不同"
        result = {
            "id": case.get("id"),
            "project_id": project_id,
            "main_sku_id": main_sku,
            "store_id": store_id,
            "store_name": self._match_agent_store_name(store_id, project_id),
            "correct_comp_sku_id": correct_sku,
            "current_comp_sku_id": current_sku,
            "category3": category3,
            "candidate_rank": rank,
            "text_score": score,
            "top10": top10,
            "rule_reason": "候选未进入当前匹配结果" if rank == 0 else f"文本候选排名第 {rank}",
            "reason": reason,
            "a_field_diff": self._match_agent_a_field_diff(main, current, correct),
            "vector_diff": self._match_agent_text_vector_diff(main, current, correct, project_id),
            "image_vector_diff": {"status": "unavailable", "reason": "首版未持久化可复用图片向量明细"},
            "rule_diagnostics": self._match_agent_rule_diagnostics(template, main, current, correct),
            "main_item": {k: _get(main, k) for k in ["skuId", "商品名称", "规格名称", "美团类目一级", "美团类目二级", "美团类目三级"] + _A_FIELDS},
            "wrong_item": {k: _get(current, k) for k in ["skuId", "商品名称", "规格名称", "美团类目一级", "美团类目二级", "美团类目三级"] + _A_FIELDS},
            "correct_item": {k: _get(correct, k) for k in ["skuId", "商品名称", "规格名称", "美团类目一级", "美团类目二级", "美团类目三级"] + _A_FIELDS},
        }
        result["trigger"] = self._match_agent_trigger(result)
        result["diagnosis_type"], result["core_reason"] = self._match_agent_diagnosis_summary(result)
        return result

    def _match_agent_template_excerpt(self, template: dict, diagnostics: List[Dict[str, Any]]) -> Dict[str, Any]:
        categories = {d.get("category3") for d in diagnostics if d.get("category3")}
        groups = []
        for group in (post_match_engine.normalize_template(template).get("rule_groups") or []):
            l3 = group.get("categories", {}).get("l3") or []
            if categories.intersection(l3):
                groups.append(group)
        return {"v": 3, "rule_groups": groups[:10]}

    def _match_agent_l1_category_paths(self, conn, project_id: int, l1: str) -> List[Dict[str, str]]:
        paths: List[Dict[str, str]] = []
        seen = set()
        for table in ("main_products", "comp_products"):
            rows = conn.execute(
                f"""
                SELECT DISTINCT COALESCE(美团类目一级, ''), COALESCE(美团类目二级, ''), COALESCE(美团类目三级, '')
                FROM {table}
                WHERE project_id = ? AND trim(COALESCE(美团类目一级, '')) = ? AND trim(COALESCE(美团类目三级, '')) <> ''
                ORDER BY 1, 2, 3
                """,
                (project_id, l1),
            ).fetchall()
            for row in rows:
                item = {"l1": _norm(row[0]), "l2": _norm(row[1]), "l3": _norm(row[2])}
                key = (item["l1"], item["l2"], item["l3"])
                if key in seen:
                    continue
                seen.add(key)
                paths.append(item)
        return paths

    def _match_agent_build_prompt(self, diagnostics: List[Dict[str, Any]], template: dict, category_paths: List[Dict[str, str]]) -> str:
        context = {
            "task": "只围绕规则模板优化。基于反馈样本和当前规则模板，从候选三级类目中选择需要按三级类目维度调整规则的类目，并给出每个三级类目的新规则指标。",
            "requirements": {
                "output_language": "zh-CN",
                "scope": "只修改规则模板，不建议修改A字段、向量拼串或商品数据。",
                "do_not": [
                    "不要输出 markdown",
                    "不要省略 JSON 字段",
                    "不要返回规则组级别建议，必须按三级类目返回",
                    "不要建议将 A品牌/A型号/A商品形态/A关键属性词/A颜色作为硬拦截；这些弱字段只用于归一和排序",
                ],
                "metric_policy": {
                    "strong_constraints": ["core_conflict", "category_gate", "core", "cat3", "net", "sell", "pack", "size", "multidim_size"],
                    "weak_ranking_only": ["brand", "model", "product_form", "key_attributes", "color"],
                    "v2_category_policy": "category_gate 只按适合的规则组开启；模式固定为二级类目一致 OR A核心品类一致。不要建议全量开启 cat3 或 core 独立硬拦截。",
                },
            },
            "output_schema": {
                "rule_changes": [{
                    "category3": "string",
                    "reason": "string",
                    "risk": "low|medium|high",
                    "metrics": {
                        "core_conflict": {"en": True},
                        "category_gate": {"en": False, "mode": "cat2_or_core", "syn": []},
                        "core": {"en": False, "syn": []},
                        "cat3": {"en": False},
                        "net": {"en": True, "max_rel": 0.2},
                        "sell": {"en": True, "max_diff": 0.0},
                        "pack": {"en": True, "syn": []},
                        "size": {"en": True, "max_rel": 0.125},
                        "multidim_size": {"en": False, "max_rel": 0.125},
                        "brand": {"en": False, "syn": []},
                        "product_form": {"en": False, "syn": []},
                        "key_attributes": {"en": False, "syn": []},
                        "color": {"en": False, "syn": []},
                        "model": {"en": False, "syn": []}
                    }
                }],
                "summary": "string"
            },
            "candidate_category_paths": category_paths,
            "current_rule_template": post_match_engine.normalize_template(template),
            "diagnostics": diagnostics,
        }
        return "请只返回一个 JSON 对象，字段必须符合 output_schema。\n" + _json_dumps(context)

    def _match_agent_strip_json_fences(self, text: str) -> str:
        s = (text or "").strip()
        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.IGNORECASE)
        s = re.sub(r"\s*```$", "", s)
        return s.strip()

    def _match_agent_normalize_gemini_model(self, model_name: str) -> str:
        model = _norm(model_name) or _DEFAULT_GEMINI_MODEL
        if model.startswith("models/"):
            model = model[len("models/"):]
        return model

    def _match_agent_format_model_error(self, provider: str, model_name: str, exc: Exception) -> str:
        raw = _norm(exc) or exc.__class__.__name__
        low = raw.lower()
        provider_norm = _norm(provider).lower()
        if provider_norm == "deepseek":
            provider_label = "DeepSeek"
            default_model = _DEFAULT_DEEPSEEK_MODEL
        elif provider_norm in ("gpt", "openai"):
            provider_label = "OpenAI/GPT"
            default_model = "gpt-4.1"
        else:
            provider_label = "Gemini"
            default_model = _DEFAULT_GEMINI_MODEL
        model = _norm(model_name) or default_model
        if any(marker in low for marker in _MODEL_NETWORK_ERROR_MARKERS):
            return (
                f"{provider_label} 模型服务连接中断：{raw}。"
                f"请检查网络/代理、API Key 权限和模型名（当前：{model}），然后重试。"
            )
        if "api key" in low or "unauthorized" in low or "permission" in low or "401" in low or "403" in low:
            return f"{provider_label} 鉴权失败或无模型权限，请检查 API Key 和模型名（当前：{model}）：{raw}"
        if "not found" in low or "404" in low or "model" in low:
            return f"{provider_label} 模型名不可用或当前 Key 无权限访问（当前：{model}）：{raw}"
        return f"{provider_label} 模型调用失败（当前：{model}）：{raw}"

    def _match_agent_call_model(self, diagnostics: List[Dict[str, Any]], template: dict, provider: str, model_name: str, temperature: float, api_key: str = "", category_paths: Optional[List[Dict[str, str]]] = None) -> Dict[str, Any]:
        provider_norm = (_norm(provider) or "gemini").lower()
        prompt = self._match_agent_build_prompt(diagnostics, template, category_paths or [])
        if provider_norm == "deepseek":
            key = _norm(api_key) or _norm(os.environ.get("DEEPSEEK_API_KEY"))
            if not key:
                raise RuntimeError("请填写 DeepSeek API Key")
            model = _norm(model_name) or _DEFAULT_DEEPSEEK_MODEL
            try:
                from openai import OpenAI
                base_url = _norm(os.environ.get("DEEPSEEK_BASE_URL")) or _DEEPSEEK_BASE_URL
                client = OpenAI(api_key=key, base_url=base_url, timeout=120.0, max_retries=2)
                kwargs = {
                    "model": model,
                    "messages": [
                        {"role": "system", "content": "你是商品匹配规则优化专家。只输出 JSON 对象。"},
                        {"role": "user", "content": prompt},
                    ],
                    "response_format": {"type": "json_object"},
                }
                resp = client.chat.completions.create(**kwargs)
            except Exception as e:
                raise RuntimeError(self._match_agent_format_model_error(provider_norm, model, e)) from e
            text = resp.choices[0].message.content or ""
        elif provider_norm in ("gpt", "openai"):
            key = _norm(api_key)
            if not key:
                raise RuntimeError("请填写 OpenAI API Key")
            model = _norm(model_name) or "gpt-4.1"
            if model == "gpt-5.2-pro":
                raise RuntimeError("gpt-5.2-pro 仅支持 Responses API；当前 Agent 请改用 gpt-5.2、gpt-5-mini 或自定义可用于 Chat Completions 的模型")
            try:
                from openai import OpenAI
                client = OpenAI(api_key=key, timeout=120.0, max_retries=2)
                resp = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": "你是商品匹配规则优化专家。只输出 JSON 对象。"},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=float(temperature or 0.2),
                    response_format={"type": "json_object"},
                )
            except Exception as e:
                raise RuntimeError(self._match_agent_format_model_error(provider_norm, model, e)) from e
            text = resp.choices[0].message.content or ""
        else:
            key = _norm(api_key)
            if not key:
                raise RuntimeError("请填写 Gemini API Key")
            model = self._match_agent_normalize_gemini_model(model_name)
            try:
                from google import genai
                client = genai.Client(api_key=key)
                resp = client.models.generate_content(
                    model=model,
                    contents=prompt,
                    config={
                        "temperature": float(temperature or 0.2),
                        "response_mime_type": "application/json",
                    },
                )
            except Exception as e:
                raise RuntimeError(self._match_agent_format_model_error(provider_norm, model, e)) from e
            text = getattr(resp, "text", "") or ""
        try:
            parsed = json.loads(self._match_agent_strip_json_fences(text))
        except Exception as e:
            raise ValueError(f"模型返回不是合法 JSON：{e}") from e
        if not isinstance(parsed, dict):
            raise ValueError("模型返回 JSON 必须是对象")
        return parsed

    def _match_agent_find_category_path(self, category_paths: List[Dict[str, str]], category3: str, fallback_l1: str = "") -> Dict[str, str]:
        c3 = _norm(category3)
        for item in category_paths:
            if _norm(item.get("l3")) == c3:
                return {"l1": _norm(item.get("l1")), "l2": _norm(item.get("l2")), "l3": c3}
        return {"l1": fallback_l1, "l2": "", "l3": c3}

    def _match_agent_base_metrics_for_category(self, template: dict, category3: str) -> Dict[str, Any]:
        fake = {"美团类目三级": category3}
        group = post_match_engine.get_rule_group_for_item(template, fake) or {}
        metrics = group.get("metrics") if isinstance(group.get("metrics"), dict) else {}
        if metrics:
            return json.loads(json.dumps(metrics, ensure_ascii=False))
        normalized = post_match_engine.normalize_template({"v": 3, "rule_groups": [{
            "id": "default", "name": "default", "categories": {"l3": [category3]}, "metrics": {}
        }]})
        return normalized["rule_groups"][0]["metrics"]

    def _match_agent_extract_rule_changes(self, model_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        raw = model_payload.get("rule_changes")
        if not isinstance(raw, list):
            raw = model_payload.get("rule_diffs") if isinstance(model_payload.get("rule_diffs"), list) else []
        out = []
        for idx, item in enumerate(raw):
            if not isinstance(item, dict):
                continue
            cat3 = _norm(item.get("category3"))
            metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else None
            if not cat3 or not metrics:
                continue
            out.append({
                "id": idx + 1,
                "category3": cat3,
                "reason": _norm(item.get("reason")) or _norm(item.get("change")) or "Gemini建议调整该三级类目规则",
                "risk": _norm(item.get("risk")) or "medium",
                "metrics": metrics,
            })
        return out

    def _match_agent_build_template_draft(self, template: dict, rule_changes: List[Dict[str, Any]], category_paths: List[Dict[str, str]], fallback_l1: str) -> Dict[str, Any]:
        base = post_match_engine.normalize_template(template)
        changed = {_norm(c.get("category3")) for c in rule_changes if _norm(c.get("category3"))}
        groups = []
        for group in base.get("rule_groups") or []:
            g = json.loads(json.dumps(group, ensure_ascii=False))
            cats = g.setdefault("categories", {})
            paths = [p for p in (cats.get("paths") or []) if _norm(p.get("l3")) not in changed]
            l3 = [_norm(x) for x in (cats.get("l3") or []) if _norm(x) not in changed]
            cats["paths"] = paths
            cats["l3"] = l3
            cats["l1"] = sorted({_norm(p.get("l1")) for p in paths if _norm(p.get("l1"))} | {_norm(x) for x in cats.get("l1") or [] if _norm(x)})
            cats["l2"] = sorted({_norm(p.get("l2")) for p in paths if _norm(p.get("l2"))} | {_norm(x) for x in cats.get("l2") or [] if _norm(x)})
            if l3 or paths:
                groups.append(g)

        for idx, change in enumerate(rule_changes, 1):
            cat3 = _norm(change.get("category3"))
            path = self._match_agent_find_category_path(category_paths, cat3, fallback_l1)
            merged_metrics = self._match_agent_base_metrics_for_category(base, cat3)
            merged_metrics.update(change.get("metrics") or {})
            groups.append({
                "id": f"agent_{int(time.time())}_{idx}",
                "name": f"Agent优化-{cat3}",
                "categories": {
                    "paths": [path],
                    "l1": [path.get("l1")] if path.get("l1") else [],
                    "l2": [path.get("l2")] if path.get("l2") else [],
                    "l3": [cat3],
                },
                "metrics": merged_metrics,
            })
        return post_match_engine.normalize_template({"v": 3, "rule_groups": groups})

    def _match_agent_category_paths_from_diagnostics(self, diagnostics: List[Dict[str, Any]], categories: List[str]) -> List[Dict[str, str]]:
        selected = {_norm(c) for c in categories if _norm(c)}
        paths: List[Dict[str, str]] = []
        seen = set()
        for diag in diagnostics or []:
            for key in ("main_item", "correct_item", "wrong_item"):
                item = diag.get(key) or {}
                l3 = _norm(item.get("美团类目三级"))
                if not l3 or l3 not in selected:
                    continue
                path = {
                    "l1": _norm(item.get("美团类目一级")),
                    "l2": _norm(item.get("美团类目二级")),
                    "l3": l3,
                }
                sig = (path["l1"], path["l2"], path["l3"])
                if sig in seen:
                    continue
                seen.add(sig)
                paths.append(path)
        return paths

    def _match_agent_find_project(self, conn, project_id: Any = None, project_name: Any = None) -> Tuple[bool, str, Optional[int]]:
        if _norm(project_id):
            row = conn.execute("SELECT id FROM projects WHERE id = ?", (int(project_id),)).fetchone()
            return (True, "", int(row[0])) if row else (False, f"项目不存在：{project_id}", None)
        name = _norm(project_name)
        if not name:
            return False, "缺少项目名或项目ID", None
        rows = conn.execute("SELECT id FROM projects WHERE name = ? ORDER BY id", (name,)).fetchall()
        if not rows:
            return False, f"项目不存在：{name}", None
        if len(rows) > 1:
            return False, f"项目名不唯一：{name}，请改用项目ID", None
        return True, "", int(rows[0][0])

    def _match_agent_resolve_store_for_skus(self, conn, project_id: int, main_sku: str, correct_sku: str, wrong_sku: str, store_id: str = "") -> Tuple[bool, str, str]:
        if store_id != "":
            correct = self._match_agent_get_comp(conn, project_id, store_id, correct_sku)
            wrong = self._match_agent_get_comp(conn, project_id, store_id, wrong_sku)
            if not correct:
                return False, f"指定竞店内正确竞店SKU不存在：{correct_sku}", ""
            if not wrong:
                return False, f"指定竞店内错误竞店SKU不存在：{wrong_sku}", ""
            return True, "", store_id

        rows = conn.execute(
            """
            SELECT c1.store_id
            FROM comp_products c1
            JOIN comp_products c2
              ON c2.project_id = c1.project_id AND c2.store_id = c1.store_id
            WHERE c1.project_id = ? AND c1.skuId = ? AND c2.skuId = ?
            ORDER BY c1.store_id
            """,
            (project_id, correct_sku, wrong_sku),
        ).fetchall()
        stores = [_norm(r[0]) for r in rows]
        if not stores:
            return False, "正确竞店SKU和错误竞店SKU没有在同一个竞店内同时找到，请补充竞店ID/名称或检查SKU", ""

        linked_all_rows = conn.execute(
            """
            SELECT store_id
            FROM product_links
            WHERE project_id = ? AND main_sku_id = ? AND comp_sku_id = ?
            ORDER BY store_id
            """,
            (project_id, main_sku, wrong_sku),
        ).fetchall()
        linked_all = [_norm(r[0]) for r in linked_all_rows]
        if linked_all and not any(sid in stores for sid in linked_all):
            return False, f"主店SKU当前错误关联所在竞店为 {','.join(linked_all)}，与正确/错误SKU共同所在竞店 {','.join(stores)} 不一致", ""

        current = self._match_agent_current_link(conn, project_id, main_sku, stores[0]) if len(stores) == 1 else {}
        if len(stores) == 1 and _norm(current.get("comp_sku_id")) == wrong_sku:
            return True, "", stores[0]

        linked = [sid for sid in linked_all if sid in stores]
        if len(linked) == 1:
            return True, "", linked[0]
        if len(stores) == 1:
            return True, "", stores[0]
        return False, f"竞店定位不唯一：{','.join(stores)}，请补充竞店ID/名称", ""

    def _match_agent_links_for_categories(self, conn, project_id: int, categories: List[str]) -> pd.DataFrame:
        if not categories:
            return pd.DataFrame(columns=["project_id", "main_sku_id", "store_id", "comp_sku_id", "similarity", "match_type", "is_new_add"])
        placeholders = ",".join(["?"] * len(categories))
        return pd.read_sql(
            f"""
            SELECT pl.project_id, pl.main_sku_id, pl.store_id, pl.comp_sku_id, pl.similarity, pl.match_type, pl.is_new_add
            FROM product_links pl
            JOIN main_products mp ON mp.project_id = pl.project_id AND mp.skuId = pl.main_sku_id
            WHERE pl.project_id = ? AND trim(COALESCE(mp.美团类目三级, '')) IN ({placeholders})
            """,
            conn,
            params=[project_id] + categories,
        )

    def _match_agent_avg_similarity(self, df: Optional[pd.DataFrame]) -> float:
        if df is None or df.empty or "similarity" not in df.columns:
            return 0.0
        vals = pd.to_numeric(df["similarity"], errors="coerce").dropna()
        return round(float(vals.sum() / len(vals)), 6) if len(vals) else 0.0

    def _match_agent_product_snapshot(self, conn, table: str, project_id: int, sku_ids: List[str], store_id: str = "") -> Dict[str, Dict[str, str]]:
        ids = sorted({_norm(x) for x in sku_ids if _norm(x)})
        if not ids:
            return {}
        if table not in ("main_products", "comp_products"):
            return {}
        placeholders = ",".join(["?"] * len(ids))
        if table == "main_products":
            sql = f"""
                SELECT skuId, 商品名称, 规格名称, 主图链接
                FROM main_products
                WHERE project_id = ? AND skuId IN ({placeholders})
            """
            params = [project_id] + ids
        else:
            sql = f"""
                SELECT skuId, 商品名称, 规格名称, 主图链接
                FROM comp_products
                WHERE project_id = ? AND store_id = ? AND skuId IN ({placeholders})
            """
            params = [project_id, store_id] + ids
        rows = conn.execute(sql, params).fetchall()
        return {
            _norm(row[0]): {
                "name": _norm(row[1]),
                "spec": _norm(row[2]),
                "image": _norm(row[3]),
            }
            for row in rows
        }

    def _match_agent_enrich_change_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not rows:
            return rows
        project_ids = {_norm(r.get("project_id")) for r in rows if _norm(r.get("project_id"))}
        if len(project_ids) != 1:
            return rows
        project_id = int(next(iter(project_ids)))
        main_ids = [r.get("main_sku_id") for r in rows]
        comp_ids_by_store: Dict[str, List[str]] = {}
        for row in rows:
            sid = _norm(row.get("store_id"))
            comp_ids_by_store.setdefault(sid, []).extend([row.get("old_comp_sku_id"), row.get("new_comp_sku_id")])
        with self._db_lock:
            conn = self._get_conn()
            try:
                main_map = self._match_agent_product_snapshot(conn, "main_products", project_id, main_ids)
                comp_maps = {
                    sid: self._match_agent_product_snapshot(conn, "comp_products", project_id, ids, store_id=sid)
                    for sid, ids in comp_ids_by_store.items()
                }
            finally:
                conn.close()

        enriched = []
        for row in rows:
            item = dict(row)
            main = main_map.get(_norm(item.get("main_sku_id")), {})
            old_comp = comp_maps.get(_norm(item.get("store_id")), {}).get(_norm(item.get("old_comp_sku_id")), {})
            new_comp = comp_maps.get(_norm(item.get("store_id")), {}).get(_norm(item.get("new_comp_sku_id")), {})
            item.update({
                "main_name": main.get("name", ""),
                "main_spec": main.get("spec", ""),
                "main_image": main.get("image", ""),
                "old_comp_name": old_comp.get("name", ""),
                "old_comp_spec": old_comp.get("spec", ""),
                "old_comp_image": old_comp.get("image", ""),
                "new_comp_name": new_comp.get("name", ""),
                "new_comp_spec": new_comp.get("spec", ""),
                "new_comp_image": new_comp.get("image", ""),
            })
            enriched.append(item)
        return enriched

    def _match_agent_change_reason(self, old_sku: str, new_sku: str, old_similarity: Any, new_similarity: Any, old_type: str, new_type: str) -> str:
        old_sku = _norm(old_sku)
        new_sku = _norm(new_sku)
        old_type = _norm(old_type) or "无"
        new_type = _norm(new_type) or "无"
        try:
            old_score = float(old_similarity)
        except (TypeError, ValueError):
            old_score = None
        try:
            new_score = float(new_similarity)
        except (TypeError, ValueError):
            new_score = None
        if old_sku == new_sku:
            return "未变化：新旧规则都匹配到同一竞品。"
        if not old_sku and new_sku:
            return f"原规则未产出可用匹配；新规则重跑后通过{new_type}匹配上该竞品。"
        if old_sku and not new_sku:
            return f"原规则曾通过{old_type}匹配到竞品；新规则重跑后无可用竞品通过。"
        if old_score is not None and new_score is not None:
            delta = round(new_score - old_score, 4)
            if delta > 0:
                return f"原规则选择旧竞品；新规则重跑后当前竞品通过{new_type}且分数提升 {delta}，成为新的可用匹配。"
            if delta < 0:
                return f"原规则选择旧竞品；新规则重跑后规则约束/候选排序变化，当前竞品虽分数低 {abs(delta)} 但成为新的可用匹配。"
        return f"原规则选择旧竞品；新规则重跑后规则约束/候选排序变化，改为通过{new_type}匹配当前竞品。"

    def _match_agent_compare_links(self, before_df: Optional[pd.DataFrame], after_df: Optional[pd.DataFrame]) -> Dict[str, Any]:
        before = before_df.copy() if before_df is not None and not before_df.empty else pd.DataFrame()
        after = after_df.copy() if after_df is not None and not after_df.empty else pd.DataFrame()
        key_cols = ["main_sku_id", "store_id"]
        for df in (before, after):
            for col in ["project_id"] + key_cols + ["comp_sku_id", "similarity", "match_type"]:
                if col not in df.columns:
                    df[col] = ""
            df["main_sku_id"] = df["main_sku_id"].astype(str)
            df["store_id"] = df["store_id"].astype(str)
        b = before.set_index(key_cols, drop=False) if not before.empty else pd.DataFrame()
        a = after.set_index(key_cols, drop=False) if not after.empty else pd.DataFrame()
        keys = sorted(set(b.index.tolist() if not b.empty else []) | set(a.index.tolist() if not a.empty else []))
        rows = []
        changed = 0
        for key in keys:
            br = b.loc[key].iloc[0] if not b.empty and key in b.index and isinstance(b.loc[key], pd.DataFrame) else (b.loc[key] if not b.empty and key in b.index else {})
            ar = a.loc[key].iloc[0] if not a.empty and key in a.index and isinstance(a.loc[key], pd.DataFrame) else (a.loc[key] if not a.empty and key in a.index else {})
            old_sku = _norm(br.get("comp_sku_id") if hasattr(br, "get") else "")
            new_sku = _norm(ar.get("comp_sku_id") if hasattr(ar, "get") else "")
            old_similarity = _norm(br.get("similarity") if hasattr(br, "get") else "")
            new_similarity = _norm(ar.get("similarity") if hasattr(ar, "get") else "")
            old_match_type = _norm(br.get("match_type") if hasattr(br, "get") else "")
            new_match_type = _norm(ar.get("match_type") if hasattr(ar, "get") else "")
            is_changed = old_sku != new_sku
            if is_changed:
                changed += 1
            rows.append({
                "project_id": _norm(br.get("project_id") if hasattr(br, "get") else "") or _norm(ar.get("project_id") if hasattr(ar, "get") else ""),
                "main_sku_id": key[0],
                "store_id": key[1],
                "old_comp_sku_id": old_sku,
                "old_similarity": old_similarity,
                "old_match_type": old_match_type,
                "new_comp_sku_id": new_sku,
                "new_similarity": new_similarity,
                "new_match_type": new_match_type,
                "changed": "是" if is_changed else "否",
                "change_reason": self._match_agent_change_reason(old_sku, new_sku, old_similarity, new_similarity, old_match_type, new_match_type),
            })
        rows = self._match_agent_enrich_change_rows(rows)
        return {
            "summary": {
                "before_match_count": int(len(before)),
                "after_match_count": int(len(after)),
                "changed_count": changed,
                "before_avg_similarity": self._match_agent_avg_similarity(before),
                "after_avg_similarity": self._match_agent_avg_similarity(after),
            },
            "details": rows,
        }

    def _match_agent_project_files(self, conn, project_id: int) -> Tuple[str, List[str]]:
        rows = conn.execute(
            "SELECT type, local_path FROM project_files WHERE project_id = ? ORDER BY id ASC",
            (project_id,),
        ).fetchall()
        main_path = ""
        comp_paths = []
        for typ, path in rows:
            if typ == "main":
                main_path = path
            elif typ == "comp":
                comp_paths.append(path)
        return main_path, comp_paths

    def _match_agent_filter_excel_by_categories(self, src_path: str, out_path: str, categories: List[str]) -> str:
        import utils
        from data_mgr_base import FIELD_MAPPINGS

        rows = utils.excel_to_list_dict(src_path)
        df = pd.DataFrame(rows)
        if not df.empty:
            df = self._apply_mappings(df, FIELD_MAPPINGS)
            if "美团类目三级" in df.columns:
                selected = set(categories)
                cat = df["美团类目三级"].fillna("").map(utils.clean_text_value).astype(str).str.strip()
                df = df[cat.isin(selected)].copy()
            else:
                df = df.iloc[0:0].copy()
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        df.to_excel(out_path, index=False)
        return out_path

    def _match_agent_rerun_with_template(self, project_id: int, categories: List[str], template: Dict[str, Any], run_id_hint: str = "") -> Dict[str, Any]:
        import main_030822

        dirs = self._ensure_project_dirs(project_id)
        with self._db_lock:
            conn = self._get_conn()
            try:
                main_path, comp_paths = self._match_agent_project_files(conn, project_id)
                row = conn.execute("SELECT COALESCE(match_config, '') FROM projects WHERE id = ?", (project_id,)).fetchone()
                match_config = row[0] if row else ""
            finally:
                conn.close()
        if not main_path or not comp_paths:
            raise RuntimeError("项目源文件不完整，无法重跑评估")

        stamp = run_id_hint or str(int(time.time()))
        cache_dir = os.path.join(dirs["cache"], f"match_agent_rerun_{stamp}")
        os.makedirs(cache_dir, exist_ok=True)
        filtered_main = self._match_agent_filter_excel_by_categories(main_path, os.path.join(cache_dir, "main.xlsx"), categories)
        filtered_comps = [
            self._match_agent_filter_excel_by_categories(path, os.path.join(cache_dir, f"comp_{idx}.xlsx"), categories)
            for idx, path in enumerate(comp_paths)
        ]
        output_name = f"match_agent_rerun_{project_id}_{stamp}"
        output_path = main_030822.run_analysis(
            filtered_main,
            filtered_comps,
            output_name=output_name,
            output_dir=dirs["outputs"],
            progress_cb=None,
            match_config=match_config,
            post_match_template=template,
            analysis_metrics={},
        )
        links_df = self.parse_links_from_output(project_id, output_path)
        return {"output_path": output_path, "links_df": links_df}

    def _match_agent_build_rule_run(self, project_id: int, diagnostics: List[Dict[str, Any]], template: dict, provider: str, model_name: str, temperature: float, api_key: str) -> Dict[str, Any]:
        triggered = [d for d in diagnostics if (d.get("trigger") or {}).get("triggered")]
        total = len(diagnostics)
        if not triggered:
            return {
                "diagnostics": diagnostics,
                "triggered": False,
                "trigger_reason": "没有反馈样本满足规则模板优化触发条件",
                "rule_changes": [],
                "rule_template_draft": {},
                "rerun_categories": [],
                "rerun_summary": {},
                "match_change_details": [],
                "can_apply": False,
                "metrics": {"total_cases": total, "triggered_cases": 0, "changed_count": 0, "before_avg_similarity": 0, "after_avg_similarity": 0},
            }

        l1 = _norm((triggered[0].get("main_item") or {}).get("美团类目一级"))
        with self._db_lock:
            conn = self._get_conn()
            try:
                category_paths = self._match_agent_l1_category_paths(conn, project_id, l1)
            finally:
                conn.close()

        model_payload = self._match_agent_call_model(
            triggered,
            template,
            provider,
            model_name,
            temperature,
            api_key=api_key,
            category_paths=category_paths,
        )
        rule_changes = self._match_agent_extract_rule_changes(model_payload)
        if not rule_changes:
            return {
                "diagnostics": diagnostics,
                "triggered": True,
                "trigger_reason": "满足触发条件，但模型未返回有效的三级类目规则修改",
                "rule_changes": [],
                "rule_template_draft": {},
                "rerun_categories": [],
                "rerun_summary": {},
                "match_change_details": [],
                "can_apply": False,
                "metrics": {"total_cases": total, "triggered_cases": len(triggered), "changed_count": 0, "before_avg_similarity": 0, "after_avg_similarity": 0},
            }

        draft = self._match_agent_build_template_draft(template, rule_changes, category_paths, l1)
        rerun_categories = sorted({_norm(c.get("category3")) for c in rule_changes if _norm(c.get("category3"))})
        with self._db_lock:
            conn = self._get_conn()
            try:
                before_scoped = self._match_agent_links_for_categories(conn, project_id, rerun_categories)
            finally:
                conn.close()
        rerun = self._match_agent_rerun_with_template(project_id, rerun_categories, draft)
        after_df = rerun.get("links_df")
        comparison = self._match_agent_compare_links(before_scoped, after_df)
        return {
            "diagnostics": diagnostics,
            "triggered": True,
            "trigger_reason": "已生成规则模板草稿并完成局部重跑评估",
            "rule_changes": rule_changes,
            "rule_template_draft": draft,
            "rerun_categories": rerun_categories,
            "rerun_output_path": rerun.get("output_path", ""),
            "rerun_summary": comparison["summary"],
            "match_change_details": comparison["details"],
            "can_apply": True,
            "model_summary": _norm(model_payload.get("summary")),
            "metrics": {
                "total_cases": total,
                "triggered_cases": len(triggered),
                "changed_count": comparison["summary"]["changed_count"],
                "before_avg_similarity": comparison["summary"]["before_avg_similarity"],
                "after_avg_similarity": comparison["summary"]["after_avg_similarity"],
            },
        }

    def list_match_feedback_cases(self, project_id: Optional[int] = None, limit: int = 200) -> Dict[str, Any]:
        pid = int(project_id or self.active_project_id or 0)
        with self._db_lock:
            conn = self._get_conn()
            try:
                df = pd.read_sql(
                    """
                    SELECT * FROM match_feedback_cases
                    WHERE project_id = ?
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    conn,
                    params=(pid, max(1, min(int(limit or 200), 1000))),
                )
            finally:
                conn.close()
        return {"items": df.fillna("").to_dict(orient="records") if not df.empty else [], "total": int(len(df))}

    def create_match_feedback_case(self, data: Dict[str, Any]) -> Tuple[bool, str, Optional[int]]:
        pid = int(data.get("project_id") or self.active_project_id or 0)
        main_sku = _norm(data.get("main_sku_id"))
        store_id = _norm(data.get("store_id"))
        correct_sku = _norm(data.get("correct_comp_sku_id"))
        current_sku = _norm(data.get("wrong_comp_sku_id")) or _norm(data.get("current_comp_sku_id"))
        if not pid or not main_sku or store_id == "" or not correct_sku or not current_sku:
            return False, "缺少项目、主店SKU、竞店、正确竞店SKU或错误竞店SKU", None
        with self._db_lock:
            conn = self._get_conn()
            try:
                main = self._match_agent_get_main(conn, pid, main_sku)
                comp = self._match_agent_get_comp(conn, pid, store_id, correct_sku)
                wrong = self._match_agent_get_comp(conn, pid, store_id, current_sku)
                if not main:
                    return False, f"主店SKU不存在：{main_sku}", None
                if not comp:
                    return False, f"竞店SKU不存在：{correct_sku}", None
                if not wrong:
                    return False, f"错误竞店SKU不存在：{current_sku}", None
                now = _now()
                with conn:
                    cur = conn.execute(
                        """
                        INSERT OR REPLACE INTO match_feedback_cases
                        (id, project_id, main_sku_id, store_id, correct_comp_sku_id, current_comp_sku_id, feedback_type, note, status, created_at, updated_at)
                        VALUES (
                            (SELECT id FROM match_feedback_cases WHERE project_id=? AND main_sku_id=? AND store_id=? AND correct_comp_sku_id=?),
                            ?, ?, ?, ?, ?, ?, ?, 'active',
                            COALESCE((SELECT created_at FROM match_feedback_cases WHERE project_id=? AND main_sku_id=? AND store_id=? AND correct_comp_sku_id=?), ?),
                            ?
                        )
                        """,
                        (
                            pid, main_sku, store_id, correct_sku,
                            pid, main_sku, store_id, correct_sku, current_sku,
                            _norm(data.get("feedback_type")) or "漏配", _norm(data.get("note")),
                            pid, main_sku, store_id, correct_sku, now, now,
                        ),
                    )
                    return True, "", int(cur.lastrowid or 0)
            finally:
                conn.close()

    def quick_run_match_agent(self, data: Dict[str, Any], provider: str = "gemini", model_name: str = "", temperature: float = 0.2, api_key: str = "") -> Dict[str, Any]:
        main_sku = _norm(data.get("main_sku_id"))
        correct_sku = _norm(data.get("correct_comp_sku_id"))
        wrong_sku = _norm(data.get("wrong_comp_sku_id")) or _norm(data.get("current_comp_sku_id"))
        store_id = _norm(data.get("store_id"))
        if not main_sku or not correct_sku or not wrong_sku:
            return {"status": "error", "message": "缺少主店SKU、正确竞店SKU或错误竞店SKU"}
        with self._db_lock:
            conn = self._get_conn()
            try:
                ok, msg, pid = self._match_agent_find_project(conn, data.get("project_id"), data.get("project_name"))
                if not ok or pid is None:
                    return {"status": "error", "message": msg}
                if not self._match_agent_get_main(conn, pid, main_sku):
                    return {"status": "error", "message": f"主店SKU不存在：{main_sku}"}
                ok, msg, resolved_store = self._match_agent_resolve_store_for_skus(conn, pid, main_sku, correct_sku, wrong_sku, store_id)
                if not ok:
                    return {"status": "error", "message": msg}
            finally:
                conn.close()

        ok, msg, case_id = self.create_match_feedback_case({
            "project_id": pid,
            "main_sku_id": main_sku,
            "store_id": resolved_store,
            "correct_comp_sku_id": correct_sku,
            "current_comp_sku_id": wrong_sku,
            "feedback_type": _norm(data.get("feedback_type")) or "错配",
            "note": _norm(data.get("note")) or "快速优化入口创建",
        })
        if not ok:
            return {"status": "error", "message": msg}
        result = self.run_match_agent(provider=provider, model_name=model_name, temperature=temperature, project_id=pid, api_key=api_key)
        if result.get("status") == "ok":
            result["case_id"] = case_id
            result["project_id"] = pid
            result["store_id"] = resolved_store
        return result

    def import_match_feedback_cases(self, file_storage, project_id: Optional[int] = None) -> Dict[str, Any]:
        df = pd.read_excel(file_storage, engine="openpyxl")
        aliases = {
            "main_sku_id": ["主店skuId", "主店SKU", "main_sku_id", "main_sku", "skuId"],
            "store_id": ["store_id", "竞店", "店铺", "竞店ID"],
            "correct_comp_sku_id": ["正确竞店skuId", "正确竞店SKU", "correct_comp_sku_id", "comp_sku_id"],
            "current_comp_sku_id": ["错误竞店skuId", "当前错误skuId", "当前匹配SKU", "wrong_comp_sku_id", "current_comp_sku_id"],
            "feedback_type": ["反馈类型", "类型", "feedback_type"],
            "note": ["备注", "note"],
        }
        col_map = {}
        cols = {str(c).strip(): c for c in df.columns}
        for key, names in aliases.items():
            for name in names:
                if name in cols:
                    col_map[key] = cols[name]
                    break
        missing = [k for k in ("main_sku_id", "store_id", "correct_comp_sku_id", "current_comp_sku_id") if k not in col_map]
        if missing:
            return {"status": "error", "message": "缺少必需列：" + "、".join(missing), "created": 0, "errors": []}
        created = 0
        errors = []
        for idx, row in df.fillna("").iterrows():
            item = {"project_id": project_id or self.active_project_id}
            for key, col in col_map.items():
                item[key] = row.get(col, "")
            ok, msg, _ = self.create_match_feedback_case(item)
            if ok:
                created += 1
            else:
                errors.append({"row": int(idx) + 2, "message": msg})
        return {"status": "ok", "created": created, "errors": errors}

    def run_match_agent(self, provider: str = "gemini", model_name: str = "", temperature: float = 0.2, project_id: Optional[int] = None, api_key: str = "") -> Dict[str, Any]:
        pid = int(project_id or self.active_project_id or 0)
        provider_norm = (_norm(provider) or "gemini").lower()
        with self._db_lock:
            conn = self._get_conn()
            try:
                cases_df = pd.read_sql(
                    "SELECT * FROM match_feedback_cases WHERE project_id = ? AND status = 'active' ORDER BY id ASC",
                    conn,
                    params=(pid,),
                )
                if cases_df.empty:
                    return {"status": "error", "message": "暂无可分析的反馈样本"}
                template = self.get_post_match_template_for_project(pid)
                diagnostics = [self._match_agent_diagnose_case(conn, row.to_dict(), template) for _, row in cases_df.fillna("").iterrows()]
            finally:
                conn.close()
        try:
            if any((d.get("trigger") or {}).get("triggered") for d in diagnostics) and not _norm(api_key):
                if provider_norm == "deepseek" and not _norm(os.environ.get("DEEPSEEK_API_KEY")):
                    return {"status": "error", "message": "请填写 DeepSeek API Key"}
                if provider_norm in ("gpt", "openai"):
                    return {"status": "error", "message": "请填写 OpenAI API Key"}
                if provider_norm != "deepseek":
                    return {"status": "error", "message": "请填写 Gemini API Key"}
            payload = self._match_agent_build_rule_run(pid, diagnostics, template, provider, model_name, temperature, api_key)
        except Exception as e:
            return {"status": "error", "message": str(e)}
        payload["provider"] = provider_norm
        payload["model_name"] = _norm(model_name) or (
            _DEFAULT_DEEPSEEK_MODEL if provider_norm == "deepseek"
            else ("gpt-4.1" if provider_norm in ("gpt", "openai") else _DEFAULT_GEMINI_MODEL)
        )
        payload["temperature"] = float(temperature or 0.2)

        now = _now()
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    cur = conn.execute(
                        """
                        INSERT INTO match_agent_runs
                        (project_id, provider, model_name, temperature, case_filter_json, suggestions_json, metrics_json, report_path, status, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, '', 'draft', ?, ?)
                        """,
                        (pid, payload["provider"], payload["model_name"], payload["temperature"], "{}", _json_dumps(payload), _json_dumps(payload.get("metrics")), now, now),
                    )
                    run_id = int(cur.lastrowid)
                    report_path = self._match_agent_write_report(pid, run_id, diagnostics, payload)
                    conn.execute("UPDATE match_agent_runs SET report_path = ?, updated_at = ? WHERE id = ?", (report_path, _now(), run_id))
            finally:
                conn.close()
        return {"status": "ok", "run": self._match_agent_run_public(run_id, payload, report_path)}

    def _match_agent_write_report(self, project_id: int, run_id: int, diagnostics: List[Dict[str, Any]], payload: Dict[str, Any]) -> str:
        dirs = self._ensure_project_dirs(project_id)
        report_dir = os.path.join(dirs["outputs"], "match_agent")
        os.makedirs(report_dir, exist_ok=True)
        path = os.path.join(report_dir, f"match_agent_run_{run_id}.xlsx")
        match_agent_report.write_report(path, diagnostics, payload)
        return path

    def _match_agent_run_public(self, run_id: int, payload: Dict[str, Any], report_path: str = "") -> Dict[str, Any]:
        return {
            "id": run_id,
            "provider": payload.get("provider"),
            "model_name": payload.get("model_name"),
            "temperature": payload.get("temperature"),
            "metrics": payload.get("metrics") or {},
            "suggestions": payload.get("rule_changes") or payload.get("suggestions") or [],
            "diagnostics": payload.get("diagnostics") or [],
            "rule_diffs": payload.get("rule_changes") or payload.get("rule_diffs") or [],
            "a_field_suggestions": payload.get("a_field_suggestions") or [],
            "vector_suggestions": payload.get("vector_suggestions") or [],
            "root_causes": payload.get("root_causes") or [],
            "rule_template_draft": payload.get("rule_template_draft") or {},
            "risk_level": payload.get("risk_level") or "",
            "triggered": payload.get("triggered", False),
            "trigger_reason": payload.get("trigger_reason") or "",
            "rule_changes": payload.get("rule_changes") or [],
            "rerun_categories": payload.get("rerun_categories") or [],
            "rerun_summary": payload.get("rerun_summary") or {},
            "match_change_details": payload.get("match_change_details") or [],
            "can_apply": bool(payload.get("can_apply")),
            "applied_to_v2": bool(payload.get("applied_to_v2")),
            "applied_to_v2_categories": payload.get("applied_to_v2_categories") or [],
            "applied_to_v2_template_id": payload.get("applied_to_v2_template_id") or "",
            "applied_to_v2_group_count": payload.get("applied_to_v2_group_count") or "",
            "status": payload.get("status") or "",
            "report_path": report_path,
        }

    def list_match_agent_runs(self, project_id: Optional[int] = None) -> Dict[str, Any]:
        pid = int(project_id or self.active_project_id or 0)
        with self._db_lock:
            conn = self._get_conn()
            try:
                rows = conn.execute(
                    """
                    SELECT id, provider, model_name, temperature, metrics_json, status, created_at
                    FROM match_agent_runs
                    WHERE project_id = ?
                    ORDER BY id DESC
                    LIMIT 50
                    """,
                    (pid,),
                ).fetchall()
                items = [
                    {
                        "id": r[0],
                        "provider": r[1],
                        "model_name": r[2],
                        "temperature": r[3],
                        "metrics": _json_loads(r[4], {}),
                        "status": r[5],
                        "created_at": r[6],
                    }
                    for r in rows
                ]
                return {"items": items}
            finally:
                conn.close()

    def get_match_agent_run(self, run_id: int) -> Optional[Dict[str, Any]]:
        with self._db_lock:
            conn = self._get_conn()
            try:
                row = conn.execute(
                    """
                    SELECT id, suggestions_json, report_path, status
                    FROM match_agent_runs
                    WHERE id = ?
                    """,
                    (run_id,),
                ).fetchone()
            finally:
                conn.close()
        if not row:
            return None
        payload = _json_loads(row[1], {})
        item = self._match_agent_run_public(int(row[0]), payload, row[2] or "")
        item["status"] = row[3] or ""
        return item

    def get_match_agent_report_path(self, run_id: int) -> str:
        with self._db_lock:
            conn = self._get_conn()
            try:
                row = conn.execute("SELECT report_path FROM match_agent_runs WHERE id = ?", (run_id,)).fetchone()
            finally:
                conn.close()
        path = row[0] if row else ""
        return path if path and os.path.exists(path) else ""

    def apply_match_agent_run(self, run_id: int) -> Tuple[bool, str, Optional[int]]:
        with self._db_lock:
            conn = self._get_conn()
            try:
                row = conn.execute(
                    "SELECT project_id, suggestions_json, status FROM match_agent_runs WHERE id = ?",
                    (run_id,),
                ).fetchone()
            finally:
                conn.close()
        if not row:
            return False, "运行记录不存在", None
        if _norm(row[2]) in ("applied", "applied_and_v2"):
            return False, "该优化方案已应用，不能重复应用", None
        project_id = int(row[0])
        payload = _json_loads(row[1], {})
        if not payload.get("can_apply"):
            return False, "本次运行没有可应用的优化方案", None
        draft = payload.get("rule_template_draft") or {}
        categories = [_norm(c) for c in (payload.get("rerun_categories") or []) if _norm(c)]
        output_path = _norm(payload.get("rerun_output_path"))
        if not isinstance(draft, dict) or not draft.get("rule_groups"):
            return False, "规则模板草稿无效", None
        if not categories:
            return False, "缺少重跑三级类目范围", None
        if not output_path or not os.path.exists(output_path):
            return False, "重跑结果文件不存在", None

        cfg = post_match_engine.normalize_template(draft)
        if not cfg.get("rule_groups"):
            return False, "规则模板草稿无有效规则组", None
        tid = self.create_rule_template(f"Agent优化方案 {run_id}", "由关联优化 Agent 规则评估生成，已审核应用。", cfg)
        links_df = self.parse_links_from_output(project_id, output_path)
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    conn.execute("UPDATE projects SET rule_template_id = ? WHERE id = ?", (tid, project_id))
            finally:
                conn.close()
        self.replace_project_links(project_id, links_df, categories=categories)
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    next_status = "applied_and_v2" if payload.get("applied_to_v2") else "applied"
                    conn.execute("UPDATE match_agent_runs SET status = ?, updated_at = ? WHERE id = ?", (next_status, _now(), run_id))
            finally:
                conn.close()
        return True, "", tid

    def publish_match_agent_rule_template(self, run_id: int) -> Tuple[bool, str, Optional[int]]:
        return self.apply_match_agent_run(run_id)

    def apply_match_agent_run_to_v2(self, run_id: int, bind_project: bool = False) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        with self._db_lock:
            conn = self._get_conn()
            try:
                row = conn.execute(
                    "SELECT project_id, suggestions_json, status FROM match_agent_runs WHERE id = ?",
                    (run_id,),
                ).fetchone()
            finally:
                conn.close()
        if not row:
            return False, "运行记录不存在", None

        project_id = int(row[0])
        payload = _json_loads(row[1], {})
        if payload.get("applied_to_v2"):
            return False, "该优化方案已实施到V2，不能重复实施", None
        rule_changes = payload.get("rule_changes") or []
        draft = payload.get("rule_template_draft") or {}
        if not rule_changes:
            return False, "本次运行没有可实施到V2的规则修改", None
        if not isinstance(draft, dict) or not draft.get("rule_groups"):
            return False, "规则模板草稿无效，不能实施到V2", None
        diagnostics = payload.get("diagnostics") or []
        categories = sorted({_norm(c.get("category3")) for c in rule_changes if isinstance(c, dict) and _norm(c.get("category3"))})
        if not categories:
            return False, "缺少可实施到V2的三级类目", None

        with self._db_lock:
            conn = self._get_conn()
            try:
                row_v2 = conn.execute(
                    "SELECT id, description, config_json FROM rule_templates WHERE name = ? ORDER BY id LIMIT 1",
                    (_PRODUCTION_RULE_V2_NAME,),
                ).fetchone()
                if not row_v2:
                    return False, "数据库中不存在生产规则V2模板", None
                v2_id = int(row_v2[0])
                old_config = post_match_engine.template_from_db_json(row_v2[2] or "{}")
                paths = self._match_agent_category_paths_from_diagnostics(diagnostics, categories)
                fallback_l1 = _norm(((diagnostics[0] if diagnostics else {}).get("main_item") or {}).get("美团类目一级"))
                merged = self._match_agent_build_template_draft(old_config, rule_changes, paths, fallback_l1)
                if not (merged.get("rule_groups") or []):
                    return False, "合并后的V2模板无有效规则组", None
                now = _now()
                payload["applied_to_v2"] = True
                payload["applied_to_v2_at"] = now
                payload["applied_to_v2_template_id"] = v2_id
                payload["applied_to_v2_categories"] = categories
                payload["applied_to_v2_old_config"] = old_config
                payload["applied_to_v2_group_count"] = len(merged.get("rule_groups") or [])
                next_status = "applied_and_v2" if _norm(row[2]) == "applied" else "applied_to_v2"
                with conn:
                    conn.execute(
                        "UPDATE rule_templates SET description = ?, config_json = ?, updated_at = ? WHERE id = ?",
                        (
                            row_v2[1] or "V2 规则模板",
                            json.dumps(merged, ensure_ascii=False, separators=(",", ":")),
                            now,
                            v2_id,
                        ),
                    )
                    if bind_project:
                        conn.execute("UPDATE projects SET rule_template_id = ? WHERE id = ?", (v2_id, project_id))
                    conn.execute(
                        "UPDATE match_agent_runs SET suggestions_json = ?, status = ?, updated_at = ? WHERE id = ?",
                        (_json_dumps(payload), next_status, now, run_id),
                    )
                return True, "", {
                    "template_id": v2_id,
                    "categories": categories,
                    "group_count": len(merged.get("rule_groups") or []),
                    "bind_project": bool(bind_project),
                }
            finally:
                conn.close()
