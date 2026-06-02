import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from data_mgr import DataManager
from db_access import Database


def _insert_products(dm):
    with dm._get_conn() as conn:
        conn.execute(
            """
            INSERT INTO main_products
            (project_id, skuId, 商品名称, 规格名称, 美团类目一级, 美团类目二级, 美团类目三级,
             A单件净含量, A售卖数量, A包装单位, A颜色, A尺寸, A型号)
            VALUES (1, 'M1', '可乐', '500ml*1瓶', '饮品', '碳酸饮料', '可乐',
                    '500ml', '1', '瓶', '', '', '')
            """
        )
        conn.execute(
            """
            INSERT INTO comp_products
            (project_id, store_id, skuId, 商品名称, 规格名称, 美团类目一级, 美团类目二级, 美团类目三级,
             A单件净含量, A售卖数量, A包装单位, A颜色, A尺寸, A型号)
            VALUES
            (1, '0', 'W1', '可乐整箱', '500ml*24瓶', '饮品', '碳酸饮料', '可乐',
             '500ml', '24', '瓶', '', '', ''),
            (1, '0', 'C1', '可乐单瓶', '500ml*1瓶', '饮品', '碳酸饮料', '可乐',
             '500ml', '1', '瓶', '', '', '')
            """
        )
        conn.execute(
            """
            INSERT INTO product_links
            (project_id, main_sku_id, store_id, comp_sku_id, similarity, match_type, is_new_add)
            VALUES (1, 'M1', '0', 'W1', 0.71, '文本匹配', '否')
            """
        )
        conn.execute(
            """
            INSERT INTO match_feedback_cases
            (project_id, main_sku_id, store_id, correct_comp_sku_id, current_comp_sku_id, feedback_type, status)
            VALUES (1, 'M1', '0', 'C1', 'W1', '错配', 'active')
            """
        )


def _trigger_rule_diag():
    return {
        "rule_group": "可乐规则",
        "wrong": {"accepted": True, "reason": "后验规则放过", "metrics": []},
        "correct": {
            "accepted": False,
            "reason": "售卖数量差值超过阈值",
            "metrics": [{"key": "sell", "enabled": True, "passed": False, "reason": "售卖数量差值超过阈值"}],
        },
    }


class MatchAgentRealFlowTests(unittest.TestCase):
    def setUp(self):
        db = Database()
        try:
            with db.engine.begin() as conn:
                conn.execute(text("DROP SCHEMA public CASCADE"))
                conn.execute(text("CREATE SCHEMA public"))
        finally:
            db.close()

    def _dm(self, tmpdir, triggered=True):
        dm = DataManager(tmpdir)
        dm._match_agent_text_vector_diff = lambda main, wrong, correct, project_id: {
            "status": "ok",
            "main_text": "可乐 500ml*1瓶",
            "wrong_text": "可乐 500ml*24瓶",
            "correct_text": "可乐 500ml*1瓶",
            "main_wrong_score": 0.71,
            "main_correct_score": 0.94 if triggered else 0.60,
            "delta_correct_minus_wrong": 0.23 if triggered else -0.11,
            "error": "",
        }
        dm._match_agent_rule_diagnostics = lambda template, main, wrong, correct: _trigger_rule_diag()
        return dm

    def test_a_field_diff_marks_same_different_and_missing(self):
        dm = DataManager(tempfile.mkdtemp())

        rows = dm._match_agent_a_field_diff(
            {"A单件净含量": "500ml", "A售卖数量": "1"},
            {"A单件净含量": "500ml", "A售卖数量": "24"},
            {"A单件净含量": "500ml", "A售卖数量": "1", "A包装单位": "瓶"},
        )

        by_field = {r["field"]: r for r in rows}
        self.assertEqual(by_field["A单件净含量"]["main_vs_correct"], "same")
        self.assertEqual(by_field["A售卖数量"]["main_vs_wrong"], "different")
        self.assertEqual(by_field["A包装单位"]["main_vs_correct"], "missing")

    def test_not_triggered_creates_diagnostic_only_without_model_key(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dm = self._dm(tmpdir, triggered=False)
            _insert_products(dm)
            dm._match_agent_call_model = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("model should not be called"))

            result = dm.run_match_agent(provider="gemini", project_id=1, api_key="")

            self.assertEqual(result["status"], "ok")
            self.assertFalse(result["run"]["can_apply"])
            self.assertFalse(result["run"]["triggered"])
            self.assertEqual(result["run"]["diagnostics"][0]["diagnosis_type"], "三级类目规则过滤")
            self.assertIn("被后验规则拦截", result["run"]["diagnostics"][0]["core_reason"])
            with dm._get_conn() as conn:
                count = conn.execute("SELECT COUNT(*) FROM match_agent_runs").fetchone()[0]
            self.assertEqual(count, 1)

    def test_template_draft_splits_changed_category_into_single_category_group(self):
        dm = DataManager(tempfile.mkdtemp())
        template = {
            "v": 3,
            "rule_groups": [{
                "id": "drink",
                "name": "饮品规则",
                "categories": {
                    "paths": [
                        {"l1": "饮品", "l2": "碳酸饮料", "l3": "可乐"},
                        {"l1": "饮品", "l2": "碳酸饮料", "l3": "雪碧"},
                    ],
                    "l1": ["饮品"],
                    "l2": ["碳酸饮料"],
                    "l3": ["可乐", "雪碧"],
                },
                "metrics": {"sell": {"en": True, "max_diff": 0.0}},
            }],
        }

        draft = dm._match_agent_build_template_draft(
            template,
            [{"category3": "可乐", "metrics": {"sell": {"en": True, "max_diff": 1.0}}, "reason": "test"}],
            [{"l1": "饮品", "l2": "碳酸饮料", "l3": "可乐"}, {"l1": "饮品", "l2": "碳酸饮料", "l3": "雪碧"}],
            "饮品",
        )

        groups = draft["rule_groups"]
        self.assertTrue(any(g["categories"]["l3"] == ["雪碧"] for g in groups))
        cola = [g for g in groups if g["categories"]["l3"] == ["可乐"]][0]
        self.assertEqual(cola["metrics"]["sell"]["max_diff"], 1.0)

    def test_triggered_run_reruns_and_apply_binds_template_and_replaces_links(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dm = self._dm(tmpdir, triggered=True)
            _insert_products(dm)

            rerun_output = Path(tmpdir) / "rerun.xlsx"
            rerun_output.write_text("placeholder", encoding="utf-8")
            dm._match_agent_call_model = lambda diagnostics, template, provider, model_name, temperature, api_key="", category_paths=None: {
                "summary": "放宽可乐售卖数量规则",
                "rule_changes": [{
                    "category3": "可乐",
                    "reason": "正确SKU向量更高但被sell拦截",
                    "risk": "medium",
                    "metrics": {"sell": {"en": True, "max_diff": 1.0}},
                }],
            }
            dm._match_agent_rerun_with_template = lambda project_id, categories, template: {
                "output_path": str(rerun_output),
                "links_df": pd.DataFrame([{
                    "project_id": 1,
                    "main_sku_id": "M1",
                    "store_id": "0",
                    "comp_sku_id": "C1",
                    "similarity": 0.94,
                    "match_type": "文本匹配",
                    "is_new_add": "否",
                }]),
            }
            dm.parse_links_from_output = lambda project_id, output_path: pd.DataFrame([{
                "project_id": 1,
                "main_sku_id": "M1",
                "store_id": "0",
                "comp_sku_id": "C1",
                "similarity": 0.94,
                "match_type": "文本匹配",
                "is_new_add": "否",
            }])

            result = dm.run_match_agent(provider="gemini", project_id=1, api_key="test-key")

            self.assertEqual(result["status"], "ok")
            run = result["run"]
            self.assertTrue(run["can_apply"])
            self.assertEqual(run["rerun_summary"]["changed_count"], 1)
            self.assertEqual(run["rerun_summary"]["before_avg_similarity"], 0.71)
            self.assertEqual(run["rerun_summary"]["after_avg_similarity"], 0.94)
            detail = run["match_change_details"][0]
            self.assertEqual(detail["main_name"], "可乐")
            self.assertEqual(detail["main_spec"], "500ml*1瓶")
            self.assertEqual(detail["old_comp_name"], "可乐整箱")
            self.assertEqual(detail["old_comp_spec"], "500ml*24瓶")
            self.assertEqual(detail["new_comp_name"], "可乐单瓶")
            self.assertEqual(detail["new_comp_spec"], "500ml*1瓶")
            self.assertIn("当前竞品", detail["change_reason"])

            ok, msg, tid = dm.apply_match_agent_run(run["id"])

            self.assertTrue(ok, msg)
            with dm._get_conn() as conn:
                project_tid = conn.execute("SELECT rule_template_id FROM projects WHERE id = 1").fetchone()[0]
                linked = conn.execute("SELECT comp_sku_id FROM product_links WHERE project_id = 1 AND main_sku_id = 'M1'").fetchone()[0]
                status = conn.execute("SELECT status FROM match_agent_runs WHERE id = ?", (run["id"],)).fetchone()[0]
            self.assertEqual(project_tid, tid)
            self.assertEqual(linked, "C1")
            self.assertEqual(status, "applied")

            ok2, msg2, _ = dm.apply_match_agent_run(run["id"])
            self.assertFalse(ok2)
            self.assertIn("已应用", msg2)

    def test_apply_run_to_v2_updates_builtin_template_once(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dm = self._dm(tmpdir, triggered=True)
            _insert_products(dm)

            rerun_output = Path(tmpdir) / "rerun.xlsx"
            rerun_output.write_text("placeholder", encoding="utf-8")
            dm._match_agent_call_model = lambda diagnostics, template, provider, model_name, temperature, api_key="", category_paths=None: {
                "summary": "放宽可乐售卖数量规则",
                "rule_changes": [{
                    "category3": "可乐",
                    "reason": "正确SKU向量更高但被sell拦截",
                    "risk": "medium",
                    "metrics": {"sell": {"en": True, "max_diff": 1.0}},
                }],
            }
            dm._match_agent_rerun_with_template = lambda project_id, categories, template: {
                "output_path": str(rerun_output),
                "links_df": pd.DataFrame([{
                    "project_id": 1,
                    "main_sku_id": "M1",
                    "store_id": "0",
                    "comp_sku_id": "C1",
                    "similarity": 0.94,
                    "match_type": "文本匹配",
                    "is_new_add": "否",
                }]),
            }

            result = dm.run_match_agent(provider="gemini", project_id=1, api_key="test-key")
            run_id = result["run"]["id"]

            ok, msg, info = dm.apply_match_agent_run_to_v2(run_id)

            self.assertTrue(ok, msg)
            self.assertIn("可乐", info["categories"])
            with dm._get_conn() as conn:
                row = conn.execute("SELECT config_json FROM rule_templates WHERE name = '生产规则V2'").fetchone()
                status, payload = conn.execute("SELECT status, suggestions_json FROM match_agent_runs WHERE id = ?", (run_id,)).fetchone()
            cfg = json.loads(row[0])
            cola_groups = [g for g in cfg["rule_groups"] if "可乐" in (g.get("categories", {}).get("l3") or [])]
            self.assertEqual(len(cola_groups), 1)
            self.assertEqual(cola_groups[0]["metrics"]["sell"]["max_diff"], 1.0)
            self.assertEqual(status, "applied_to_v2")
            self.assertTrue(json.loads(payload)["applied_to_v2"])

            ok2, msg2, _ = dm.apply_match_agent_run_to_v2(run_id)
            self.assertFalse(ok2)
            self.assertIn("已实施到V2", msg2)

    def test_quick_run_resolves_store_from_main_correct_and_wrong_skus(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dm = self._dm(tmpdir, triggered=False)
            _insert_products(dm)
            dm._match_agent_call_model = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("model should not be called"))

            result = dm.quick_run_match_agent(
                {
                    "project_name": "默认项目",
                    "main_sku_id": "M1",
                    "correct_comp_sku_id": "C1",
                    "wrong_comp_sku_id": "W1",
                },
                provider="gemini",
                api_key="",
            )

            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["store_id"], "0")
            self.assertEqual(result["project_id"], 1)
            with dm._get_conn() as conn:
                row = conn.execute(
                    "SELECT store_id, current_comp_sku_id FROM match_feedback_cases WHERE project_id = 1 AND main_sku_id = 'M1' AND correct_comp_sku_id = 'C1'"
                ).fetchone()
            self.assertEqual(row, ("0", "W1"))


if __name__ == "__main__":
    unittest.main()
