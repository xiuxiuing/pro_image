from typing import Any, Dict, List

from openpyxl import Workbook


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        import json
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _write_sheet(wb: Workbook, title: str, rows: List[Dict[str, Any]], headers: List[str]):
    ws = wb.create_sheet(title)
    ws.append(headers)
    for row in rows:
        ws.append([_text(row.get(h, "")) for h in headers])
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = min(max(len(_text(c.value)) for c in col) + 2, 42)


def write_report(path: str, diagnostics: List[Dict[str, Any]], payload: Dict[str, Any]):
    wb = Workbook()
    wb.remove(wb.active)
    summary_rows = []
    for item in diagnostics:
        vector = item.get("vector_diff") or {}
        image = item.get("image_vector_diff") or {}
        topk = item.get("text_topk_comparison") or {}
        trigger = item.get("trigger") or {}
        summary_rows.append({
            "main_sku_id": item.get("main_sku_id"),
            "store_name": item.get("store_name"),
            "correct_comp_sku_id": item.get("correct_comp_sku_id"),
            "wrong_comp_sku_id": item.get("current_comp_sku_id"),
            "category3": item.get("category3"),
            "诊断类型": item.get("diagnosis_type"),
            "核心未匹配原因": item.get("core_reason") or item.get("reason"),
            "文本候选排名": item.get("candidate_rank"),
            "触发规则优化": "是" if trigger.get("triggered") else "否",
            "触发说明": trigger.get("reason"),
            "正确SKU是否规则拦截": "是" if trigger.get("correct_rule_blocked") else "否",
            "错误SKU是否规则拦截": "是" if trigger.get("wrong_rule_blocked") else "否",
            "正确SKU文本向量分": vector.get("main_correct_score"),
            "错误SKU文本向量分": vector.get("main_wrong_score"),
            "旧BGE正确SKU排名": topk.get("old_rank"),
            "新BGE正确SKU排名": topk.get("new_rank"),
            "新BGE是否进TopK": "是" if topk.get("new_in_topk") else "否",
            "BGE排名变化": topk.get("rank_delta"),
            "旧BGE正确SKU分": topk.get("old_score"),
            "新BGE正确SKU分": topk.get("new_score"),
            "旧BGE错误SKU排名": topk.get("old_wrong_rank"),
            "新BGE错误SKU排名": topk.get("new_wrong_rank"),
            "A增强评估结果": topk.get("result") or topk.get("error"),
            "正确SKU图片向量分": image.get("main_correct_score"),
            "错误SKU图片向量分": image.get("main_wrong_score"),
            "规则拦截维度": ",".join(trigger.get("blocked_metrics") or []),
        })
    _write_sheet(wb, "诊断摘要", summary_rows, [
        "main_sku_id", "store_name", "correct_comp_sku_id", "wrong_comp_sku_id", "category3",
        "诊断类型", "核心未匹配原因", "文本候选排名", "触发规则优化", "触发说明", "正确SKU是否规则拦截", "错误SKU是否规则拦截", "正确SKU文本向量分", "错误SKU文本向量分",
        "旧BGE正确SKU排名", "新BGE正确SKU排名", "新BGE是否进TopK", "BGE排名变化", "旧BGE正确SKU分", "新BGE正确SKU分", "旧BGE错误SKU排名", "新BGE错误SKU排名", "A增强评估结果",
        "正确SKU图片向量分", "错误SKU图片向量分", "规则拦截维度",
    ])

    topk_rows = []
    for item in diagnostics:
        topk = item.get("text_topk_comparison") or {}
        topk_rows.append({
            "main_sku_id": item.get("main_sku_id"),
            "store_name": item.get("store_name"),
            "correct_comp_sku_id": item.get("correct_comp_sku_id"),
            "wrong_comp_sku_id": item.get("current_comp_sku_id"),
            "状态": topk.get("status"),
            "TopK": topk.get("topk"),
            "旧排名": topk.get("old_rank"),
            "新排名": topk.get("new_rank"),
            "旧是否进TopK": "是" if topk.get("old_in_topk") else "否",
            "新是否进TopK": "是" if topk.get("new_in_topk") else "否",
            "排名变化": topk.get("rank_delta"),
            "旧正确SKU分": topk.get("old_score"),
            "新正确SKU分": topk.get("new_score"),
            "旧错误SKU排名": topk.get("old_wrong_rank"),
            "新错误SKU排名": topk.get("new_wrong_rank"),
            "旧错误SKU分": topk.get("old_wrong_score"),
            "新错误SKU分": topk.get("new_wrong_score"),
            "评估结果": topk.get("result"),
            "错误": topk.get("error"),
            "旧Top10": topk.get("old_top10"),
            "新Top10": topk.get("new_top10"),
        })
    _write_sheet(wb, "A增强TopK评估", topk_rows, [
        "main_sku_id", "store_name", "correct_comp_sku_id", "wrong_comp_sku_id", "状态", "TopK",
        "旧排名", "新排名", "旧是否进TopK", "新是否进TopK", "排名变化",
        "旧正确SKU分", "新正确SKU分", "旧错误SKU排名", "新错误SKU排名", "旧错误SKU分", "新错误SKU分",
        "评估结果", "错误", "旧Top10", "新Top10",
    ])

    a_rows = []
    for item in diagnostics:
        for row in item.get("a_field_diff") or []:
            a_rows.append({
                "main_sku_id": item.get("main_sku_id"),
                "correct_comp_sku_id": item.get("correct_comp_sku_id"),
                "wrong_comp_sku_id": item.get("current_comp_sku_id"),
                **row,
            })
    _write_sheet(wb, "A信息", a_rows, ["main_sku_id", "correct_comp_sku_id", "wrong_comp_sku_id", "field", "main", "wrong", "correct", "main_vs_wrong", "main_vs_correct", "status"])

    rule_rows = []
    for row in payload.get("rule_changes") or []:
        rule_rows.append({
            "category3": row.get("category3"),
            "修改理由": row.get("reason"),
            "风险": row.get("risk"),
            "修改后规则": row.get("metrics"),
        })
    _write_sheet(wb, "规则模板优化点", rule_rows, ["category3", "修改理由", "风险", "修改后规则"])

    rerun = payload.get("rerun_summary") or {}
    _write_sheet(wb, "重跑汇总", [{
        "重跑三级类目": ",".join(payload.get("rerun_categories") or []),
        "重跑前匹配数量": rerun.get("before_match_count", 0),
        "重跑后匹配数量": rerun.get("after_match_count", 0),
        "匹配变化数量": rerun.get("changed_count", 0),
        "重跑前平均匹配度": rerun.get("before_avg_similarity", 0),
        "重跑后平均匹配度": rerun.get("after_avg_similarity", 0),
        "是否可应用": "是" if payload.get("can_apply") else "否",
        "说明": payload.get("trigger_reason", ""),
    }], ["重跑三级类目", "重跑前匹配数量", "重跑后匹配数量", "匹配变化数量", "重跑前平均匹配度", "重跑后平均匹配度", "是否可应用", "说明"])

    _write_sheet(wb, "匹配变化明细", payload.get("match_change_details") or [], [
        "main_sku_id", "main_name", "main_spec", "main_image", "store_id",
        "old_comp_sku_id", "old_comp_name", "old_comp_spec", "old_comp_image", "old_similarity", "old_match_type",
        "new_comp_sku_id", "new_comp_name", "new_comp_spec", "new_comp_image", "new_similarity", "new_match_type", "changed", "change_reason",
    ])
    wb.save(path)
