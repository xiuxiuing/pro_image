import unittest

import post_match_engine as pme
from a_field_normalizer import (
    ATTRIBUTE_SYNONYM_GROUPS,
    FORM_SYNONYM_GROUPS,
    HIGH_RISK_CORE_CONFLICT_PAIRS,
    MODEL_SYNONYM_GROUPS,
    core_category_conflict_pair,
    normalize_brand,
    normalize_core_category,
    normalize_key_attributes,
    normalize_model,
    normalize_product_form,
    weak_a_signal_summary,
)


CORE_RULE = {
    "core_conflict": {"en": False},
    "core": {"en": True},
    "cat3": {"en": False},
    "net": {"en": False},
    "sell": {"en": False},
    "pack": {"en": False},
    "color": {"en": False},
    "size": {"en": False},
    "multidim_size": {"en": False},
    "model": {"en": False},
}

MULTIDIM_RULE = {
    "core_conflict": {"en": False},
    "core": {"en": False},
    "cat3": {"en": False},
    "net": {"en": False},
    "sell": {"en": False},
    "pack": {"en": False},
    "color": {"en": False},
    "size": {"en": False},
    "multidim_size": {"en": True, "max_rel": 0.125},
    "model": {"en": False},
}

CONFLICT_RULE = {
    "core_conflict": {"en": True},
    "category_gate": {"en": False},
    "core": {"en": False},
    "cat3": {"en": False},
    "net": {"en": False},
    "sell": {"en": False},
    "pack": {"en": False},
    "color": {"en": False},
    "size": {"en": False},
    "multidim_size": {"en": False},
    "model": {"en": False},
}

CATEGORY_GATE_RULE = {
    "core_conflict": {"en": False},
    "category_gate": {"en": True, "mode": "cat2_or_core", "syn": []},
    "core": {"en": False},
    "cat3": {"en": False},
    "net": {"en": False},
    "sell": {"en": False},
    "pack": {"en": False},
    "color": {"en": False},
    "size": {"en": False},
    "multidim_size": {"en": False},
    "model": {"en": False},
}


class AFieldNormalizerTests(unittest.TestCase):
    def test_core_category_synonyms_from_observed_samples(self):
        self.assertEqual(normalize_core_category("宠物零食猫条"), "猫条")
        self.assertEqual(normalize_core_category("水果牛奶饮料"), "含乳饮料")
        self.assertEqual(normalize_core_category("营养快线"), "含乳饮料")
        self.assertEqual(normalize_core_category("饮用天然泉水"), "包装饮用水")
        self.assertEqual(normalize_core_category("情趣丝袜"), "丝袜")
        self.assertEqual(normalize_core_category("安睡裤"), "裤型卫生巾")
        self.assertEqual(normalize_core_category("洗发露"), "洗发水")
        self.assertEqual(normalize_core_category("封箱胶带"), "透明胶带")
        self.assertEqual(normalize_core_category("尿不湿"), "纸尿裤")
        self.assertEqual(normalize_core_category("纸灯笼"), "灯笼")
        self.assertEqual(normalize_core_category("利是封"), "红包袋")
        self.assertEqual(normalize_core_category("压缩袋"), "真空压缩袋")
        self.assertEqual(normalize_core_category("雨鞋"), "雨靴")
        self.assertEqual(normalize_core_category("手机贴膜"), "手机膜")
        self.assertEqual(normalize_core_category("猫笼子"), "宠物围栏")
        self.assertEqual(normalize_core_category("封床底挡板"), "宠物挡板")

    def test_default_weak_synonym_groups_are_populated(self):
        self.assertGreaterEqual(len(FORM_SYNONYM_GROUPS), 10)
        self.assertGreaterEqual(len(ATTRIBUTE_SYNONYM_GROUPS), 20)
        self.assertGreaterEqual(len(MODEL_SYNONYM_GROUPS), 8)
        self.assertGreaterEqual(len(HIGH_RISK_CORE_CONFLICT_PAIRS), 30)

    def test_high_risk_conflict_table_normalizes_core_categories(self):
        self.assertEqual(core_category_conflict_pair("热熔胶条", "固体胶棒"), ("热熔胶棒", "固体胶"))
        self.assertEqual(core_category_conflict_pair("牙膏", "洗发露"), ("牙膏", "洗发水"))
        self.assertIsNone(core_category_conflict_pair("营养快线", "乳饮料"))
        self.assertIsNone(core_category_conflict_pair("猫砂", "猫沙"))
        self.assertEqual(core_category_conflict_pair("猫砂盆", "猫砂"), ("猫砂盆", "猫砂"))
        self.assertEqual(core_category_conflict_pair("猫笼子", "猫砂"), ("宠物围栏", "猫砂"))
        self.assertEqual(core_category_conflict_pair("封床底挡板", "猫砂"), ("宠物挡板", "猫砂"))

    def test_product_form_synonyms_normalize_common_shapes(self):
        self.assertEqual(normalize_product_form("液态喷雾"), "液体")
        self.assertEqual(normalize_product_form("牙膏膏状"), "膏体")
        self.assertEqual(normalize_product_form("成卷"), "卷装")
        self.assertEqual(normalize_product_form("礼盒装"), "盒装")

    def test_model_synonyms_normalize_common_aliases(self):
        self.assertEqual(normalize_model("型号: typec"), "Type-C")
        self.assertEqual(normalize_model("USB-C"), "Type-C")
        self.assertEqual(normalize_model("m/l"), "M-L")

    def test_brand_synonyms_normalize_common_aliases(self):
        self.assertEqual(normalize_brand("Coca-Cola"), "可口可乐")
        self.assertEqual(normalize_brand("Deli"), "得力")
        self.assertEqual(normalize_brand("品牌随机"), "")

    def test_brand_fallback_ignores_compatible_device_brand(self):
        from extract_info_rules import _heuristic_batch

        rows = _heuristic_batch([{"name": "适用苹果14手机壳透明防摔保护套", "spec": "1个/份"}])
        self.assertNotEqual(rows[0].brand, "苹果")

    def test_core_metric_rejects_real_conflict_when_enabled(self):
        main = {"A核心品类": "热熔胶棒"}
        comp = {"A核心品类": "固体胶"}
        self.assertFalse(pme.should_accept_post_match(main, comp, CORE_RULE))

    def test_core_metric_accepts_synonyms_when_enabled(self):
        main = {"A核心品类": "营养快线"}
        comp = {"A核心品类": "含乳饮料"}
        self.assertTrue(pme.should_accept_post_match(main, comp, CORE_RULE))

    def test_core_metric_missing_value_does_not_block(self):
        main = {"A核心品类": "猫条"}
        comp = {"A核心品类": ""}
        self.assertTrue(pme.should_accept_post_match(main, comp, CORE_RULE))

    def test_core_metric_defaults_to_off_when_missing(self):
        main = {"A核心品类": "热熔胶棒"}
        comp = {"A核心品类": "固体胶"}
        self.assertTrue(pme.should_accept_post_match(main, comp, {}))

    def test_high_risk_conflict_rejects_even_when_core_metric_is_off(self):
        main = {"A核心品类": "热熔胶棒"}
        comp = {"A核心品类": "固体胶"}
        self.assertFalse(pme.should_accept_post_match(main, comp, CONFLICT_RULE))

    def test_high_risk_conflict_missing_value_does_not_block(self):
        main = {"A核心品类": "热熔胶棒"}
        comp = {"A核心品类": ""}
        self.assertTrue(pme.should_accept_post_match(main, comp, CONFLICT_RULE))

    def test_category_gate_accepts_same_cat2_even_when_core_differs(self):
        main = {"美团类目二级": "饮料", "A核心品类": "茶饮料"}
        comp = {"美团类目二级": "饮料", "A核心品类": "果汁饮料"}
        self.assertTrue(pme.should_accept_post_match(main, comp, CATEGORY_GATE_RULE))

    def test_category_gate_accepts_core_synonym_when_cat2_differs(self):
        main = {"美团类目二级": "饮料", "A核心品类": "营养快线"}
        comp = {"美团类目二级": "乳饮冲调", "A核心品类": "含乳饮料"}
        self.assertTrue(pme.should_accept_post_match(main, comp, CATEGORY_GATE_RULE))

    def test_category_gate_rejects_when_cat2_and_core_both_differ(self):
        main = {"美团类目二级": "洗发护发", "A核心品类": "洗发水"}
        comp = {"美团类目二级": "口腔护理", "A核心品类": "牙膏"}
        self.assertFalse(pme.should_accept_post_match(main, comp, CATEGORY_GATE_RULE))

    def test_category_gate_missing_values_do_not_hard_block(self):
        main = {"美团类目二级": "饮料", "A核心品类": "茶饮料"}
        comp = {"美团类目二级": "", "A核心品类": ""}
        self.assertTrue(pme.should_accept_post_match(main, comp, CATEGORY_GATE_RULE))

    def test_category_gate_rejects_sensitive_l1_cross_category_when_core_missing(self):
        main = {"美团类目一级": "个人洗护", "美团类目二级": "身体清洁", "A核心品类": ""}
        comp = {"美团类目一级": "成人用品", "美团类目二级": "安全避孕", "A核心品类": ""}
        self.assertFalse(pme.should_accept_post_match(main, comp, CATEGORY_GATE_RULE))

    def test_high_risk_conflict_precedes_category_gate(self):
        rule = dict(CATEGORY_GATE_RULE)
        rule["core_conflict"] = {"en": True}
        main = {"美团类目二级": "洗护用品", "A核心品类": "牙膏"}
        comp = {"美团类目二级": "洗护用品", "A核心品类": "洗发水"}
        self.assertFalse(pme.should_accept_post_match(main, comp, rule))

    def test_soap_and_condom_are_high_risk_conflict(self):
        main = {"A核心品类": "香皂"}
        comp = {"A核心品类": "避孕套"}
        self.assertFalse(pme.should_accept_post_match(main, comp, CONFLICT_RULE))

    def test_multidim_size_metric_accepts_same_dimensions(self):
        main = {"A多维尺寸": "17x25x8cm"}
        comp = {"A多维尺寸": "8x25x17cm"}
        self.assertTrue(pme.should_accept_post_match(main, comp, MULTIDIM_RULE))

    def test_multidim_size_metric_rejects_large_difference(self):
        main = {"A多维尺寸": "17x25x8cm"}
        comp = {"A多维尺寸": "22x32x10cm"}
        self.assertFalse(pme.should_accept_post_match(main, comp, MULTIDIM_RULE))

    def test_multidim_size_metric_accepts_mixed_units(self):
        main = {"A多维尺寸": "50mmx20m"}
        comp = {"A多维尺寸": "5cmx20m"}
        self.assertTrue(pme.should_accept_post_match(main, comp, MULTIDIM_RULE))

    def test_multidim_size_metric_missing_value_does_not_block(self):
        main = {"A多维尺寸": "17x25x8cm"}
        comp = {"A多维尺寸": ""}
        self.assertTrue(pme.should_accept_post_match(main, comp, MULTIDIM_RULE))

    def test_explain_includes_core_metric(self):
        main = {"A核心品类": "情趣丝袜"}
        comp = {"A核心品类": "丝袜"}
        explanation = pme.explain_post_match(main, comp, CORE_RULE)
        core = next(x for x in explanation["metrics"] if x["key"] == "core")
        self.assertTrue(explanation["accepted"])
        self.assertEqual(core["values"]["main_norm"], "丝袜")
        self.assertEqual(core["values"]["candidate_norm"], "丝袜")

    def test_explain_includes_high_risk_conflict_metric(self):
        main = {"A核心品类": "牙膏"}
        comp = {"A核心品类": "洗发水"}
        explanation = pme.explain_post_match(main, comp, CONFLICT_RULE)
        metric = next(x for x in explanation["metrics"] if x["key"] == "core_conflict")
        self.assertFalse(explanation["accepted"])
        self.assertFalse(metric["passed"])

    def test_explain_includes_category_gate_metric(self):
        main = {"美团类目二级": "饮料", "A核心品类": "茶饮料"}
        comp = {"美团类目二级": "乳饮冲调", "A核心品类": "果汁饮料"}
        explanation = pme.explain_post_match(main, comp, CATEGORY_GATE_RULE)
        metric = next(x for x in explanation["metrics"] if x["key"] == "category_gate")
        self.assertFalse(explanation["accepted"])
        self.assertFalse(metric["passed"])
        self.assertEqual(metric["values"]["main_cat2"], "饮料")

    def test_explain_includes_multidim_size_metric(self):
        main = {"A多维尺寸": "17x25x8cm"}
        comp = {"A多维尺寸": "22x32x10cm"}
        explanation = pme.explain_post_match(main, comp, MULTIDIM_RULE)
        metric = next(x for x in explanation["metrics"] if x["key"] == "multidim_size")
        self.assertFalse(explanation["accepted"])
        self.assertFalse(metric["passed"])

    def test_weak_fields_are_normalized_without_blocking_semantics(self):
        attrs = normalize_key_attributes("0糖 | 极薄 | 夜间 | 抗菌 | 旅行装")
        self.assertEqual(attrs, ["无糖", "超薄", "夜用", "抑菌", "便携"])
        summary = weak_a_signal_summary({"A商品形态": "卷状", "A关键属性词": "免洗", "A颜色": "黑色 | 白色", "A型号": "usb-c", "A品牌": "Deli"})
        self.assertEqual(summary["form"], "卷装")
        self.assertEqual(summary["attributes"], ["一次性"])
        self.assertEqual(summary["color"], ["黑色", "白色"])
        self.assertEqual(summary["model"], "Type-C")
        self.assertEqual(summary["brand"], "得力")

    def test_weak_ranking_score_adds_bonus_without_rejection(self):
        main = {"A品牌": "Deli", "A型号": "TypeC", "A商品形态": "液态喷雾", "A关键属性词": "0糖", "A颜色": "黑色"}
        comp = {"A品牌": "得力", "A型号": "USB-C", "A商品形态": "喷剂", "A关键属性词": "无糖", "A颜色": "纯黑"}
        score = pme.weak_ranking_score(main, comp, {})

        self.assertGreater(score["bonus"], 0.05)
        self.assertTrue(score["details"]["brand"]["matched"])
        self.assertTrue(score["details"]["model"]["matched"])
        self.assertTrue(score["details"]["product_form"]["matched"])
        self.assertEqual(score["details"]["key_attributes"]["matched"], ["无糖"])
        self.assertTrue(pme.should_accept_post_match(main, comp, {"model": {"en": False}, "color": {"en": False}}))

    def test_weak_ranking_score_does_not_reward_missing_or_conflicting_values(self):
        main = {"A型号": "TypeC", "A商品形态": "膏状", "A关键属性词": "夜用", "A颜色": "黑色"}
        comp = {"A型号": "", "A商品形态": "液体", "A关键属性词": "日用", "A颜色": "白色"}
        score = pme.weak_ranking_score(main, comp, {})

        self.assertEqual(score["bonus"], 0.0)
        self.assertFalse(score["details"]["model"]["matched"])
        self.assertFalse(score["details"]["product_form"]["matched"])


if __name__ == "__main__":
    unittest.main()
