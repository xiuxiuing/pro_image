import unittest

import post_match_engine as pme


def make_item(cat1, cat2, cat3, net):
    return {
        "美团类目一级": cat1,
        "美团类目二级": cat2,
        "美团类目三级": cat3,
        "A单件净含量": net,
        "A售卖数量": "1",
        "A包装单位": "瓶",
    }


NET_RULE = {
    "cat3": {"en": True},
    "net": {"en": True, "max_rel": 0.1},
    "sell": {"en": True, "max_diff": 0.0},
    "pack": {"en": True, "syn": [["瓶", "支", "罐"], ["袋", "包"]]},
}


class PostMatchNetEquivalenceTests(unittest.TestCase):
    def test_drink_allows_g_and_ml_when_values_are_close(self):
        main = make_item("饮品", "饮料", "含乳/乳酸菌饮料（常温）", "500g")
        comp = make_item("饮品", "饮料", "含乳/乳酸菌饮料（常温）", "500ml")

        self.assertTrue(pme.should_accept_post_match(main, comp, NET_RULE))

    def test_drink_still_rejects_g_and_ml_when_values_are_not_close(self):
        main = make_item("饮品", "饮料", "含乳/乳酸菌饮料（常温）", "500g")
        comp = make_item("饮品", "饮料", "含乳/乳酸菌饮料（常温）", "330ml")

        self.assertFalse(pme.should_accept_post_match(main, comp, NET_RULE))

    def test_liquid_condiment_allows_g_and_ml_when_values_are_close(self):
        main = make_item("粮油调味干货", "调味汁", "蚝油/鲜贝露", "500g")
        comp = make_item("粮油调味干货", "调味汁", "蚝油/鲜贝露", "500ml")

        self.assertTrue(pme.should_accept_post_match(main, comp, NET_RULE))

    def test_skin_care_liquid_allows_g_and_ml_when_values_are_close(self):
        main = make_item("美容护肤", "男士护肤", "男士洁面", "150g")
        comp = make_item("美容护肤", "男士护肤", "男士洁面", "150ml")

        self.assertTrue(pme.should_accept_post_match(main, comp, NET_RULE))

    def test_adult_liquid_allows_g_and_ml_when_values_are_close(self):
        main = make_item("成人用品", "润滑/延时", "润滑液", "80g")
        comp = make_item("成人用品", "润滑/延时", "润滑液", "80ml")

        self.assertTrue(pme.should_accept_post_match(main, comp, NET_RULE))

    def test_solid_food_does_not_allow_g_and_ml_equivalence(self):
        main = make_item("休闲食品", "饼干", "曲奇饼干", "500g")
        comp = make_item("休闲食品", "饼干", "曲奇饼干", "500ml")

        self.assertFalse(pme.should_accept_post_match(main, comp, NET_RULE))

    def test_grain_powder_does_not_allow_g_and_ml_equivalence(self):
        main = make_item("营养冲调", "谷物冲调", "麦片/谷物片", "500g")
        comp = make_item("营养冲调", "谷物冲调", "麦片/谷物片", "500ml")

        self.assertFalse(pme.should_accept_post_match(main, comp, NET_RULE))

    def test_dry_baking_material_does_not_allow_g_and_ml_equivalence(self):
        main = make_item("粮油调味干货", "烘焙材料", "食品添加剂", "10g")
        comp = make_item("粮油调味干货", "烘焙材料", "食品添加剂", "10ml")

        self.assertFalse(pme.should_accept_post_match(main, comp, NET_RULE))

    def test_both_sides_must_have_the_same_category_path(self):
        main = make_item("饮品", "饮料", "含乳/乳酸菌饮料（常温）", "500g")
        comp = make_item("饮品", "饮料", "果蔬汁饮料", "500ml")

        self.assertFalse(pme.should_accept_post_match(main, comp, NET_RULE))

    def test_same_unit_keeps_existing_relative_threshold(self):
        main = make_item("饮品", "饮料", "果蔬汁饮料", "500ml")
        comp = make_item("饮品", "饮料", "果蔬汁饮料", "550ml")

        self.assertTrue(pme.should_accept_post_match(main, comp, NET_RULE))


if __name__ == "__main__":
    unittest.main()
