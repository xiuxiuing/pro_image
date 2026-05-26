import json
import re
from extract_info_schema import (
    ALLOWED_PACKAGING,
    MODEL_EXTRACTION_SOURCE,
    RULE_EXTRACTION_SOURCE,
    ProductInfo,
)
from product_text_extract import extract_product_keys
from a_field_normalizer import normalize_brand


def _ai_log(log_tag: str, msg: str) -> None:
    """终端可检索前缀：[AI][主.xlsx] ..."""
    tag = (log_tag or "").strip() or "-"
    print(f"[AI][{tag}] {msg}", flush=True)


def _mark_extraction_source(items: list[ProductInfo], source: str) -> list[ProductInfo]:
    for item in items:
        item = _normalize_product_info(item)
        try:
            object.__setattr__(item, "_extraction_source", source)
        except Exception:
            pass
    return items


def _get_extraction_source(item: ProductInfo) -> str:
    return str(getattr(item, "_extraction_source", "") or "").strip()


_MODEL_EXCLUDE = {"大楷", "中楷", "小楷"}
_MODEL_DERIVE_PATTERNS = []
_MODEL_AS_SIZE_OR_OPTION = re.compile(
    r"(?i)^(?:XXXL|XXL|XL|L|M|S)(?:-(?:XXXL|XXL|XL|L|M|S))?码?$"
    r"|^(?:小号|中号|大号|特大号|均码)$"
    r"|^(?:大楷|中楷|小楷|极细)$"
)

_CORE_CATEGORY_CANONICAL = {
    "礼品袋": "礼袋",
    "手提袋": "礼袋",
    "包装袋": "礼袋",
    "服装纸袋": "礼袋",
    "礼品盒": "礼盒",
    "包装盒": "礼盒",
    "喜糖盒": "礼盒",
    "喜糖礼盒": "礼盒",
    "一次性手套": "手套",
    "防护手套": "手套",
    "一次性桌布": "桌布",
    "餐桌布": "桌布",
    "方盘": "托盘",
    "发夹": "发饰",
    "发卡": "发饰",
    "头饰": "发饰",
    "红包": "红包袋",
    "利是封": "红包袋",
    "纸袋": "礼袋",
    "牛皮纸袋": "礼袋",
    "购物袋": "礼袋",
    "手提纸袋": "礼袋",
    "拉菲草": "拉菲草",
    "填充物": "拉菲草",
    "包装填充物": "拉菲草",
    "碎纸丝": "拉菲草",
    "搬家袋": "收纳袋",
    "编织袋": "收纳袋",
    "行李袋": "收纳袋",
    "储物袋": "收纳袋",
    "压缩袋": "真空压缩袋",
    "密实袋": "自封袋",
    "自粘袋": "自封袋",
    "OPP自粘袋": "自封袋",
    "食品袋": "食品包装袋",
    "食品纸袋": "食品包装袋",
    "打包袋": "食品包装袋",
    "外卖袋": "食品包装袋",
    "防油纸袋": "食品包装袋",
    "气柱袋": "气泡袋",
    "气泡膜": "气泡袋",
    "封口膜": "保鲜膜",
    "打包膜": "保鲜膜",
    "缠绕膜": "保鲜膜",
    "锡纸": "铝箔纸",
    "锡纸碗": "铝箔盒",
    "锡纸盒": "铝箔盒",
    "床垫/床褥": "床垫",
    "床褥": "床垫",
    "非医用口罩": "口罩",
    "一次性口罩": "口罩",
    "纸灯笼": "灯笼",
    "红灯笼": "灯笼",
    "数字气球": "气球",
    "铝膜气球": "气球",
    "铝箔气球": "气球",
    "生日蜡烛": "蜡烛",
    "数字蜡烛": "蜡烛",
    "酥油灯": "蜡烛",
    "装饰花环": "圣诞花环",
    "花环": "圣诞花环",
    "装饰挂件": "圣诞挂饰",
    "挂饰": "圣诞挂饰",
    "门贴": "贴纸",
    "喜字贴": "贴纸",
    "窗贴": "贴纸",
    "雨鞋": "雨靴",
    "水靴": "雨靴",
    "胶鞋": "雨靴",
    "棉拖鞋": "拖鞋",
    "男士拖鞋": "拖鞋",
    "女士拖鞋": "拖鞋",
    "儿童拖鞋": "拖鞋",
    "吊带连衣裙": "连衣裙",
    "腰带": "皮带",
    "腰带/皮带/腰链": "皮带",
    "裤带": "皮带",
    "遮阳帽": "防晒帽",
    "棒球帽": "帽子",
    "鸭舌帽": "帽子",
    "男士皮鞋": "皮鞋",
    "商务皮鞋": "皮鞋",
    "休闲鞋": "运动鞋",
    "老爹鞋": "运动鞋",
    "西梅汁": "果蔬汁饮料",
    "苹果汁": "果蔬汁饮料",
    "果蔬汁": "果蔬汁饮料",
    "定型喷雾": "发胶",
    "头发定型": "发胶",
    "手机贴膜": "手机膜",
    "宠物背包": "宠物包",
    "宠物便携包": "宠物包",
    "猫包": "宠物包",
    "狗包": "宠物包",
    "航空箱": "宠物航空箱",
    "网卡": "无线网卡",
    "wifi接收器": "无线网卡",
    "WiFi接收器": "无线网卡",
    "洗脸盆": "脸盆",
    "奶瓶清洗液": "奶瓶清洗剂",
    "情趣套装": "JK制服套装",
    "jk制服套装": "JK制服套装",
    "调味面制品": "辣条",
    "面筋制品": "辣条",
    "烤面筋": "辣条",
    "辣棒": "辣条",
    "薄荷糖": "糖果",
    "润喉糖": "糖果",
    "小鱼干": "鱼干",
    "长筒袜": "丝袜",
    "过膝袜": "丝袜",
    "美腿袜": "丝袜",
    "打底袜": "丝袜",
    "男士休闲鞋": "板鞋",
    "女靴": "马丁靴",
    "女士泳衣": "泳衣",
    "手机指套": "游戏指套",
    "储存卡": "内存卡",
}


def _model_should_be_size_or_empty(value: str) -> bool:
    return bool(_MODEL_AS_SIZE_OR_OPTION.fullmatch((value or "").strip()))


def _postprocess_model(raw_model: str, name: str, spec: str) -> str:
    """
    Make A型号 conservative:
    - Avoid treating calligraphy nib types like 中楷/大楷 as 型号.
    - Do not derive clothing size codes or option labels as 型号; those belong to A尺寸.
    """
    m = (raw_model or "").strip()
    if m in _MODEL_EXCLUDE or _model_should_be_size_or_empty(m):
        m = ""
    if m:
        return m
    text = f"{name} {spec}".strip()
    for pat, grp in _MODEL_DERIVE_PATTERNS:
        mm = pat.search(text)
        if mm:
            v = (mm.group(grp) or "").strip()
            # normalize casing for S/M/L/XL codes
            v = re.sub(r"(?i)^(xxxl|xxl|xl|l|m|s)码$", lambda x: x.group(1).upper() + "码", v)
            return v
    return ""


def _build_extraction_prompt(items) -> str:
    """与 Gemini/DeepSeek 相同的抽取说明，供所有模型复用。"""
    return f"""
    You are a senior retail product-operations expert and product attribute extractor.

    Return ONLY valid JSON that matches the provided response schema.
    Do NOT include any markdown, code fences, or explanations.

    Business goal: extract comparable-product fields for price comparison.
    We care about finding similar/comparable goods, not exact same brand SKUs.
    Brand, shop name, series slogans, celebrity/IP words, marketing adjectives,
    and gift/promotional words are noise unless they change the usable product.

    Field roles for later matching:
    - Strong fields: core_category, net_content, sell_quantity, packaging_unit,
      size, multidim_size. Extract them accurately and conservatively.
    - Weak ranking fields: brand, model, product_form, key_attributes, color.
      They help normalize and rank candidates only. Do NOT over-extract them
      in a way that would block comparable products.

    Optional category context may be provided as l1/l2/l3. Use it only to
    disambiguate the title/spec. Do not copy category names mechanically.
    Example: l1=宠物生活 and title=宠物洗澡免洗手套 => core_category=宠物手套,
    not 卫生巾, 手套, or 宠物美容用具.

    Specification name has priority over long marketing title for quantity,
    packaging unit, size, color, and option values. Example: title says 21支
    but spec says 口味随机*1条 => sell_quantity=1, packaging_unit=条.

    Extract fields for each item:
    1. net_content (A单件净含量): per-unit net content only, standardized units: ml / L / g / kg (e.g. 330ml, 1.5L, 18g). If unclear, empty.
       Do NOT compute total net content.
    2. sell_quantity (A售卖数量): numeric quantity ONLY (e.g. 24, 6, 7, 2, 1). If unclear, empty.
       Do NOT include packaging unit in sell_quantity. Wrong: "24罐"; correct: "24".
    3. packaging_unit (A包装单位): the packaging unit corresponding to sell_quantity. Choose ONE from:
       ["袋","盒","瓶","罐","桶","箱","听","杯","支","条","片","套","枚","个","只","包","件","根","张","双","副","板","组","卷","团","未知"].
       Examples:
       - 330ml*24罐/箱 => sell_quantity=24, packaging_unit=罐
       - 7片/包 => sell_quantity=7, packaging_unit=片
       - 1个 => sell_quantity=1, packaging_unit=个
       - 5片/包*2包 => sell_quantity=10, packaging_unit=片
       - title has 240mm*8片/包 and spec says 【5包】 => sell_quantity=40, packaging_unit=片
       For piece-count consumables such as 卫生巾/湿巾/尿垫, prefer total leaf-unit count (片/张/枚/条) over outer pack count.
       sell_quantity + packaging_unit will become A售卖规格 at rule level, but output them separately.
    4. color (A颜色): list of explicit colors only. Normalize one-character colors to xx色. Color is weak; do not infer from image/style/brand.
    5. size (A尺寸): one-dimensional size, length, capacity-related non-net dimensions, fit range, or size code.
       Examples: 240mm, 80-105cm, XL, M-L码, 20寸, 直径19cm.
       Do NOT put multi-axis dimensions like 17x25x8cm here; put those in multidim_size only.
       If a value contains x / × / * between two or three dimensions, it is multidim_size, not size.
    6. brand (A品牌): real product brand/trademark only. Weak ranking field, never a hard filter.
       Extract clear brands such as 可口可乐, 雪碧, 娃哈哈, 农夫山泉, 得力, 维达, 高洁丝, 杜蕾斯, 小米.
       Do NOT extract shop names, platform names, marketing words, IP/collaboration names, series names, flavors,
       function words, "品牌随机", "款式随机", "优选", "网红", "爆款", "同款", or uncertain words.
       If uncertain, empty. Brand should not affect core_category.
    7. model (A型号): true model/part identifier only (e.g. AB-123, X1, 货号). Do NOT put S/M/L/XL, 小号/大号, 单层/双层 here.
    8. core_category (A核心品类): concise controlled product noun, NOT a long title and NOT a broad platform category.
       It should answer: "what comparable product is this?"
       Good: 宠物手套, 裤型卫生巾, 热熔胶棒, 固体胶, 洗发水, 护发素, 沐浴露, 牙膏, 床品四件套, 自封袋, 手机壳.
       Bad: 宠物美容用具, 女士护理, 洗护美容, 日用品, 一次性用品, 6片/包, 片状, brand+series names.
       High-risk distinctions:
       - 宠物手套 / 宠物湿巾 must NOT become 裤型卫生巾 or 卫生巾.
       - 裤型卫生巾 / 安睡裤 / 安心裤 should normalize to 裤型卫生巾.
       - 猫条 is more comparable than broad 猫零食/宠物零食 when the title says 猫条.
       - 含乳饮料 / 乳饮料 are better than brand-series names like 营养快线.
       - 包装饮用水 is better than brand-series names or over-specific water source names for bottled water; put 纯净水/天然泉水 as weak key_attributes if useful.
       - 情趣丝袜, 打底袜, 美腿袜, 长筒袜 should normally normalize to 丝袜; put 长筒/过膝/薄款/蕾丝 as weak attributes.
       - 礼袋 and 礼盒 are different core categories; use spec to decide when title contains both.
       - If title and spec conflict, spec wins for the exact sellable SKU. Example title says 礼盒 but spec says 礼袋22x32x10cm => core_category=礼袋.
       - If a SKU is a real bundle containing different product nouns, use a bundle core_category such as 浴巾干发帽套装 instead of only one component.
       - 热熔胶棒 and 固体胶 are different core categories.
       - 洗发水, 护发素, 沐浴露, 洗面奶, 牙膏 are different core categories.
       - 纸尿裤, 拉拉裤, 护理垫 are different core categories.
       Avoid being too fine: brand, flavor, color, scent, pattern, and marketing style normally do not change core_category.
    9. product_form (A商品形态): concise physical/commercial form, not the category. Weak ranking field.
       Examples: 液体, 膏体, 粉末, 颗粒, 棒状, 片状, 袋装, 瓶装, 套装, 手套, 器具, 耗材, 配件.
    10. key_attributes (A关键属性词): weak comparable attributes only, such as 一次性, 双人, 单人, 四件套, 三件套, 儿童, 婴儿, 医用, 无糖, 低温, 常温, 超薄.
       Do not put brand, color, broad category, normal material, flavor, scent, style slogans, or duplicate core_category words here.
    11. multidim_size (A多维尺寸): multi-axis dimensions only, such as 17x25x8cm, 2.2x2.4m, 25x30cm, 50mmx20m.
        Any two or three numeric measures joined by x / × / * are multidim_size, not size.
        Exception: 280*10片 means size=280mm and sell_quantity=10, not multidim_size.
        For dimension goods such as 礼袋/收纳袋/宠物航空箱/宠物尿垫/干发帽, values like 25*65cm,
        48*32*30cm, 60*90cm must go to multidim_size. Do not duplicate them in size.

    Normalization rules:
    - Convert full-width to half-width where applicable.
    - Standardize units: 毫升/ml/ML->ml; 升/L/l->L; 克/g/G->g; 千克/公斤/kg->kg
    - Treat x/×/* as multipliers. Example: 330ml*24罐/箱 => net_content=330ml, sell_quantity=24, packaging_unit=罐

    Top-level JSON shape: {{"items": [ ... ]}} — same length and order as input.

    Examples (few-shot). Follow the same output style:
    Input:
    [
      {{"name":"【整箱】雪碧 碳酸饮料 330ml*24罐/箱","spec":"330ml*24罐/箱"}},
      {{"name":"高洁丝 纯棉240mm*7片/包 极薄卫生巾","spec":"7片/包"}},
      {{"name":"礼袋 1个 礼品包装","spec":"礼袋17x25x8cm*1个"}},
      {{"name":"KIMHOME 一次性宠物洗澡湿巾免洗手套猫咪清洁干洗宠物用品 6片/包","spec":"一次性宠物手套","l1":"宠物生活","l2":"洗护美容","l3":"宠物美容用具"}},
      {{"name":"子初 超薄蚕丝安睡裤M-L码 6片/包 防漏安心裤一次性裤型卫生巾(臀围80-105cm）","spec":"6片/包","l1":"个人洗护","l2":"女士护理","l3":"裤型卫生巾"}},
      {{"name":"胶棒热熔胶棒条7mm11mm胶水高粘透明热熔胶棒热熔胶枪棒胶","spec":"11mm"}},
      {{"name":"得力 透明手工固体胶棒 学生专用高颜值办公高粘固体胶 8g/个","spec":"8g/个"}},
      {{"name":"娃哈哈 营养快线 草莓风味水果牛奶饮料 500g*3瓶 早餐风味调制乳休闲饮品","spec":"500g*3瓶","l1":"饮品","l2":"饮料","l3":"含乳/乳酸菌饮料（常温）"}},
      {{"name":"美纹纸胶带美术分色纸（50mm*20m) 1卷/份 装修遮蔽喷漆保护瓷砖美缝无痕贴纸胶带","spec":"1卷/份"}},
      {{"name":"【口味可选】嘻适宝 宠物零食猫条 15g/条 宠物湿粮猫咪零食","spec":"金枪鱼味","l1":"宠物生活","l2":"宠物零食","l3":"猫零食"}},
      {{"name":"景田 饮用天然泉水 1.5L*12瓶/份 会议宴会饭店聚餐饮用水","spec":"1.5L*12瓶/份","l1":"饮品","l2":"饮料","l3":"包装饮用水"}},
      {{"name":"长筒黑丝袜女性感纯欲风夏季薄款过膝高筒大腿袜黑色半截蝴蝶结蕾丝网袜子/条","spec":"【黑丝花边】","l1":"成人用品","l2":"情趣内衣","l3":"情趣丝袜"}},
      {{"name":"【5包】Herlab她研社 卫生巾深藏BLUE棉柔日用姨妈巾240mm*8片/包","spec":"【5包】","l1":"个人洗护","l2":"女士护理","l3":"卫生巾/卫生棉"}}
    ]
    Output:
    {{
      "items":[
        {{"net_content":"330ml","sell_quantity":"24","packaging_unit":"罐","color":[],"size":[],"brand":"雪碧","model":"","core_category":"碳酸饮料","product_form":"液体","key_attributes":[],"multidim_size":[]}},
        {{"net_content":"","sell_quantity":"7","packaging_unit":"片","color":[],"size":["240mm"],"brand":"高洁丝","model":"","core_category":"卫生巾","product_form":"片状","key_attributes":["日用"],"multidim_size":[]}},
        {{"net_content":"","sell_quantity":"1","packaging_unit":"个","color":[],"size":[],"brand":"","model":"","core_category":"礼袋","product_form":"袋装","key_attributes":[],"multidim_size":["17x25x8cm"]}},
        {{"net_content":"","sell_quantity":"6","packaging_unit":"片","color":[],"size":[],"brand":"KIMHOME","model":"","core_category":"宠物手套","product_form":"手套","key_attributes":["一次性"],"multidim_size":[]}},
        {{"net_content":"","sell_quantity":"6","packaging_unit":"片","color":[],"size":["M-L码"],"brand":"子初","model":"","core_category":"裤型卫生巾","product_form":"片状","key_attributes":["一次性","超薄"],"multidim_size":[]}},
        {{"net_content":"","sell_quantity":"1","packaging_unit":"根","color":["透明"],"size":["11mm"],"brand":"","model":"","core_category":"热熔胶棒","product_form":"棒状","key_attributes":["热熔"],"multidim_size":[]}},
        {{"net_content":"8g","sell_quantity":"1","packaging_unit":"个","color":["透明"],"size":[],"brand":"得力","model":"","core_category":"固体胶","product_form":"棒状","key_attributes":["固体"],"multidim_size":[]}},
        {{"net_content":"500g","sell_quantity":"3","packaging_unit":"瓶","color":[],"size":[],"brand":"娃哈哈","model":"","core_category":"含乳饮料","product_form":"液体","key_attributes":[],"multidim_size":[]}},
        {{"net_content":"","sell_quantity":"1","packaging_unit":"卷","color":[],"size":[],"brand":"","model":"","core_category":"美纹纸胶带","product_form":"卷装","key_attributes":[],"multidim_size":["50mmx20m"]}},
        {{"net_content":"15g","sell_quantity":"1","packaging_unit":"条","color":[],"size":[],"brand":"嘻适宝","model":"","core_category":"猫条","product_form":"条状","key_attributes":[],"multidim_size":[]}},
        {{"net_content":"1.5L","sell_quantity":"12","packaging_unit":"瓶","color":[],"size":[],"brand":"景田","model":"","core_category":"包装饮用水","product_form":"液体","key_attributes":["天然泉水"],"multidim_size":[]}},
        {{"net_content":"","sell_quantity":"1","packaging_unit":"条","color":["黑色"],"size":[],"brand":"","model":"","core_category":"丝袜","product_form":"服饰","key_attributes":["长筒","过膝","薄款"],"multidim_size":[]}},
        {{"net_content":"","sell_quantity":"40","packaging_unit":"片","color":[],"size":["240mm"],"brand":"Herlab她研社","model":"","core_category":"卫生巾","product_form":"片状","key_attributes":["日用"],"multidim_size":[]}}
      ]
    }}

    Input items (JSON), keep output order exactly the same:
    {json.dumps(items, ensure_ascii=False, indent=2)}
    """


def _packaging_unit_from_sell_quantity(sell_quantity: str, default: str = "未知") -> str:
    s = (sell_quantity or "").strip()
    m = re.search(r"(袋|盒|瓶|罐|桶|箱|听|杯|碗|支|条|片|套|枚|粒|颗|个|只|包|件|份|把|本|台|床|顶|贴|块|卡|根|张|双|副|对|板|组|卷|团)$", s)
    unit = m.group(1) if m else (default or "未知")
    return unit if unit in ALLOWED_PACKAGING else "未知"


def _normalize_sell_quantity_value(sell_quantity: str) -> str:
    s = (sell_quantity or "").strip()
    if not s:
        return ""
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    return m.group(1) if m else ""


def _fill_packaging_from_sell_quantity(packaging_unit: str, sell_quantity: str) -> str:
    unit = (packaging_unit or "").strip()
    if unit and unit in ALLOWED_PACKAGING and unit != "未知":
        return unit
    return _packaging_unit_from_sell_quantity(sell_quantity, default=unit or "未知")


def _normalize_product_info(item: ProductInfo) -> ProductInfo:
    raw_sell = getattr(item, "sell_quantity", "") or ""
    item.sell_quantity = _normalize_sell_quantity_value(raw_sell)
    item.packaging_unit = _fill_packaging_from_sell_quantity(getattr(item, "packaging_unit", "") or "", raw_sell)
    core = (getattr(item, "core_category", "") or "").strip()
    if core:
        item.core_category = _CORE_CATEGORY_CANONICAL.get(core, core)
    item.brand = normalize_brand(getattr(item, "brand", "") or "")
    sizes = list(getattr(item, "size", []) or [])
    multidims = list(getattr(item, "multidim_size", []) or [])
    moved_multidims: list[str] = []
    kept_sizes: list[str] = []
    for value in sizes:
        v = (value or "").strip()
        if not v:
            continue
        if re.search(r"\d+(?:\.\d+)?\s*[x×*]\s*\d+", v, flags=re.IGNORECASE):
            moved_multidims.append(v.replace("×", "x").replace("*", "x"))
        else:
            kept_sizes.append(v)
    for v in moved_multidims:
        if v not in multidims:
            multidims.append(v)
    item.size = kept_sizes
    item.multidim_size = multidims
    model = (getattr(item, "model", "") or "").strip()
    if _model_should_be_size_or_empty(model):
        sizes = list(getattr(item, "size", []) or [])
        if model and model not in sizes:
            sizes.append(model)
        item.size = sizes
        item.model = ""
    return item


def _fallback_dimensions(text: str) -> list[str]:
    s = str(text or "")
    out: list[str] = []
    # 11mm*15cm / 2.2m*2.4m
    pat2_mixed = re.compile(
        r"(\d+(?:\.\d+)?)\s*(mm|cm|m)\s*[x×*]\s*(\d+(?:\.\d+)?)\s*(mm|cm|m)(?=$|[^A-Za-z0-9])",
        re.IGNORECASE,
    )
    mixed_spans: list[tuple[int, int]] = []
    for m in pat2_mixed.finditer(s):
        mixed_spans.append(m.span())
        out.append(f"{m.group(1)}{m.group(2).lower()}x{m.group(3)}{m.group(4).lower()}")
    # 18*16*10cm / 25×20×12 cm
    pat3 = re.compile(
        r"(\d+(?:\.\d+)?)\s*[x×*]\s*(\d+(?:\.\d+)?)\s*[x×*]\s*(\d+(?:\.\d+)?)\s*(mm|cm|m)(?=$|[^A-Za-z0-9])",
        re.IGNORECASE,
    )
    triple_spans: list[tuple[int, int]] = []
    for m in pat3.finditer(s):
        triple_spans.append(m.span())
        out.append(f"{m.group(1)}x{m.group(2)}x{m.group(3)}{m.group(4).lower()}")
    # 17x25cm / 10*20 cm
    pat2 = re.compile(
        r"(\d+(?:\.\d+)?)\s*[x×*]\s*(\d+(?:\.\d+)?)\s*(mm|cm|m)(?=$|[^A-Za-z0-9])",
        re.IGNORECASE,
    )
    for m in pat2.finditer(s):
        if any(not (m.end() <= a or m.start() >= b) for a, b in triple_spans + mixed_spans):
            continue
        out.append(f"{m.group(1)}x{m.group(2)}{m.group(3).lower()}")
    # Bags, storage goods and home textiles often omit the unit: 34*27*11, 180*120.
    # In this marketplace data those naked dimension groups are overwhelmingly cm.
    occupied = triple_spans[:] + mixed_spans[:]
    occupied.extend(m.span() for m in pat2.finditer(s))
    pat3_no_unit = re.compile(
        r"(?<![\dA-Za-z])(\d{2,3}(?:\.\d+)?)\s*[x×*]\s*(\d{2,3}(?:\.\d+)?)\s*[x×*]\s*(\d{1,3}(?:\.\d+)?)(?!\s*(?:片|张|枚|条|支|瓶|罐|包|袋|盒|个|只|套|团|对|g|kg|ml|l|cm|mm|m|[A-Za-z0-9]))",
        re.IGNORECASE,
    )
    for m in pat3_no_unit.finditer(s):
        if any(not (m.end() <= a or m.start() >= b) for a, b in occupied):
            continue
        occupied.append(m.span())
        out.append(f"{m.group(1)}x{m.group(2)}x{m.group(3)}cm")
    pat2_no_unit = re.compile(
        r"(?<![\dA-Za-z])(\d{2,3}(?:\.\d+)?)\s*[x×*]\s*(\d{2,3}(?:\.\d+)?)(?!\s*(?:片|张|枚|条|支|瓶|罐|包|袋|盒|个|只|套|团|对|g|kg|ml|l|cm|mm|m|[A-Za-z0-9]))",
        re.IGNORECASE,
    )
    for m in pat2_no_unit.finditer(s):
        if any(not (m.end() <= a or m.start() >= b) for a, b in occupied):
            continue
        occupied.append(m.span())
        out.append(f"{m.group(1)}x{m.group(2)}cm")
    single_size_pat = re.compile(r"(\d+(?:\.\d+)?)\s*(mm|cm|m)(?=$|[^A-Za-z0-9])", re.IGNORECASE)
    for m in single_size_pat.finditer(s):
        if any(not (m.end() <= a or m.start() >= b) for a, b in occupied):
            continue
        out.append(f"{m.group(1)}{m.group(2).lower()}")
    # 卫生巾等常见 “280*10片”：前一个数通常是长度 mm，后一个才是售卖数量。
    pat_pad = re.compile(r"(\d{2,4})\s*[x×*]\s*\d+\s*片")
    for m in pat_pad.finditer(s):
        out.append(f"{m.group(1)}mm")
    cleaned: list[str] = []
    seen: set[str] = set()
    for x in out:
        if x not in seen:
            seen.add(x)
            cleaned.append(x)
    return cleaned


def _fallback_sell_quantity(text: str, current: str, spec: str = "") -> str:
    s = str(text or "")
    sp = str(spec or "")
    current = "" if str(current or "").strip() in ("未知", "nan", "None", "null") else (current or "")
    leaf_units = "片|张|枚|粒|颗|条|支|个|只|对|贴"
    # Piece-count consumables: 5片/包*2包 should compare as 10片, not 2包.
    m = re.search(rf"(\d+(?:\.\d+)?)\s*({leaf_units})\s*/\s*包\s*[x×*]\s*(\d+(?:\.\d+)?)\s*包?", s)
    if m:
        total = float(m.group(1)) * float(m.group(3))
        total_s = str(int(total)) if total.is_integer() else str(total)
        return f"{total_s}{m.group(2)}"
    # Spec may only say 【5包】 while title contains 8片/包.
    m_outer = re.search(r"(\d+(?:\.\d+)?)\s*包", sp)
    m_inner = re.search(rf"(\d+(?:\.\d+)?)\s*({leaf_units})\s*/\s*包", s)
    if m_outer and m_inner:
        total = float(m_outer.group(1)) * float(m_inner.group(1))
        total_s = str(int(total)) if total.is_integer() else str(total)
        return f"{total_s}{m_inner.group(2)}"
    # 500ml/瓶*3、500ml/瓶×3 这类标题把单位放在斜杠前后，乘数后常省略单位。
    m = re.search(r"(?i)\d+(?:\.\d+)?\s*(?:g|kg|ml|l|克|千克|公斤|毫升|升)\s*/\s*(袋|盒|瓶|罐|桶|听|杯|碗|支|条|片|套|枚|粒|颗|个|只|包|件|份|把|本|台|床|顶|贴|块|卡|根|张|双|副|对|卷|团)\s*[x×*]\s*(\d+(?:\.\d+)?)\b", s)
    if m:
        return f"{m.group(2)}{m.group(1)}"
    m = re.search(r"(\d+(?:\.\d+)?)\s*对\s*/\s*份", s)
    if m:
        return f"{m.group(1)}对"
    for zh, num in (("一", "1"), ("两", "2"), ("二", "2"), ("三", "3"), ("四", "4"), ("五", "5")):
        m = re.search(rf"{zh}\s*(团|套|个|只|双|副|对|件|份|把|本|台|床|顶|贴|块|卡|包|袋|盒|瓶|罐|碗|杯|卷|张|片|枚|粒|颗|条|支)", s)
        if m:
            return f"{num}{m.group(1)}"
    # 规格名通常比长标题更接近真实 SKU。例：标题写“21支”，规格写“口味随机*1条”。
    for src in (sp, s):
        m = re.search(r"(?:^|[x×*])\s*(\d+(?:\.\d+)?)\s*(袋|盒|瓶|罐|桶|箱|听|杯|碗|支|条|片|套|枚|粒|颗|个|只|包|件|份|把|本|台|床|顶|贴|块|卡|根|张|双|副|对|板|组|卷|团)\b", src)
        if m:
            return f"{m.group(1)}{m.group(2)}"
    # Correct false merges such as 280*10片 -> 10片.
    m = re.search(r"\d{2,4}\s*[x×*]\s*(\d+(?:\.\d+)?)\s*(片|枚|个|只|包|袋|对|贴)\b", s)
    if m:
        return f"{m.group(1)}{m.group(2)}"
    # 20g/袋、400ml/瓶 这类单件规格默认是 1 个售卖单位。
    m = re.search(r"(?i)\d+(?:\.\d+)?\s*(?:g|kg|ml|l|克|千克|公斤|毫升|升)\s*/\s*(袋|盒|瓶|罐|桶|听|杯|碗|支|条|片|套|枚|粒|颗|个|只|包|件|份|把|本|台|床|顶|贴|块|卡|根|张|双|副|对|卷|团)\b", s)
    if m and (not current or re.fullmatch(r"\d+(?:\.\d+)?", str(current).strip())):
        qty = str(current).strip() if re.fullmatch(r"\d+(?:\.\d+)?", str(current).strip()) else "1"
        return f"{qty}{m.group(1)}"
    m = re.search(r"/\s*(袋|盒|瓶|罐|桶|箱|听|杯|碗|支|条|片|套|枚|粒|颗|个|只|包|件|份|把|本|台|床|顶|贴|块|卡|根|张|双|副|对|板|组|卷|团)\b", s)
    if m and (not current or re.fullmatch(r"\d+(?:\.\d+)?", str(current).strip())):
        qty = str(current).strip() if re.fullmatch(r"\d+(?:\.\d+)?", str(current).strip()) else "1"
        return f"{qty}{m.group(1)}"
    return current or ""


def _fallback_net_content(text: str, current: str) -> str:
    s = str(text or "")
    if re.search(r"(建议|适合|体重|斤以内|岁|码).{0,12}\d+(?:\.\d+)?\s*(?:斤|kg|千克|公斤)", s):
        current = ""
    patterns = [
        (re.compile(r"(?i)(\d+(?:\.\d+)?)\s*(?:千克|公斤|kg)\b"), "kg"),
        (re.compile(r"(?i)(\d+(?:\.\d+)?)\s*(?:毫升|ml)\b"), "ml"),
        (re.compile(r"(?i)(\d+(?:\.\d+)?)\s*(?:克|g)\b"), "g"),
        (re.compile(r"(?i)(\d+(?:\.\d+)?)\s*(?:升|l)\b"), "L"),
    ]
    for pat, unit in patterns:
        m = pat.search(s)
        if m:
            return f"{m.group(1)}{unit}"
    return current or ""


_CORE_CATEGORY_TERMS = (
    "一次性手套", "一次性内裤", "剃须刀套装", "头发定型喷雾", "冰箱除味剂",
    "调味面制品", "面筋制品", "烤面筋", "辣棒", "辣条", "薄荷糖", "润喉糖", "鱼干", "小鱼干", "果冻", "茶叶", "啤酒", "酸奶", "香油", "鱼饵",
    "茶饮料", "果汁饮料", "果蔬汁饮料", "西梅汁", "苹果汁", "果蔬汁", "格瓦斯",
    "真空压缩袋", "压缩袋", "自粘袋", "OPP自粘袋", "食品包装袋", "食品纸袋", "防油纸袋", "打包袋", "外卖袋",
    "气泡膜", "购物袋", "牛皮纸袋", "手提纸袋", "快递袋", "手提袋", "包装袋", "红包袋", "利是封", "红包",
    "数字气球", "铝膜气球", "铝箔气球", "气球", "气球支架", "气球链", "许愿牌", "祈愿牌", "南瓜灯", "南瓜桶",
    "孔明灯", "生日蜡烛", "数字蜡烛", "酥油灯", "蜡烛", "纸灯笼", "红灯笼", "灯笼",
    "圣诞花环", "装饰花环", "圣诞挂饰", "装饰挂件", "门贴", "喜字贴", "窗贴", "贴纸",
    "拉菲草", "包装填充物", "填充物", "碎纸丝", "编绳", "缎带", "丝带", "洒金纸", "剪纸纸张",
    "蒸笼纸", "蒸笼垫", "保鲜膜", "封口膜", "打包膜", "缠绕膜", "铝箔纸", "锡纸", "铝箔盒", "锡纸碗",
    "一次性手套", "防护手套", "手套", "一次性桌布", "餐桌布", "桌布", "方盘", "托盘", "气柱袋",
    "保温袋", "冰袋", "过滤袋", "滤网勺", "漏勺", "菜刀", "汤锅", "泡酒瓶", "储物瓶",
    "泳衣", "冲锋衣", "防晒衣", "雨衣", "雨靴", "雨鞋", "水靴", "泳镜", "马丁靴", "板鞋", "洞洞鞋", "拖鞋", "棉拖鞋", "儿童拖鞋",
    "连衣裙", "吊带连衣裙", "牛仔裤", "牛仔短裤", "短裤", "T恤", "t恤", "睡衣套装", "家居服套装", "高跟鞋", "帽子", "棒球帽", "鸭舌帽", "渔夫帽", "防晒帽", "遮阳帽", "皮带", "腰带", "发饰", "发夹", "发卡", "头饰", "皮鞋", "男士皮鞋", "商务皮鞋", "运动鞋", "休闲鞋", "老爹鞋",
    "抽纸", "扫把", "三角阀", "车位锁", "花瓶", "喂药器", "扑克牌", "卡牌", "收藏卡牌",
    "有线耳机", "耳钉", "耳棒", "防水贴", "伤口敷料", "内存卡", "存储卡", "游戏指套", "手机指套",
    "美纹纸胶带", "纸胶带",
    "热熔胶棒", "固体胶", "液体胶", "白乳胶", "美纹纸胶带", "纸胶带", "双面胶", "胶带", "胶水", "胶棒",
    "含乳饮料", "乳酸菌饮料", "乳饮料", "水果牛奶饮料", "水果酸奶饮品", "碳酸饮料", "汽水",
    "火鸡面", "方便面", "速食面", "拌面", "泡面",
    "洗发水", "护发素", "沐浴露", "洗面奶", "香皂", "肥皂", "牙膏", "牙刷", "漱口水",
    "洗衣液", "洗衣粉", "柔顺剂", "消毒液", "清洁剂", "空气清新剂",
    "裤型卫生巾", "卫生巾", "安睡裤", "安心裤", "卫生护垫",
    "姨妈巾",
    "床品四件套", "床品三件套", "四件套", "三件套", "床单", "被套", "枕套", "被子", "蚊帐",
    "自封袋", "密封袋", "密实袋", "收纳袋", "搬家袋", "编织袋", "行李袋", "储物袋", "收纳箱", "收纳盒", "纸箱", "礼品袋", "礼袋", "纸袋", "礼品盒", "礼盒", "喜糖盒", "气泡袋",
    "脸盆", "水桶", "保鲜盒", "水杯", "奶茶杯", "一次性杯子", "一次性餐具",
    "手机壳", "手机膜", "充电器", "数据线", "插排", "鼠标", "键盘",
    "秀丽笔", "自动铅笔", "铅笔", "无线网卡", "网卡",
    "奶瓶清洗剂", "料理盆", "不锈钢盆", "毛线", "牛奶棉", "JK制服套装", "jk制服套装",
    "干发帽", "浴巾", "毛巾",
    "床垫/床褥", "床垫", "床褥",
    "情趣丝袜", "蕾丝丝袜", "长筒过膝袜", "过膝袜", "丝袜",
    "纸尿裤", "拉拉裤", "护理垫", "避孕套", "润滑液", "宠物手套", "宠物湿巾", "宠物主粮", "宠物零食",
    "宠物航空箱", "航空箱", "宠物包", "便携包", "宠物围栏", "宠物栅栏", "宠物笼", "猫笼", "宠物尿垫", "猫砂盆", "宠物挡板",
    "膨润土猫砂", "豆腐猫砂", "猫砂", "猫条", "猫零食",
)

_KEY_ATTRIBUTE_TERMS = (
    "一次性", "双人", "单人", "四件套", "三件套", "儿童", "婴儿", "宝宝", "医用",
    "无糖", "0糖", "低糖", "低温", "常温", "加厚", "超薄", "极薄", "防水",
    "防滑", "热熔", "固体", "液体", "替换装", "补充装", "日用", "夜用",
)


def _fallback_core_category(text: str, fallback: str = "") -> str:
    compact = re.sub(r"\s+", "", str(text or ""))
    if "浴巾" in compact and "干发帽" in compact:
        return "浴巾干发帽套装"
    if any(x in compact for x in ("拉菲草", "填充物", "碎纸丝")):
        return "拉菲草"
    if "礼袋" in compact or "礼品袋" in compact:
        return "礼袋"
    if any(x in compact for x in ("食品包装袋", "食品纸袋", "防油纸袋")) or (
        any(x in compact for x in ("食品", "外卖", "小吃", "防油", "烘焙", "饼干"))
        and any(x in compact for x in ("包装袋", "打包袋", "密封袋", "纸袋"))
    ):
        return "食品包装袋"
    if any(x in compact for x in ("自封袋", "自粘袋", "OPP自粘袋", "密实袋")):
        return "自封袋"
    if any(x in compact for x in ("真空压缩袋", "压缩袋")) and any(x in compact for x in ("被子", "棉被", "衣物", "羽绒服", "抽真空")):
        return "真空压缩袋"
    if any(x in compact for x in ("搬家袋", "编织袋", "行李袋", "储物袋")) or ("收纳袋" in compact and not any(x in compact for x in ("食品", "外卖", "小吃", "防油"))):
        return "收纳袋"
    if "密封袋" in compact and not any(x in compact for x in ("礼品", "礼物", "礼盒")):
        return "自封袋"
    if "内裤" in compact:
        return "内裤"
    if "丝袜" in compact or "打底袜" in compact or "美腿袜" in compact or "长筒袜" in compact or "过膝袜" in compact:
        return "丝袜"
    if "袜" in compact:
        return "袜子"
    if any(x in compact for x in ("乌龙茶", "冰红茶", "绿茶")) and "茶" in compact:
        return "茶饮料"
    if any(x in compact for x in ("拉面", "泡面", "方便面", "速食面", "拌面")):
        return "方便面"
    if "猫条" in compact:
        return "猫条"
    if any(x in compact for x in ("裤型卫生巾", "安睡裤", "安心裤")):
        return "裤型卫生巾"
    if "营养快线" in compact or "水果牛奶饮料" in compact or "水果酸奶饮品" in compact:
        return "含乳饮料"
    if any(x in compact for x in ("饮用天然泉水", "饮用纯净水", "包装饮用水", "饮用水")):
        return "包装饮用水"
    if "礼盒" in compact or "礼品盒" in compact:
        return "礼盒"
    for term in sorted(_CORE_CATEGORY_TERMS, key=len, reverse=True):
        if term in compact:
            if term == "四件套":
                return "床品四件套" if any(x in compact for x in ("床", "被套", "床单", "枕套")) else term
            if term == "三件套":
                return "床品三件套" if any(x in compact for x in ("床", "被套", "床单", "枕套")) else term
            if term in ("安睡裤", "安心裤"):
                return "裤型卫生巾"
            if term == "姨妈巾":
                return "卫生巾"
            return _CORE_CATEGORY_CANONICAL.get(term, term)
    fb = (fallback or "").strip()
    if not fb:
        return ""
    canonical_fb = _CORE_CATEGORY_CANONICAL.get(fb)
    if canonical_fb:
        return canonical_fb
    if "/" in fb or "／" in fb:
        return ""
    if any(x in fb for x in ("用品", "服务", "配件", "工具", "用具", "其他", "其它")):
        return ""
    return fb if len(fb) <= 12 else ""


def _fallback_product_form(text: str, core_category: str, packaging_unit: str = "") -> str:
    s = str(text or "")
    core = core_category or ""
    if any(x in core for x in ("洗发水", "护发素", "沐浴露", "漱口水", "洗衣液", "柔顺剂", "消毒液", "清洁剂", "空气清新剂", "润滑液")):
        return "液体"
    if any(x in core for x in ("牙膏", "洗面奶")):
        return "膏体"
    if any(x in core for x in ("热熔胶棒", "固体胶", "胶棒")):
        return "棒状"
    if any(x in core for x in ("猫条",)):
        return "条状"
    if any(x in core for x in ("胶带", "双面胶")) and (packaging_unit or "").strip() == "卷":
        return "卷装"
    if any(x in core for x in ("卫生巾", "安睡裤", "安心裤", "护垫", "手机膜", "胶带", "双面胶", "护理垫", "宠物湿巾")):
        return "片状"
    if any(x in core for x in ("宠物手套",)):
        return "手套"
    if any(x in core for x in ("四件套", "三件套")) or "套装" in s:
        return "套装"
    if any(x in core for x in ("自封袋", "密封袋", "收纳袋", "礼品袋", "礼袋", "气泡袋")):
        return "袋装"
    if any(x in core for x in ("收纳箱", "收纳盒", "纸箱", "保鲜盒")):
        return "盒箱"
    if any(x in core for x in ("料理盆", "不锈钢盆", "脸盆", "猫砂盆")):
        return "盆"
    if any(x in core for x in ("充电器", "数据线", "插排", "鼠标", "键盘", "牙刷")):
        return "器具"
    if any(x in core for x in ("毛线", "牛奶棉")):
        return "线材"
    return {
        "瓶": "瓶装",
        "袋": "袋装",
        "盒": "盒装",
        "碗": "碗装",
        "罐": "罐装",
        "套": "套装",
        "片": "片状",
        "条": "条状",
        "卷": "卷装",
        "团": "团装",
        "把": "单件",
        "台": "单件",
        "床": "单件",
        "顶": "单件",
        "贴": "片状",
        "块": "块状",
    }.get((packaging_unit or "").strip(), "")


def _fallback_key_attributes(text: str) -> list[str]:
    s = str(text or "")
    out = [term for term in _KEY_ATTRIBUTE_TERMS if term in s]
    if "0糖" in out and "无糖" in out:
        out.remove("0糖")
    if "宝宝" in out and "婴儿" in out:
        out.remove("宝宝")
    return out


def _fallback_multidim_sizes(text: str) -> list[str]:
    s = str(text or "")
    out = [x for x in _fallback_dimensions(s) if "x" in x]
    # Some marketplace specs omit units on dimension goods, e.g. 猫砂盆「大号50*35」.
    # Infer cm only when the surrounding text is clearly a size-bearing durable good.
    if re.search(r"(尺寸|大号|小号|特大|猫砂盆|航空箱|宠物箱|尿垫|礼袋|收纳|干发帽|浴巾|盆)", s):
        for m in re.finditer(r"(?<!\d)(\d{2,3})\s*[x×*]\s*(\d{2,3})(?!\s*(?:片|张|枚|条|支|瓶|罐|包|袋|盒|个|只|套|团|g|kg|ml|l|cm|mm|m))", s, flags=re.IGNORECASE):
            v = f"{m.group(1)}x{m.group(2)}cm"
            if v not in out:
                out.append(v)
    return out


def _fallback_size_codes(text: str) -> list[str]:
    s = str(text or "")
    out: list[str] = []
    patterns = (
        re.compile(r"(?<![A-Za-z0-9])((?:XXXL|XXL|XL|L|M|S)(?:\s*-\s*(?:XXXL|XXL|XL|L|M|S))?\s*码?)(?![A-Za-z0-9])", re.IGNORECASE),
        re.compile(r"\b(均码)\b"),
        re.compile(r"\b(\d+(?:\.\d+)?\s*寸)\b"),
        re.compile(r"\b([A-H]\s*罩杯)\b", re.IGNORECASE),
        re.compile(r"(臀围\s*\d+(?:\.\d+)?\s*-\s*\d+(?:\.\d+)?\s*cm)", re.IGNORECASE),
        re.compile(r"(\d+(?:\.\d+)?\s*-\s*\d+(?:\.\d+)?\s*cm)", re.IGNORECASE),
    )
    for pat in patterns:
        for m in pat.finditer(s):
            v = re.sub(r"\s+", "", m.group(1).strip())
            if re.fullmatch(r"(?i)(?:XXXL|XXL|XL|L|M|S)(?:-(?:XXXL|XXL|XL|L|M|S))?码?", v):
                v = re.sub(r"(?i)(xxxl|xxl|xl|l|m|s)", lambda x: x.group(1).upper(), v)
            else:
                v = re.sub(r"(?i)(mm|cm|m)$", lambda x: x.group(1).lower(), v)
            if v and v not in out:
                out.append(v)
    out = [x for x in out if not any(x != y and y.endswith(x) for y in out)]
    return out


def _fallback_colors(text: str, current: list[str]) -> list[str]:
    s = str(text or "")
    out: list[str] = []
    for c in current or []:
        v = (c or "").strip()
        if len(v) == 1:
            v = f"{v}色"
        if v == "金色" and "金枪鱼" in s:
            continue
        if v and v not in out:
            out.append(v)
    color_pat = re.compile(r"(透明|米白|乳白|奶白|香槟|白色|黑色|灰色|银色|金色|红色|粉色|橙色|黄色|绿色|蓝色|紫色|棕色|咖色|白(?!菜|桃)|黑(?!椒)|灰|银|金(?!枪|桔|针菇)|红(?!枣|豆|茶)|粉(?!末|条)|橙(?!子)|黄(?!瓜|豆|油)|绿(?!茶|豆)|蓝(?!莓)|紫(?!菜|薯)|棕|咖)")
    for m in color_pat.finditer(s):
        v = m.group(1)
        if len(v) == 1:
            v = f"{v}色"
        if v == "金色" and "金枪鱼" in s:
            continue
        if v not in out:
            out.append(v)
    return [v for v in out if not any(v != other and other.startswith(v) for other in out)]


def _clean_fallback_sizes(raw_sizes: list[str], text: str) -> list[str]:
    size_codes = _fallback_size_codes(text)
    range_values = {x for x in size_codes if "-" in x and re.search(r"(?i)(mm|cm|m)$", x)}
    multidim_values = [x for x in _fallback_dimensions(text) if "x" in x]
    dims = []
    for x in _fallback_dimensions(text):
        if "x" in x:
            continue
        if any(x in rv and x != rv for rv in range_values):
            continue
        if any(x in md and x != md for md in multidim_values):
            continue
        dims.append(x)
    out: list[str] = []
    for x in raw_sizes or []:
        v = (x or "").strip()
        if not v:
            continue
        if "x" in v.lower() or "×" in v or "*" in v:
            continue
        if any(v in rv and v != rv for rv in range_values):
            continue
        if any(v in md and v != md for md in multidim_values):
            continue
        # Drop concatenation artifacts from strings like 18*16*10cm after "*" was stripped.
        if re.fullmatch(r"\d{5,}(?:mm|cm|m)", v, flags=re.IGNORECASE):
            continue
        out.append(v)
    out = dims + size_codes + out
    cleaned: list[str] = []
    seen: set[str] = set()
    for x in out:
        if x not in seen:
            seen.add(x)
            cleaned.append(x)
    return cleaned


def _fallback_model(text: str, current: str) -> str:
    cur = (current or "").strip()
    if cur:
        return cur
    s = str(text or "")
    for pat in (
        re.compile(r"(?:型号|货号|款号|model)\s*[:：]?\s*([A-Za-z0-9][A-Za-z0-9\-_/]{1,30})", re.IGNORECASE),
        re.compile(r"\b([A-Za-z]{1,6}[-_/]?\d{2,6}[A-Za-z0-9\-_/]{0,10})\b"),
    ):
        m = pat.search(s)
        if m:
            v = m.group(1).strip()
            if re.fullmatch(r"(?i)(xxxl|xxl|xl|l|m|s)码?", v):
                continue
            return v
    return ""


_KNOWN_BRAND_TERMS = (
    "Coca-Cola", "Coca Cola", "可口可乐", "百事可乐", "百事", "雪碧", "娃哈哈", "农夫山泉", "怡宝", "景田",
    "康师傅", "统一", "蒙牛", "伊利", "特仑苏", "光明", "三只松鼠", "百草味", "良品铺子", "奥利奥", "乐事",
    "溜溜梅", "劲仔", "得力", "晨光", "齐心", "维达", "洁柔", "清风", "心相印", "蓝月亮", "立白", "雕牌",
    "汰渍", "超能", "威露士", "滴露", "海飞丝", "清扬", "潘婷", "飘柔", "多芬", "舒肤佳", "云南白药",
    "高洁丝", "护舒宝", "苏菲", "子初", "Babycare", "babycare", "帮宝适", "好奇", "杜蕾斯", "冈本", "名流",
    "小米", "华为", "苹果", "品胜", "公牛", "飞利浦", "美的", "苏泊尔", "九阳", "雷达", "榄菊", "超威",
)

_BRAND_PREFIX_STOPWORDS = {
    "整箱", "规格可选", "颜色可选", "口味可选", "新款", "爆款", "网红", "品牌随机", "款式随机", "优选好物",
    "加厚", "超薄", "儿童适用", "实惠装", "买一送一", "送", "可选",
}


def _fallback_brand(text: str, current: str = "") -> str:
    cur = normalize_brand(current)
    if cur:
        return cur
    s = str(text or "").strip()
    compact = re.sub(r"\s+", "", s)
    if any(x in compact for x in ("品牌随机", "随机品牌", "不限品牌")):
        return ""
    for term in sorted(_KNOWN_BRAND_TERMS, key=len, reverse=True):
        if term and term in s:
            if term in {"苹果", "Apple"} and any(x in compact.lower() for x in ("适用苹果", "适配苹果", "兼容苹果", "for苹果", "forapple")):
                continue
            return normalize_brand(term)
    cleaned = re.sub(r"^【[^】]{1,20}】", "", s).strip()
    cleaned = re.sub(r"^\[[^\]]{1,20}\]", "", cleaned).strip()
    cleaned = re.sub(r"^[（(][^）)]{1,20}[）)]", "", cleaned).strip()
    m = re.match(r"^([A-Za-z][A-Za-z0-9&.\\-]{1,20}(?:[\\s·][\\u4e00-\\u9fffA-Za-z0-9]{1,12})?|[\\u4e00-\\u9fffA-Za-z][\\u4e00-\\u9fffA-Za-z0-9·&]{1,12})\\s+", cleaned)
    if not m:
        return ""
    candidate = m.group(1).strip()
    if candidate in _BRAND_PREFIX_STOPWORDS:
        return ""
    if re.search(r"(一次性|规格|颜色|口味|尺寸|新款|加厚|超薄|高弹|家用|儿童|成人|男女|通用|适用|款式)", candidate):
        return ""
    return normalize_brand(candidate)


def _heuristic_product_info(item) -> ProductInfo:
    if isinstance(item, dict):
        name = item.get("name", "")
        spec = item.get("spec", "")
        l3 = item.get("l3", "")
        l2 = item.get("l2", "")
    else:
        name = str(item or "")
        spec = ""
        l3 = ""
        l2 = ""
    keys = extract_product_keys(name=name, spec=spec)
    raw_text = f"{name or ''} {spec or ''}".strip()
    net_content = _fallback_net_content(raw_text, keys.net_content or "")
    raw_sell_quantity = _fallback_sell_quantity(raw_text, keys.sell_quantity or "", spec=spec or "")
    sell_quantity = _normalize_sell_quantity_value(raw_sell_quantity)
    packaging_unit = _packaging_unit_from_sell_quantity(raw_sell_quantity)
    color = _fallback_colors(raw_text, list(keys.colors or ()))
    size = _clean_fallback_sizes(list(keys.size or ()), raw_text)
    brand = _fallback_brand(raw_text)
    model = _fallback_model(raw_text, keys.model or "")
    category_fallback = keys.core_product_name or ""
    for candidate in (l3, l2):
        c = str(candidate or "").strip()
        if (category_fallback and len(category_fallback) <= 12) or not c or c in {"节日庆典用品", "其他生活日用", "其他手机配件", "其他美妆工具", "其他汽车配件"}:
            continue
        category_fallback = c
    core_category = _fallback_core_category(raw_text, category_fallback)
    product_form = _fallback_product_form(raw_text, core_category, packaging_unit)
    key_attributes = _fallback_key_attributes(raw_text)
    multidim_size = _fallback_multidim_sizes(raw_text)
    return ProductInfo(
        net_content=net_content,
        sell_quantity=sell_quantity,
        packaging_unit=packaging_unit,
        color=color,
        size=size,
        brand=brand,
        model=_postprocess_model(model, name or "", spec or ""),
        core_category=core_category,
        product_form=product_form,
        key_attributes=key_attributes,
        multidim_size=multidim_size,
    )


def _heuristic_batch(items, log_tag: str = "") -> list[ProductInfo]:
    out = [_heuristic_product_info(item) for item in items]
    non_empty_sell = sum(1 for x in out if (x.sell_quantity or "").strip())
    non_empty_net = sum(1 for x in out if (x.net_content or "").strip())
    non_empty_brand = sum(1 for x in out if (x.brand or "").strip())
    _ai_log(
        log_tag,
        f"本地规则兜底完成: 条数={len(out)} A售卖非空={non_empty_sell} A净含量非空={non_empty_net} A品牌非空={non_empty_brand}",
    )
    return _mark_extraction_source(out, RULE_EXTRACTION_SOURCE)


def _strip_markdown_json_fences(text: str) -> str:
    t = (text or "").strip()
    if not t.startswith("```"):
        return t
    lines = t.splitlines()
    lines = lines[1:]
    while lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


def _normalize_batch_dict_for_validate(data: dict) -> dict:
    if not isinstance(data, dict):
        return {"items": []}
    items = data.get("items")
    if not isinstance(items, list):
        return {"items": []}
    out_items = []
    for it in items:
        if not isinstance(it, dict):
            it = {}
        d = dict(it)
        pu = d.get("packaging_unit", "未知")
        if pu not in ALLOWED_PACKAGING:
            d["packaging_unit"] = "未知"
        for key in ("color", "size", "key_attributes", "multidim_size"):
            v = d.get(key)
            if v is None:
                d[key] = []
            elif isinstance(v, str):
                d[key] = [v.strip()] if str(v).strip() else []
            elif isinstance(v, list):
                d[key] = [str(x) for x in v if x is not None and str(x).strip()]
            else:
                d[key] = []
        for key in ("net_content", "sell_quantity", "brand", "model", "core_category", "product_form"):
            v = d.get(key, "")
            d[key] = "" if v is None else str(v).strip()
        d["brand"] = normalize_brand(d.get("brand", ""))
        d["packaging_unit"] = _fill_packaging_from_sell_quantity(d.get("packaging_unit", ""), d.get("sell_quantity", ""))
        d["sell_quantity"] = _normalize_sell_quantity_value(d.get("sell_quantity", ""))
        out_items.append(d)
    return {"items": out_items}
