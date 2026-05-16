from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Dict, Iterable, List, Optional

from utils import clean_text_value


REQUIRED_STANDARD_FIELDS = [
    "skuId",
    "商品名称",
    "规格名称",
    "主图链接",
    "商品条码",
    "美团类目一级",
    "美团类目二级",
    "美团类目三级",
    "活动价",
    "原价",
    "月销量",
]

RULE_ATTRIBUTE_FIELDS = [
    "A单件净含量",
    "A售卖数量",
    "A包装单位",
    "A颜色",
    "A尺寸",
    "A型号",
]

FIELD_ALIASES: Dict[str, List[str]] = {
    "skuId": [
        "skuId", "SKUID", "skuid", "SkuId", "sku_id", "SKU ID", "SKU", "sku",
        "商品SKU", "商品sku", "商品SKU ID", "商品skuid", "美团SKU", "美团skuid",
        "规格ID", "规格id", "规格编码", "优购哆SKU", "主档SKU", "主档SKU（覆盖过）",
    ],
    "商品名称": [
        "商品名称", "菜单名", "名称", "商品名", "产品名称", "品名", "商品标题", "标题",
        "菜品名称", "外卖商品名称", "A商品名称", "商品", "货品名称", "物品名称",
    ],
    "规格名称": [
        "规格名称", "规格", "规格名", "售卖规格", "商品规格", "SKU规格", "sku规格",
        "规格属性", "属性", "包装规格", "A规格", "销售规格", "商品规格名称",
    ],
    "主图链接": [
        "主图链接", "图片", "商品图片", "主图", "首图", "图片链接", "商品图", "商品主图",
        "主图URL", "图片URL", "URL", "url", "image", "image_url", "pic", "pic_url",
        "img", "img_url", "商品图片链接", "商品主图链接",
    ],
    "商品条码": [
        "商品条码", "条码", "商品条形码", "条形码", "EAN", "ean", "UPC", "upc",
        "barcode", "Barcode", "国际条码", "69码",
    ],
    "美团类目一级": [
        "美团类目一级", "一级类目", "一级分类", "美团一级分类", "类目一级", "后台一级类目",
        "美团一级类目", "美团类目1级", "美团1级类目", "美团一级", "美团一级分类名称",
    ],
    "美团类目二级": [
        "美团类目二级", "二级类目", "二级分类", "美团二级分类", "类目二级", "后台二级类目",
        "美团二级类目", "美团类目2级", "美团2级类目", "美团二级", "美团二级分类名称",
    ],
    "美团类目三级": [
        "美团类目三级", "三级类目", "三级分类", "美团三级分类", "类目三级", "后台三级类目",
        "美团三级类目", "美团类目3级", "美团3级类目", "美团分类三级", "美团三级",
        "美团三级分类名称",
    ],
    "活动价": [
        "活动价", "单件折扣价", "折扣价", "活动价格", "优惠价", "促销价", "到手价",
        "现价", "售价", "新活动价", "折后价", "实际售价", "商品活动价",
    ],
    "原价": [
        "原价", "单件原价", "美团外卖渠道售价", "渠道价格", "渠道价", "划线价",
        "门店价", "零售价", "销售价", "新售价", "商品原价", "平台售价",
    ],
    "月销量": [
        "月销量", "销售", "销量", "销售量", "月售", "月销售量", "近30日销量",
        "30天销量", "销量（月）", "销售量（单）", "月销", "售卖量", "近30天销量",
    ],
    "A单件净含量": ["A单件净含量", "单件净含量", "净含量", "容量", "规格净含量", "A净含量", "单瓶容量"],
    "A售卖数量": ["A售卖数量", "售卖数量", "销售数量", "包装数量", "件数", "瓶数", "数量", "A数量", "售卖件数"],
    "A包装单位": ["A包装单位", "包装单位", "售卖单位", "单位", "计量单位", "A单位", "包装形式"],
    "A颜色": ["A颜色", "颜色", "色号", "色系", "A色", "商品颜色"],
    "A尺寸": ["A尺寸", "尺寸", "规格尺寸", "长宽高", "尺码", "大小", "A尺码", "商品尺寸"],
    "A型号": ["A型号", "型号", "货号", "款号", "model", "Model", "A款号", "商品型号"],
}

INTERNAL_FIELD_MAP = {"月销量": "销售"}


def canonical_storage_field(standard_field: str) -> str:
    return INTERNAL_FIELD_MAP.get(standard_field, standard_field)


def _norm_header(value) -> str:
    text = clean_text_value(value)
    if text is None:
        return ""
    return str(text).strip()


def _loose_key(value) -> str:
    text = _norm_header(value).lower()
    return re.sub(r"[\s_\-()/（）【】\\[\\]{}:：.．]+", "", text)


def build_field_mappings() -> Dict[str, str]:
    mappings = {}
    for standard, aliases in FIELD_ALIASES.items():
        target = canonical_storage_field(standard)
        for alias in aliases:
            mappings[alias] = target
    return mappings


def detect_field_mapping(
    headers: Iterable,
    user_mappings: Optional[Dict[str, str]] = None,
    standards: Optional[Iterable[str]] = None,
) -> Dict[str, dict]:
    header_list = [_norm_header(h) for h in headers if _norm_header(h)]
    loose_to_header = {_loose_key(h): h for h in header_list}
    wanted = list(standards or (REQUIRED_STANDARD_FIELDS + RULE_ATTRIBUTE_FIELDS))
    user_mappings = user_mappings or {}
    result: Dict[str, dict] = {}

    for standard in wanted:
        chosen = ""
        source = "missing"
        confidence = 0.0
        user_col = _norm_header(user_mappings.get(standard))
        if user_col and user_col in header_list:
            chosen, source, confidence = user_col, "user", 1.0
        else:
            for alias in FIELD_ALIASES.get(standard, [standard]):
                key = _loose_key(alias)
                if key in loose_to_header:
                    chosen, source, confidence = loose_to_header[key], "builtin", 1.0
                    break
        suggestions = []
        if not chosen:
            target_key = _loose_key(standard)
            scored = []
            for header in header_list:
                score = SequenceMatcher(None, target_key, _loose_key(header)).ratio()
                if score >= 0.58:
                    scored.append((score, header))
            suggestions = [
                {"column": h, "confidence": round(score, 2)}
                for score, h in sorted(scored, reverse=True)[:3]
            ]
            if suggestions:
                source = "suggested"
                confidence = suggestions[0]["confidence"]
        result[standard] = {
            "standard": standard,
            "column": chosen,
            "storage_field": canonical_storage_field(standard),
            "source": source,
            "confidence": confidence,
            "suggestions": suggestions,
        }
    return result
