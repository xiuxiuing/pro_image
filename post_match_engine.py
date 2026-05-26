# -*- coding: utf-8 -*-
"""
类目规则：在向量/条码产生命中候选后执行；与 BGE 拼串无关。

当前主路径使用 v3 结构：
{
  "v": 3,
  "rule_groups": [
    {
      "id": "...",
      "name": "...",
      "categories": {
        "l1": [...],
        "l2": [...],
        "l3": [...]
      },
      "metrics": {
        "net": {"en": true, "max_rel": 0.2},
        ...
      }
    }
  ]
}

规则真正按主店商品的美团三级类目命中；一级/二级仅用于 UI 展示与辅助筛选。
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Tuple

from a_field_normalizer import (
    ATTRIBUTE_SYNONYM_GROUPS,
    CORE_CATEGORY_SYNONYM_GROUPS,
    FORM_SYNONYM_GROUPS,
    MODEL_SYNONYM_GROUPS,
    BRAND_SYNONYM_GROUPS,
    core_category_conflict_pair,
    normalize_key_attributes,
    normalize_core_category,
    normalize_brand,
    normalize_model,
    normalize_product_form,
    split_a_tokens,
)
from utils import clean_text_value

# Excel / AI 列名
COL_CAT1 = "美团类目一级"
COL_ALIASES_CAT1 = ("美团一级类目", "一级类目", "美团类目1级", "美团1级类目")
COL_CAT2 = "美团类目二级"
COL_ALIASES_CAT2 = ("美团二级类目", "二级类目", "美团类目2级", "美团2级类目")
COL_CAT3 = "美团类目三级"
COL_ALIASES_CAT3 = ("美团三级类目", "三级类目", "美团类目3级", "美团3级类目")
COL_NET = "A单件净含量"
COL_SELL = "A售卖数量"
COL_PACK = "A包装单位"
COL_COLOR = "A颜色"
COL_SIZE = "A尺寸"
COL_MULTIDIM_SIZE = "A多维尺寸"
COL_MODEL = "A型号"
COL_BRAND = "A品牌"
COL_CORE = "A核心品类"
COL_FORM = "A商品形态"
COL_ATTRS = "A关键属性词"

_WEAK_RANKING_WEIGHTS = {
    "brand": 0.02,
    "model": 0.025,
    "product_form": 0.015,
    "key_attributes": 0.015,
    "color": 0.005,
}

_SENSITIVE_GATE_L1 = {"成人用品", "医疗器械"}

_METRIC_DEFAULTS: Dict[str, Dict[str, Any]] = {
    "core_conflict": {"en": True},
    "category_gate": {"en": False, "syn": CORE_CATEGORY_SYNONYM_GROUPS, "allow_core_pairs": []},
    "core": {"en": False, "syn": CORE_CATEGORY_SYNONYM_GROUPS},
    "cat3": {"en": True},
    "net": {"en": True, "max_rel": 0.2},
    "sell": {"en": True, "max_diff": 0.0},
    "pack": {
        "en": True,
        "syn": [
            [
                "瓶", "听", "支", "罐", "小瓶", "玻璃瓶", "PET瓶", "易拉罐", "铁罐", "易开罐",
                "杯", "碗", "杯装", "碗装", "塑杯", "纸杯", "件", "份",
            ],
            [
                "袋", "包", "小袋", "小包", "真空袋", "自立袋", "盒", "纸盒", "礼盒", "小盒", "方盒",
                "根", "条", "棒", "枚", "个", "只", "颗", "粒", "团", "把", "本", "台", "床", "顶", "贴", "块", "卡", "对",
            ],
        ],
    },
    "color": {
        "en": False,
        "syn": [
            [
                "黑色", "纯黑", "炭黑", "曜石黑", "哑光黑", "墨黑", "酷黑", "深灰", "铁灰", "烟灰", "高级灰", "碳灰", "中灰",
                "藏青", "藏蓝", "海军蓝", "午夜蓝", "深宝蓝", "墨蓝", "咖啡色", "深棕", "巧克力色", "焦糖色", "栗色", "古铜色",
                "墨绿", "墨玉绿", "森林绿", "军绿", "暗绿", "深翠", "深紫", "葡萄紫", "暗紫", "紫罗兰(深)", "魅紫",
                "酒红", "枣红", "暗红", "勃艮第红", "赭石色",
            ],
            [
                "白色", "纯白", "象牙白", "奶白", "米白", "珍珠白", "月光白", "浅灰", "银灰", "麻灰", "亮灰", "冰川灰",
                "米色", "杏色", "浅咖", "香槟金", "燕麦色", "浅黄", "驼色", "粉色", "浅粉", "樱花粉", "肉粉", "藕粉",
                "淡粉", "水粉", "浅蓝", "天蓝", "水鸟蓝", "冰蓝色", "淡蓝", "浅绿", "薄荷绿", "淡绿", "果绿", "嫩草绿",
                "透明", "无色", "全透", "磨砂透",
            ],
        ],
    },
    "size": {"en": True, "max_rel": 0.125},
    "multidim_size": {"en": False, "max_rel": 0.125},
    "product_form": {"en": False, "syn": FORM_SYNONYM_GROUPS},
    "key_attributes": {"en": False, "syn": ATTRIBUTE_SYNONYM_GROUPS},
    "model": {"en": False, "syn": MODEL_SYNONYM_GROUPS},
    "brand": {"en": False, "syn": BRAND_SYNONYM_GROUPS},
}


def get_builtin_default_template() -> Dict[str, Any]:
    """新结构默认返回空规则组模板。"""
    return {"v": 3, "rule_groups": []}


def _g(item: dict, keys: tuple) -> str:
    for k in keys:
        v = item.get(k)
        v = clean_text_value(v)
        if v is not None and str(v).strip() != "" and str(v).lower() not in ("nan", "none"):
            return str(v).strip()
    return ""


def get_cat1(item: dict) -> str:
    return _g(item, (COL_CAT1,) + COL_ALIASES_CAT1).strip()


def get_cat2(item: dict) -> str:
    return _g(item, (COL_CAT2,) + COL_ALIASES_CAT2).strip()


def get_cat3(item: dict) -> str:
    return _g(item, (COL_CAT3,) + COL_ALIASES_CAT3).strip()


def _norm_str(s: Any) -> str:
    if s is None:
        return ""
    t = str(clean_text_value(s)).strip()
    if t.lower() in ("nan", "none", "null"):
        return ""
    return t


def _uniq_text_list(values: Any) -> List[str]:
    out: List[str] = []
    seen = set()
    if not isinstance(values, list):
        return out
    for raw in values:
        v = _norm_str(raw)
        if not v or v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def _normalize_category_paths(raw_paths: Any) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    seen = set()
    if not isinstance(raw_paths, list):
        return out
    for item in raw_paths:
        if not isinstance(item, dict):
            continue
        l1 = _norm_str(item.get("l1"))
        l2 = _norm_str(item.get("l2"))
        l3 = _norm_str(item.get("l3"))
        if not l3:
            continue
        key = (l1, l2, l3)
        if key in seen:
            continue
        seen.add(key)
        out.append({"l1": l1, "l2": l2, "l3": l3})
    return out


def _parse_net(s: str) -> Optional[Tuple[str, float]]:
    s = _norm_str(s).lower()
    if not s:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)\s*(ml|l|g|kg)\b", s)
    if not m:
        return None
    v = float(m.group(1))
    u = m.group(2)
    if u == "ml":
        return ("ml", v)
    if u == "l":
        return ("ml", v * 1000.0)
    if u == "g":
        return ("g", v)
    if u == "kg":
        return ("g", v * 1000.0)
    return None


CategoryPattern = Tuple[Optional[str], Optional[str], Optional[str]]

_MASS_VOLUME_EQUIVALENT_CATEGORIES: Tuple[CategoryPattern, ...] = (
    ("饮品", None, None),
    ("乳品", None, None),
    ("酒类", None, None),
    ("雪糕/冰淇淋/食用冰", None, None),
    ("营养冲调", "乳饮冲调", None),
    ("营养冲调", "果饮/可可冲调", None),
    ("营养冲调", "蜂蜜冲饮", None),
    ("营养冲调", "速溶咖啡/咖啡豆/粉", "咖啡液"),
    ("粮油调味干货", "调味汁", None),
    ("粮油调味干货", "烹饪油", None),
    ("粮油调味干货", "调味酱", "沙拉酱"),
    ("粮油调味干货", "调味酱", "番茄酱/沙司"),
    ("粮油调味干货", "调味酱", "果酱"),
    ("粮油调味干货", "调味料", "椰蓉/椰浆"),
    ("速食/罐头", "方便罐头", None),
    ("速食/罐头", "方便速食", "即食汤"),
    ("速食/罐头", "方便速食", "即食粥"),
    ("个人洗护", None, None),
    ("家庭清洁", None, None),
    ("洗涤清洁", None, None),
    ("美容护肤", "男士护肤", None),
    ("美容护肤", "面部护理", None),
    ("美容护肤", "眼部护理", "眼部护理液/清洁液"),
    ("彩妆香水", "香水", None),
    ("彩妆香水", "面部彩妆", "粉底液/膏"),
    ("彩妆香水", "面部彩妆", "气垫BB/BB霜"),
    ("彩妆香水", "面部彩妆", "隔离/妆前"),
    ("彩妆香水", "面部彩妆", "定妆喷雾"),
    ("医疗器械", "居家护理", "皮肤消毒"),
    ("医疗器械", "居家护理", "医用美护"),
    ("宠物生活", "洗护美容", None),
    ("成人用品", "润滑/延时", None),
    ("母婴用品", "洗护清洁", None),
    ("母婴用品", "日常护理", "儿童口腔护理"),
    ("母婴用品", "日常护理", "儿童驱蚊用品"),
    ("母婴用品", "日常护理", "婴儿湿巾/纸巾"),
    ("汽车用品", "清洗保养", "玻璃水"),
    ("汽车用品", "清洗保养", "汽车用剂"),
    ("汽车用品", "清洗保养", "汽机油"),
    ("汽车用品", "清洗保养", "保养用品"),
)

_MASS_VOLUME_EQUIVALENT_EXCLUDES: Tuple[CategoryPattern, ...] = (
    ("休闲食品", None, None),
    ("粮油调味干货", "杂粮", None),
    ("粮油调味干货", "米类/面类", None),
    ("粮油调味干货", "烘焙材料", None),
    ("粮油调味干货", "干货", None),
    ("营养冲调", "谷物冲调", None),
    ("营养冲调", "速溶咖啡/咖啡豆/粉", "速溶咖啡"),
)


def _category_tuple(item: dict) -> Tuple[str, str, str]:
    return (_norm_str(get_cat1(item)), _norm_str(get_cat2(item)), _norm_str(get_cat3(item)))


def _matches_category_pattern(cat: Tuple[str, str, str], pattern: CategoryPattern) -> bool:
    return all(expected is None or actual == expected for actual, expected in zip(cat, pattern))


def _category_allows_mass_volume_equivalence(item: dict) -> bool:
    cat = _category_tuple(item)
    if any(_matches_category_pattern(cat, p) for p in _MASS_VOLUME_EQUIVALENT_EXCLUDES):
        return False
    return any(_matches_category_pattern(cat, p) for p in _MASS_VOLUME_EQUIVALENT_CATEGORIES)


def _net_values_match(
    qn: Tuple[str, float],
    hn: Tuple[str, float],
    max_rel: float,
    query_item: dict,
    hit_item: dict,
) -> bool:
    if qn[1] <= 0 or hn[1] <= 0:
        return True
    if qn[0] == hn[0]:
        rel = abs(hn[1] - qn[1]) / max(qn[1], 1e-9)
        return rel <= max_rel + 1e-9
    if {qn[0], hn[0]} != {"g", "ml"}:
        return False
    if _category_tuple(query_item) != _category_tuple(hit_item):
        return False
    if not (_category_allows_mass_volume_equivalence(query_item) and _category_allows_mass_volume_equivalence(hit_item)):
        return False
    rel = abs(hn[1] - qn[1]) / max(qn[1], 1e-9)
    return rel <= max_rel + 1e-9


def _parse_size_mm(s: str) -> Optional[float]:
    s = _norm_str(s).lower()
    if not s:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)\s*(mm|cm|m)\b", s)
    if not m:
        return None
    v = float(m.group(1))
    u = m.group(2)
    if u == "mm":
        return v
    if u == "cm":
        return v * 10.0
    if u == "m":
        return v * 1000.0
    return None


def _unit_to_mm(value: float, unit: str) -> Optional[float]:
    unit = (unit or "").lower()
    if unit == "mm":
        return value
    if unit == "cm":
        return value * 10.0
    if unit == "m":
        return value * 1000.0
    return None


def _parse_multidim_mm(s: str) -> Optional[Tuple[float, ...]]:
    text = _norm_str(s).lower()
    if not text:
        return None
    expr_pat = re.compile(
        r"\d+(?:\.\d+)?\s*(?:mm|cm|m)?(?:\s*[x×*]\s*\d+(?:\.\d+)?\s*(?:mm|cm|m)?)+",
        re.IGNORECASE,
    )
    part_pat = re.compile(r"(\d+(?:\.\d+)?)\s*(mm|cm|m)?", re.IGNORECASE)
    for expr in expr_pat.findall(text):
        parts = [(float(m.group(1)), (m.group(2) or "").lower()) for m in part_pat.finditer(expr)]
        if len(parts) < 2:
            continue
        default_unit = ""
        for _value, unit in reversed(parts):
            if unit:
                default_unit = unit
                break
        if not default_unit:
            continue
        dims: List[float] = []
        ok = True
        for value, unit in parts:
            mm = _unit_to_mm(value, unit or default_unit)
            if mm is None or mm <= 0:
                ok = False
                break
            dims.append(mm)
        if ok and len(dims) >= 2:
            return tuple(sorted(dims))
    return None


def _multidim_values_match(q_dims: Tuple[float, ...], h_dims: Tuple[float, ...], max_rel: float) -> bool:
    if len(q_dims) != len(h_dims):
        return False
    for q, h in zip(q_dims, h_dims):
        if q <= 0 or h <= 0:
            continue
        if abs(h - q) / max(q, 1e-9) > max_rel + 1e-9:
            return False
    return True


# 与 extract 中「X罐 / X瓶…」的售卖件数一致，避免取到 330ml 里的 330
_SELL_UNITS = "罐|瓶|包|个|条|片|袋|盒|听|杯|碗|支|件|份|枚|粒|颗|只|把|本|台|床|顶|贴|块|卡|根|张|双|副|对|团"
_SELL_NUM_BEFORE_UNIT = re.compile(
    rf"(\d+(?:\.\d+)?)\s*(?:{_SELL_UNITS})",
    re.IGNORECASE,
)


def _parse_sell_num(s: str) -> Optional[float]:
    s = _norm_str(s)
    if not s:
        return None
    # 优先：紧邻「罐/瓶/…」前的数量（如 330ml*24罐、7片/包、1罐）
    matches = _SELL_NUM_BEFORE_UNIT.findall(s)
    if matches:
        return float(matches[-1])
    # 退化：无单位时取首个数字（纯「24」等）
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return None
    return float(m.group(1))


def _synonym_map(groups: Any) -> Dict[str, str]:
    m: Dict[str, str] = {}
    if not isinstance(groups, list):
        return m
    for g in groups:
        if not isinstance(g, list) or not g:
            continue
        rep = _norm_str(g[0])
        if not rep:
            continue
        for x in g:
            k = _norm_str(x)
            if k:
                m[k] = rep
    return m


def _normalize_syn_groups(groups: Any) -> List[List[str]]:
    out: List[List[str]] = []
    if not isinstance(groups, list):
        return out
    for group in groups:
        if not isinstance(group, list):
            continue
        items = _uniq_text_list(group)
        if items:
            out.append(items)
    return out


def _apply_syn(s: str, smap: Dict[str, str]) -> str:
    t = _norm_str(s)
    if not t:
        return ""
    return smap.get(t, t)


def _split_color_tokens(s: str) -> List[str]:
    if not s:
        return []
    parts = re.split(r"[,，|/、\s]+", s)
    return [p.strip() for p in parts if p.strip()]


def _color_sig(s: str, smap: Dict[str, str]) -> str:
    toks = sorted(_apply_syn(t, smap) for t in _split_color_tokens(s) if t)
    return "|".join(toks)


def _core_norm(s: str, custom_syn: Any = None) -> str:
    raw = _norm_str(s)
    if not raw:
        return ""
    custom_map = _synonym_map(custom_syn or [])
    if custom_map:
        mapped = _apply_syn(raw, custom_map)
        if mapped != raw:
            return mapped
    return normalize_core_category(raw)


def _core_compatible(a: str, b: str, custom_syn: Any = None) -> bool:
    na = _core_norm(a, custom_syn)
    nb = _core_norm(b, custom_syn)
    return not (na and nb) or na == nb


def _normalize_allow_core_pairs(raw: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    out: List[Dict[str, Any]] = []
    seen = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        pair = {
            "main_l2": _norm_str(item.get("main_l2")),
            "candidate_l2": _norm_str(item.get("candidate_l2")),
            "main_l3": _norm_str(item.get("main_l3")),
            "candidate_l3": _norm_str(item.get("candidate_l3")),
            "core": _uniq_text_list(item.get("core")),
            "bidirectional": bool(item.get("bidirectional", True)),
        }
        if not (pair["main_l2"] or pair["candidate_l2"] or pair["main_l3"] or pair["candidate_l3"]):
            continue
        key = (
            pair["main_l2"],
            pair["candidate_l2"],
            pair["main_l3"],
            pair["candidate_l3"],
            tuple(pair["core"]),
            pair["bidirectional"],
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(pair)
    return out


def _allow_core_pair_matches(
    pair: Dict[str, Any],
    q2: str,
    h2: str,
    q3: str,
    h3: str,
    core_norm: str,
    custom_syn: Any = None,
) -> bool:
    def _side_matches(main_l2: str, candidate_l2: str, main_l3: str, candidate_l3: str) -> bool:
        if main_l2 and main_l2 != q2:
            return False
        if candidate_l2 and candidate_l2 != h2:
            return False
        if main_l3 and main_l3 != q3:
            return False
        if candidate_l3 and candidate_l3 != h3:
            return False
        return True

    cores = [_core_norm(x, custom_syn) for x in (pair.get("core") or [])]
    cores = [x for x in cores if x]
    if cores and core_norm not in cores:
        return False

    main_l2 = _norm_str(pair.get("main_l2"))
    candidate_l2 = _norm_str(pair.get("candidate_l2"))
    main_l3 = _norm_str(pair.get("main_l3"))
    candidate_l3 = _norm_str(pair.get("candidate_l3"))
    if _side_matches(main_l2, candidate_l2, main_l3, candidate_l3):
        return True
    if pair.get("bidirectional", True) and _side_matches(candidate_l2, main_l2, candidate_l3, main_l3):
        return True
    return False


def _category_gate_pass(query_item: dict, hit_item: dict, config: Optional[Dict[str, Any]]) -> tuple[bool, str, Dict[str, Any]]:
    """V2 category gate: same cat3 OR allowlisted cross-category pair with same normalized core."""
    config = config or {}
    q1 = _norm_str(get_cat1(query_item))
    h1 = _norm_str(get_cat1(hit_item))
    q2 = _norm_str(get_cat2(query_item))
    h2 = _norm_str(get_cat2(hit_item))
    q3 = _norm_str(get_cat3(query_item))
    h3 = _norm_str(get_cat3(hit_item))
    qc = _g(query_item, (COL_CORE,))
    hc = _g(hit_item, (COL_CORE,))
    qn = _core_norm(qc, config.get("syn"))
    hn = _core_norm(hc, config.get("syn"))
    values = {
        "main_cat1": q1,
        "candidate_cat1": h1,
        "main_cat2": q2,
        "candidate_cat2": h2,
        "main_cat3": q3,
        "candidate_cat3": h3,
        "main_core": qc,
        "candidate_core": hc,
        "main_core_norm": qn,
        "candidate_core_norm": hn,
        "allow_core_pair": None,
    }
    if q1 and h1 and q1 != h1 and ({q1, h1} & _SENSITIVE_GATE_L1):
        return False, "敏感一级类目跨类", values
    if q3 and h3 and q3 == h3:
        return True, "三级类目一致", values
    if qn and hn and qn == hn:
        for pair in _normalize_allow_core_pairs(config.get("allow_core_pairs")):
            if _allow_core_pair_matches(pair, q2, h2, q3, h3, qn, config.get("syn")):
                values["allow_core_pair"] = pair
                return True, "三级不同但命中核心品类白名单", values
        return False, "三级不同且未命中核心品类白名单", values
    return False, "三级不同且核心品类不同", values


def _norm_with_custom_syn(value: str, syn: Any = None) -> str:
    raw = _norm_str(value)
    if not raw:
        return ""
    smap = _synonym_map(syn or [])
    return _apply_syn(raw, smap) if smap else raw


def weak_ranking_score(query_item: dict, hit_item: dict, block: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Weak A-field ranking signal. This never rejects candidates; it only gives a
    small deterministic bonus used to order candidates that already came from
    vector recall and still pass strong post-match rules.
    """
    block = block or {}
    details: Dict[str, Any] = {}
    total = 0.0

    r = block.get("brand") or _METRIC_DEFAULTS.get("brand") or {}
    qb = normalize_brand(_norm_with_custom_syn(_g(query_item, (COL_BRAND,)), r.get("syn")))
    hb = normalize_brand(_norm_with_custom_syn(_g(hit_item, (COL_BRAND,)), r.get("syn")))
    brand_hit = bool(qb and hb and qb == hb)
    if brand_hit:
        total += _WEAK_RANKING_WEIGHTS["brand"]
    details["brand"] = {"main": qb, "candidate": hb, "matched": brand_hit, "bonus": _WEAK_RANKING_WEIGHTS["brand"] if brand_hit else 0.0}

    r = block.get("model") or _METRIC_DEFAULTS.get("model") or {}
    qm = normalize_model(_norm_with_custom_syn(_g(query_item, (COL_MODEL,)), r.get("syn")))
    hm = normalize_model(_norm_with_custom_syn(_g(hit_item, (COL_MODEL,)), r.get("syn")))
    model_hit = bool(qm and hm and qm == hm)
    if model_hit:
        total += _WEAK_RANKING_WEIGHTS["model"]
    details["model"] = {"main": qm, "candidate": hm, "matched": model_hit, "bonus": _WEAK_RANKING_WEIGHTS["model"] if model_hit else 0.0}

    r = block.get("product_form") or _METRIC_DEFAULTS.get("product_form") or {}
    qf = normalize_product_form(_norm_with_custom_syn(_g(query_item, (COL_FORM,)), r.get("syn")))
    hf = normalize_product_form(_norm_with_custom_syn(_g(hit_item, (COL_FORM,)), r.get("syn")))
    form_hit = bool(qf and hf and qf == hf)
    if form_hit:
        total += _WEAK_RANKING_WEIGHTS["product_form"]
    details["product_form"] = {"main": qf, "candidate": hf, "matched": form_hit, "bonus": _WEAK_RANKING_WEIGHTS["product_form"] if form_hit else 0.0}

    r = block.get("key_attributes") or _METRIC_DEFAULTS.get("key_attributes") or {}
    qa = [_norm_with_custom_syn(x, r.get("syn")) for x in normalize_key_attributes(_g(query_item, (COL_ATTRS,)))]
    ha = [_norm_with_custom_syn(x, r.get("syn")) for x in normalize_key_attributes(_g(hit_item, (COL_ATTRS,)))]
    qset = {x for x in qa if x}
    hset = {x for x in ha if x}
    aset = qset & hset
    attr_ratio = (len(aset) / max(len(qset | hset), 1)) if qset and hset else 0.0
    attr_bonus = _WEAK_RANKING_WEIGHTS["key_attributes"] * attr_ratio
    total += attr_bonus
    details["key_attributes"] = {
        "main": sorted(qset),
        "candidate": sorted(hset),
        "matched": sorted(aset),
        "overlap_ratio": attr_ratio,
        "bonus": attr_bonus,
    }

    r = block.get("color") or _METRIC_DEFAULTS.get("color") or {}
    smap = _synonym_map(r.get("syn") or [])
    qc = {_apply_syn(x, smap) for x in split_a_tokens(_g(query_item, (COL_COLOR,))) if x}
    hc = {_apply_syn(x, smap) for x in split_a_tokens(_g(hit_item, (COL_COLOR,))) if x}
    cset = {x for x in (qc & hc) if x}
    color_ratio = (len(cset) / max(len(qc | hc), 1)) if qc and hc else 0.0
    color_bonus = _WEAK_RANKING_WEIGHTS["color"] * color_ratio
    total += color_bonus
    details["color"] = {
        "main": sorted(qc),
        "candidate": sorted(hc),
        "matched": sorted(cset),
        "overlap_ratio": color_ratio,
        "bonus": color_bonus,
    }

    return {"bonus": round(total, 6), "details": details}


def _normalize_metric(metric_key: str, raw: Any) -> Dict[str, Any]:
    base = dict(_METRIC_DEFAULTS.get(metric_key, {"en": False}))
    if not isinstance(raw, dict):
        return base
    out = dict(base)
    out["en"] = bool(raw.get("en", base.get("en", False)))
    if metric_key in ("net", "size", "multidim_size"):
        try:
            out["max_rel"] = float(raw.get("max_rel", base.get("max_rel", 0.0)) or 0.0)
        except Exception:
            out["max_rel"] = float(base.get("max_rel", 0.0))
    elif metric_key == "sell":
        try:
            out["max_diff"] = float(raw.get("max_diff", base.get("max_diff", 0.0)) or 0.0)
        except Exception:
            out["max_diff"] = float(base.get("max_diff", 0.0))
    elif metric_key in ("cat3", "core_conflict"):
        pass
    elif metric_key == "category_gate":
        out["syn"] = _normalize_syn_groups(raw.get("syn"))
        out["allow_core_pairs"] = _normalize_allow_core_pairs(raw.get("allow_core_pairs"))
    elif metric_key in ("pack", "color", "brand", "model", "core", "product_form", "key_attributes"):
        out["syn"] = _normalize_syn_groups(raw.get("syn"))
    else:
        out["syn"] = _normalize_syn_groups(raw.get("syn"))
    return out


def _normalize_rule_group(raw: Any, idx: int) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None
    categories = raw.get("categories")
    if not isinstance(categories, dict):
        categories = raw.get("category_scope")
    categories = categories if isinstance(categories, dict) else {}
    paths = _normalize_category_paths(categories.get("paths"))
    l1 = _uniq_text_list(categories.get("l1"))
    l2 = _uniq_text_list(categories.get("l2"))
    l3 = _uniq_text_list(categories.get("l3"))
    if paths:
        l1 = _uniq_text_list([p.get("l1") for p in paths] + l1)
        l2 = _uniq_text_list([p.get("l2") for p in paths] + l2)
        l3 = _uniq_text_list([p.get("l3") for p in paths] + l3)
    if not l3:
        return None
    raw_metrics = raw.get("metrics") if isinstance(raw.get("metrics"), dict) else {}
    metrics = {k: _normalize_metric(k, raw_metrics.get(k)) for k in _METRIC_DEFAULTS.keys()}
    name = _norm_str(raw.get("name")) or f"规则组 {idx + 1}"
    gid = _norm_str(raw.get("id")) or f"group_{idx + 1:03d}"
    return {
        "id": gid,
        "name": name,
        "categories": {"paths": paths, "l1": l1, "l2": l2, "l3": l3},
        "metrics": metrics,
    }


def _upgrade_v1_template(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    仅用于兜底读取旧结构，便于本地现有数据不阻塞新页面与运行时。
    旧结构会被转换成若干按三级类目作用未知的“一级类目组”，实际运行时只会在存在三级类目列表时生效；
    因此旧结构在新模型下默认视为无生效规则。
    """
    if not isinstance(raw, dict):
        return get_builtin_default_template()
    default_metrics = {}
    old_default = raw.get("default") if isinstance(raw.get("default"), dict) else {}
    for key in _METRIC_DEFAULTS.keys():
        default_metrics[key] = _normalize_metric(key, old_default.get(key))
    groups = []
    by_cat1 = raw.get("by_cat1") if isinstance(raw.get("by_cat1"), dict) else {}
    for idx, (cat1, block) in enumerate(by_cat1.items()):
        metrics = {}
        block = block if isinstance(block, dict) else {}
        for key in _METRIC_DEFAULTS.keys():
            metrics[key] = _normalize_metric(key, block.get(key))
        groups.append({
            "id": f"legacy_group_{idx + 1:03d}",
            "name": f"旧版导入 - {_norm_str(cat1) or ('规则组 ' + str(idx + 1))}",
            "categories": {"paths": [], "l1": [_norm_str(cat1)] if _norm_str(cat1) else [], "l2": [], "l3": []},
            "metrics": metrics,
        })
    # 不将旧 default 自动作用到全部类目，避免新模型下出现隐式全局规则。
    return {"v": 3, "rule_groups": groups, "_legacy_default_metrics": default_metrics}


def normalize_template(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """归一化为 v3 结构。"""
    if not raw or not isinstance(raw, dict):
        return get_builtin_default_template()
    if int(raw.get("v") or 0) == 3 or "rule_groups" in raw:
        groups = []
        for idx, item in enumerate(raw.get("rule_groups") or []):
            group = _normalize_rule_group(item, idx)
            if group:
                groups.append(group)
        return {"v": 3, "rule_groups": groups}
    return _upgrade_v1_template(raw)


def template_from_db_json(s: str) -> Dict[str, Any]:
    if not (s or "").strip():
        return get_builtin_default_template()
    try:
        d = json.loads(s)
        return normalize_template(d) if isinstance(d, dict) else get_builtin_default_template()
    except Exception:
        return get_builtin_default_template()


def get_rule_group_for_item(template: Dict[str, Any], query_item: dict) -> Optional[Dict[str, Any]]:
    t = normalize_template(template)
    cat3 = _norm_str(get_cat3(query_item))
    if not cat3:
        return None
    for group in t.get("rule_groups") or []:
        l3 = group.get("categories", {}).get("l3") or []
        if cat3 in l3:
            return group
    return None


def rules_for_item(template: Dict[str, Any], query_item: dict) -> Optional[Dict[str, Any]]:
    group = get_rule_group_for_item(template, query_item)
    if not group:
        return None
    return group.get("metrics") or {}


def should_accept_post_match(query_item: dict, hit_item: dict, block: Optional[Dict[str, Any]]) -> bool:
    """
    对单条规则块（六维）做与过滤。未命中任何规则组时 block 为 None，直接放过。
    某一维关闭则跳过该维。除「售卖数量 sell」外，解析失败时多默认放过，尽量避免脏数据误杀；
    sell 在两侧若均有文本则须能解析且落在阈值内；仅一侧有值则拦截。
    """
    if not block:
        return True

    # 1. high-risk core conflict table (V2 guardrail)
    r = block.get("core_conflict") or {}
    if r.get("en", False):
        qc = _g(query_item, (COL_CORE,))
        hc = _g(hit_item, (COL_CORE,))
        if core_category_conflict_pair(qc, hc):
            return False

    # 2. V2 category gate: same cat3 OR allowlisted same-core cross-category pair.
    r = block.get("category_gate") or {}
    if r.get("en", False):
        passed, _, _ = _category_gate_pass(query_item, hit_item, r)
        if not passed:
            return False

    # 3. core category (advanced optional)
    r = block.get("core") or {}
    if r.get("en", False):
        qc = _g(query_item, (COL_CORE,))
        hc = _g(hit_item, (COL_CORE,))
        if qc and hc and not _core_compatible(qc, hc, r.get("syn")):
            return False

    # 4. cat3
    r = block.get("cat3") or {}
    if r.get("en", False):
        q3 = _norm_str(get_cat3(query_item))
        h3 = _norm_str(get_cat3(hit_item))
        if q3 and h3 and q3 != h3:
            return False

    # 5. net
    r = block.get("net") or {}
    if r.get("en", False):
        max_rel = float(r.get("max_rel", 0.2) or 0.0)
        qn = _parse_net(_g(query_item, (COL_NET,)))
        hn = _parse_net(_g(hit_item, (COL_NET,)))
        if qn and hn and not _net_values_match(qn, hn, max_rel, query_item, hit_item):
            return False

    # 6. sell
    r = block.get("sell") or {}
    if r.get("en", False):
        md = float(r.get("max_diff", 0.0) or 0.0)
        qs = _g(query_item, (COL_SELL,))
        hs = _g(hit_item, (COL_SELL,))
        qv = _parse_sell_num(qs)
        hv = _parse_sell_num(hs)
        has_q, has_h = bool(qs), bool(hs)
        if not has_q and not has_h:
            pass
        elif (has_q and not has_h) or (has_h and not has_q):
            return False
        else:
            if qv is None or hv is None:
                return False
            if abs(hv - qv) > md + 1e-9:
                return False

    # 7. pack
    r = block.get("pack") or {}
    if r.get("en", False):
        smap = _synonym_map(r.get("syn") or [])
        a = _apply_syn(_g(query_item, (COL_PACK,)), smap)
        b = _apply_syn(_g(hit_item, (COL_PACK,)), smap)
        if a and b and a != b:
            return False

    # 8. color
    r = block.get("color") or {}
    if r.get("en", False):
        smap = _synonym_map(r.get("syn") or [])
        sa = _color_sig(_g(query_item, (COL_COLOR,)), smap)
        sb = _color_sig(_g(hit_item, (COL_COLOR,)), smap)
        if sa and sb and sa != sb:
            return False

    # 9. size
    r = block.get("size") or {}
    if r.get("en", False):
        max_rel = float(r.get("max_rel", 0.125) or 0.0)
        qs = _parse_size_mm(_g(query_item, (COL_SIZE,)))
        hs = _parse_size_mm(_g(hit_item, (COL_SIZE,)))
        if qs is not None and qs > 0 and hs is not None and hs > 0:
            rel = abs(hs - qs) / max(qs, 1e-9)
            if rel > max_rel + 1e-9:
                return False

    # 10. multidim size
    r = block.get("multidim_size") or {}
    if r.get("en", False):
        max_rel = float(r.get("max_rel", 0.125) or 0.0)
        qs = _parse_multidim_mm(_g(query_item, (COL_MULTIDIM_SIZE,)))
        hs = _parse_multidim_mm(_g(hit_item, (COL_MULTIDIM_SIZE,)))
        if qs is not None and hs is not None and not _multidim_values_match(qs, hs, max_rel):
            return False

    # 11. model
    r = block.get("model") or {}
    if r.get("en", False):
        smap = _synonym_map(r.get("syn") or [])
        a = _apply_syn(_g(query_item, (COL_MODEL,)), smap)
        b = _apply_syn(_g(hit_item, (COL_MODEL,)), smap)
        if a and b and a != b:
            return False

    return True


def explain_post_match(query_item: dict, hit_item: dict, block: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Explain the same post-match decision made by should_accept_post_match().
    The final accepted value must stay equivalent to should_accept_post_match().
    """
    metric_order = ("core_conflict", "category_gate", "core", "cat3", "net", "sell", "pack", "size", "multidim_size", "color", "model")
    if not block:
        return {
            "accepted": True,
            "reason": "未命中规则组，后验规则放过",
            "metrics": [
                {"key": k, "enabled": False, "passed": True, "reason": "维度未启用", "config": {}}
                for k in metric_order
            ],
        }

    metrics: List[Dict[str, Any]] = []

    def add(key: str, enabled: bool, passed: bool, reason: str, config: Optional[Dict[str, Any]] = None, values: Optional[Dict[str, Any]] = None):
        item = {
            "key": key,
            "enabled": bool(enabled),
            "passed": bool(passed),
            "reason": reason,
            "config": config or {},
        }
        if values is not None:
            item["values"] = values
        metrics.append(item)

    r = block.get("core_conflict") or {}
    if r.get("en", False):
        qc = _g(query_item, (COL_CORE,))
        hc = _g(hit_item, (COL_CORE,))
        pair = core_category_conflict_pair(qc, hc)
        passed = pair is None
        add(
            "core_conflict",
            True,
            passed,
            "未命中高风险冲突表" if passed else "命中高风险核心品类冲突表",
            r,
            {"main": qc, "candidate": hc, "conflict_pair": list(pair) if pair else []},
        )
    else:
        add("core_conflict", False, True, "维度未启用", r)

    r = block.get("category_gate") or {}
    if r.get("en", False):
        passed, reason, values = _category_gate_pass(query_item, hit_item, r)
        add("category_gate", True, passed, reason, r, values)
    else:
        add("category_gate", False, True, "维度未启用", r)

    r = block.get("core") or {}
    if r.get("en", False):
        qc = _g(query_item, (COL_CORE,))
        hc = _g(hit_item, (COL_CORE,))
        qn = _core_norm(qc, r.get("syn"))
        hn = _core_norm(hc, r.get("syn"))
        passed = _core_compatible(qc, hc, r.get("syn"))
        add(
            "core",
            True,
            passed,
            "核心品类一致或缺失放过" if passed else "核心品类不同",
            r,
            {"main": qc, "candidate": hc, "main_norm": qn, "candidate_norm": hn},
        )
    else:
        add("core", False, True, "维度未启用", r)

    r = block.get("cat3") or {}
    if r.get("en", False):
        q3 = _norm_str(get_cat3(query_item))
        h3 = _norm_str(get_cat3(hit_item))
        passed = not (q3 and h3 and q3 != h3)
        add("cat3", True, passed, "三级类目一致" if passed else "三级类目不同", r, {"main": q3, "candidate": h3})
    else:
        add("cat3", False, True, "维度未启用", r)

    r = block.get("net") or {}
    if r.get("en", False):
        max_rel = float(r.get("max_rel", 0.2) or 0.0)
        q_raw = _g(query_item, (COL_NET,))
        h_raw = _g(hit_item, (COL_NET,))
        qn = _parse_net(q_raw)
        hn = _parse_net(h_raw)
        passed = True
        reason = "净含量缺失或无法解析，按现有规则放过"
        if qn and hn:
            passed = _net_values_match(qn, hn, max_rel, query_item, hit_item)
            reason = "净含量在阈值内" if passed else "净含量超过阈值"
        add("net", True, passed, reason, r, {"main": q_raw, "candidate": h_raw, "max_rel": max_rel})
    else:
        add("net", False, True, "维度未启用", r)

    r = block.get("sell") or {}
    if r.get("en", False):
        md = float(r.get("max_diff", 0.0) or 0.0)
        qs = _g(query_item, (COL_SELL,))
        hs = _g(hit_item, (COL_SELL,))
        qv = _parse_sell_num(qs)
        hv = _parse_sell_num(hs)
        has_q, has_h = bool(qs), bool(hs)
        if not has_q and not has_h:
            passed, reason = True, "两侧售卖数量均为空，按现有规则放过"
        elif (has_q and not has_h) or (has_h and not has_q):
            passed, reason = False, "仅一侧有售卖数量"
        elif qv is None or hv is None:
            passed, reason = False, "售卖数量无法解析"
        else:
            passed = abs(hv - qv) <= md + 1e-9
            reason = "售卖数量差值在阈值内" if passed else "售卖数量差值超过阈值"
        add("sell", True, passed, reason, r, {"main": qs, "candidate": hs, "main_num": qv, "candidate_num": hv, "max_diff": md})
    else:
        add("sell", False, True, "维度未启用", r)

    r = block.get("pack") or {}
    if r.get("en", False):
        smap = _synonym_map(r.get("syn") or [])
        qa = _g(query_item, (COL_PACK,))
        hb = _g(hit_item, (COL_PACK,))
        a = _apply_syn(qa, smap)
        b = _apply_syn(hb, smap)
        passed = not (a and b and a != b)
        add("pack", True, passed, "包装单位一致或缺失放过" if passed else "包装单位不同", r, {"main": qa, "candidate": hb, "main_norm": a, "candidate_norm": b})
    else:
        add("pack", False, True, "维度未启用", r)

    r = block.get("color") or {}
    if r.get("en", False):
        smap = _synonym_map(r.get("syn") or [])
        qa = _g(query_item, (COL_COLOR,))
        hb = _g(hit_item, (COL_COLOR,))
        sa = _color_sig(qa, smap)
        sb = _color_sig(hb, smap)
        passed = not (sa and sb and sa != sb)
        add("color", True, passed, "颜色一致或缺失放过" if passed else "颜色不同", r, {"main": qa, "candidate": hb, "main_norm": sa, "candidate_norm": sb})
    else:
        add("color", False, True, "维度未启用", r)

    r = block.get("size") or {}
    if r.get("en", False):
        max_rel = float(r.get("max_rel", 0.125) or 0.0)
        qs_raw = _g(query_item, (COL_SIZE,))
        hs_raw = _g(hit_item, (COL_SIZE,))
        qs = _parse_size_mm(qs_raw)
        hs = _parse_size_mm(hs_raw)
        if qs is not None and qs > 0 and hs is not None and hs > 0:
            rel = abs(hs - qs) / max(qs, 1e-9)
            passed = rel <= max_rel + 1e-9
            reason = "尺寸差异在阈值内" if passed else "尺寸差异超过阈值"
        else:
            rel = None
            passed, reason = True, "尺寸缺失或无法解析，按现有规则放过"
        add("size", True, passed, reason, r, {"main": qs_raw, "candidate": hs_raw, "relative_diff": rel, "max_rel": max_rel})
    else:
        add("size", False, True, "维度未启用", r)

    r = block.get("multidim_size") or {}
    if r.get("en", False):
        max_rel = float(r.get("max_rel", 0.125) or 0.0)
        qs_raw = _g(query_item, (COL_MULTIDIM_SIZE,))
        hs_raw = _g(hit_item, (COL_MULTIDIM_SIZE,))
        qs = _parse_multidim_mm(qs_raw)
        hs = _parse_multidim_mm(hs_raw)
        if qs is not None and hs is not None:
            rels = [
                abs(h - q) / max(q, 1e-9)
                for q, h in zip(qs, hs)
            ] if len(qs) == len(hs) else None
            passed = _multidim_values_match(qs, hs, max_rel)
            reason = "多维尺寸差异在阈值内" if passed else "多维尺寸差异超过阈值"
        else:
            rels = None
            passed, reason = True, "多维尺寸缺失或无法解析，按现有规则放过"
        add(
            "multidim_size",
            True,
            passed,
            reason,
            r,
            {"main": qs_raw, "candidate": hs_raw, "main_dims": qs, "candidate_dims": hs, "relative_diffs": rels, "max_rel": max_rel},
        )
    else:
        add("multidim_size", False, True, "维度未启用", r)

    r = block.get("model") or {}
    if r.get("en", False):
        smap = _synonym_map(r.get("syn") or [])
        qa = _g(query_item, (COL_MODEL,))
        hb = _g(hit_item, (COL_MODEL,))
        a = _apply_syn(qa, smap)
        b = _apply_syn(hb, smap)
        passed = not (a and b and a != b)
        add("model", True, passed, "型号一致或缺失放过" if passed else "型号不同", r, {"main": qa, "candidate": hb, "main_norm": a, "candidate_norm": b})
    else:
        add("model", False, True, "维度未启用", r)

    accepted = all((not m.get("enabled")) or bool(m.get("passed")) for m in metrics)
    failed = [m for m in metrics if m.get("enabled") and not m.get("passed")]
    return {
        "accepted": accepted,
        "reason": "后验规则放过" if accepted else "；".join(m.get("reason", "") for m in failed if m.get("reason")),
        "metrics": metrics,
    }


def summarize_template(template: Dict[str, Any]) -> Dict[str, int]:
    t = normalize_template(template)
    groups = t.get("rule_groups") or []
    l3_count = 0
    enabled_metric_total = 0
    for group in groups:
        l3_count += len(group.get("categories", {}).get("l3") or [])
        enabled_metric_total += sum(1 for v in (group.get("metrics") or {}).values() if isinstance(v, dict) and v.get("en"))
    return {
        "group_count": len(groups),
        "category3_count": l3_count,
        "enabled_metric_total": enabled_metric_total,
    }
