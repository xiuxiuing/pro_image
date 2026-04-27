# -*- coding: utf-8 -*-
"""
后验规则：在向量/条码产生命中候选后执行；与 BGE 拼串无关。

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
COL_MODEL = "A型号"

_METRIC_DEFAULTS: Dict[str, Dict[str, Any]] = {
    "cat3": {"en": True},
    "net": {"en": True, "max_rel": 0.2},
    "sell": {"en": True, "max_diff": 0.0},
    "pack": {
        "en": True,
        "syn": [
            [
                "瓶", "听", "支", "罐", "小瓶", "玻璃瓶", "PET瓶", "易拉罐", "铁罐", "易开罐",
                "杯", "杯装", "塑杯", "纸杯", "件",
            ],
            [
                "袋", "包", "小袋", "小包", "真空袋", "自立袋", "盒", "纸盒", "礼盒", "小盒", "方盒",
                "根", "条", "棒", "枚", "个", "只", "颗", "粒",
            ],
        ],
    },
    "color": {
        "en": True,
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
    "model": {"en": True, "syn": []},
}


def get_builtin_default_template() -> Dict[str, Any]:
    """新结构默认返回空规则组模板。"""
    return {"v": 3, "rule_groups": []}


def _g(item: dict, keys: tuple) -> str:
    for k in keys:
        v = item.get(k)
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
    t = str(s).strip()
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


def _parse_sell_num(s: str) -> Optional[float]:
    s = _norm_str(s)
    if not s:
        return None
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


def _normalize_metric(metric_key: str, raw: Any) -> Dict[str, Any]:
    base = dict(_METRIC_DEFAULTS.get(metric_key, {"en": False}))
    if not isinstance(raw, dict):
        return base
    out = dict(base)
    out["en"] = bool(raw.get("en", base.get("en", False)))
    if metric_key in ("net", "size"):
        try:
            out["max_rel"] = float(raw.get("max_rel", base.get("max_rel", 0.0)) or 0.0)
        except Exception:
            out["max_rel"] = float(base.get("max_rel", 0.0))
    elif metric_key == "sell":
        try:
            out["max_diff"] = float(raw.get("max_diff", base.get("max_diff", 0.0)) or 0.0)
        except Exception:
            out["max_diff"] = float(base.get("max_diff", 0.0))
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
    某一维关闭则跳过该维。解析失败时默认放过，尽量避免脏数据误杀。
    """
    if not block:
        return True

    # 1. net
    r = block.get("cat3") or {}
    if r.get("en", False):
        q3 = _norm_str(get_cat3(query_item))
        h3 = _norm_str(get_cat3(hit_item))
        if q3 and h3 and q3 != h3:
            return False

    # 2. net
    r = block.get("net") or {}
    if r.get("en", False):
        max_rel = float(r.get("max_rel", 0.2) or 0.0)
        qn = _parse_net(_g(query_item, (COL_NET,)))
        hn = _parse_net(_g(hit_item, (COL_NET,)))
        if qn and hn and qn[0] == hn[0] and qn[1] > 0 and hn[1] > 0:
            rel = abs(hn[1] - qn[1]) / max(qn[1], 1e-9)
            if rel > max_rel + 1e-9:
                return False
        if qn and hn and qn[0] != hn[0]:
            return False

    # 3. sell
    r = block.get("sell") or {}
    if r.get("en", False):
        md = float(r.get("max_diff", 0.0) or 0.0)
        qv = _parse_sell_num(_g(query_item, (COL_SELL,)))
        hv = _parse_sell_num(_g(hit_item, (COL_SELL,)))
        if qv is not None and hv is not None and abs(hv - qv) > md + 1e-9:
            return False

    # 4. pack
    r = block.get("pack") or {}
    if r.get("en", False):
        smap = _synonym_map(r.get("syn") or [])
        a = _apply_syn(_g(query_item, (COL_PACK,)), smap)
        b = _apply_syn(_g(hit_item, (COL_PACK,)), smap)
        if a and b and a != b:
            return False

    # 5. color
    r = block.get("color") or {}
    if r.get("en", False):
        smap = _synonym_map(r.get("syn") or [])
        sa = _color_sig(_g(query_item, (COL_COLOR,)), smap)
        sb = _color_sig(_g(hit_item, (COL_COLOR,)), smap)
        if sa and sb and sa != sb:
            return False

    # 6. size
    r = block.get("size") or {}
    if r.get("en", False):
        max_rel = float(r.get("max_rel", 0.125) or 0.0)
        qs = _parse_size_mm(_g(query_item, (COL_SIZE,)))
        hs = _parse_size_mm(_g(hit_item, (COL_SIZE,)))
        if qs is not None and qs > 0 and hs is not None and hs > 0:
            rel = abs(hs - qs) / max(qs, 1e-9)
            if rel > max_rel + 1e-9:
                return False

    # 7. model
    r = block.get("model") or {}
    if r.get("en", False):
        smap = _synonym_map(r.get("syn") or [])
        a = _apply_syn(_g(query_item, (COL_MODEL,)), smap)
        b = _apply_syn(_g(hit_item, (COL_MODEL,)), smap)
        if a and b and a != b:
            return False

    return True


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
