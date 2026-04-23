# -*- coding: utf-8 -*-
"""
后验规则：在向量/条码产生候选 (主店行, 竞对行) 后执行；与 BGE 拼串无关。
七维 + 按美团一级类覆盖 (by_cat1)；无覆盖时使用 default。

配置短键名（与 UI 导出的 JSON 一致）:
  cat3, net, sell, pack, color, size, model
  enabled -> "en", max_rel, max_diff, syn (同义分组 [["听","罐"], ...])
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Tuple

# Excel / AI 列名
COL_CAT3 = "美团类目三级"
COL_ALIASES_CAT3 = ("美团三级类目", "三级类目", "美团类目三级", "美团类目3级")
COL_CAT1 = "美团类目一级"
COL_ALIASES_CAT1 = ("美团一级类目", "一级类目", "美团类目1级", "美团1级类目")
COL_NET = "A单件净含量"
COL_SELL = "A售卖数量"
COL_PACK = "A包装单位"
COL_COLOR = "A颜色"
COL_SIZE = "A尺寸"
COL_MODEL = "A型号"

_CANON = {
    "cat3": "cat3",
    "net": "net",
    "sell": "sell",
    "pack": "pack",
    "color": "color",
    "size": "size",
    "model": "model",
}

_BUILTIN_DEFAULT: Dict[str, Any] = {
    "v": 1,
    "default": {
        "cat3": {"en": True},
        "net": {"en": True, "max_rel": 0.2},
        "sell": {"en": True, "max_diff": 0.0},
        "pack": {"en": True, "syn": []},
        "color": {"en": True, "syn": []},
        "size": {"en": True, "max_rel": 0.125},
        "model": {"en": True, "syn": []},
    },
    "by_cat1": {},
}


def get_builtin_default_template() -> Dict[str, Any]:
    return json.loads(json.dumps(_BUILTIN_DEFAULT))


def _g(item: dict, keys: tuple) -> str:
    for k in keys:
        v = item.get(k)
        if v is not None and str(v).strip() != "" and str(v).lower() not in ("nan", "none"):
            return str(v).strip()
    return ""


def get_cat1(item: dict) -> str:
    v = _g(item, (COL_CAT1,) + COL_ALIASES_CAT1)
    return v.strip()


def get_cat3(item: dict) -> str:
    v = _g(item, (COL_CAT3,) + COL_ALIASES_CAT3)
    return v.strip()


def _norm_str(s: str) -> str:
    if s is None:
        return ""
    t = str(s).strip()
    if t.lower() in ("nan", "none", "null"):
        return ""
    return t


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


def _synonym_map(groups: List[List[str]]) -> Dict[str, str]:
    m: Dict[str, str] = {}
    for g in groups or []:
        if not g:
            continue
        rep = _norm_str(g[0])
        if not rep:
            continue
        for x in g:
            k = _norm_str(x)
            if k:
                m[k] = rep
    return m


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


def _deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(a)
    for k, v in b.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def normalize_template(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """合并为完整结构；raw 为 None 时用内置。"""
    if not raw or not isinstance(raw, dict):
        return get_builtin_default_template()
    d = get_builtin_default_template()
    if "default" in raw and isinstance(raw["default"], dict):
        d["default"] = _deep_merge(d["default"], raw["default"])
    d["by_cat1"] = {}
    if "by_cat1" in raw and isinstance(raw["by_cat1"], dict):
        ddef = d["default"]
        for k, v in raw["by_cat1"].items():
            if not k or not isinstance(v, dict):
                continue
            d["by_cat1"][_norm_str(k)] = _deep_merge(dict(ddef), v)
    return d


def rules_for_item(template: Dict[str, Any], query_item: dict) -> Dict[str, Any]:
    """主店行 -> default + 一级类覆盖 合并后的一条块。"""
    t = template if isinstance(template, dict) else get_builtin_default_template()
    ddef = t.get("default") or get_builtin_default_template()["default"]
    byc = t.get("by_cat1") or {}
    c1 = _norm_str(get_cat1(query_item))
    if c1 and c1 in byc and isinstance(byc[c1], dict):
        return _deep_merge(ddef, byc[c1])
    return dict(ddef)


def should_accept_post_match(
    query_item: dict, hit_item: dict, block: Dict[str, Any]
) -> bool:
    """
    对单条规则块 (七维) 做与 (或)。失败返回 False = 不采纳该候选。
    某一维关闭则跳过该维。两侧皆空时多数维度不拦截（类目除外：缺省若两侧空则过）。
    """
    # 1. cat3
    r = block.get("cat3") or {}
    if r.get("en", False):
        a, b = get_cat3(query_item), get_cat3(hit_item)
        if a and b and a != b:
            return False
        # 一侧有一侧无：不拦截
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


def template_from_db_json(s: str) -> Dict[str, Any]:
    if not (s or "").strip():
        return get_builtin_default_template()
    try:
        d = json.loads(s)
        return normalize_template(d) if isinstance(d, dict) else get_builtin_default_template()
    except Exception:
        return get_builtin_default_template()
