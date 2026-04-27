"""
从「商品名称 + 规格」中提取用于匹配的关键信息（第一步：结构化抽取）。

目标：把“杂乱文本”收敛为可控字段（净含量/售卖数量/包装形式/颜色/外观/尺寸/材质/型号等），
后续再讨论如何按三级类目加权拼接进 BGE。

职责：
- Unicode/全半角归一（NFKC）
- 去噪（空白折叠、零宽字符、常见乘号分隔符归一）
- 单位归一（ml/L/g/kg 等）
- 结构化抽取：单件净含量、售卖数量、包装形式、颜色、外观、尺寸、材质、型号

不负责：
- 不同三级类目下的字段权重（后续步骤）
- 如何拼接进 BGE / 如何进行向量检索（后续步骤）

典型用法::

    from product_text_extract import extract_product_keys
    keys = extract_product_keys(name="【促销】可乐 500ML", spec="500毫升/瓶")
    # keys.net_content, keys.sell_quantity, keys.packaging_form, keys.size, ...
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Callable, Union

# --- 基础：空值与类型 ---

def _as_str(v: object) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and str(v) == "nan":
        return ""
    s = str(v).strip()
    if s.lower() in ("nan", "none", "null"):
        return ""
    return s


# --- Unicode：NFKC（含常见全角字母数字标点 → 半角兼容形）---

def normalize_unicode_width(s: str) -> str:
    """全角/半角与兼容字符：统一为 NFKC 规范化形式。"""
    if not s:
        return ""
    return unicodedata.normalize("NFKC", s)


# --- 去噪 ---

_ZERO_WIDTH = re.compile(r"[\u200b\u200c\u200d\ufeff]")
# 连续空白（含全角空格 U+3000）
_WS = re.compile(r"[\s\u3000]+")


def denoise_text(s: str) -> str:
    """
    去掉零宽字符、折叠空白、首尾 strip。
    将常见乘号/分隔符（× x * 全角Ｘ）规范为单个空格，便于「128克 2包」被分别识别。
    不删除正文汉字；括号营销词在后续可按需扩展。
    """
    if not s:
        return ""
    t = _ZERO_WIDTH.sub("", s)
    t = re.sub(r"\s*[×xＸ*]\s*", " ", t)
    t = _WS.sub(" ", t).strip()
    return t


# --- 单位归一（在已 NFKC 的串上调用）---


def _jin_to_g_str(num: str) -> str:
    """斤 → 克（按 1 斤 = 500g）。"""
    try:
        v = float(num) * 500.0
        if v == int(v):
            return f"{int(v)}g"
        return f"{round(v, 2)}g"
    except ValueError:
        return f"{num}g"


# 匹配顺序：先长后短，避免「毫升」被「升」误伤等
_Repl = Union[str, Callable[[re.Match[str]], str]]
_UNIT_SUBS: list[tuple[re.Pattern[str], _Repl]] = [
    # 毫升
    (re.compile(r"(?i)(\d+(?:\.\d+)?)\s*(?:毫升|c\.c\.|cc)\b"), r"\1ml"),
    (re.compile(r"(?i)(\d+(?:\.\d+)?)\s*m\s*l\b"), r"\1ml"),
    (re.compile(r"(?i)(\d+(?:\.\d+)?)\s*ml\b"), r"\1ml"),
    # 升（中文「升」；拉丁 L 需避免匹配到单词中的 l，仅跟数字）
    (re.compile(r"(\d+(?:\.\d+)?)\s*升\b"), r"\1L"),
    (re.compile(r"(?i)(\d+(?:\.\d+)?)\s*L\b"), r"\1L"),
    # 克 / 千克
    (re.compile(r"(\d+(?:\.\d+)?)\s*克\b"), r"\1g"),
    (re.compile(r"(?i)(\d+(?:\.\d+)?)\s*g\b"), r"\1g"),
    (re.compile(r"(?i)(\d+(?:\.\d+)?)\s*(?:千克|公斤)\b"), r"\1kg"),
    (re.compile(r"(?i)(\d+(?:\.\d+)?)\s*kg\b"), r"\1kg"),
    # 斤 → 500g 便于与克统一比较（可选约定；若业务不用斤可再关）
    (re.compile(r"(\d+(?:\.\d+)?)\s*斤\b"), lambda m: _jin_to_g_str(m.group(1))),
]

# 常见「数 + 计量包装」：归一成无空格「6瓶」形式，便于与 ml/g 并列抽取
_PACK_UNIT = re.compile(
    r"(\d+(?:\.\d+)?)\s*(瓶|盒|袋|包|杯|支|条|片|听|罐|桶|箱|板|套|枚|个|只|块|卷|组|件|枚装|瓶装|盒装)\b"
)


def normalize_units(text: str) -> str:
    """
    将常见体积/质量写法收敛为 ml、L、g、kg（小写质量单位、大写 L）。
    斤按 500g 换算为克（与商超常见写法对齐；若需保留「斤」可再改）。
    """
    if not text:
        return ""
    t = text
    for pat, repl in _UNIT_SUBS:
        if callable(repl):
            t = pat.sub(repl, t)
        else:
            t = pat.sub(repl, t)
    t = _PACK_UNIT.sub(lambda m: f"{m.group(1)}{m.group(2)}", t)
    return t


# --- 从整段文字中抽取「数+单位」短语（归一后）---

_QTY_SCAN = re.compile(
    r"(?i)(?:\d+(?:\.\d+)?(?:ml|g|kg)|\d+(?:\.\d+)?L(?![a-z])|\d+(?:\.\d+)?(?:瓶|盒|袋|包|杯|支|条|片|听|罐|桶|箱|板|套|枚|个|只|块|卷|组|件))"
)


def _canonical_qty_token(raw: str) -> str:
    """统一展示：ml/g/kg 小写，升为 L。"""
    s = raw.strip()
    m = re.fullmatch(r"(?i)(\d+(?:\.\d+)?)(ml|g|kg)", s)
    if m:
        return f"{m.group(1)}{m.group(2).lower()}"
    m = re.fullmatch(r"(?i)(\d+(?:\.\d+)?)l", s)
    if m:
        return f"{m.group(1)}L"
    m = re.fullmatch(r"(\d+(?:\.\d+)?)(瓶|盒|袋|包|杯|支|条|片|听|罐|桶|箱|板|套|枚|个|只|块|卷|组|件)", s)
    if m:
        return f"{m.group(1)}{m.group(2)}"
    return s


def _dedupe_key_for_qty(token: str) -> str:
    return _canonical_qty_token(token).lower()


def extract_quantity_snippets(text: str) -> tuple[str, ...]:
    """
    在已做单位归一的字符串上，从左到右扫描数+体积/质量/包装单位片段，
    去重且保持顺序（名称与规格合并串中重复出现的只保留一次）。
    """
    if not text:
        return ()
    compact = re.sub(r"\s+", "", text)
    ordered: list[str] = []
    seen: set[str] = set()
    for m in _QTY_SCAN.finditer(compact):
        raw = m.group(0)
        display = _canonical_qty_token(raw)
        key = _dedupe_key_for_qty(display)
        if key not in seen:
            seen.add(key)
            ordered.append(display)
    return tuple(ordered)


# --- 结构化字段抽取（基于 normalized 文本）---

_PACK_FORM_CANON = (
    ("袋装", ("袋装", "袋")),
    ("盒装", ("盒装", "盒")),
    ("瓶装", ("瓶装", "瓶")),
    ("罐装", ("罐装", "罐")),
    ("桶装", ("桶装", "桶")),
    ("箱装", ("箱装", "箱")),
    ("听装", ("听装", "听")),
    ("杯装", ("杯装", "杯")),
    ("支装", ("支装", "支")),
    ("条装", ("条装", "条")),
    ("片装", ("片装", "片")),
    ("套装", ("套装", "套")),
)

_COLOR_WORDS = (
    "米白",
    "乳白",
    "透明",
    "香槟",
    "白",
    "黑",
    "灰",
    "银",
    "金",
    "红",
    "粉",
    "橙",
    "黄",
    "绿",
    "青",
    "蓝",
    "紫",
    "棕",
    "咖",
)

_APPEARANCE_WORDS = (
    "超薄",
    "加厚",
    "极薄",
    "轻薄",
    "柔软",
    "大号",
    "小号",
    "中号",
    "加大",
    "特大",
    "迷你",
    "便携",
    "长",
    "短",
    "宽",
    "窄",
    "圆形",
    "方形",
    "条形",
    "片状",
    "颗粒",
)

_MATERIAL_WORDS = (
    "纯棉",
    "棉",
    "无纺布",
    "塑料",
    "硅胶",
    "不锈钢",
    "玻璃",
    "陶瓷",
    "铝",
    "铁",
    "铜",
    "木",
    "竹",
    "皮革",
    "橡胶",
    "乳胶",
    "尼龙",
    "聚酯",
    "涤纶",
    "毛绒",
    "蚕丝",
)

_DIM_PAIR_SCAN = re.compile(
    r"(?i)(\d+(?:\.\d+)?)\s*(mm|cm|m)\s*[x×*]\s*(\d+(?:\.\d+)?)\s*(mm|cm|m)\b"
)
_SIZE_SCAN = re.compile(r"(?i)(\d+(?:\.\d+)?)\s*(mm|cm|m)\b")

_MODEL_HINT = re.compile(r"(?:型号|model)\s*[:：]?\s*([A-Za-z0-9][A-Za-z0-9\-_/]{1,30})", re.IGNORECASE)
_MODEL_TOKEN = re.compile(r"\b[A-Za-z]{1,6}[-_/]?\d{2,6}[A-Za-z0-9\-_/]{0,10}\b")


def _extract_core_product_name(normalized_name: str) -> str:
    """
    核心商品名称（规则启发式，先保证“可用”而不是“完美”）。

    思路：
    - 从名称中移除常见营销括号/符号段
    - 去掉净含量/尺寸/售卖数量等数值片段
    - 去掉常见修饰词（新老包装随机、网红、怀旧等）
    - 返回一个尽量短的“名词短语”
    """
    s = (normalized_name or "").strip()
    if not s:
        return ""

    # 1) 去括号/书名号等里的营销信息（保守：只删除明显的“【...】”块）
    s = re.sub(r"【[^】]{1,20}】", " ", s)

    # 2) 去掉数值与单位相关片段（净含量/件数/尺寸/乘法）
    s = re.sub(r"(?i)\d+(?:\.\d+)?(?:ml|g|kg|l)\b", " ", s)
    s = re.sub(r"(?i)\d+(?:\.\d+)?(?:mm|cm|m)\b", " ", s)
    s = re.sub(r"\d+(?:\.\d+)?(?:瓶|盒|袋|包|杯|支|条|片|听|罐|桶|箱|板|套|枚|个|只|块|卷|组|件)\b", " ", s)
    s = re.sub(r"(?i)(?:\*|×|x)\s*\d+", " ", s)

    # 3) 去掉常见无信息修饰
    noise = (
        "新老包装随机",
        "随机",
        "网红",
        "爆款",
        "同款",
        "经典",
        "升级",
        "加量",
        "大促",
        "促销",
        "特价",
        "包邮",
        "正品",
        "官方",
        "旗舰",
        "家庭装",
        "袋装",
        "盒装",
        "瓶装",
        "罐装",
        "整箱",
        "整盒",
        "单袋",
        "大袋",
        "小袋",
    )
    for w in noise:
        s = s.replace(w, " ")

    # 4) 清理空白与明显的类目尾缀（如“零食”“小吃”等是否保留取决于业务；这里保守保留）
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return ""

    # 5) 取前 1~2 个“词块”（按空格切），避免长串
    parts = s.split(" ")
    if len(parts) == 1:
        return parts[0]
    # 优先保留更像名词的前两段
    return " ".join(parts[:2]).strip()


def _dedupe_str(seq: list[str]) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for x in seq:
        x = (x or "").strip()
        if not x:
            continue
        k = x.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(x)
    return tuple(out)


def _extract_packaging_form(text: str) -> str:
    for canon, alts in _PACK_FORM_CANON:
        for a in alts:
            if a and a in text:
                return canon
    return ""


def _extract_colors(text: str) -> tuple[str, ...]:
    hits: list[str] = []
    for c in _COLOR_WORDS:
        if c and c in text:
            hits.append(c)
    # 更长的词优先（米白 > 白）
    hits = sorted(set(hits), key=lambda x: (-len(x), x))
    return tuple(hits)


def _extract_appearance(text: str) -> tuple[str, ...]:
    hits: list[str] = []
    for w in _APPEARANCE_WORDS:
        if w and w in text:
            hits.append(w)
    return tuple(sorted(set(hits), key=lambda x: (-len(x), x)))


def _extract_material(text: str) -> tuple[str, ...]:
    hits: list[str] = []
    for w in _MATERIAL_WORDS:
        if w and w in text:
            hits.append(w)
    hits = sorted(set(hits), key=lambda x: (-len(x), x))
    if "纯棉" in hits and "棉" in hits:
        hits = [h for h in hits if h != "棉"]
    return tuple(hits)


def _extract_size(text: str) -> tuple[str, ...]:
    out: list[str] = []
    compact = text.replace(" ", "")
    for m in _DIM_PAIR_SCAN.finditer(compact):
        out.append(f"{m.group(1)}{m.group(2)}x{m.group(3)}{m.group(4)}")
    for m in _SIZE_SCAN.finditer(compact):
        out.append(f"{m.group(1)}{m.group(2)}")
    for s in ("XXXL", "XXL", "XL", "L", "M", "S"):
        if re.search(rf"(?i)\b{s}\b", text):
            out.append(s.upper())
            break
    return _dedupe_str(out)


def _extract_model(text: str) -> str:
    m = _MODEL_HINT.search(text)
    if m:
        return m.group(1).strip()
    m2 = _MODEL_TOKEN.search(text)
    if m2:
        return m2.group(0).strip()
    return ""


def _pick_net_content(quantity_snippets: tuple[str, ...]) -> str:
    for tok in quantity_snippets:
        if re.fullmatch(r"(?i)\d+(?:\.\d+)?(ml|g|kg)", tok):
            return tok.lower()
        if re.fullmatch(r"(?i)\d+(?:\.\d+)?L", tok):
            return tok[:-1] + "L"
    return ""


def _pick_sell_quantity(quantity_snippets: tuple[str, ...], text: str) -> str:
    for tok in quantity_snippets:
        if re.fullmatch(r"\d+(?:\.\d+)?(瓶|盒|袋|包|杯|支|条|片|听|罐|桶|箱|板|套|枚|个|只|块|卷|组|件)", tok):
            return tok
    m = re.search(
        r"(\d+(?:\.\d+)?)\s*(?:x|×|\*)\s*(\d+(?:\.\d+)?)\s*(袋|包|片|瓶|盒|罐|听|条|支|个|只)\b",
        text,
        flags=re.IGNORECASE,
    )
    if m:
        return f"{m.group(2)}{m.group(3)}"
    return ""


@dataclass(frozen=True)
class ExtractedProductKeys:
    """规范化后的名称、规格及可结构化使用的数量片段。"""

    raw_name: str
    raw_spec: str
    normalized_name: str
    normalized_spec: str
    """经 NFKC、去噪、单位归一后的名称 / 规格（整串）。"""
    quantity_snippets: tuple[str, ...]
    """从名称+规格合并串中按顺序去重抽取的数+单位短语（ml/L/g/kg/瓶等）。"""
    net_content: str
    """单件净含量（如 18g、500ml、1.5L）。"""
    sell_quantity: str
    """售卖数量（规则底层会保留单位，如 5袋、7片、1包；AI 写回层会拆成数量值 + 包装单位）。"""
    packaging_form: str
    """包装形式（袋装/盒装/瓶装/罐装/桶装/箱装/听装/杯装/条装/片装/套装）。"""
    colors: tuple[str, ...]
    """颜色候选（可能多值）。"""
    appearance: tuple[str, ...]
    """外观/形态候选（可能多值）。"""
    size: tuple[str, ...]
    """尺寸/长度/码数候选（可能多值，如 240mm、10cmx20cm、XL）。"""
    material: tuple[str, ...]
    """材质候选（可能多值）。"""
    model: str
    """型号（如 AB-123、型号: X1 等）。"""
    core_product_name: str
    """核心商品名称：描述商品是什么的短语（规则启发式版本，后续可按类目优化）。"""


def extract_product_keys(name: str | None, spec: str | None) -> ExtractedProductKeys:
    """
    从商品名称与规格各抽取一条规范化主串，并给出合并去重后的数量短语列表。

    后续可按「美团三级类目」等对 quantity_snippets、normalized_* 分别加权或裁剪，
    再决定如何拼进 BGE；本函数不依赖类目。
    """
    raw_name = _as_str(name)
    raw_spec = _as_str(spec)

    nn = normalize_unicode_width(raw_name)
    ns = normalize_unicode_width(raw_spec)
    nn = denoise_text(nn)
    ns = denoise_text(ns)
    nn = normalize_units(nn)
    ns = normalize_units(ns)

    combined = f"{nn} {ns}".strip()
    qty = extract_quantity_snippets(combined)
    net_content = _pick_net_content(qty)
    sell_quantity = _pick_sell_quantity(qty, combined)
    packaging_form = _extract_packaging_form(combined)
    colors = _extract_colors(combined)
    appearance = _extract_appearance(combined)
    size = _extract_size(combined)
    material = _extract_material(combined)
    model = _extract_model(combined)
    core_product_name = _extract_core_product_name(nn)

    return ExtractedProductKeys(
        raw_name=raw_name,
        raw_spec=raw_spec,
        normalized_name=nn,
        normalized_spec=ns,
        quantity_snippets=qty,
        net_content=net_content,
        sell_quantity=sell_quantity,
        packaging_form=packaging_form,
        colors=colors,
        appearance=appearance,
        size=size,
        material=material,
        model=model,
        core_product_name=core_product_name,
    )


def _run_file_demo(path: str, output_path: str | None = None) -> None:
    """从 xlsx / csv 读入若干行，输出或打印 extract_product_keys 结果（需列 商品名称 + 规格 或 规格名称）。"""
    import pandas as pd

    if path.lower().endswith(".csv"):
        df = pd.read_csv(path, encoding="utf-8-sig")
    else:
        df = pd.read_excel(path, engine="openpyxl")

    cols = {str(c).strip(): c for c in df.columns}
    name_key = None
    for k in ("商品名称", "名称", "name"):
        if k in cols:
            name_key = cols[k]
            break
    spec_key = None
    for k in ("规格", "规格名称"):
        if k in cols:
            spec_key = cols[k]
            break

    def _guess_columns_by_content(_df: "pd.DataFrame") -> tuple[object, object]:
        """
        有些源表第一行不是规范表头，会导致列名是店名/skuId/Unnamed。
        这里基于内容做一次启发式列猜测：找“像商品名称”的列和“像规格”的列。
        """
        sample = _df.head(200)

        # 规格：优先匹配包含 g/ml/L/kg/mm/cm 或 “*N袋/包/片/罐/瓶/箱/件”等的短字符串列
        qty_pat = re.compile(
            r"(?i)\d+(?:\.\d+)?(?:ml|g|kg|l|mm|cm)\b|(?:\*|×|x)\s*\d+|/\s*(?:袋|包|片|罐|瓶|箱|件|盒|听|支|条)|\d+\s*(?:袋|包|片|罐|瓶|箱|件|盒|听|支|条)\b"
        )
        # 名称：中文占比高、长度更长，且不太像纯数字/URL
        url_pat = re.compile(r"^https?://", re.IGNORECASE)
        num_pat = re.compile(r"^\d+(\.\d+)?$")

        best_name = (None, -1.0)
        best_spec = (None, -1.0)
        for c in sample.columns:
            s = sample[c].astype(str).fillna("").map(lambda x: x.strip())
            if s.empty:
                continue

            non_empty = s[s != ""]
            if non_empty.empty:
                continue

            # skip url-like columns
            url_ratio = float(non_empty.map(lambda x: bool(url_pat.match(x))).mean())
            if url_ratio > 0.6:
                continue

            # name score
            avg_len = float(non_empty.map(len).mean())
            cn_ratio = float(non_empty.map(lambda x: sum("\u4e00" <= ch <= "\u9fff" for ch in x) / max(1, len(x))).mean())
            num_ratio = float(non_empty.map(lambda x: bool(num_pat.match(x))).mean())
            name_score = (avg_len * cn_ratio) - (50.0 * num_ratio)
            if name_score > best_name[1]:
                best_name = (c, name_score)

            # spec score
            avg_len_short = float(non_empty.map(len).mean())
            qty_ratio = float(non_empty.map(lambda x: bool(qty_pat.search(x))).mean())
            spec_score = (qty_ratio * 100.0) - (avg_len_short * 0.3)
            if spec_score > best_spec[1]:
                best_spec = (c, spec_score)

        if best_name[0] is None or best_spec[0] is None:
            raise SystemExit("无法从内容猜测「商品名称/规格」列，请将列名改为「商品名称」「规格」。")
        if best_name[0] == best_spec[0]:
            # 极端情况：同列既像名称又像规格，退化为相邻列策略
            cols_list = list(sample.columns)
            idx = cols_list.index(best_name[0])
            if idx + 1 < len(cols_list):
                return best_name[0], cols_list[idx + 1]
            if idx - 1 >= 0:
                return best_name[0], cols_list[idx - 1]
        return best_name[0], best_spec[0]

    if name_key is None or spec_key is None:
        name_key, spec_key = _guess_columns_by_content(df)

    rows_out = []
    for _, row in df.iterrows():
        keys = extract_product_keys(row.get(name_key), row.get(spec_key))
        rows_out.append(
            {
                "raw_name": keys.raw_name,
                "raw_spec": keys.raw_spec,
                "normalized_name": keys.normalized_name,
                "normalized_spec": keys.normalized_spec,
                "quantity_snippets": " | ".join(keys.quantity_snippets),
                "net_content": keys.net_content,
                "sell_quantity": keys.sell_quantity,
                "packaging_form": keys.packaging_form,
                "colors": " | ".join(keys.colors),
                "appearance": " | ".join(keys.appearance),
                "size": " | ".join(keys.size),
                "material": " | ".join(keys.material),
                "model": keys.model,
                "core_product_name": keys.core_product_name,
            }
        )
    out = pd.DataFrame(rows_out)
    table = pd.concat(
        [
            df[[name_key, spec_key]].rename(columns={name_key: "商品名称", spec_key: "规格"}).reset_index(drop=True),
            out[
                [
                    "normalized_name",
                    "normalized_spec",
                    "quantity_snippets",
                    "net_content",
                    "sell_quantity",
                    "packaging_form",
                    "colors",
                    "appearance",
                    "size",
                    "material",
                    "model",
                    "core_product_name",
                ]
            ],
        ],
        axis=1,
    )

    if output_path:
        op = output_path.strip()
        if op.lower().endswith(".csv"):
            table.to_csv(op, index=False, encoding="utf-8-sig")
        else:
            table.to_excel(op, index=False, engine="openpyxl")
        print(f"已写入: {op}")
        return

    with pd.option_context("display.max_colwidth", 80, "display.width", 200):
        print(table.to_string(index=False))


if __name__ == "__main__":
    import argparse
    import sys

    ap = argparse.ArgumentParser(description="商品名称+规格 关键信息抽取（自检或测表格文件）")
    ap.add_argument(
        "path",
        nargs="?",
        help="可选：.xlsx / .csv，需含 商品名称、规格（或 规格名称）",
    )
    ap.add_argument(
        "-o",
        "--output",
        metavar="FILE",
        help="结果输出为 .xlsx 或 .csv（默认仅打印到终端）",
    )
    args = ap.parse_args()

    if args.path:
        _run_file_demo(args.path, output_path=args.output)
        sys.exit(0)

    k = extract_product_keys("【促销】可口可乐　５００ＭＬ", "500毫升 / 瓶")
    assert "500" in k.normalized_spec or "500" in k.normalized_name
    assert any("ml" in x.lower() or "瓶" in x for x in k.quantity_snippets)
    k2 = extract_product_keys("薯片", "１２８克×２包")
    assert "128" in k2.normalized_spec or "g" in k2.normalized_spec.lower()
    print("product_text_extract self-check ok:", k, k2, sep="\n")
