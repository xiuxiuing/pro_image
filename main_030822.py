"""
主店 vs 竞店比对流水线：下载图片 → 构建 FAISS 图/文向量索引 → 对主店 SKU 做条码优先匹配，
再对未匹配项做批量向量检索，合并图/文相似度与三级类目校验后写出 Excel 结果。

依赖：DINOv2（图）、BGE（文），向量维度见全局 dim。
"""
import utils
import os
import requests
import traceback
import torch
import numpy as np
import faiss
import sys
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed
from transformers import AutoImageProcessor, AutoModel, AutoTokenizer

# --- Environment & Setup ---
os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"
# os.environ["OMP_NUM_THREADS"] = "1"  # 已解除线程限制以提升多核使用率
if torch.cuda.is_available():
    device = "cuda"
elif torch.backends.mps.is_available():
    device = "mps"
else:
    device = "cpu"
print(f"Using device: {device}")
dim = 768

# Define model paths with frozen/MEIPASS support
models_base = os.path.join(sys._MEIPASS, "models") if getattr(sys, 'frozen', False) else os.path.join(os.path.dirname(os.path.abspath(__file__)), "models")

def load_model_from_path(name, fallback):
    """若本地 models_base 下存在 name 目录则用之，否则回退到 HuggingFace 标识 fallback。"""
    p = os.path.join(models_base, name)
    return p if os.path.exists(p) else fallback

# Load Models
dinov2_p = load_model_from_path("dinov2-base", "facebook/dinov2-base")
img_processor = AutoImageProcessor.from_pretrained(dinov2_p)
img_model = AutoModel.from_pretrained(dinov2_p).to(device).eval()

bge_p = load_model_from_path("bge-base-zh-v1.5", "BAAI/bge-base-zh-v1.5")
text_tokenizer = AutoTokenizer.from_pretrained(bge_p)
text_model = AutoModel.from_pretrained(bge_p).to(device).eval()

# --- Field Getters ---
def g(item, keys, default=""):
    """从行字典 item 中按 keys 顺序取第一个非空字段，均空则返回 default。"""
    for k in keys:
        v = item.get(k)
        if v is not None and str(v).strip() != "": return v
    return default

def get_sku_id(item):
    """SKU 主键，与 utils.get_sku_id 一致。"""
    return utils.get_sku_id(item)

def get_条码(item):
    """商品条码（多列名兼容）。"""
    return g(item, ["条码", "商品条码"])

def get_规格(item):
    """规格展示名（多列名兼容）。"""
    return g(item, ["规格", "规格名称"])

def get_活动价(item):
    """活动/折扣价（多列名兼容）。"""
    return g(item, ["单件折扣价", "新活动价", "活动价"])

def get_原价(item):
    """原价或渠道售价（多列名兼容）。"""
    return g(item, ["单件原价", "新售价", "美团外卖渠道售价", "采购价"])

def get_销售(item):
    """销量（多列名兼容）。"""
    return g(item, ["销售", "月销量"])

def get_美团类名3(item):
    """美团三级类目，用于与竞店命中行做一致性过滤。"""
    return g(item, ["美团三级类目", "美团类目三级", "三级类目"])

def get_美团类名2(item):
    return g(item, ["美团类目二级", "美团类目2级", "美团二级类目", "二级类目"])

def get_美团类名1(item):
    return g(item, ["美团类目一级", "美团类目1级", "美团一级类目", "一级类目"])

_DEFAULT_MATCH_CONFIG = {
    "category_level": 1,
    "sections": {
        "SEM": {"A核心名称": 5, "A品牌": 2, "A材质": 2, "A外观": 1, "A颜色": 0},
        "SPEC": {"A单件净含量": 3, "A售卖数量": 2, "A包装单位": 2, "A尺寸": 2, "A型号": 2},
    },
}

def _load_match_config(raw):
    import json
    if not raw:
        return _DEFAULT_MATCH_CONFIG
    if isinstance(raw, dict):
        return {**_DEFAULT_MATCH_CONFIG, **raw}
    try:
        d = json.loads(raw) if isinstance(raw, str) else {}
        if not isinstance(d, dict):
            return _DEFAULT_MATCH_CONFIG
        # merge shallowly
        out = {**_DEFAULT_MATCH_CONFIG, **d}
        out.setdefault("sections", _DEFAULT_MATCH_CONFIG["sections"])
        for sec in ("SEM", "SPEC"):
            out["sections"].setdefault(sec, {})
        return out
    except Exception:
        return _DEFAULT_MATCH_CONFIG

def _pick_category(item, level: int):
    if level == 2:
        return get_美团类名2(item)
    if level == 3:
        return get_美团类名3(item)
    return get_美团类名1(item)

def _norm_val(v):
    if v is None:
        return ""
    s = str(v).strip()
    if s.lower() in ("nan", "none", "null"):
        return ""
    return s

def _build_segmented_text(item, match_cfg):
    """Build single segmented text for BGE based on match_config weights."""
    cfg = _load_match_config(match_cfg)
    level = int(cfg.get("category_level") or 1)
    cat = _pick_category(item, level)
    parts = []
    parts.append(f"[CAT{level}]={_norm_val(cat)}")

    # fixed field sets per your spec
    sem_keys = ["A核心名称", "A品牌", "A材质", "A外观", "A颜色"]
    spec_keys = ["A单件净含量", "A售卖数量", "A包装单位", "A尺寸", "A型号"]

    sem_w = cfg.get("sections", {}).get("SEM", {}) or {}
    spec_w = cfg.get("sections", {}).get("SPEC", {}) or {}

    for k in sem_keys:
        w = int(sem_w.get(k, 0) or 0)
        if w <= 0:
            continue
        val = _norm_val(item.get(k, ""))
        if not val:
            continue
        for _ in range(min(5, max(0, w))):
            parts.append(f"[SEM][{k}]={val}")

    for k in spec_keys:
        w = int(spec_w.get(k, 0) or 0)
        if w <= 0:
            continue
        val = _norm_val(item.get(k, ""))
        if not val:
            continue
        for _ in range(min(5, max(0, w))):
            parts.append(f"[SPEC][{k}]={val}")

    # fallback: keep minimal original text to avoid empty embeddings
    if len(parts) <= 1:
        base = f"{get_规格(item)}, {item.get('商品名称','')}"
        parts.append(_norm_val(base))

    return "\n".join([p for p in parts if p])

def get_text(item):
    """
    拼成 BGE 文本向量用的字段串。
    默认走分段格式（SEM/SPEC）并读取全局 _MATCH_CONFIG（由 run_analysis 注入）。
    """
    cfg = globals().get("_MATCH_CONFIG", None)
    return _build_segmented_text(item, cfg)

def _parse_net_content(s: str):
    """Return (kind, value_in_base_unit) where kind in {'ml','g'}."""
    import re
    s = _norm_val(s).lower()
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

def _parse_size_mm(s: str):
    """Extract first length-like value and convert to mm."""
    import re
    s = _norm_val(s).lower()
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

def _apply_default_spec_penalty(query_item, hit_item, base_score: float):
    """
    Default consistency penalty for TEXT matching.
    Does NOT affect image matching or cat3 gate.
    """
    score = float(base_score)
    # net content: relative diff max 20%, penalty 0.15
    qn = _parse_net_content(query_item.get("A单件净含量", ""))
    hn = _parse_net_content(hit_item.get("A单件净含量", ""))
    if qn and hn and qn[0] == hn[0] and qn[1] > 0 and hn[1] > 0:
        rel = abs(qn[1] - hn[1]) / max(qn[1], hn[1])
        if rel > 0.20:
            score -= 0.15

    # size: abs diff max 30mm, penalty 0.15
    qs = _parse_size_mm(query_item.get("A尺寸", ""))
    hs = _parse_size_mm(hit_item.get("A尺寸", ""))
    if qs is not None and hs is not None:
        if abs(qs - hs) > 30.0:
            score -= 0.15

    # model: if both exist and not equal -> penalty 0.20
    qm = _norm_val(query_item.get("A型号", ""))
    hm = _norm_val(hit_item.get("A型号", ""))
    if qm and hm and qm != hm:
        score -= 0.20
    return score

# --- Result Construction ---
def build_match_item(item, prefix=""):
    """将一行 Excel 字典转为输出列：原列加 prefix，并统一写出 skuId/主图/价量等展示字段。"""
    res = {f"{prefix}{k}": v for k, v in item.items()}
    res.update({
        f"{prefix}skuId": get_sku_id(item),
        f"{prefix}主图链接": g(item, ["图片", "主图链接"]),
        f"{prefix}菜单名": g(item, ["商品名称", "菜单名"]),
        f"{prefix}规格名": get_规格(item),
        f"{prefix}活动价": get_活动价(item),
        f"{prefix}原价": get_原价(item),
        f"{prefix}销售": get_销售(item),
        f"{prefix}条码": get_条码(item)
    })
    return res

def append_match_result(res_item, sear_item, sim, match, prefix=""):
    """把竞店命中行合并进主店结果行，并写入 {prefix}相似度、{prefix}匹配（如 0 匹配、条码匹配）。"""
    res_item.update(build_match_item(sear_item, prefix))
    res_item[f"{prefix}相似度"], res_item[f"{prefix}匹配"] = sim, match

# --- Image Utilities ---
def download_img(url, sku_id, folder):
    """按 url 下载主图到 folder/{sku_id}.webp，已存在则跳过；失败静默忽略。"""
    os.makedirs(folder, exist_ok=True); path = f"{folder}/{sku_id}.webp"
    if os.path.exists(path): return
    try:
        r = requests.get(url, timeout=20); r.raise_for_status()
        with open(path, "wb") as f: f.write(r.content)
    except: pass

def download_imgs(data, folder="img", workers=30):
    """并发下载 data 中每行的「图片」URL 到 folder，文件名为 skuId.webp。"""
    with ThreadPoolExecutor(max_workers=workers) as ex:
        [ex.submit(download_img, (item.get("图片") or "").strip(), get_sku_id(item), folder) for item in data]

# --- Embedding & Index ---
def images_to_embeddings(paths, batch_size=32):
    """DINOv2 批量提图向量，L2 归一；缺图或失败的位置为 None，与 paths 等长。"""
    all_embeddings = []
    for i in range(0, len(paths), batch_size):
        batch_paths = paths[i:i + batch_size]
        batch_images = []
        valid_indices = []
        for idx, p in enumerate(batch_paths):
            try:
                if os.path.exists(p):
                    with Image.open(p) as img:
                        batch_images.append(img.convert("RGB"))
                        valid_indices.append(idx)
            except: pass
        
        if not batch_images:
            all_embeddings.extend([None] * len(batch_paths))
            continue
            
        try:
            inputs = img_processor(images=batch_images, return_tensors="pt").to(device)
            with torch.no_grad():
                outputs = img_model(**inputs)
                embeddings = outputs.last_hidden_state[:, 0]
                embeddings = embeddings / embeddings.norm(dim=-1, keepdim=True)
                embeddings = embeddings.cpu().numpy().astype("float32")
                
            batch_out = [None] * len(batch_paths)
            for vi, embed in zip(valid_indices, embeddings):
                batch_out[vi] = embed
            all_embeddings.extend(batch_out)
        except:
            all_embeddings.extend([None] * len(batch_paths))
    return all_embeddings

def texts_to_embeddings(texts, batch_size=32):
    """BGE 批量提句向量，按 batch 归一；本 batch 失败则该 batch 全为 None。"""
    all_embeddings = []
    for i in range(0, len(texts), batch_size):
        batch_texts = texts[i:i + batch_size]
        try:
            inputs = text_tokenizer(batch_texts, padding=True, truncation=True, max_length=512, return_tensors="pt").to(device)
            with torch.no_grad():
                outputs = text_model(**inputs)
                embeddings = outputs.last_hidden_state[:, 0]
                embeddings = torch.nn.functional.normalize(embeddings, dim=1).cpu().numpy().astype("float32")
            all_embeddings.extend([embeddings[j] for j in range(len(batch_texts))])
        except:
            all_embeddings.extend([None] * len(batch_texts))
    return all_embeddings

def image_to_embedding(path):
    """单张图片向量，供外部少量调用。"""
    res = images_to_embeddings([path])
    return res[0]

def text_to_embedding(text):
    """单条文本向量，供外部少量调用。"""
    res = texts_to_embeddings([text])
    return res[0]

def build_index(data, mode="img", folder="img", path="index", batch_size=32):
    """
    将竞店 Excel 行列表转为 FAISS 索引并写入 path。
    mode 为 img：读 folder/{skuId}.webp；为 text：用 get_text 拼串。
    使用 IndexFlatIP + 已归一向量，检索分数为内积（等价余弦相似度）。
    """
    vecs, ids = [], []
    valid_items = []
    for item in data:
        sid = get_sku_id(item)
        if sid: valid_items.append((sid, item))
        
    for i in range(0, len(valid_items), batch_size):
        batch = valid_items[i:i + batch_size]
        sids = [b[0] for b in batch]
        
        if mode == "img":
            paths = [f"{folder}/{sid}.webp" for sid in sids]
            batch_vecs = images_to_embeddings(paths, batch_size=batch_size)
        else:
            texts = [get_text(b[1]) for b in batch]
            batch_vecs = texts_to_embeddings(texts, batch_size=batch_size)
            
        for (sid, item), v in zip(batch, batch_vecs):
            if v is not None:
                try:
                    numeric_id = int(float(sid))
                    vecs.append(v)
                    ids.append(numeric_id)
                except: continue
    if not vecs: return
    v_stack = np.vstack(vecs); id_arr = np.array(ids, dtype="int64")
    index = faiss.IndexIDMap2(faiss.IndexFlatIP(dim)); index.add_with_ids(v_stack, id_arr)
    faiss.write_index(index, path)

# --- Analysis Pipeline ---
def run_analysis(target_xlsx, source_xlsxs, output_name="res", output_dir=".", progress_cb=None, match_config=None):
    """
    主分析入口：主店表 target_xlsx，竞店表列表 source_xlsxs。

    每个竞店：下载图 →（按需）构建 img_*/txt_* 的 FAISS 索引到 output_dir/../cache。
    主店：下载到 query_img → 预计算全量查询图/文向量。
    对每个竞店：先条码字典精确匹配，再对仍未出现「{idx}匹配」列的行做 Faiss top-1，
    图/文结果按阈值合并，且要求主店与命中行美团三级类目一致，最后写出 output_{output_name}.xlsx。

    progress_cb(event, idx, detail) 可选：source_start/source_done、query_start、query_progress。
    返回：生成结果文件的绝对路径。
    """
    os.makedirs(output_dir, exist_ok=True)
    cache_dir = os.path.join(output_dir, "..", "cache") if output_dir != "." else "."
    os.makedirs(cache_dir, exist_ok=True)

    sources = []
    for idx, xlsx in enumerate(source_xlsxs):
        fname = os.path.basename(xlsx)
        print(f"Loading source: {xlsx}")
        if progress_cb:
            progress_cb("source_start", idx, f"加载 {fname}")
        data = utils.excel_to_list_dict(xlsx, "Sheet1")
        if progress_cb:
            progress_cb("source_start", idx, f"下载图片 ({len(data)} 件)")
        download_imgs(data)
        
        i_path = os.path.join(cache_dir, f"img_{output_name}{idx}.index")
        t_path = os.path.join(cache_dir, f"txt_{output_name}{idx}.index")
        
        if not os.path.exists(i_path):
            if progress_cb:
                progress_cb("source_start", idx, f"图片向量 ({len(data)} 件)")
            build_index(data, "img", "img", i_path)
        if not os.path.exists(t_path):
            if progress_cb:
                progress_cb("source_start", idx, f"文本向量 ({len(data)} 件)")
            build_index(data, "text", "", t_path)
        if progress_cb:
            progress_cb("source_done", idx)
        sources.append({
            "sku_dict": {get_sku_id(i): i for i in data}, "tiaoma_dict": {get_条码(i): i for i in data if get_条码(i)},
            "i_idx": faiss.read_index(i_path) if os.path.exists(i_path) else None,
            "t_idx": faiss.read_index(t_path) if os.path.exists(t_path) else None
        })

    print(f"Loading query: {target_xlsx}")
    query_data = utils.excel_to_list_dict(target_xlsx, "Sheet1")
    if progress_cb:
        progress_cb("query_start", 0, f"下载查询图片 ({len(query_data)} 件)")
    download_imgs(query_data, "query_img")
    
    total_q = len(query_data)
    if progress_cb:
        progress_cb("query_progress", 0, f"生成查询向量 0/{total_q}")
    
    # Inject match_config for get_text() globally within this module.
    # (This keeps call sites simple and avoids changing many function signatures.)
    globals()["_MATCH_CONFIG"] = match_config

    # Pre-compute all query embeddings in batches
    query_img_paths = [f"query_img/{get_sku_id(item)}.webp" for item in query_data]
    query_texts = [get_text(item) for item in query_data]
    
    query_img_vecs = images_to_embeddings(query_img_paths, batch_size=32)
    query_txt_vecs = texts_to_embeddings(query_texts, batch_size=32)
    
    res_data = [build_match_item(item) for item in query_data]
    
    for idx, src in enumerate(sources):
        print(f"Analyzing source {idx}...")
        if progress_cb:
            progress_cb("query_progress", 0, f"分析来源 {idx+1}/{len(sources)}")
            
        # 1. Barcode match (fast)
        for qi, item in enumerate(query_data):
            hit = src["tiaoma_dict"].get(get_条码(item))
            if hit:
                append_match_result(res_data[qi], hit, 1, "条码匹配", str(idx))
                
        # 2. Batch vector search for items not yet matched by barcode in this source
        unmatched_indices = [i for i, rd in enumerate(res_data) if f"{idx}匹配" not in rd]
        if not unmatched_indices: continue
        
        # Prepare vectors for Faiss batch search
        search_img_vecs = []
        search_img_map = [] # idx in unmatched_indices
        search_txt_vecs = []
        search_txt_map = []
        
        for ui in unmatched_indices:
            iv, tv = query_img_vecs[ui], query_txt_vecs[ui]
            if iv is not None and src["i_idx"] is not None:
                search_img_vecs.append(iv)
                search_img_map.append(ui)
            if tv is not None and src["t_idx"] is not None:
                search_txt_vecs.append(tv)
                search_txt_map.append(ui)
                
        # Image search
        img_hits = {} # ui -> (s_id, score)
        if search_img_vecs:
            v_block = np.vstack(search_img_vecs)
            scores, ids = src["i_idx"].search(v_block, 1)
            for i, ui in enumerate(search_img_map):
                img_hits[ui] = (ids[i][0], float(scores[i][0]))
                
        # Text search
        txt_hits = {}
        if search_txt_vecs:
            v_block = np.vstack(search_txt_vecs)
            scores, ids = src["t_idx"].search(v_block, 1)
            for i, ui in enumerate(search_txt_map):
                txt_hits[ui] = (ids[i][0], float(scores[i][0]))
                
        # Merge results with thresholds and category check
        for ui in unmatched_indices:
            item = query_data[ui]
            s_id, score, match = -1, 0, ""
            
            i_hit = img_hits.get(ui)
            if i_hit:
                s_id, score, match = i_hit[0], i_hit[1], "图片匹配"
                
            t_hit = txt_hits.get(ui)
            if t_hit:
                if score < 0.9 or (t_hit[1] > score):
                    if t_hit[1] > score:
                        s_id, score, match = t_hit[0], t_hit[1], "文本匹配"
            
            if s_id != -1:
                hit = src["sku_dict"].get(str(int(s_id)))
                if hit and get_美团类名3(item) == get_美团类名3(hit):
                    if match == "文本匹配":
                        score = _apply_default_spec_penalty(item, hit, score)
                    append_match_result(res_data[ui], hit, score, match, str(idx))

    out_path = os.path.join(output_dir, f"output_{output_name}.xlsx")
    utils.write_dict_list_to_excel(res_data, out_path)
    return out_path

if __name__ == '__main__':
    run_analysis("优购哆.xlsx", ["乐购达.xlsx", "沃玛希.xlsx", "犀牛.xlsx", "AA百货.xlsx"])