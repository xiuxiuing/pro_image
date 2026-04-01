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
    for k in keys:
        v = item.get(k)
        if v is not None and str(v).strip() != "": return v
    return default

def get_sku_id(item):
    return utils.get_sku_id(item)

def get_条码(item): return g(item, ["条码", "商品条码"])
def get_规格(item): return g(item, ["规格", "规格名称"])
def get_活动价(item): return g(item, ["单件折扣价", "新活动价", "活动价"])
def get_原价(item): return g(item, ["单件原价", "新售价", "美团外卖渠道售价", "采购价"])
def get_销售(item): return g(item, ["销售", "月销量"])
def get_美团类名3(item): return g(item, ["美团三级类目", "美团类目三级", "三级类目"])

def get_text(item):
    return f"{get_规格(item)}, {get_美团类名3(item)}, {item.get('商品名称','')}, {item.get('A品牌', '')}, {item.get('A商品名称', '')}, {item.get('A规格', '')}"

# --- Result Construction ---
def build_match_item(item, prefix=""):
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
    res_item.update(build_match_item(sear_item, prefix))
    res_item[f"{prefix}相似度"], res_item[f"{prefix}匹配"] = sim, match

# --- Image Utilities ---
def download_img(url, sku_id, folder):
    os.makedirs(folder, exist_ok=True); path = f"{folder}/{sku_id}.webp"
    if os.path.exists(path): return
    try:
        r = requests.get(url, timeout=20); r.raise_for_status()
        with open(path, "wb") as f: f.write(r.content)
    except: pass

def download_imgs(data, folder="img", workers=30):
    with ThreadPoolExecutor(max_workers=workers) as ex:
        [ex.submit(download_img, (item.get("图片") or "").strip(), get_sku_id(item), folder) for item in data]

# --- Embedding & Index ---
def images_to_embeddings(paths, batch_size=32):
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
    res = images_to_embeddings([path])
    return res[0]

def text_to_embedding(text):
    res = texts_to_embeddings([text])
    return res[0]

def build_index(data, mode="img", folder="img", path="index", batch_size=32):
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
def run_analysis(target_xlsx, source_xlsxs, output_name="res", output_dir=".", progress_cb=None):
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
                
        # Merge results with threshholds and category check
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
                    append_match_result(res_data[ui], hit, score, match, str(idx))

    out_path = os.path.join(output_dir, f"output_{output_name}.xlsx")
    utils.write_dict_list_to_excel(res_data, out_path)
    return out_path

if __name__ == '__main__':
    run_analysis("优购哆.xlsx", ["乐购达.xlsx", "沃玛希.xlsx", "犀牛.xlsx", "AA百货.xlsx"])