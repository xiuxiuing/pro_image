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
os.environ["OMP_NUM_THREADS"] = "1"
device = "cuda" if torch.cuda.is_available() else "cpu"
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
    v = g(item, ["skuid", "SKUID"])
    if not v: return ""
    try:
        # Handle float strings like "123.0"
        return str(int(float(v)))
    except:
        return str(v).strip()

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
def image_to_embedding(path):
    try:
        with Image.open(path) as img: i = img_processor(images=img.convert("RGB"), return_tensors="pt").to(device)
        with torch.no_grad(): o = img_model(**i); e = o.last_hidden_state[:, 0]
        e = e / e.norm(dim=-1, keepdim=True)
        return e.cpu().numpy().astype("float32")
    except: return None

def text_to_embedding(text):
    i = text_tokenizer(text, padding=True, truncation=True, max_length=512, return_tensors="pt").to(device)
    with torch.no_grad(): o = text_model(**i); e = o.last_hidden_state[:, 0]
    return torch.nn.functional.normalize(e, dim=1).cpu().numpy().astype("float32")

def build_index(data, mode="img", folder="img", path="index"):
    vecs, ids = [], []
    for item in data:
        sid = get_sku_id(item)
        if not sid: continue
        try:
            v = image_to_embedding(f"{folder}/{sid}.webp") if mode == "img" else text_to_embedding(get_text(item))
            if v is not None:
                try:
                    # FAISS IndexIDMap2 requires int64 IDs
                    numeric_id = int(float(sid))
                    vecs.append(v)
                    ids.append(numeric_id)
                except:
                    # Skip non-numeric IDs for FAISS index, but they will still be in tiaoma_dict
                    continue
        except Exception as e: print(f"Embedding err {sid}: {e}")
    if not vecs: return
    v_stack = np.vstack(vecs); id_arr = np.array(ids, dtype="int64")
    index = faiss.IndexIDMap2(faiss.IndexFlatIP(dim)); index.add_with_ids(v_stack, id_arr)
    faiss.write_index(index, path)

# --- Analysis Pipeline ---
def run_analysis(target_xlsx, source_xlsxs, output_name="res"):
    sources = []
    for idx, xlsx in enumerate(source_xlsxs):
        print(f"Loading source: {xlsx}"); data = utils.excel_to_list_dict(xlsx, "Sheet1")
        download_imgs(data); i_path, t_path = f"img_{output_name}{idx}.index", f"txt_{output_name}{idx}.index"
        if not os.path.exists(i_path): build_index(data, "img", "img", i_path)
        if not os.path.exists(t_path): build_index(data, "text", "", t_path)
        sources.append({
            "sku_dict": {get_sku_id(i): i for i in data}, "tiaoma_dict": {get_条码(i): i for i in data if get_条码(i)},
            "i_idx": faiss.read_index(i_path) if os.path.exists(i_path) else None,
            "t_idx": faiss.read_index(t_path) if os.path.exists(t_path) else None
        })

    print(f"Loading query: {target_xlsx}"); query_data = utils.excel_to_list_dict(target_xlsx, "Sheet1")
    download_imgs(query_data, "query_img"); res_data = []
    for item in query_data:
        try:
            sid = get_sku_id(item); i_vec, t_vec = image_to_embedding(f"query_img/{sid}.webp"), text_to_embedding(get_text(item))
            res_item = build_match_item(item)
            for idx, src in enumerate(sources):
                hit = src["tiaoma_dict"].get(get_条码(item))
                if hit: append_match_result(res_item, hit, 1, "条码匹配", str(idx)); continue
                
                s_id, score, match = -1, 0, ""
                if i_vec is not None and src["i_idx"] is not None:
                    sc, ids = src["i_idx"].search(i_vec, 1); s_id, score, match = ids[0][0], float(sc[0][0]), "图片匹配"
                if score < 0.9 and t_vec is not None and src["t_idx"] is not None:
                    sc, ids = src["t_idx"].search(t_vec, 1)
                    if float(sc[0][0]) > score: s_id, score, match = ids[0][0], float(sc[0][0]), "文本匹配"
                
                hit = src["sku_dict"].get(str(int(s_id)))
                if hit and get_美团类名3(item) == get_美团类名3(hit): append_match_result(res_item, hit, score, match, str(idx))
            res_data.append(res_item)
        except: traceback.print_exc()

    out = f"output_{output_name}.xlsx"; utils.write_dict_list_to_excel(res_data, out); return out

if __name__ == '__main__':
    run_analysis("优购哆.xlsx", ["乐购达.xlsx", "沃玛希.xlsx", "犀牛.xlsx", "AA百货.xlsx"])