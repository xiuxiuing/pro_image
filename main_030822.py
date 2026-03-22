import utils
import os

os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"
os.environ["OMP_NUM_THREADS"] = "1"
import requests
from PIL import Image
import traceback

from concurrent.futures import ThreadPoolExecutor, as_completed

import torch
import numpy as np
import faiss

from transformers import AutoImageProcessor, AutoModel, AutoTokenizer

device = "cuda" if torch.cuda.is_available() else "cpu"

# -----------------------------
# model
# -----------------------------

img_processor = AutoImageProcessor.from_pretrained("facebook/dinov2-base")
img_model = AutoModel.from_pretrained("facebook/dinov2-base").to(device).eval()

model_name = "BAAI/bge-base-zh-v1.5"
text_tokenizer = AutoTokenizer.from_pretrained(model_name)
text_model = AutoModel.from_pretrained(model_name).to(device).eval()

dim = 768


# -----------------------------
# 字段读取
# -----------------------------

def get_sku_id(item):
    return int(item.get("skuid") or item.get("SKUID"))


def get_条码(item):
    return item.get("条码") or item.get("商品条码")


def get_规格(item):
    return item.get("规格") or item.get("规格名称")


def get_活动价(item):
    return item.get("单件折扣价") or item.get("新活动价") or item.get("活动价")


def get_原价(item):
    return item.get("单件原价") or item.get("新售价") or item.get("美团外卖渠道售价") or item.get("采购价")


def get_销售(item):
    return item.get("销售") or item.get("月销量")


def get_美团类名1(item):
    return item.get("美团一级类目") or item.get("美团类目一级")


def get_美团类名2(item):
    return item.get("美团二级类目") or item.get("美团类目二级")


def get_美团类名3(item):
    return item.get("美团三级类目") or item.get("美团类目三级")


def get_text(item):
    return f"{get_规格(item)}, {get_美团类名3(item)}, {item['商品名称']}, {item.get('A品牌', '')}, {item.get('A商品名称', '')}, {item.get('A规格', '')}, {item.get('A材质口味', '')}"


# -----------------------------
# 结果封装
# -----------------------------

def build_match_item(item, prefix=""):
    return {f"{prefix}{k}": v for k, v in item.items()}


def append_match_result(res_item, sear_item, similarity, match, prefix=""):
    res_item.update(build_match_item(sear_item, prefix))
    res_item[f"{prefix}相似度"] = similarity
    res_item[f"{prefix}匹配"] = match


# -----------------------------
# 下载图片
# -----------------------------

def download_img(url, sku_id, file_path):
    os.makedirs(file_path, exist_ok=True)

    file_name = f"{file_path}/{sku_id}.webp"

    if os.path.exists(file_name):
        return

    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()

        with open(file_name, "wb") as f:
            f.write(r.content)
    except:
        pass


def download_worker(item, file_path="img"):
    sku_id = get_sku_id(item)

    try:
        url = item["图片"].strip()
        download_img(url, sku_id, file_path)
        return f"成功 {sku_id}"
    except Exception as e:
        return f"失败 {sku_id} {e}"


def download_imgs(data, file_path="img", max_workers=30):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download_worker, item, file_path) for item in data]

        for future in as_completed(futures):
            future.result()


# -----------------------------
# embedding
# -----------------------------

def image_to_embedding(image_path):
    try:
        with Image.open(image_path) as img:
            image = img.convert("RGB")

        inputs = img_processor(images=image, return_tensors="pt").to(device)

        with torch.no_grad():
            outputs = img_model(**inputs)
            emb = outputs.last_hidden_state[:, 0]

        emb = emb / emb.norm(dim=-1, keepdim=True)

        return emb.cpu().numpy().astype("float32")
    except:
        return None


def text_to_embedding(text):
    inputs = text_tokenizer(
        text,
        padding=True,
        truncation=True,
        max_length=512,
        return_tensors="pt"
    ).to(device)

    with torch.no_grad():
        outputs = text_model(**inputs)
        emb = outputs.last_hidden_state[:, 0]

    emb = torch.nn.functional.normalize(emb, dim=1)

    return emb.cpu().numpy().astype("float32")


# -----------------------------
# build index
# -----------------------------

def build_img_index(data, image_dir, index_path):
    vectors = []
    ids = []

    for item in data:

        sku_id = get_sku_id(item)

        try:

            vec = image_to_embedding(f"{image_dir}/{sku_id}.webp")
            if vec is not None:
                vectors.append(vec)
                ids.append(sku_id)

        except Exception as e:
            print(f"embedding失败 {sku_id}: {e}")

    if not vectors:
        return

    vectors = np.vstack(vectors)

    ids = np.array(ids, dtype="int64")

    index = faiss.IndexFlatIP(dim)

    index = faiss.IndexIDMap2(index)

    index.add_with_ids(vectors, ids)

    faiss.write_index(index, index_path)

    print("index saved", index.ntotal)


def build_text_index(data, index_path):
    vectors = []
    ids = []

    for item in data:

        sku_id = get_sku_id(item)

        try:

            vec = text_to_embedding(get_text(item))

            vectors.append(vec)

            ids.append(sku_id)

        except Exception:

            print("text embedding失败", sku_id)

    if not vectors:
        return

    vectors = np.vstack(vectors)

    ids = np.array(ids, dtype="int64")

    index = faiss.IndexFlatIP(dim)

    index = faiss.IndexIDMap2(index)

    index.add_with_ids(vectors, ids)

    faiss.write_index(index, index_path)


# -----------------------------
# search
# -----------------------------

def search(index_img, index_text, img_vec, text_vec):
    s1 = 0
    ids_img = [[-1]]
    if img_vec is not None:
        scores_img, ids_img = index_img.search(img_vec, 1)
        s1 = float(scores_img[0][0])
        if s1 >= 0.9:
            return ids_img[0][0], s1, "图片匹配"

    scores_text, ids_text = index_text.search(text_vec, 1)
    s2 = float(scores_text[0][0])

    if s2 >= 0.9:
        return ids_text[0][0], s2, "文本匹配"

    if s1 >= s2:
        return ids_img[0][0], s1, "图片匹配"

    return ids_text[0][0], s2, "文本匹配"


# -----------------------------
# Analysis Logic
# -----------------------------

def run_analysis(target_xlsx, source_xlsxs, output_name="031511"):
    sources = []

    # -----------------------------
    # 初始化source
    # -----------------------------
    for idx, xlsx in enumerate(source_xlsxs):
        print(f"Loading source: {xlsx}")
        data = utils.excel_to_list_dict(xlsx, "Sheet1")
        download_imgs(data)

        img_index_path = f"sku_img_{output_name}{idx}.index"
        text_index_path = f"sku_text_{output_name}{idx}.index"

        if not os.path.exists(img_index_path):
            build_img_index(data, "img", img_index_path)

        if not os.path.exists(text_index_path):
            build_text_index(data, text_index_path)

        img_index = faiss.read_index(img_index_path)
        text_index = faiss.read_index(text_index_path)

        sku_dict = {get_sku_id(i): i for i in data}
        tiaoma_dict = {get_条码(i): i for i in data if get_条码(i)}

        sources.append({
            "data": data,
            "sku_dict": sku_dict,
            "tiaoma_dict": tiaoma_dict,
            "img_index": img_index,
            "text_index": text_index
        })

    # -----------------------------
    # query
    # -----------------------------
    print(f"Loading query: {target_xlsx}")
    query_data = utils.excel_to_list_dict(target_xlsx, "Sheet1")
    download_imgs(query_data, "query_img")

    res_data = []
    for item in query_data:
        try:
            sku_id = get_sku_id(item)
            img_path = f"query_img/{sku_id}.webp"
            img_vec = image_to_embedding(img_path)
            text_vec = text_to_embedding(get_text(item))

            res_item = build_match_item(item)
            for idx, source in enumerate(sources):
                sear_item = source["tiaoma_dict"].get(get_条码(item))

                if sear_item:
                    append_match_result(res_item, sear_item, 1, "条码匹配", str(idx))
                    continue

                sid, score, match = search(
                    source["img_index"],
                    source["text_index"],
                    img_vec,
                    text_vec
                )
                sear_item = source["sku_dict"].get(int(sid))
                desc = ""
                if get_美团类名1(item) != get_美团类名1(sear_item) or \
                        get_美团类名2(item) != get_美团类名2(sear_item):
                    desc = "类目不同"

                append_match_result(
                    res_item,
                    sear_item,
                    score,
                    match + desc,
                    str(idx)
                )
            res_data.append(res_item)
        except Exception as e:
            traceback.print_exc()

    output_file = f"output_{output_name}.xlsx"
    utils.write_dict_list_to_excel(
        res_data,
        file_path=output_file
    )
    print(f"完成, 输出文件: {output_file}")
    return output_file

if __name__ == '__main__':
    target_f = "优购哆.xlsx"
    sources_f = ["乐购达.xlsx", "沃玛希.xlsx", "犀牛.xlsx", "AA百货.xlsx"]
    run_analysis(target_f, sources_f)