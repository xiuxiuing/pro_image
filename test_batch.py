import main_030822 as m
import torch
import os

print(f"Device: {m.device}")

# Test text embedding batch
texts = ["测试商品1", "测试商品2 500ml", "苹果手机"]
embeddings = m.texts_to_embeddings(texts)
print(f"Text embeddings count: {len(embeddings)}")
if embeddings[0] is not None:
    print(f"Embedding 0 shape: {embeddings[0].shape}")

# Test building a small index
dummy_data = [
    {"商品名称": "王老吉", "规格": "310ml", "skuId": "1001", "美团三级类目": "饮料"},
    {"商品名称": "可口可乐", "规格": "500ml", "skuId": "1002", "美团三级类目": "饮料"}
]

if not os.path.exists("test_cache"):
    os.makedirs("test_cache")

m.build_index(dummy_data, mode="text", path="test_cache/text_test.index")
print("Index built successfully.")
