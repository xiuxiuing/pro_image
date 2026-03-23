from transformers import AutoImageProcessor, AutoModel, AutoTokenizer
import os

models_dir = "models"
os.makedirs(models_dir, exist_ok=True)

# 1. Download Dinov2
print("Downloading Dinov2...")
repo_dinov2 = "facebook/dinov2-base"
path_dinov2 = os.path.join(models_dir, "dinov2-base")
processor = AutoImageProcessor.from_pretrained(repo_dinov2)
model = AutoModel.from_pretrained(repo_dinov2)
processor.save_pretrained(path_dinov2)
model.save_pretrained(path_dinov2)

# 2. Download BGE
print("Downloading BGE...")
repo_bge = "BAAI/bge-base-zh-v1.5"
path_bge = os.path.join(models_dir, "bge-base-zh-v1.5")
tokenizer = AutoTokenizer.from_pretrained(repo_bge)
model_text = AutoModel.from_pretrained(repo_bge)
tokenizer.save_pretrained(path_bge)
model_text.save_pretrained(path_bge)

print("Models downloaded successfully to 'models/' directory.")
