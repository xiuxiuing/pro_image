# Windows 打包说明（.exe 分发）

## 依赖说明

- **Python 环境**：建议在 **Windows 64 位** 上安装 Python 3.12，与 `requirements.txt` 一致。
- **运行时依赖**：`pip install -r requirements.txt` 会安装 Flask、PyTorch、transformers、FAISS、SQLite（Python 自带 `sqlite3`）等。**数据库文件 `pro_image.db` 不在仓库内**，首次运行会在用户机器上自动创建。
- **打包工具**：`pip install -r requirements-build.txt`（PyInstaller）。

## 必须随程序分发的资源

1. **`models/` 目录（强烈建议放入 exe 同级的打包目录）**  
   - `dinov2-base/`（或从 Hugging Face 缓存复制 `facebook--dinov2-base`）  
   - `bge-base-zh-v1.5/`（或 `BAAI--bge-base-zh-v1.5`）  
   程序已支持从 `sys._MEIPASS/models` 加载（见 `main_030822.py`）。打包前将上述目录放在项目根目录的 `models/` 下，以便打进 `_internal`。若未打包模型，首次运行会尝试联网下载，离线环境会失败。

2. **`templates/`、`static/`**  
   已由 `ProImage_Windows.spec` 的 `datas` 打入。请将前端用到的 `bg.mp4`、`bg_meituan.jpg` 等放在 `static/` 下（若尚未纳入版本库）。

3. **用户数据（不会打进 exe）**  
   运行后会在 **exe 同目录** 生成 `ProImage_data/`，内含：
   - `pro_image.db`（SQLite）
   - `license.dat`（用户本地授权，由你单独发给该用户）
   - `uploads/`、`img/` 等  

   请勿把用户数据库或 **`license.dat`** 打进安装包；升级程序时保留 `ProImage_data` 即可保留数据与授权。

## 不要打进发行包

以下仅供开发/内部使用，**不要**写入 PyInstaller 的 `--add-data` / `datas`，也不要复制进 `dist` 给用户：

- `packaging_guide_zh.md`（混淆与打包流程说明）
- `BUILD_WINDOWS.md`（本说明）
- **`license.dat`**（每台机器的授权文件，由用户在本地放置；打包机上的 `license.dat` 也**不要**打进包或随 zip 发给所有用户）
- 私钥、`keygen_tool.py` 等授权相关敏感文件

当前 `ProImage_Windows.spec` 仅打包 `templates`、`static`、`models`，不会包含上述文件。程序在运行时从 **exe 旁的 `ProImage_data/license.dat`** 读取授权（见 `app.py`），与安装包内容无关。

## 打包命令（在项目根目录）

```bat
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
pip install -r requirements-build.txt
pyinstaller ProImage_Windows.spec
```

产物：`dist\ProImage\ProImage.exe` 及 `_internal` 目录。将整个 `ProImage` 文件夹打包成 zip 分发给用户即可。

## 常见问题

- **体积大**：PyTorch + transformers 通常数百 MB 以上，属正常情况。
- **杀毒误报**：可对 exe 做代码签名；或先提供控制台版（将 spec 中 `console=True`）便于排错。
- **首次启动慢**：onefolder 模式会解压依赖到 `_internal`，略慢于脚本直接运行。
