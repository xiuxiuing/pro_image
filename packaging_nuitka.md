# Nuitka 核心混合打包操作手册

> **内部文档**：本文仅用于开发与打包流程说明，不随产物分发给终端用户。

## 方案概述

正式打包路线采用 **Nuitka 编译核心模块 + PyInstaller 收集业务壳与依赖**：

1. **Nuitka `--module`**：只把核心算法、规则、AI 提取和授权模块编译为原生 `.so` / `.pyd`。
2. **PyInstaller**：继续负责收集 Flask、torch、transformers、faiss、scipy、openai 等重依赖，生成 `.app` / `.exe`。
3. **ops-tools 页面**：程序打包按钮已直接接入本方案，并在压缩前自动做结构验证。

以前尝试过纯 Nuitka standalone，但第三方依赖收集不稳定，容易出现产物过小、运行时缺包的问题。本方案保留 PyInstaller 处理依赖，Nuitka 只负责保护真正核心代码。

## 保护边界

### 编译保护的核心模块

核心清单在 `packaging_core.py` 中维护，当前为：

```text
main_030822
post_match_engine
product_text_extract
extract_info_ai2
extract_info_rules
extract_info_schema
license_utils
utils
merge_sku_data
```

这些模块会编译为 `.so` / `.pyd`，打包产物中不应出现对应 `.py` 源码。

### 普通业务壳源码

以下模块保留为普通源码/字节码交给 PyInstaller 打包：

```text
app.py
app_ops*.py
app_data*.py
data_mgr*.py
packaging_core.py
templates/
static/
data/
```

这些代码主要是页面路由、项目管理、数据库 CRUD、前端资源和默认数据，允许作为业务壳存在。这样可以避开 PyArmor 大小限制，也降低 Nuitka 编译边界和依赖问题。

## 前置准备

### macOS

```bash
xcode-select --install
python3 -m pip install -r requirements.txt
python3 -m pip install -r requirements-build.txt
python3 -m pip install nuitka ordered-set
```

建议打包前修补 PyInstaller 冻结环境下的 torch/scipy 已知问题：

```bash
python3 tools/patch_pyinstaller_site_packages.py
```

### Windows

1. 安装 Python 3.12 64 位。
2. 安装 Visual Studio Build Tools，勾选 **C++ 桌面开发**。
3. 安装依赖：

```powershell
python -m pip install -r requirements.txt
python -m pip install -r requirements-build.txt
python -m pip install nuitka ordered-set
```

### 本地模型

发行包必须包含本地模型目录，否则 Windows/macOS 离线启动或首次分析时会报 `Can't load image processor for 'facebook/dinov2-base'`。

ops-tools 打包时会自动检查并在缺失时运行：

```bash
python download_models.py
```

最终项目根目录需要包含：

```text
models/dinov2-base/preprocessor_config.json
models/dinov2-base/config.json
models/bge-base-zh-v1.5/tokenizer_config.json
models/bge-base-zh-v1.5/config.json
```

如果打包机无法访问 Hugging Face，需要先从可联网机器准备好完整 `models/` 目录再打包。

## 推荐方式：在 ops-tools 页面打包

1. 启动开发版应用。
2. 打开 `/ops-tools`。
3. 在“程序打包”中选择目标：
   - `macOS .app`：必须在 macOS 打包机上执行。
   - `Windows 程序包`：必须在 Windows 打包机上执行。
4. 点击“开始打包”。

页面会按以下步骤执行：

```text
检查环境
Nuitka 编译核心
准备打包目录
PyInstaller 打包
验证产物
压缩产物
```

结构验证会检查：

- 产物目录存在。
- 9 个核心模块的 `.so` / `.pyd` 都在产物中。
- 核心模块对应 `.py` 没有泄露到产物文件系统。
- `templates/`、`static/`、`data/default_rule_templates/production_rule_v1.json` 存在。
- `models/dinov2-base/` 和 `models/bge-base-zh-v1.5/` 的关键配置文件存在。

验证通过后会生成 ZIP 下载链接。

## 手动打包流程

### macOS

```bash
rm -rf _build_src
mkdir -p nuitka_modules

for mod in main_030822 post_match_engine product_text_extract extract_info_ai2 extract_info_rules extract_info_schema license_utils utils merge_sku_data; do
  echo "=== 编译 $mod ==="
  python3 -m nuitka --module --output-dir=nuitka_modules "$mod.py"
done

mkdir -p _build_src
cp app.py app_ops.py app_ops_extra.py app_ops_tasks.py _build_src/
cp app_data.py app_data_projects.py app_data_rules.py app_data_grid.py _build_src/
cp data_mgr.py data_mgr_base.py data_mgr_import.py data_mgr_query.py data_mgr_query_unlinked.py _build_src/
cp data_mgr_ops.py data_mgr_export.py data_mgr_rule_templates.py _build_src/
cp packaging_core.py _build_src/
cp -r templates static data _build_src/
cp nuitka_modules/*.so _build_src/

python3 -m PyInstaller -y ProImage_nuitka_macOS.spec
xattr -cr dist/ProImage_AI.app
codesign --force --deep --sign - dist/ProImage_AI.app
```

### Windows

```powershell
Remove-Item -Recurse -Force _build_src -ErrorAction SilentlyContinue
mkdir nuitka_modules -ErrorAction SilentlyContinue

$mods = @(
  "main_030822", "post_match_engine", "product_text_extract",
  "extract_info_ai2", "extract_info_rules", "extract_info_schema",
  "license_utils", "utils", "merge_sku_data"
)

foreach ($mod in $mods) {
  Write-Host "=== 编译 $mod ==="
  python -m nuitka --module --output-dir=nuitka_modules "$mod.py"
}

mkdir _build_src
Copy-Item app.py,app_ops.py,app_ops_extra.py,app_ops_tasks.py _build_src\
Copy-Item app_data.py,app_data_projects.py,app_data_rules.py,app_data_grid.py _build_src\
Copy-Item data_mgr.py,data_mgr_base.py,data_mgr_import.py,data_mgr_query.py,data_mgr_query_unlinked.py _build_src\
Copy-Item data_mgr_ops.py,data_mgr_export.py,data_mgr_rule_templates.py _build_src\
Copy-Item packaging_core.py _build_src\
Copy-Item templates,static,data _build_src\ -Recurse
Copy-Item nuitka_modules\*.pyd _build_src\

python -m PyInstaller -y ProImage_nuitka_Windows.spec
```

Windows 复制 `.pyd` 必须使用通配符，不要写死 `cp312-win_amd64`，避免 Python patch 版本或架构标记变化导致复制失败。

## 产物位置

macOS：

```text
dist/ProImage_AI.app
```

Windows：

```text
dist/ProImage_AI/ProImage_AI.exe
```

将对应 `.app` 或整个 `ProImage_AI/` 文件夹压缩后分发。

## 不要打进发行包

以下内容仅供开发或用户本地运行时生成，不应随发行包分发：

```text
vendor/private_key.pem
vendor/keygen_tool.py
license.dat
pro_image.db
uploads/
img/
outputs/
build/
dist/
_build_src/
nuitka_modules/*.build
```

程序首次运行会在产物同级目录创建 `ProImage_data/`，用户数据库、上传文件、图片缓存和授权文件都在这里维护。

## 常见问题

### Nuitka 编译失败

- macOS 确认已安装 Xcode Command Line Tools。
- Windows 确认已安装 Visual Studio Build Tools 的 C++ 桌面开发组件。
- 查看 `nuitka_modules/<module>.build/` 下的详细日志。

### PyInstaller 后启动缺模块

- 先确认 `_build_src/` 中业务壳 `.py`、资源目录和核心 `.so/.pyd` 都存在。
- 在 `ProImage_nuitka_*.spec` 的 `hiddenimports` 中补缺失第三方模块。
- 不要把 `data_mgr*`、`app_*` 加入 excludes；excludes 只应等于 `packaging_core.CORE_NUITKA_MODULES`。

### Windows 打开时报 `facebook/dinov2-base`

这是产物缺少本地 DINOv2/BGE 模型导致的。重新在 Windows 打包机上进入 ops-tools 打包；新流程会自动下载并验证 `models/`。如果下载失败，先手动运行 `python download_models.py`，确认 `models/dinov2-base/preprocessor_config.json` 存在后再打包。

### 产物过小

正常产物会包含 torch、transformers、faiss 等依赖，体积通常数百 MB。若产物明显过小，大概率是 `_build_src/` 准备不完整或 PyInstaller 未收集到重依赖。

### PyArmor 是否还需要

不需要作为正式路线。PyArmor 旧方案可以作为备用，但不推荐继续全量混淆，因为 trial/许可证和单文件大小限制会反复卡住。
