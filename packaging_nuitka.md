# ProImage 打包操作手册

> 内部文档：本文只用于开发、交付和打包验证，不随最终发行包分发。

## 当前唯一打包路线

当前项目只保留 **Nuitka 编译核心模块 + PyInstaller 收集业务壳与依赖** 这一套方案。

- 推荐入口：应用内 `ops-tools` 页面里的“程序打包”。
- macOS spec：`ProImage_nuitka_macOS.spec`
- Windows spec：`ProImage_nuitka_Windows.spec`
- 共享清单和打包辅助：`packaging_core.py`
- 打包依赖：`requirements-build.txt`

旧的另一套打包方案已经移除，不再维护。

## 打包原理

### 核心模块

以下模块由 Nuitka 逐个编译成原生扩展，macOS 为 `.so`，Windows 为 `.pyd`。最终发行包中不应出现这些模块对应的项目源码 `.py` 文件。

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

清单来源是 `packaging_core.CORE_NUITKA_MODULES`。

### 业务壳和资源

以下内容复制到 `_build_src/` 后由 PyInstaller 打进发行包：

```text
app.py
app_ops.py
app_ops_extra.py
app_ops_tasks.py
app_data.py
app_data_projects.py
app_data_rules.py
app_data_grid.py
data_mgr.py
data_mgr_base.py
data_mgr_import.py
data_mgr_query.py
data_mgr_query_unlinked.py
data_mgr_ops.py
data_mgr_export.py
data_mgr_rule_templates.py
field_registry.py
quality_preflight.py
packaging_core.py
templates/
static/
data/
models/
```

清单来源是 `packaging_core.BUSINESS_SOURCE_FILES`、`RESOURCE_DIRS` 和 `REQUIRED_MODEL_FILES`。

## 前置准备

### 通用要求

1. 在项目根目录执行所有命令，也就是包含 `app.py` 的目录。
2. 使用 Python 3.12 64 位。
3. 先安装运行依赖和打包依赖：

macOS：

```bash
python3 -m pip install -r requirements.txt
python3 -m pip install -r requirements-build.txt
```

Windows PowerShell：

```powershell
python -m pip install -r requirements.txt
python -m pip install -r requirements-build.txt
```

`ops-tools` 打包入口会检查 `nuitka`、`PyInstaller`、`ordered_set`，缺失时会尝试自动执行 `pip install -r requirements-build.txt`。但正式打包机建议提前手动安装，便于排查网络或权限问题。

### macOS

安装 Xcode Command Line Tools：

```bash
xcode-select --install
```

打包流程会自动运行：

```bash
python3 tools/patch_pyinstaller_site_packages.py
```

该脚本用于修补 PyInstaller 冻结环境下 torch/scipy 的已知启动问题。

### Windows

1. 安装 Python 3.12 64 位。
2. 安装 Visual Studio Build Tools。
3. 在安装器里勾选“使用 C++ 的桌面开发”。
4. 建议用 PowerShell 在项目根目录执行打包。

### 本地模型

发行包必须包含本地模型，否则离线启动或首次分析可能报：

```text
Can't load image processor for 'facebook/dinov2-base'
```

项目根目录需要有：

```text
models/dinov2-base/preprocessor_config.json
models/dinov2-base/config.json
models/bge-base-zh-v1.5/tokenizer_config.json
models/bge-base-zh-v1.5/config.json
```

`ops-tools` 打包时会检查这些文件。缺失时会自动运行：

```bash
python download_models.py
```

如果打包机无法访问 Hugging Face，请先在可联网机器准备完整 `models/` 目录，再复制到打包机的项目根目录。

## 推荐流程：使用 ops-tools 页面打包

1. 启动开发版应用。
2. 打开 `/ops-tools`。
3. 找到“程序打包”。
4. 选择目标：
   - `macOS .app`：必须在 macOS 打包机上执行。
   - `Windows 程序包`：必须在 Windows 打包机上执行。
5. 点击“开始打包”。
6. 等待任务完成后下载 ZIP。

页面执行步骤：

```text
检查环境
Nuitka 编译核心
准备打包目录
PyInstaller 打包
验证产物
压缩产物
```

打包过程中会自动做这些事：

- 检查当前系统是否匹配目标平台。
- 检查并安装打包依赖。
- 检查本地模型，缺失时尝试下载。
- 清理 ABI 不匹配的旧 Nuitka 编译产物。
- 将 9 个核心模块编译为 `.so` / `.pyd`。
- 准备 `_build_src/`，复制业务壳、资源、模型和编译产物。
- 执行对应的 `ProImage_nuitka_*.spec`。
- macOS 产物执行 `xattr -cr` 和 ad-hoc `codesign`。
- 验证产物结构。
- 压缩 ZIP。
- 压缩完成后清理 `_build_src/`、`build/`、`nuitka_modules/` 和本次产物目录。

## 输出位置

### macOS

构建阶段产物：

```text
dist/ProImage_AI.app
```

最终下载 ZIP 名称：

```text
ProImage_AI_macOS_YYYYMMDD_HHMMSS.zip
```

### Windows

构建阶段产物：

```text
dist/ProImage_AI/ProImage_AI.exe
```

最终下载 ZIP 名称：

```text
ProImage_Windows_YYYYMMDD_HHMMSS.zip
```

### ZIP 输出目录规则

ZIP 输出目录按以下优先级选择：

1. 环境变量 `PROIMAGE_PACKAGE_ZIP_DIR`
2. Windows 且存在 `H:\` 时使用 `H:\ProImage_packages`
3. 当前打包任务目录

Windows 上如果 C 盘空间紧张，建议提前设置：

```powershell
$env:PROIMAGE_PACKAGE_ZIP_DIR = "H:\ProImage_packages"
```

## 手动打包流程

手动流程只作为页面打包失败时的兜底。正式交付优先使用 `ops-tools`，因为页面流程包含模型检查、ABI 清理、结构验证、压缩空间检查和自动清理。

### macOS 手动流程

```bash
rm -rf _build_src build dist nuitka_modules
mkdir -p nuitka_modules

python3 tools/patch_pyinstaller_site_packages.py
python3 download_models.py

for mod in main_030822 post_match_engine product_text_extract extract_info_ai2 extract_info_rules extract_info_schema license_utils utils merge_sku_data; do
  echo "=== 编译 $mod ==="
  python3 -m nuitka --module --output-dir=nuitka_modules "$mod.py"
done

mkdir -p _build_src
cp app.py app_ops.py app_ops_extra.py app_ops_tasks.py _build_src/
cp app_data.py app_data_projects.py app_data_rules.py app_data_grid.py _build_src/
cp data_mgr.py data_mgr_base.py data_mgr_import.py data_mgr_query.py data_mgr_query_unlinked.py _build_src/
cp data_mgr_ops.py data_mgr_export.py data_mgr_rule_templates.py _build_src/
cp field_registry.py quality_preflight.py packaging_core.py _build_src/
cp -R templates static data models _build_src/
cp nuitka_modules/*.so _build_src/

python3 -m PyInstaller -y ProImage_nuitka_macOS.spec
xattr -cr dist/ProImage_AI.app
codesign --force --deep --sign - dist/ProImage_AI.app
```

启动验证：

```bash
./dist/ProImage_AI.app/Contents/MacOS/ProImage_AI
```

压缩：

```bash
ditto -c -k --sequesterRsrc --keepParent dist/ProImage_AI.app dist/ProImage_AI_macOS.zip
```

### Windows 手动流程

```powershell
Remove-Item -Recurse -Force _build_src,build,dist,nuitka_modules -ErrorAction SilentlyContinue
mkdir nuitka_modules

python download_models.py

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
Copy-Item field_registry.py,quality_preflight.py,packaging_core.py _build_src\
Copy-Item templates,static,data,models _build_src\ -Recurse
Copy-Item nuitka_modules\*.pyd _build_src\

python -m PyInstaller -y ProImage_nuitka_Windows.spec
```

Windows 复制 `.pyd` 必须使用通配符，不要写死 `cp312-win_amd64`，避免 Python patch 版本或架构标记变化导致复制失败。

## 产物验证

页面打包会自动验证以下内容：

- 产物目录存在。
- 9 个核心模块的 `.so` / `.pyd` 都在产物中。
- 核心模块对应的项目源码 `.py` 未泄露到产物文件系统。
- `templates/` 存在。
- `static/` 存在。
- `data/default_rule_templates/production_rule_v1.json` 存在。
- `models/dinov2-base/` 和 `models/bge-base-zh-v1.5/` 的关键配置文件存在。

本地开发可运行打包相关测试：

```bash
python3 -m unittest tests.test_windows_packaging_runtime tests.test_nuitka_core_packaging tests.test_packaging_structure tests.test_builtin_rule_templates
```

Windows 上使用：

```powershell
python -m unittest tests.test_windows_packaging_runtime tests.test_nuitka_core_packaging tests.test_packaging_structure tests.test_builtin_rule_templates
```

## 不要打进发行包

以下内容不得随发行包分发：

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
nuitka_modules/
__pycache__/
*.pyc
```

程序首次运行会在产物同级目录创建：

```text
ProImage_data/
```

用户数据库、上传文件、图片缓存和授权文件都在 `ProImage_data/` 下维护。

## 打包后清理

页面打包成功后会自动清理：

```text
_build_src/
build/
nuitka_modules/
dist/ProImage_AI.app 或 dist/ProImage_AI/
```

手动清理命令：

macOS：

```bash
rm -rf dist build _build_src nuitka_modules __pycache__ tests/__pycache__
find . -name '*.pyc' -delete
```

Windows PowerShell：

```powershell
Remove-Item -Recurse -Force dist,build,_build_src,nuitka_modules,__pycache__,tests\__pycache__ -ErrorAction SilentlyContinue
Get-ChildItem -Recurse -Filter *.pyc | Remove-Item -Force
```

## 常见问题

### Nuitka 编译失败

- macOS 确认已安装 Xcode Command Line Tools。
- Windows 确认已安装 Visual Studio Build Tools 的 C++ 桌面开发组件。
- 查看 `nuitka_modules/<module>.build/` 下的详细日志。
- 确认当前 Python 是 3.12 64 位。

### PyInstaller 后启动缺模块

- 确认 `_build_src/` 中业务壳 `.py`、资源目录、模型目录和核心 `.so/.pyd` 都存在。
- 在 `ProImage_nuitka_*.spec` 的 `hiddenimports` 中补缺失第三方模块。
- 不要把 `data_mgr*`、`app_*` 加入 `excludes`；`excludes` 只能基于 `packaging_core.CORE_NUITKA_MODULES`。

### 产物缺模型或报 facebook/dinov2-base

重新运行：

```bash
python download_models.py
```

确认存在：

```text
models/dinov2-base/preprocessor_config.json
```

然后重新打包。

### Windows 压缩失败或磁盘空间不足

页面流程会在压缩前估算空间。需要预留约：

```text
max(产物大小 * 1.15 + 512MB, 1GB)
```

如果 C 盘不足，优先设置：

```powershell
$env:PROIMAGE_PACKAGE_ZIP_DIR = "H:\ProImage_packages"
```

也可以先清理：

```text
dist/
build/
_build_src/
nuitka_modules/
uploads/
```

### 产物过小

正常产物会包含 torch、transformers、faiss、scipy 等依赖，体积通常为数百 MB 到 1GB 以上。若产物明显过小，通常是 `_build_src/` 准备不完整、模型未复制，或 PyInstaller 未收集到重依赖。
