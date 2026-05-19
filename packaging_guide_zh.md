# 项目打包与代码混淆指南（PyArmor 旧方案）

> **内部文档（控制条件）**：本文仅用于开发与打包流程说明，**不要**加入 PyInstaller 的 `--add-data` 或随 `.exe` 分发给终端用户。

为了保护您的软件不被破解和非法传播，请按照以下步骤对代码进行混淆，并将其打包为可执行文件。

> 当前项目更推荐使用 `packaging_nuitka.md` 中的 **Nuitka 混合方案**：它会把一方业务模块编译为原生 `.so` / `.pyd`，保护强度高于 PyArmor。本文件保留为 PyArmor + PyInstaller 旧方案/备用方案，但清单已按当前项目同步。

## 前置准备

1. **确保路径正确**：必须在项目根目录下执行（包含 `app.py` 的目录）。
   ```bash
   cd /path/to/pro_image   # 包含 app.py 的根目录
   ```
2. **安装必要工具**：
   ```bash
   pip3 install -r requirements-build.txt
   pip3 install pyarmor
   ```
3. **（macOS）将用户级 bin 目录加入 PATH**（若 pip 使用 `--user` 安装）：
   ```bash
   export PATH="$HOME/Library/Python/3.12/bin:$PATH"
   ```

---

## 第一步：代码混淆（PyArmor）

> 如果 PyArmor 许可证过期或不可用，可以**跳过本步骤**。`ProImage_macOS.spec` 会自动检测：若 `dist/obfuscated/app.py` 不存在，则回退到源码打包。

### 1.1 需要混淆的业务文件（28 个）

| 文件 | 说明 |
|------|------|
| `app.py` | Flask 入口 |
| `app_ops.py` / `app_ops_tasks.py` / `app_ops_extra.py` | 运营工具与打包辅助接口 |
| `app_data.py` / `app_data_projects.py` / `app_data_rules.py` / `app_data_grid.py` | 项目、规则、工作台数据接口 |
| `data_mgr.py` | 数据管理入口 |
| `data_mgr_base.py` | 数据管理 - 基础 & DB |
| `data_mgr_import.py` | 数据管理 - 导入逻辑 |
| `data_mgr_query.py` | 数据管理 - 查询与分页 |
| `data_mgr_query_unlinked.py` | 数据管理 - 未关联池分页 |
| `data_mgr_ops.py` | 数据管理 - 业务操作 |
| `data_mgr_export.py` | 数据管理 - 导出逻辑 |
| `data_mgr_rule_templates.py` | 数据管理 - 规则模板 |
| `license_utils.py` | 授权校验 |
| `main_030822.py` | 核心算法 |
| `extract_info_ai2.py` | AI 信息提取 |
| `extract_info_schema.py` / `extract_info_rules.py` | AI 提取 schema 与本地兜底规则 |
| `product_text_extract.py` | 商品文本规则提取 |
| `post_match_engine.py` | 匹配后规则引擎 |
| `utils.py` | 工具函数 |
| `merge_sku_data.py` | SKU 合并 |

### 1.2 执行混淆

#### Windows CMD

> CMD 中不要使用 `\` 换行；如需换行请用 `^`，且 `^` 后不能有空格。最稳妥是直接使用下面这一行。

```bat
pyarmor gen -O dist\obfuscated app.py app_ops.py app_ops_tasks.py app_ops_extra.py app_data.py app_data_projects.py app_data_rules.py app_data_grid.py data_mgr.py data_mgr_base.py data_mgr_import.py data_mgr_query.py data_mgr_query_unlinked.py data_mgr_ops.py data_mgr_export.py data_mgr_rule_templates.py field_registry.py quality_preflight.py packaging_core.py license_utils.py main_030822.py extract_info_ai2.py extract_info_schema.py extract_info_rules.py product_text_extract.py post_match_engine.py utils.py merge_sku_data.py
```

#### Windows PowerShell

```powershell
$files = @(
  "app.py", "app_ops.py", "app_ops_tasks.py", "app_ops_extra.py",
  "app_data.py", "app_data_projects.py", "app_data_rules.py", "app_data_grid.py",
  "data_mgr.py", "data_mgr_base.py", "data_mgr_import.py", "data_mgr_query.py",
  "data_mgr_query_unlinked.py", "data_mgr_ops.py", "data_mgr_export.py",
  "data_mgr_rule_templates.py", "field_registry.py", "quality_preflight.py",
  "packaging_core.py", "license_utils.py", "main_030822.py",
  "extract_info_ai2.py", "extract_info_schema.py", "extract_info_rules.py",
  "product_text_extract.py", "post_match_engine.py", "utils.py", "merge_sku_data.py"
)

pyarmor gen -O dist\obfuscated @files
```

#### macOS / bash

```bash
pyarmor gen -O dist/obfuscated \
  app.py app_ops.py app_ops_tasks.py app_ops_extra.py \
  app_data.py app_data_projects.py app_data_rules.py app_data_grid.py \
  data_mgr.py data_mgr_base.py data_mgr_import.py data_mgr_query.py \
  data_mgr_query_unlinked.py data_mgr_ops.py data_mgr_export.py \
  data_mgr_rule_templates.py field_registry.py quality_preflight.py \
  packaging_core.py license_utils.py main_030822.py \
  extract_info_ai2.py extract_info_schema.py extract_info_rules.py \
  product_text_extract.py post_match_engine.py utils.py merge_sku_data.py
```

### 1.3 验证混淆结果

> **关键步骤**：必须确认 `dist/obfuscated/` 下包含上述 **全部 28 个** `.py` 文件和 `pyarmor_runtime_*` 目录。
> 如果有文件缺失，打包后 `.app`/`.exe` 启动会报 `ModuleNotFoundError`。

```bash
ls dist/obfuscated/
# 预期输出应包含：
# app.py app_ops.py app_ops_tasks.py app_ops_extra.py
# app_data.py app_data_projects.py app_data_rules.py app_data_grid.py
# data_mgr.py data_mgr_base.py data_mgr_export.py data_mgr_import.py
# data_mgr_ops.py data_mgr_query.py data_mgr_query_unlinked.py data_mgr_rule_templates.py
# extract_info_ai2.py extract_info_schema.py extract_info_rules.py
# product_text_extract.py post_match_engine.py license_utils.py main_030822.py
# merge_sku_data.py utils.py
# pyarmor_runtime_000000/
```

如果文件不全（例如只有 `app.py`），说明 PyArmor 许可证受限，请**删除 `dist/obfuscated/` 目录**后直接跳到第二步用源码打包：

```bash
rm -rf dist/obfuscated
```

---

## 第二步：打包（PyInstaller）

`.spec` 文件已预配置了 `templates`、`static`、`data` 资源目录以及 `flask`、`torch`、`pandas`、`openai` 等 30 多个 `hiddenimports`。**不要**手动拼命令行参数，极易导致依赖缺失。

### Windows 系统打包（生成 EXE）

1. 确保已完成"第一步"生成了**完整的** `dist/obfuscated` 目录（或已删除该目录以使用源码模式）。
2. 执行打包命令：
   ```bash
   pyinstaller -y ProImage_Windows.spec
   ```

### macOS 系统打包（生成 .app）

1. 建议先修补本机 `site-packages` 中的 torch/scipy（避免打包后启动即因 NameError 崩溃）：
   ```bash
   python3 tools/patch_pyinstaller_site_packages.py
   ```
2. **确认入口模式**（二选一）：
   - **混淆模式**：`dist/obfuscated/` 下有完整的 28 个 `.py` 文件 → spec 自动使用混淆入口。
   - **源码模式**：`dist/obfuscated/` 目录不存在或已删除 → spec 自动回退到项目根目录的 `app.py`。
3. 在项目根目录执行打包：
   ```bash
   pyinstaller -y ProImage_macOS.spec
   ```
4. 产物在 `dist/ProImage_AI.app`。
5. **清理隔离属性、重新签名并压缩**：
   ```bash
   xattr -cr dist/ProImage_AI.app
   codesign --force --deep --sign - dist/ProImage_AI.app
   ditto -c -k --sequesterRsrc --keepParent dist/ProImage_AI.app dist/ProImage_AI_macOS.zip
   ```
6. **验证打包结果**（在终端直接运行可看到完整报错日志）：
   ```bash
   ./dist/ProImage_AI.app/Contents/MacOS/ProImage_AI
   ```
   确认无 `ModuleNotFoundError` 后再分发。

### 优化说明 (Optimizations)

本次打包配置已进行以下优化：
- **体积控制**：排除了 `matplotlib`、`notebook`、`IPython`、`jupyter_client` 等大型未直接使用的库。
- **字节码策略**：当前 spec 使用 `optimize=0`，方便保留运行时报错定位信息；如需更强压缩可自行评估改为 `optimize=2`。
- **符号剥离 (macOS)**：开启 `strip=True` 移除调试符号。
- **健壮性增强**：`.spec` 文件现在能自动识别环境路径，并更可靠地处理混淆与源码模式切换。

---

## 第三步：软件分发

分发软件时：

1. **Windows**：提供 `dist/ProImage/` 整个文件夹（包含 `ProImage.exe` 和 `_internal`）。
2. **macOS**：提供 `dist/ProImage_AI.app`（可以压缩为 ZIP 分发）。
3. **绝对不要**分发 `private_key.pem` 或 `vendor/keygen_tool.py`。
4. 引导用户运行程序，复制他们的**机器指纹 (HWID)** 并发送给您。
5. 在您的电脑上使用 `keygen_tool.py` 生成 `license.dat` 并发送给用户。用户将其放在程序根目录下（或者 macOS 下的 `.app` 同级目录）即可激活。

### 签发 license 流程

在项目根目录执行下面两步：

**1）先生成密钥（只需一次）**

```bash
cd vendor
python3 keygen_tool.py init
```

会生成 `vendor/private_key.pem` 和 `vendor/public_key.pem`。

**2）再签发 license.dat（默认 30 天）**

```bash
python3 keygen_tool.py sign <HWID>
```

如果要指定天数：

```bash
python3 keygen_tool.py sign <HWID> 30
```

---

## 常见问题排查 (Troubleshooting)

### 1. 启动报 `ModuleNotFoundError: No module named 'data_mgr'`

- **原因**：`dist/obfuscated/` 目录中只有部分文件（如仅 `app.py`），其他业务模块缺失。PyArmor 许可证受限时只会混淆第一个文件，而 `.spec` 检测到 `dist/obfuscated/app.py` 存在就会使用混淆模式，`pathex` 指向 `dist/obfuscated/`，导致项目根目录的其他 `.py` 文件不在搜索路径中。
- **解决方法**：
  1. 检查 `dist/obfuscated/` 是否包含全部 28 个 `.py` 文件。
  2. 如果不全，删除整个 `dist/obfuscated/` 目录，重新执行 `pyinstaller -y ProImage_macOS.spec`，此时 spec 会自动回退到源码模式。

### 2. 点击程序后没有反应

- **原因**：程序运行在后台（Flask 服务器），且使用了 `--windowed` 或 `--noconsole` 导致没有终端窗口弹出。
- **解决方法**：
  - 打开浏览器访问 `http://127.0.0.1:5001` 查看程序是否已启动。
  - **调试模式**：打包时去掉 `--windowed` (Mac) 或 `--noconsole` (Windows)，这样程序运行报错时会在终端显示原因。
  - **Mac 查看日志**：在终端运行 `./dist/ProImage_AI.app/Contents/MacOS/ProImage_AI`。

### 3. 报错 "Script file 'dist/obfuscated/app.py' does not exist"

- **原因**：没有先运行 PyArmor 混淆脚本，或者运行路径不对。
- **解决方法**：请确保先运行"第一步：代码混淆"，且生成了 `dist/obfuscated` 目录。或者删除 `dist/obfuscated/` 使用源码模式打包。

### 4. 运行后提示 "License file missing"

- **原因**：根目录下缺少 `license.dat`。
- **解决方法**：按照"第三步：软件分发"的流程生成并放置授权文件。

### 5. macOS 提示“App 已损坏，无法打开”

- **常见原因**：
  - `.app` 被浏览器下载或跨机器复制后带有 Gatekeeper 隔离属性。
  - `.app` 没有 Apple Developer ID 签名和 notarization，`spctl` 会拒绝执行。
  - 使用普通 zip 工具压缩 `.app`，破坏或展开了内部符号链接，导致签名结构异常。
- **打包端处理**：PyInstaller 完成后务必执行以下步骤，再分发 ZIP：
  ```bash
  xattr -cr dist/ProImage_AI.app
  codesign --force --deep --sign - dist/ProImage_AI.app
  ditto -c -k --sequesterRsrc --keepParent dist/ProImage_AI.app dist/ProImage_AI_macOS.zip
  ```
- **验证方式**：
  ```bash
  codesign --verify --deep --strict --verbose=2 dist/ProImage_AI.app
  ```
  看到 `valid on disk` 和 `satisfies its Designated Requirement` 表示本地签名结构正常。
- **注意**：`spctl --assess --type execute dist/ProImage_AI.app` 对 ad-hoc 签名的包仍可能显示 `rejected`，这是因为没有正式 Developer ID 签名/公证，不等于包结构损坏。
- **用户端临时解决**：如果用户机器仍提示损坏，让用户在终端执行：
  ```bash
  xattr -cr /path/to/ProImage_AI.app
  ```
  然后右键点击 `.app`，选择“打开”。彻底避免该提示需要使用 Apple Developer ID 对 `.app` 签名并完成 notarization。

### 6. PyArmor 混淆报 `ERROR out of license`

- **原因**：当前 PyArmor trial 许可证不能混淆较大的脚本。本项目实测 `app.py` 约 64 KB 时会触发 `out of license`，而 24-32 KB 左右的脚本可正常混淆。
- **解决方法**：
  - 推荐购买/配置正式 PyArmor 许可证。
  - 或将过大的脚本拆分为更小模块后再混淆。
  - tools 页面已做兜底：混淆失败时会删除不完整的 `dist/obfuscated/`，自动改用源码模式继续 PyInstaller 打包。
