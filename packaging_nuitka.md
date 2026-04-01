# Nuitka 打包操作手册（macOS）

> **内部文档**：本文仅用于开发与打包流程说明，不随产物分发给终端用户。

## 方案概述

使用 Nuitka 将 7 个业务模块编译为**原生二进制**（无法反编译），第三方库原样打包，最终生成 macOS `.app`。

| 层级 | 文件 | 处理方式 | 保护级别 |
|------|------|---------|---------|
| 业务层 | `app.py` `data_mgr.py` `license_utils.py` `main_030822.py` `extract_info_ai2.py` `utils.py` `merge_sku_data.py` | 编译为 C → 原生 `.so` | 极高 |
| 第三方库 | torch, transformers, pandas, flask 等 | 原样打包（.pyc） | 无需保护 |
| 资源文件 | templates/, static/ | 原样复制 | 不涉及 |

---

## 前置准备

### 1. 环境要求

- **Python**: 3.12（与开发环境一致）
- **Xcode Command Line Tools**: 提供 C 编译器
  ```bash
  xcode-select --install   # 如未安装
  ```
- **项目根目录**: 必须在包含 `app.py` 的目录下执行

### 2. 安装 Nuitka

```bash
pip3 install nuitka ordered-set
```

验证安装：

```bash
python3 -m nuitka --version
```

---

## 构建步骤

### 1. 关闭正在运行的项目

确保 5001 端口未被占用：

```bash
lsof -ti :5001 | xargs kill -9 2>/dev/null
```

### 2. 执行构建

在项目根目录执行以下命令（预计 10-20 分钟）：

```bash
python3 -m nuitka \
    --standalone \
    --macos-create-app-bundle \
    --macos-app-name="ProImage_AI" \
    --output-dir=dist_nuitka \
    \
    --include-data-dir=templates=templates \
    --include-data-dir=static=static \
    \
    --include-module=data_mgr \
    --include-module=license_utils \
    --include-module=main_030822 \
    --include-module=extract_info_ai2 \
    --include-module=utils \
    --include-module=merge_sku_data \
    \
    --nofollow-import-to=torch \
    --nofollow-import-to=torchvision \
    --nofollow-import-to=torchaudio \
    --nofollow-import-to=transformers \
    --nofollow-import-to=sentence_transformers \
    --nofollow-import-to=faiss \
    --nofollow-import-to=numpy \
    --nofollow-import-to=pandas \
    --nofollow-import-to=scipy \
    --nofollow-import-to=sklearn \
    --nofollow-import-to=PIL \
    --nofollow-import-to=google \
    --nofollow-import-to=pydantic \
    --nofollow-import-to=openpyxl \
    \
    --include-package=torch \
    --include-package=torchvision \
    --include-package=torchaudio \
    --include-package=transformers \
    --include-package=sentence_transformers \
    --include-package=faiss \
    --include-package=numpy \
    --include-package=pandas \
    --include-package=scipy \
    --include-package=sklearn \
    --include-package=PIL \
    --include-package=google \
    --include-package=pydantic \
    --include-package=openpyxl \
    \
    app.py
```

**参数说明**：

| 参数 | 作用 |
|------|------|
| `--standalone` | 生成自包含目录，包含 Python 运行时和所有依赖 |
| `--macos-create-app-bundle` | 自动生成 `.app` 目录结构 |
| `--macos-app-name` | 设置 `.app` 名称 |
| `--output-dir` | 产物输出目录（与 PyInstaller 的 `dist/` 分开） |
| `--include-data-dir` | 将资源目录原样复制到产物中 |
| `--include-module=data_mgr` | 将业务模块编译为原生 C 二进制（核心保护） |
| `--nofollow-import-to=torch` | 不将 torch 的 Python 源码编译为 C（节省时间、避免兼容问题） |
| `--include-package=torch` | 将 torch 整包打入产物中（以 .pyc 字节码形式） |

> **核心逻辑**：`--nofollow-import-to` + `--include-package` 配合使用 = "打包但不编译"。
> 业务模块不加 `--nofollow`，所以会被编译为原生二进制。

### 3. 验证构建结果

构建完成后，在终端直接运行查看是否有报错：

```bash
./dist_nuitka/ProImage_AI.app/Contents/MacOS/ProImage_AI
```

预期输出应包含：

```
 * Serving Flask app 'app'
 * Running on http://127.0.0.1:5001
```

确认无 `ModuleNotFoundError` 或其他异常后，按 `Ctrl+C` 停止。

### 4. 处理 macOS 安全提示

首次在其他 Mac 上运行可能提示"无法验证开发者"，执行：

```bash
xattr -cr dist_nuitka/ProImage_AI.app
```

---

## 产物说明

```
dist_nuitka/
└── ProImage_AI.app/         ← 分发给用户的 .app
    └── Contents/
        ├── MacOS/
        │   └── ProImage_AI  ← 原生二进制入口
        └── Resources/
            ├── app.so       ← 业务代码（已编译，无法反编译）
            ├── data_mgr.so
            ├── license_utils.so
            ├── ...
            ├── templates/   ← 前端模板
            ├── static/      ← 静态资源
            ├── torch/       ← 第三方库（.pyc 形式）
            └── ...
```

> `models/` 目录不打包。应用运行时会自动下载模型到 `ProImage_data/` 目录。

---

## 常见问题排查

### 1. 构建时报 `ModuleNotFoundError: No module named 'xxx'`

某个第三方库的子模块未被自动收集。添加对应参数后重新构建：

```bash
# 例如缺少 numexpr
--include-package=numexpr
```

### 2. 构建时内存不足或编译器崩溃

限制并行编译任务数：

```bash
python3 -m nuitka --jobs=2 ...   # 默认使用全部 CPU 核心
```

### 3. 运行时报 `ImportError: cannot import name 'xxx' from 'torch'`

torch 的某些子模块有延迟加载。补充对应子包：

```bash
--include-package=torch.nn
--include-package=torch.utils
```

### 4. 启动后浏览器无反应

程序在后台运行（无终端窗口），手动打开浏览器访问：

```
http://127.0.0.1:5001
```

### 5. 重新构建前需要清理吗？

Nuitka 有增量编译缓存，一般不需要清理。如需完全重建：

```bash
rm -rf dist_nuitka/ app.build/
```

---

## 与 PyInstaller 方案对比

| 维度 | PyInstaller（当前方案） | Nuitka（本方案） |
|------|----------------------|-----------------|
| 代码保护 | .pyc 字节码（可反编译） | **原生二进制（无法反编译）** |
| 额外许可证 | 无（PyArmor 需付费） | **无** |
| 构建时间 | ~90 秒 | ~10-20 分钟 |
| 产物大小 | ~2.5 GB | ~2.5 GB |
| 启动速度 | 相当 | 略快 |
| 兼容性 | 成熟稳定 | 良好（需验证） |
| 代码改动 | 无 | **无** |

---

## 分发流程

与 PyInstaller 方案一致：

1. 将 `dist_nuitka/ProImage_AI.app` 压缩为 ZIP 分发
2. **不要**分发 `private_key.pem` 或 `vendor/keygen_tool.py`
3. 用户首次运行后获取 HWID，使用 `keygen_tool.py sign <HWID>` 签发 `license.dat`
4. 用户将 `license.dat` 放在 `.app` 同级目录即可激活
