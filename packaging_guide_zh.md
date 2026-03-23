# 项目打包与代码混淆指南

为了保护您的软件不被破解和非法传播，请按照以下步骤对代码进行混淆，并将其打包为单个 EXE 文件。

## 前置准备
1. **确保路径正确**: 必须在项目根目录下执行（包含 `app.py` 的目录）。
   ```bash
   cd /Users/admin/Documents/Antigravity_projects/pro_image/0307/sys_0314
   ```
2. **安装必要工具**:
```bash
pip install pyarmor pyinstaller
```

## 第一步：代码混淆 (PyArmor)
PyArmor 会对 Python 字节码进行混淆，使人类和反编译器无法读取。

1.  **混淆核心逻辑**:
    ```bash
    pyarmor gen -O dist/obfuscated app.py data_mgr.py license_utils.py main_030822.py extract_info_ai2.py utils.py
    ```
    执行后会生成一个 `obfuscated` 目录，其中包含受保护的文件以及 `pyarmor_runtime` 运行库。

## 第二步：打包 (PyInstaller)
使用 PyInstaller 将所有内容打包。

### Windows 系统打包 (生成 EXE)
```bash
pyinstaller --onefile --noconsole \
    --add-data "templates;templates" \
    --hidden-import cryptography \
    --name "ProImage_AI" \
    dist/obfuscated/app.py
```
> [!NOTE]
> 在 Windows 下，`--add-data` 的路径分隔符是分号 `;`。

PyInstaller ProImage_AI.spec

### macOS 系统打包 (生成 .app)
```bash
pyinstaller --windowed \
    --add-data "templates:templates" \
    --hidden-import cryptography \
    --name "ProImage_AI" \
    dist/obfuscated/app.py
```
> [!NOTE]
> 在 macOS 下，`--add-data` 的路径分隔符是冒号 `:`，且使用 `--windowed` 代替 `--noconsole`。

## 第三步：软件分发
分发软件时：
1.  **Windows**: 仅提供 `dist/ProImage_AI.exe`。
2.  **macOS**: 提供 `dist/ProImage_AI.app`（可以压缩为 ZIP 分发）。
3.  **绝对不要**分发 `private_key.pem` 或 `vendor/keygen_tool.py`。
4.  引导用户运行程序，复制他们的**机器指纹 (HWID)** 并发送给您。
5.  在您的电脑上使用 `vendor/keygen_tool.py sign <HWID>` 生成 `license.dat` 并发送给用户。用户将其放在程序根目录下（或者 macOS 下的 `.app` 同级目录）即可激活。

## 常见问题排查 (Troubleshooting)

### 1. 点击程序后没有反应
- **原因**: 程序运行在后台（Flask 服务器），且使用了 `--windowed` 或 `--noconsole` 导致没有终端窗口弹出。
- **解决方法**: 
  - 打开浏览器访问 `http://127.0.0.1:5001` 查看程序是否已启动。
  - **调试模式**: 打包时去掉 `--windowed` (Mac) 或 `--noconsole` (Windows)，这样程序运行报错时会在终端显示原因。
  - **Mac 查看日志**: 在终端运行 `./dist/ProImage_AI.app/Contents/MacOS/ProImage_AI`。

### 2. 报错 "Script file 'dist/obfuscated/app.py' does not exist"
- **原因**: 没有先运行 PyArmor 混淆脚本，或者运行路径不对。
- **解决方法**: 请确保先运行“第一步：代码混淆”，且生成了 `dist/obfuscated` 目录。

### 3. 运行后提示 "License file missing"
- **原因**: 根目录下缺少 `license.dat`。
- **解决方法**: 按照“第三步：软件分发”的流程生成并放置授权文件。
