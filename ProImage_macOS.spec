# -*- mode: python ; coding: utf-8 -*-
# macOS：在项目根目录执行
#   python3 tools/patch_pyinstaller_site_packages.py   # 打包前修补 torch/scipy，避免 .app 启动即崩溃
#   export PATH="$HOME/Library/Python/3.12/bin:$PATH"   # 若 pip --user 安装
#   pyinstaller -y ProImage_macOS.spec
#
# 说明：若 PyArmor 试用/许可证无法完成混淆，可直接用本 spec（入口为源码 app.py）。
# 禁止加入 datas：packaging_guide_zh.md、license.dat、私钥等（仅白名单见下）

import os
from PyInstaller.utils.hooks import copy_metadata

try:
    _root = os.path.abspath(os.path.dirname(__file__))
except NameError:
    _root = os.getcwd()
_obf_app = os.path.join(_root, 'dist', 'obfuscated', 'app.py')
if os.path.isfile(_obf_app):
    _entry = [_obf_app]
    _pathex = [os.path.join(_root, 'dist', 'obfuscated')]
else:
    # 未运行 PyArmor 时使用源码入口（与 packaging_guide_zh.md 故障排查一致）
    _entry = [os.path.join(_root, 'app.py')]
    _pathex = [_root]

_datas = [
    ('templates', 'templates'),
    ('static', 'static'),
] + copy_metadata('regex') + copy_metadata('tqdm') + copy_metadata('transformers')
if os.path.isdir(os.path.join(_root, 'models')):
    _datas.append(('models', 'models'))

a = Analysis(
    _entry,
    pathex=_pathex,
    binaries=[],
    datas=_datas,
    hiddenimports=[
        'flask', 'pandas', 'numpy', 'torch', 'torchvision', 'torchaudio',
        'openpyxl', 'PIL', 'PIL.Image', 'faiss',
        'transformers', 'google.genai', 'pydantic', 'cryptography',
        'data_mgr', 'data_mgr_base', 'data_mgr_import', 'data_mgr_query', 'data_mgr_ops', 'data_mgr_export',
        'license_utils', 'main_030822', 'extract_info_ai2', 'utils',
        'merge_sku_data', 'werkzeug', 'jinja2', 'markupsafe', 'itsdangerous', 
        'click', 'tqdm', 'requests', 'filelock', 'regex', 'safetensors',
        'scipy',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'matplotlib', 'notebook', 'scipy.io.wavfile', 'tkinter',
        'PIL.ImageQt', 'PIL.ImageTk', 'IPython', 'jupyter_client',
        'torch.utils.tensorboard',
    ],
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='ProImage_AI',
    debug=False,
    bootloader_ignore_signals=False,
    strip=True,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=True,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=True,
    upx=True,
    upx_exclude=[],
    name='ProImage_AI',
)

app = BUNDLE(
    coll,
    name='ProImage_AI.app',
    icon=None,
    bundle_identifier='com.proimage.ai',
)
