# -*- mode: python ; coding: utf-8 -*-
# 混合方案：Nuitka 编译业务模块 (.so) + PyInstaller 打包
# 步骤：
#   1. 先用 Nuitka --module 编译业务模块到 nuitka_modules/
#   2. 运行 build_nuitka_hybrid.sh 准备 _build_src/
#   3. pyinstaller -y ProImage_nuitka_macOS.spec

import os, glob
from PyInstaller.utils.hooks import copy_metadata

try:
    _root = os.path.abspath(os.path.dirname(__file__))
except NameError:
    _root = os.getcwd()

_src = os.path.join(_root, '_build_src')
_entry = [os.path.join(_src, 'app.py')]
_pathex = [_src]

_binaries = []
for so in glob.glob(os.path.join(_src, '*.cpython-312-darwin.so')):
    _binaries.append((so, '.'))

_datas = [
    (os.path.join(_src, 'templates'), 'templates'),
    (os.path.join(_src, 'static'), 'static'),
] + copy_metadata('regex') + copy_metadata('tqdm') + copy_metadata('transformers')

if os.path.isdir(os.path.join(_root, 'models')):
    _datas.append(('models', 'models'))

a = Analysis(
    _entry,
    pathex=_pathex,
    binaries=_binaries,
    datas=_datas,
    hiddenimports=[
        'flask', 'pandas', 'numpy', 'torch', 'torchvision', 'torchaudio',
        'openpyxl', 'PIL', 'PIL.Image', 'faiss',
        'transformers', 'google.genai', 'pydantic', 'cryptography',
        'werkzeug', 'jinja2', 'markupsafe', 'itsdangerous',
        'click', 'tqdm', 'requests', 'filelock', 'regex', 'safetensors',
        'scipy', 'sentence_transformers',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'data_mgr', 'data_mgr_base', 'data_mgr_import', 'data_mgr_query',
        'data_mgr_ops', 'data_mgr_export',
        'license_utils', 'main_030822',
        'extract_info_ai2', 'utils', 'merge_sku_data',
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
    strip=False,
    upx=False,
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
    strip=False,
    upx=False,
    upx_exclude=[],
    name='ProImage_AI',
)

app = BUNDLE(
    coll,
    name='ProImage_AI.app',
    icon=None,
    bundle_identifier='com.proimage.ai',
)
