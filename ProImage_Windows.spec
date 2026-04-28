# -*- mode: python ; coding: utf-8 -*-
# 在 Windows 上、项目根目录执行: pyinstaller ProImage_Windows.spec
# 详见 BUILD_WINDOWS.md
#
# 禁止加入 datas（内部/敏感，勿随 exe 分发）：
#   packaging_guide_zh.md、BUILD_WINDOWS.md、license.dat、keygen_tool.py、private_key.pem、*.pem 等
# 仅白名单：templates、static、models（见下方 _datas）

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
    name='ProImage',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
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
    upx=True,
    upx_exclude=[],
    name='ProImage',
)
