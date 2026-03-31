# -*- mode: python ; coding: utf-8 -*-
# macOS：在项目根目录执行
#   python3 tools/patch_pyinstaller_site_packages.py   # 打包前修补 torch/scipy，避免 .app 启动即崩溃
#   export PATH="$HOME/Library/Python/3.12/bin:$PATH"   # 若 pip --user 安装
#   pyinstaller -y ProImage_macOS.spec
#
# 说明：若 PyArmor 试用/许可证无法完成混淆，可直接用本 spec（入口为源码 app.py）。
# 禁止加入 datas：packaging_guide_zh.md、license.dat、私钥等（仅白名单见下）

import os

_datas = [
    ('templates', 'templates'),
    ('static', 'static'),
]
if os.path.isdir('models'):
    _datas.append(('models', 'models'))

a = Analysis(
    ['dist/obfuscated/app.py'],
    pathex=[os.path.abspath('dist/obfuscated')],
    binaries=[],
    datas=_datas,
    hiddenimports=[
        'flask', 'pandas', 'numpy', 'torch', 'torchvision', 'torchaudio',
        'openpyxl', 'PIL', 'PIL.Image', 'sentence_transformers', 'faiss',
        'transformers', 'google.genai', 'pydantic', 'cryptography',
        'data_mgr', 'license_utils', 'main_030822', 'extract_info_ai2', 'utils',
        'merge_sku_data', 'werkzeug', 'jinja2', 'markupsafe', 'itsdangerous', 
        'click', 'tqdm', 'requests', 'filelock', 'regex', 'safetensors',
        'scipy', 'sklearn', 'sklearn.utils._cython_blas', 'sklearn.neighbors.typedefs',
        'sklearn.neighbors.quad_tree', 'sklearn.tree._utils',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
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
