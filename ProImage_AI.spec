import os

a = Analysis(
    ['dist/obfuscated/app.py'],
    pathex=[os.path.abspath('dist/obfuscated')],
    binaries=[],
    datas=[
        ('templates', 'templates'), 
        ('static', 'static'),
        ('models', 'models')
    ],
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
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='ProImage_AI',
)
app = BUNDLE(
    coll,
    name='ProImage_AI.app',
    icon=None,
    bundle_identifier=None,
)
