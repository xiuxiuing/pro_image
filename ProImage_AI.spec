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

a = Analysis(
    _entry,
    pathex=_pathex,
    binaries=[],
    datas=[
        ('templates', 'templates'), 
        ('static', 'static'),
        ('data', 'data'),
        ('models', 'models')
    ],
    hiddenimports=[
        'flask', 'pandas', 'numpy', 'torch', 'torchvision',
        'openpyxl', 'PIL', 'PIL.Image', 'faiss', 
        'transformers', 'google.genai', 'openai', 'pydantic', 'cryptography',
        'data_mgr', 'data_mgr_base', 'data_mgr_import', 'data_mgr_query', 'data_mgr_query_unlinked',
        'data_mgr_ops', 'data_mgr_export', 'data_mgr_rule_templates',
        'auth_manager', 'app_ops', 'app_ops_tasks', 'app_data', 'app_data_projects', 'app_data_rules', 'app_data_grid', 'app_ops_extra',
        'field_registry', 'quality_preflight', 'packaging_core',
        'license_utils', 'main_030822', 'extract_info_ai2', 'extract_info_schema', 'extract_info_rules',
        'product_text_extract', 'post_match_engine', 'utils',
        'merge_sku_data', 'werkzeug', 'jinja2', 'markupsafe', 'itsdangerous', 
        'click', 'tqdm', 'requests', 'filelock', 'regex', 'safetensors',
        'scipy',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'matplotlib', 'notebook', 'scipy.io.wavfile', 'tkinter', 'torchaudio',
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
