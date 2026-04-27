import pandas as pd
import os
import re
import threading
import sqlite3
import time
import json
import utils

# --- Constants ---
INTERNAL_COLUMNS = ['淘汰标记', '是否淘汰', '新活动价', '新售价', '跟价店']
INTERNAL_EXPORT_KEYS = ['__idx', '淘汰标记', '_row_orig_idx', 'ref_name_store', 'ref_image_store']

FIELD_MAPPINGS = {
    '图片': '主图链接', 'SKUID': 'skuId', '商品名称': '商品名称', '菜单名': '商品名称',
    'A商品名称': '商品名称', '规格': '规格名称', '规格名称': '规格名称', '规格名': '规格名称',
    'A规格': '规格名称', '美团外卖渠道售价': '原价',
    '活动价': '活动价', '单件折扣价': '活动价',
    '月销量': '销售', '销售': '销售', '条码': '商品条码', '商品条码': '商品条码',
    '美团类目一级': '美团类目一级',
    '美团类目二级': '美团类目二级',
    '美团类目三级': '美团类目三级', '三级类目': '美团类目三级'
}

# --- Core Database Columns ---
CORE_MAIN_COLUMNS = [
    'project_id', 'skuId', '_row_orig_idx', '商品名称', '规格名称', '原价', '活动价', '销售', 
    '主图链接', '商品条码', 'SPUID', '美团类目一级', '美团类目二级', '美团类目三级', '采购价', '采购单价', '采购链接',
    'A单件净含量', 'A售卖数量', 'A包装单位', 'A颜色', 'A尺寸', 'A型号',
    '淘汰标记', '是否淘汰', '新活动价', '新售价', '跟价店', '现价毛利', '跟价毛利'
]

CORE_COMP_COLUMNS = [
    'project_id', 'store_id', 'skuId', '商品名称', '规格名称', '原价', '活动价', '销售', 
    '主图链接', '商品条码', 'SPUID', '美团类目一级', '美团类目二级', '美团类目三级',
    'A单件净含量', 'A售卖数量', 'A包装单位', 'A颜色', 'A尺寸', 'A型号',
]

MAPPING_VERSION = "3.2"  # 3.2: 主/竞 SQLite 保留 Gemini A* 六列，工作台可显示 0A*/1A* 等

_SAFE_COL_RE = re.compile(r'^[\w\u4e00-\u9fff]+$')

class DataManagerBase:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.db_path = os.path.join(base_dir, "pro_image.db")
        self._db_lock = threading.RLock()
        
        # Project-specific state
        self.active_project_id = 1
        self.active_project_name = "默认项目"
        self.target_file = ""
        self.output_file = ""
        self.project_dir = "" # Base dir for this project's files
        self.source_files = []
        self.store_names = []
        self.main_store_name = ""
        self.match_config = {}
        
        self.grid_df = None
        self.main_df = None
        self.store_dfs = {} 
        
        self._init_db()
        self._load_active_project()
        self.load_data()

    def _get_project_dirs(self, pid):
        """Returns standard subdirectories for a project."""
        pdir = os.path.join(self.base_dir, "uploads", f"project_{pid}")
        return {
            "root": pdir,
            "sources": os.path.join(pdir, "sources"),
            "outputs": os.path.join(pdir, "outputs"),
            "cache": os.path.join(pdir, "cache")
        }

    def _ensure_project_dirs(self, pid):
        """Ensures all standard project subdirectories exist."""
        dirs = self._get_project_dirs(pid)
        for _, path in dirs.items():
            os.makedirs(path, exist_ok=True)
        return dirs

    def _load_active_project(self):
        with self._db_lock:
            conn = self._get_conn()
            try:
                # Get active project
                cur = conn.execute("SELECT id, name, COALESCE(match_config, '') FROM projects WHERE is_active = 1 LIMIT 1")
                row = cur.fetchone()
                if not row:
                    cur = conn.execute("SELECT id, name, COALESCE(match_config, '') FROM projects LIMIT 1")
                    row = cur.fetchone()
                
                if row:
                    self.active_project_id, self.active_project_name = row[0], row[1]
                    try:
                        self.match_config = json.loads(row[2]) if row[2] else {}
                    except Exception:
                        self.match_config = {}
                    
                    # Ensure dirs exist and set project_dir
                    dirs = self._ensure_project_dirs(self.active_project_id)
                    self.project_dir = dirs["root"]
                    
                    # Load files
                    cur = conn.execute("SELECT type, local_path, store_name FROM project_files WHERE project_id = ? ORDER BY id ASC", (self.active_project_id,))
                    files = cur.fetchall()
                    
                    self.source_files = []
                    self.store_names = []
                    for f_type, path, store in files:
                        if f_type == 'main':
                            self.target_file = path
                            self.main_store_name = store
                        elif f_type == 'comp':
                            self.source_files.append(path)
                            self.store_names.append(store)
                    
                    # Output file defaults to outputs/ subfolder
                    self.output_file = os.path.join(dirs["outputs"], f"output_{self.active_project_id}.xlsx")
            finally:
                conn.close()

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _init_db(self):
        with self._db_lock:
            conn = self._get_conn()
            try:
                with conn:
                    # Meta and Project Management
                    conn.execute("CREATE TABLE IF NOT EXISTS meta_info (key TEXT PRIMARY KEY, value TEXT)")
                    conn.execute("CREATE TABLE IF NOT EXISTS projects (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, is_active INTEGER DEFAULT 0, status TEXT DEFAULT 'ready', analysis_started_at TEXT, match_config TEXT DEFAULT '')")
                    conn.execute("CREATE TABLE IF NOT EXISTS project_files (id INTEGER PRIMARY KEY AUTOINCREMENT, project_id INTEGER, type TEXT, local_path TEXT, store_name TEXT, FOREIGN KEY(project_id) REFERENCES projects(id))")
                    
                    # Core Data Tables (with project_id awareness)
                    main_cols = ", ".join([f"{c} TEXT" for c in CORE_MAIN_COLUMNS if c not in ['project_id', 'skuId', '_row_orig_idx']])
                    conn.execute(f"CREATE TABLE IF NOT EXISTS main_products (project_id INTEGER, skuId TEXT, _row_orig_idx INT, {main_cols}, PRIMARY KEY(project_id, skuId))")
                    
                    conn.execute("CREATE TABLE IF NOT EXISTS product_links (project_id INTEGER, main_sku_id TEXT, store_id TEXT, comp_sku_id TEXT, similarity REAL, match_type TEXT, is_new_add TEXT)")
                    
                    comp_cols = ", ".join([f"{c} TEXT" for c in CORE_COMP_COLUMNS if c not in ['project_id', 'store_id', 'skuId']])
                    conn.execute(f"CREATE TABLE IF NOT EXISTS comp_products (project_id INTEGER, store_id TEXT, skuId TEXT, {comp_cols})")
                    
                    # --- Robust Migration: Add missing columns to all core tables ---
                    # 1. Check main_products
                    cursor = conn.execute("PRAGMA table_info(main_products)")
                    existing_main = [c[1] for c in cursor.fetchall()]
                    for col in CORE_MAIN_COLUMNS:
                        if col not in existing_main:
                            conn.execute(f"ALTER TABLE main_products ADD COLUMN {col} TEXT")
                            print(f"Migration: Added missing column [{col}] to main_products")

                    if "is_handled" not in existing_main:
                        conn.execute("ALTER TABLE main_products ADD COLUMN is_handled TEXT DEFAULT '0'")
                        print("Migration: Added is_handled column to main_products")

                    for ref_col in ("ref_name_store", "ref_image_store"):
                        if ref_col not in existing_main:
                            conn.execute(f"ALTER TABLE main_products ADD COLUMN {ref_col} TEXT DEFAULT ''")
                            print(f"Migration: Added {ref_col} column to main_products")

                    # 2. Check comp_products
                    cursor = conn.execute("PRAGMA table_info(comp_products)")
                    existing_comp = [c[1] for c in cursor.fetchall()]
                    for col in CORE_COMP_COLUMNS:
                        if col not in existing_comp:
                            conn.execute(f"ALTER TABLE comp_products ADD COLUMN {col} TEXT")
                            print(f"Migration: Added missing column [{col}] to comp_products")

                    # 3. Check extra columns (status, project_id etc)
                    tables_to_check = ["main_products", "product_links", "comp_products"]
                    for table in tables_to_check:
                        cursor = conn.execute(f"PRAGMA table_info({table})")
                        cols = [c[1] for c in cursor.fetchall()]
                        if "project_id" not in cols:
                            conn.execute(f"ALTER TABLE {table} ADD COLUMN project_id INTEGER DEFAULT 1")
                            print(f"Migration: Added project_id to {table}")

                    # 4. Check projects table extra columns
                    cursor = conn.execute("PRAGMA table_info(projects)")
                    proj_cols = [c[1] for c in cursor.fetchall()]
                    if "status" not in proj_cols:
                        conn.execute("ALTER TABLE projects ADD COLUMN status TEXT DEFAULT 'ready'")
                    if "analysis_started_at" not in proj_cols:
                        conn.execute("ALTER TABLE projects ADD COLUMN analysis_started_at TEXT")
                    if "match_config" not in proj_cols:
                        conn.execute("ALTER TABLE projects ADD COLUMN match_config TEXT DEFAULT ''")
                    if "rule_template_id" not in proj_cols:
                        conn.execute("ALTER TABLE projects ADD COLUMN rule_template_id INTEGER")

                    # --- rule_templates (后验规则模板) ---
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS rule_templates (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            name TEXT NOT NULL,
                            description TEXT DEFAULT '',
                            config_json TEXT NOT NULL DEFAULT '{}',
                            created_at TEXT,
                            updated_at TEXT
                        )
                        """
                    )
                    _rtc = conn.execute("SELECT COUNT(*) FROM rule_templates").fetchone()[0]
                    if _rtc == 0:
                        import json as _json
                        try:
                            import post_match_engine as _pme
                            _cfg = _json.dumps(
                                _pme.get_builtin_default_template(), ensure_ascii=False, separators=(",", ":")
                            )
                        except Exception:
                            _cfg = '{"v":3,"rule_groups":[]}'
                        _now = __import__("time").strftime("%Y-%m-%d %H:%M:%S")
                        conn.execute(
                            "INSERT INTO rule_templates (name, description, config_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                            ("空白规则模板", "按三级类目创建规则组；未覆盖到的类目不做后验过滤", _cfg, _now, _now),
                        )
                        print("Created default rule_templates row.")

                    # Performance indexes for unlinked-pool / grid queries
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_product_links_lookup ON product_links(project_id, store_id, comp_sku_id)")
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_comp_products_store ON comp_products(project_id, store_id)")
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_product_links_main ON product_links(project_id, main_sku_id)")

                    # Initialize default project if none exist
                    cursor = conn.execute("SELECT COUNT(*) FROM projects")
                    if cursor.fetchone()[0] == 0:
                        conn.execute("INSERT INTO projects (id, name, is_active) VALUES (1, '默认项目', 1)")
                        print("Created default project.")

                    # Startup recovery: fix orphaned 'analyzing' status from crashed runs
                    for row in conn.execute("SELECT id FROM projects WHERE status = 'analyzing'").fetchall():
                        orphan_pid = row[0]
                        output_path = os.path.join(self.base_dir, "uploads",
                                                   f"project_{orphan_pid}", "outputs",
                                                   f"output_{orphan_pid}.xlsx")
                        new_status = 'ready' if os.path.exists(output_path) else 'failed'
                        conn.execute("UPDATE projects SET status = ?, analysis_started_at = NULL WHERE id = ?",
                                     (new_status, orphan_pid))
                        print(f"Startup recovery: project {orphan_pid} → {new_status}")
            finally:
                conn.close()
