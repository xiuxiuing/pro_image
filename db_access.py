import os
import re
from contextlib import contextmanager

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


_QMARK_RE = re.compile(r"\?")
_PRAGMA_TABLE_INFO_RE = re.compile(r"^\s*PRAGMA\s+table_info\(([^)]+)\)\s*$", re.IGNORECASE)
_INSERT_TABLE_RE = re.compile(r"^\s*INSERT\s+INTO\s+\"?([A-Za-z_][\w]*)\"?\s*\(", re.IGNORECASE)

_CASE_SENSITIVE_COLS = [
    "skuId", "A核心品类", "A单件净含量", "A售卖数量", "A包装单位", "A尺寸",
    "A多维尺寸", "A品牌", "A型号", "A商品形态", "A关键属性词", "A颜色"
]
_UPPERCASE_IDENT_RES = [
    (ident, re.compile(rf'(?<!["\w\u4e00-\u9fff]){re.escape(ident)}(?!["\w\u4e00-\u9fff])'))
    for ident in _CASE_SENSITIVE_COLS
]


def translate_qmarks(sql: str):
    out = str(sql)
    # Auto-quote case sensitive columns if they are not already quoted
    parts = re.split(r"('(?:''|[^'])*')", out)
    for idx in range(0, len(parts), 2):
        part = parts[idx]
        for ident, rx in _UPPERCASE_IDENT_RES:
            part = rx.sub(f'"{ident}"', part)
        parts[idx] = part
    out = "".join(parts)

    idx = 0
    def repl(_match):
        nonlocal idx
        name = f"p{idx}"
        idx += 1
        return f":{name}"
    out = _QMARK_RE.sub(repl, out)
    return out, idx


def database_url_required() -> str:
    url = (os.environ.get("DATABASE_URL") or "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL is required for PostgreSQL mode")
    if url.startswith("sqlite:"):
        raise RuntimeError("SQLite DATABASE_URL is not supported in PostgreSQL single-db mode")
    return url


def params_to_mapping(params, count):
    if params is None:
        return {}
    if isinstance(params, dict):
        return params
    if not isinstance(params, (list, tuple)):
        params = [params]
    return {f"p{i}": params[i] if i < len(params) else None for i in range(count)}


class DbResult:
    def __init__(self, result=None, rows=None, lastrowid=None):
        self._result = result
        self._rows = rows
        self.rowcount = len(rows) if rows is not None else getattr(result, "rowcount", -1)
        self.lastrowid = lastrowid

    def fetchone(self):
        if self._rows is not None:
            return self._rows[0] if self._rows else None
        row = self._result.fetchone()
        return tuple(row) if row is not None else None

    def fetchall(self):
        if self._rows is not None:
            return list(self._rows)
        return [tuple(row) for row in self._result.fetchall()]


class DbConnection:
    _AUTOBEGIN = object()

    def __init__(self, engine: Engine):
        self.engine = engine
        self._conn = engine.connect()
        self._tx = None

    def get_table_columns(self, table: str):
        table_clean = table.strip('`"')
        rows = self._conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = :table
                ORDER BY ordinal_position
                """
            ),
            {"table": table_clean},
        ).fetchall()
        return [row[0] for row in rows]

    def execute(self, sql, params=None):
        pragma = _PRAGMA_TABLE_INFO_RE.match(str(sql))
        if pragma:
            table = pragma.group(1).strip().strip('`"')
            rows = self._conn.execute(
                text(
                    """
                    SELECT ordinal_position - 1 AS cid,
                           column_name AS name,
                           data_type AS type,
                           CASE WHEN is_nullable = 'NO' THEN 1 ELSE 0 END AS notnull,
                           column_default AS dflt_value,
                           0 AS pk
                    FROM information_schema.columns
                    WHERE table_schema = current_schema() AND table_name = :table
                    ORDER BY ordinal_position
                    """
                ),
                {"table": table},
            ).fetchall()
            return DbResult(rows=[tuple(row) for row in rows])

        translated, count = translate_qmarks(sql)
        result = self._conn.execute(text(translated), params_to_mapping(params, count))
        lastrowid = None
        m = _INSERT_TABLE_RE.match(translated)
        if m:
            table = m.group(1)
            has_id = self._conn.execute(
                text(
                    """
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = current_schema()
                      AND table_name = :table
                      AND column_name = 'id'
                    LIMIT 1
                    """
                ),
                {"table": table},
            ).fetchone()
            if has_id:
                seq = self._conn.execute(
                    text("SELECT pg_get_serial_sequence(:table, 'id')"),
                    {"table": table},
                ).fetchone()
                if seq and seq[0]:
                    row = self._conn.execute(
                        text(
                            """
                            SELECT last_value
                            FROM pg_sequences
                            WHERE schemaname = COALESCE(NULLIF(split_part(:seq, '.', 1), ''), current_schema())
                              AND sequencename = split_part(:seq, '.', 2)
                            """
                        ),
                        {"seq": seq[0]},
                    ).fetchone()
                    lastrowid = int(row[0]) if row and row[0] is not None else None
        return DbResult(result, lastrowid=lastrowid)

    def executemany(self, sql, seq_of_params):
        translated, count = translate_qmarks(sql)
        rows = [params_to_mapping(params, count) for params in (seq_of_params or [])]
        result = self._conn.execute(text(translated), rows)
        return DbResult(result)

    def read_sql(self, sql, params=None):
        translated, count = translate_qmarks(sql)
        return pd.read_sql(text(translated), self._conn, params=params_to_mapping(params, count))

    def to_sql(self, df, table_name, if_exists="append", index=False):
        return df.to_sql(table_name, self._conn, index=index, if_exists=if_exists, method="multi")

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    def __enter__(self):
        if self._tx is None:
            if self._conn.in_transaction():
                self._tx = self._AUTOBEGIN
            else:
                self._tx = self._conn.begin()
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._tx is not None:
            if self._tx is self._AUTOBEGIN:
                if exc_type is None:
                    self._conn.commit()
                else:
                    self._conn.rollback()
            else:
                if exc_type is None:
                    self._tx.commit()
                else:
                    self._tx.rollback()
            self._tx = None
        self.close()
        return False


class Database:
    def __init__(self, url=None):
        self.url = url or database_url_required()
        self.engine = create_engine(self.url, pool_pre_ping=True, future=True)

    def connect(self):
        return DbConnection(self.engine)

    def close(self):
        self.engine.dispose()

    @contextmanager
    def begin(self):
        with self.connect() as conn:
            yield conn
