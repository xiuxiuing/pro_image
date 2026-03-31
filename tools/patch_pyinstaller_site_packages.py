#!/usr/bin/env python3
"""
在**当前 Python 环境**的 site-packages 中打补丁后再执行 pyinstaller。

1) torch._numpy._ufuncs：`vars()[name]` 在冻结环境下 NameError（见 StackOverflow 78375284）
2) scipy.stats._distn_infrastructure：若 `for obj in ...` 循环 0 次，Python 3.12 中 `del obj` 会 NameError（PyInstaller 下 dir() 可能无 _doc_*）

用法（项目根目录）:
  python3 tools/patch_pyinstaller_site_packages.py
"""
from __future__ import annotations

import pathlib
import sys


def patch_torch_ufuncs() -> bool:
    try:
        import torch as T
    except ImportError:
        print("torch 未安装，跳过 torch 补丁。", file=sys.stderr)
        return True

    path = pathlib.Path(T.__file__).resolve().parent / "_numpy" / "_ufuncs.py"
    if not path.is_file():
        print(f"未找到 {path}，跳过 torch 补丁。", file=sys.stderr)
        return True

    text = path.read_text(encoding="utf-8")
    if "ufunc_name = name" in text and "vars()[ufunc_name]" in text:
        print(f"torch 已打过补丁: {path}")
        return True

    old_b = (
        "for name in _binary:\n"
        "    ufunc = getattr(_binary_ufuncs_impl, name)\n"
        "    vars()[name] = deco_binary_ufunc(ufunc)\n"
    )
    new_b = (
        "for name in _binary:\n"
        "    ufunc = getattr(_binary_ufuncs_impl, name)\n"
        "    ufunc_name = name\n"
        "    vars()[ufunc_name] = deco_binary_ufunc(ufunc)\n"
    )
    old_u = (
        "for name in _unary:\n"
        "    ufunc = getattr(_unary_ufuncs_impl, name)\n"
        "    vars()[name] = deco_unary_ufunc(ufunc)\n"
    )
    new_u = (
        "for name in _unary:\n"
        "    ufunc = getattr(_unary_ufuncs_impl, name)\n"
        "    ufunc_name = name\n"
        "    vars()[ufunc_name] = deco_unary_ufunc(ufunc)\n"
    )

    if old_b not in text or old_u not in text:
        print(
            f"{path} 与预期不符（torch 版本可能已变）。请对照 "
            "https://stackoverflow.com/questions/78375284",
            file=sys.stderr,
        )
        return False

    path.write_text(text.replace(old_b, new_b).replace(old_u, new_u), encoding="utf-8")
    print(f"已写入 torch 补丁: {path}")
    return True


def patch_scipy_distn() -> bool:
    try:
        import scipy.stats as sps
    except ImportError:
        print("scipy 未安装，跳过 scipy 补丁。", file=sys.stderr)
        return True

    path = (
        pathlib.Path(sps.__file__).resolve().parent / "_distn_infrastructure.py"
    )
    if not path.is_file():
        print(f"未找到 {path}，跳过 scipy 补丁。", file=sys.stderr)
        return True

    text = path.read_text(encoding="utf-8")
    if "except NameError:\n    pass" in text and "del obj" in text:
        # 可能已手动改过；避免重复插入
        marker = "try:\n    del obj\nexcept NameError:\n    pass"
        if marker in text:
            print(f"scipy 已打过补丁: {path}")
            return True

    old = (
        "# clean up all the separate docstring elements, we do not need them anymore\n"
        "for obj in [s for s in dir() if s.startswith('_doc_')]:\n"
        "    exec('del ' + obj)\n"
        "del obj\n"
    )
    new = (
        "# clean up all the separate docstring elements, we do not need them anymore\n"
        "for obj in [s for s in dir() if s.startswith('_doc_')]:\n"
        "    exec('del ' + obj)\n"
        "try:\n"
        "    del obj\n"
        "except NameError:\n"
        "    pass\n"
    )

    if old not in text:
        if "try:\n    del obj\nexcept NameError" in text:
            print(f"scipy 似已兼容空循环: {path}")
            return True
        print(
            f"{path} 与预期不符（scipy 版本可能已变）。",
            file=sys.stderr,
        )
        return False

    path.write_text(text.replace(old, new, 1), encoding="utf-8")
    print(f"已写入 scipy 补丁: {path}")
    return True


def main() -> int:
    ok = patch_torch_ufuncs() and patch_scipy_distn()
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
