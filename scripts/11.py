from __future__ import annotations

import re
import shutil
from pathlib import Path
from datetime import datetime

TS = datetime.now().strftime("%Y%m%d-%H%M%S")

def backup(path: Path) -> Path:
    bak = path.with_name(path.name + f".bak-{TS}")
    shutil.copy2(path, bak)
    return bak

def read_text(p: Path) -> str:
    return p.read_text(encoding="utf-8", errors="replace")

def write_text(p: Path, s: str) -> None:
    p.write_text(s, encoding="utf-8", newline="\n")

HELPERS = """
# --- auto_unwrap_tenant_recursion_v1 helpers ---
from types import SimpleNamespace as _SimpleNamespace

def _ci_header_get(_headers, _name: str):
    if _headers is None:
        return None
    try:
        v = _headers.get(_name)
        if v is not None:
            return v
        v = _headers.get(_name.lower())
        if v is not None:
            return v
    except Exception:
        pass
    try:
        for k, v in _headers.items():
            if str(k).lower() == _name.lower():
                return v
    except Exception:
        pass
    return None

def _set_tenant_on_state(_request, tenant_id: str, run_id: str | None, source: str):
    state = getattr(_request, "state", None)
    if state is None:
        return
    ctx = None
    try:
        ctx = TenantContext(tenant_id=tenant_id, run_id=run_id, source=source)
    except Exception:
        try:
            ctx = TenantContext(tenant_id=tenant_id, run_id=run_id)
            try:
                setattr(ctx, "source", source)
            except Exception:
                pass
        except Exception:
            ctx = _SimpleNamespace(tenant_id=tenant_id, run_id=run_id, source=source)

    try:
        setattr(state, "tenant", ctx)
    except Exception:
        pass
"""

def remove_wrappers(text: str) -> str:
    # Remove any helper blocks from older scripts
    text = re.sub(r"\n# --- auto_fix_tenant_.*?helpers ---.*?(?=\n[^ \n]|\Z)", "\n", text, flags=re.S)
    text = re.sub(r"\n# --- auto_fix_tenant_.*?\n.*", "\n", text, flags=re.S)

    # Remove all _dispatch_wrapped function blocks at top-level
    # Matches: ^async def _dispatch_wrapped(...): <indented body>
    text = re.sub(
        r"(?ms)^\s*async\s+def\s+_dispatch_wrapped\s*\(.*?\)\s*:\s*\n(?:^[ \t]+.*\n)+",
        "",
        text,
    )

    # Remove any assignments like:
    # TenantMiddleware.dispatch = _dispatch_wrapped
    # setattr(TenantMiddleware, "dispatch", _dispatch_wrapped)
    text = re.sub(r"(?m)^\s*TenantMiddleware\.dispatch\s*=\s*_dispatch_wrapped\s*\n", "", text)
    text = re.sub(r"(?m)^\s*setattr\(\s*TenantMiddleware\s*,\s*['\"]dispatch['\"]\s*,\s*_dispatch_wrapped\s*\)\s*\n", "", text)

    return text

def inject_into_dispatch(text: str) -> str:
    if "auto_unwrap_tenant_recursion_v1 helpers" not in text:
        text = text.rstrip() + "\n\n" + HELPERS.strip() + "\n"

    # Find TenantMiddleware.dispatch signature
    dispatch_re = re.compile(
        r"^\s+async\s+def\s+dispatch\s*\(\s*self\s*,\s*request\s*,\s*call_next\s*\)\s*:\s*$",
        re.M
    )
    m = dispatch_re.search(text)
    if not m:
        raise RuntimeError("TenantMiddleware.dispatch not found")

    if "auto_unwrap_tenant_recursion_v1: header injection" in text:
        return text

    injection = """
        # --- auto_unwrap_tenant_recursion_v1: header injection ---
        try:
            _tenant_id = _ci_header_get(getattr(request, "headers", None), "x-tenant-id")
            _run_id = _ci_header_get(getattr(request, "headers", None), "x-run-id")
            if _tenant_id:
                _set_tenant_on_state(request, str(_tenant_id), str(_run_id) if _run_id is not None else None, "header")
        except Exception:
            pass
"""

    insert_pos = m.end()
    return text[:insert_pos] + injection + text[insert_pos:]

def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    tenant_py = repo_root / "hfa-tools" / "src" / "hfa_tools" / "middleware" / "tenant.py"
    print(f"[auto_unwrap_tenant_recursion_v1] repo_root={repo_root}")

    if not tenant_py.exists():
        print(f"ERROR: tenant.py not found: {tenant_py}")
        return 1

    bak = backup(tenant_py)
    print(f"[tenant.py] backup={bak.name}")

    text = read_text(tenant_py)
    before_wrapped = len(re.findall(r"_dispatch_wrapped", text))
    text = remove_wrappers(text)
    after_wrapped = len(re.findall(r"_dispatch_wrapped", text))

    text = inject_into_dispatch(text)
    write_text(tenant_py, text)

    print(f"[tenant.py] unwrap done ✅ (_dispatch_wrapped refs: {before_wrapped} -> {after_wrapped})")
    print("Run:\n  pytest hfa-tools/tests/test_sprint2.py -k TenantExtraction -q\n  pytest hfa-tools/tests/test_sprint2.py")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())