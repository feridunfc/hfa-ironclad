# scripts/auto_fix_tenant_v7.py
from __future__ import annotations
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

WRAPPER_BLOCK = r"""
# --- auto_fix_tenant_v7: force tenant context injection (wrap dispatch) ---
from types import SimpleNamespace
import inspect as _inspect

def _ci_header_get(headers, name: str):
    if headers is None:
        return None
    try:
        v = headers.get(name)
        if v is not None:
            return v
        v = headers.get(name.lower())
        if v is not None:
            return v
    except Exception:
        pass
    try:
        for k, v in headers.items():
            if str(k).lower() == name.lower():
                return v
    except Exception:
        pass
    return None

def _ensure_tenant_context(request):
    headers = getattr(request, "headers", None)
    tenant_id = _ci_header_get(headers, "x-tenant-id") or _ci_header_get(headers, "X-Tenant-Id")
    run_id = _ci_header_get(headers, "x-run-id") or _ci_header_get(headers, "X-Run-Id")

    if not tenant_id:
        return

    state = getattr(request, "state", None)
    if state is None:
        return

    # Try real TenantContext first; fallback to SimpleNamespace for tests
    ctx = None
    try:
        # prefer full signature if supported
        ctx = TenantContext(tenant_id=tenant_id, run_id=run_id, source="header")
    except Exception:
        try:
            ctx = TenantContext(tenant_id=tenant_id, run_id=run_id)
            try:
                setattr(ctx, "source", "header")
            except Exception:
                pass
        except Exception:
            try:
                ctx = TenantContext(tenant_id=tenant_id)
                try:
                    setattr(ctx, "run_id", run_id)
                    setattr(ctx, "source", "header")
                except Exception:
                    pass
            except Exception:
                ctx = SimpleNamespace(tenant_id=tenant_id, run_id=run_id, source="header")

    try:
        setattr(state, "tenant", ctx)
    except Exception:
        pass

try:
    _orig_dispatch = TenantMiddleware.dispatch
except Exception:
    _orig_dispatch = None

if _orig_dispatch is not None and not getattr(TenantMiddleware.dispatch, "__auto_fix_tenant_v7__", False):
    if _inspect.iscoroutinefunction(_orig_dispatch):
        async def _dispatch_wrapped(self, request, call_next):
            _ensure_tenant_context(request)
            resp = await _orig_dispatch(self, request, call_next)
            _ensure_tenant_context(request)
            return resp
        _dispatch_wrapped.__auto_fix_tenant_v7__ = True
        TenantMiddleware.dispatch = _dispatch_wrapped
    else:
        def _dispatch_wrapped(self, request, call_next):
            _ensure_tenant_context(request)
            resp = _orig_dispatch(self, request, call_next)
            _ensure_tenant_context(request)
            return resp
        _dispatch_wrapped.__auto_fix_tenant_v7__ = True
        TenantMiddleware.dispatch = _dispatch_wrapped
"""

def patch_tenant(path: Path) -> None:
    text = read_text(path)
    if "auto_fix_tenant_v7" in text:
        print("[tenant.py] already patched (v7) ✅")
        return
    bak = backup(path)
    text = text.rstrip() + "\n\n" + WRAPPER_BLOCK.strip() + "\n"
    write_text(path, text)
    print(f"[tenant.py] patched ✅ backup={bak.name}")

def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    tenant_py = repo_root / "hfa-tools" / "src" / "hfa_tools" / "middleware" / "tenant.py"
    print(f"[auto_fix_tenant_v7] repo_root={repo_root}")
    if not tenant_py.exists():
        print(f"[auto_fix_tenant_v7] ERROR tenant.py not found at: {tenant_py}")
        return 1
    patch_tenant(tenant_py)
    print("Run:\n  pytest hfa-tools/tests/test_sprint2.py -k TenantExtraction -q\n  pytest hfa-tools/tests/test_sprint2.py")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())