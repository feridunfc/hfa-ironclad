override_test = """
def test_build_audit_logger_disabled_on_key_load_failure():
    import os
    from unittest import mock
    from hfa_control.audit import build_audit_logger

    env_backup = os.environ.copy()
    os.environ["HFA_LEDGER_KEY_ID"] = "test-key"
    try:
        # Sistemin çöküş senaryosunu test edebilmek için anahtar yükleyiciyi zorla patlatıyoruz (Mock)
        with mock.patch("hfa.governance.signed_ledger_v1.Ed25519EnvKeyProvider", side_effect=Exception("Zorunlu Cokus")):
            logger = build_audit_logger(redis=None)

        # Çöküş gerçekleştiğinde logger güvenli bir şekilde kendini kapatmalı
        assert logger._enabled is False
    finally:
        os.environ.clear()
        os.environ.update(env_backup)
"""

with open("tests/core/test_sprint17_audit.py", "a", encoding="utf-8") as f:
    f.write(override_test)

print("Final Boss'a son kılıç darbesi vuruldu! 🗡️")