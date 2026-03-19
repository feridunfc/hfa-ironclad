import hmac
import os
from unittest.mock import patch

from hfa_control import auth


class TestSprint17Auth:
    def teardown_method(self):
        auth.reset_cache()

    def test_auth_disabled_when_secret_missing_dev(self):
        with patch.dict(os.environ, {"ENVIRONMENT": "development"}, clear=True):
            auth.reset_cache()
            assert auth.is_enabled() is False
            assert auth.validate_operator_token("anything") is True

    def test_auth_fail_closed_in_production_when_secret_missing(self):
        with patch.dict(os.environ, {"ENVIRONMENT": "production"}, clear=True):
            auth.reset_cache()
            assert auth.validate_operator_token("anything") is False

    def test_auth_enabled_when_secret_set(self):
        with patch.dict(os.environ, {"CP_AUTH_SECRET": "test-secret"}, clear=True):
            auth.reset_cache()
            assert auth.is_enabled() is True

    def test_operator_token_validation(self):
        secret = "test-secret"
        with patch.dict(os.environ, {"CP_AUTH_SECRET": secret}, clear=True):
            auth.reset_cache()
            valid_token = hmac.new(secret.encode(), b"operator", "sha256").hexdigest()
            assert auth.validate_operator_token(valid_token) is True
            assert auth.validate_operator_token("wrong-token") is False

    def test_tenant_token_validation(self):
        secret = "test-secret"
        tenant_id = "tenant-123"
        with patch.dict(os.environ, {"CP_AUTH_SECRET": secret}, clear=True):
            auth.reset_cache()
            valid_token = hmac.new(secret.encode(), f"tenant:{tenant_id}".encode(), "sha256").hexdigest()
            assert auth.validate_tenant_token(valid_token, tenant_id) is True
            assert auth.validate_tenant_token(valid_token, "different-tenant") is False
            assert auth.validate_tenant_token("wrong-token", tenant_id) is False

    def test_empty_token_rejected_when_enabled(self):
        with patch.dict(os.environ, {"CP_AUTH_SECRET": "test-secret"}, clear=True):
            auth.reset_cache()
            assert auth.validate_operator_token("") is False

    def test_invalid_token_rejected(self):
        with patch.dict(os.environ, {"CP_AUTH_SECRET": "test-secret"}, clear=True):
            auth.reset_cache()
            assert auth.validate_operator_token("bad") is False

    def test_tenant_token_wrong_tenant_rejected(self):
        secret = "test-secret"
        with patch.dict(os.environ, {"CP_AUTH_SECRET": secret}, clear=True):
            auth.reset_cache()
            token = hmac.new(secret.encode(), b"tenant:tenant-a", "sha256").hexdigest()
            assert auth.validate_tenant_token(token, "tenant-b") is False
