"""Comprehensive tests for streaming analytics security features."""

import base64
import os
import tempfile
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from libs.analytics_core.auth import AuthService, TokenData
from libs.api_common.middleware import DDoSProtectionMiddleware
from libs.streaming_analytics.config import KafkaConfig
from libs.streaming_analytics.kafka_manager import KafkaSecurityManager
from libs.streaming_analytics.secrets_manager import (
    EnvironmentSecretsBackend,
    FileSecretsBackend,
    SecretMetadata,
    SecretsManager,
)
from libs.streaming_analytics.websocket_server import (
    MessageType,
    WebSocketAuthenticator,
    WebSocketClient,
    WebSocketConfig,
    WebSocketMessage,
)


class TestKafkaSecurityManager:
    """Test Kafka security features."""

    def test_encryption_initialization(self):
        """Test encryption key initialization."""
        # Test with provided key (32 bytes when base64 decoded)
        from cryptography.fernet import Fernet

        test_key = Fernet.generate_key()
        test_key_b64 = base64.urlsafe_b64encode(test_key).decode()

        config = KafkaConfig(enable_data_encryption=True, encryption_key=test_key_b64)
        security_manager = KafkaSecurityManager(config)

        test_data = b"test message"
        encrypted = security_manager.encrypt_data(test_data)
        decrypted = security_manager.decrypt_data(encrypted)

        assert decrypted == test_data

    def test_encryption_disabled(self):
        """Test when encryption is disabled."""
        config = KafkaConfig(enable_data_encryption=False)
        security_manager = KafkaSecurityManager(config)

        test_data = b"test message"
        result = security_manager.encrypt_data(test_data)

        assert result == test_data

    def test_ssl_config_generation(self):
        """Test SSL configuration generation."""
        config = KafkaConfig(
            security_protocol="SSL",
            ssl_ca_location="/path/to/ca.pem",
            ssl_cert_location="/path/to/cert.pem",
            ssl_key_location="/path/to/key.pem",
            ssl_key_password="password123",
        )
        security_manager = KafkaSecurityManager(config)

        ssl_config = security_manager.get_ssl_config()

        assert ssl_config["ssl_cafile"] == "/path/to/ca.pem"
        assert ssl_config["ssl_certfile"] == "/path/to/cert.pem"
        assert ssl_config["ssl_keyfile"] == "/path/to/key.pem"
        assert ssl_config["ssl_password"] == "password123"
        assert ssl_config["ssl_check_hostname"] is True

    def test_sasl_config_generation(self):
        """Test SASL configuration generation."""
        config = KafkaConfig(
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_username="test_user",
            sasl_password="test_password",
        )
        security_manager = KafkaSecurityManager(config)

        sasl_config = security_manager.get_sasl_config()

        assert sasl_config["sasl_mechanism"] == "SCRAM-SHA-256"
        assert sasl_config["sasl_plain_username"] == "test_user"
        assert sasl_config["sasl_plain_password"] == "test_password"

    def test_audit_logging(self):
        """Test audit logging functionality."""
        config = KafkaConfig()
        security_manager = KafkaSecurityManager(config)

        with patch.object(security_manager.logger, "info") as mock_log:
            security_manager.audit_log_access("PRODUCE", "test-topic", "test-user")

            mock_log.assert_called_once()
            call_args = mock_log.call_args
            assert "Kafka security audit event" in call_args[0][0]
            assert call_args[1]["operation"] == "PRODUCE"
            assert call_args[1]["topic"] == "test-topic"


class TestWebSocketSecurity:
    """Test WebSocket security features."""

    @pytest.fixture
    def websocket_config(self):
        """Create WebSocket config for testing."""
        return WebSocketConfig(
            require_auth=True,
            auth_timeout_seconds=5,
            enable_rate_limiting=True,
            rate_limit_per_minute=10,
        )

    @pytest.fixture
    def websocket_authenticator(self, websocket_config):
        """Create WebSocket authenticator for testing."""
        return WebSocketAuthenticator(websocket_config)

    @pytest.mark.asyncio
    async def test_jwt_authentication_success(self, websocket_authenticator):
        """Test successful JWT authentication."""
        # Mock AuthService.verify_token
        mock_token_data = TokenData(user_id=123, permissions=["read", "write"])

        with patch.object(
            AuthService, "verify_token", return_value=mock_token_data
        ) as mock_verify:
            auth_message = WebSocketMessage(
                type=MessageType.AUTH, data={"token": "valid_jwt_token"}
            )

            is_authenticated, token_data = await websocket_authenticator.authenticate(
                auth_message
            )

            assert is_authenticated is True
            assert token_data.user_id == 123
            assert "read" in token_data.permissions
            mock_verify.assert_called_once_with("valid_jwt_token")

    @pytest.mark.asyncio
    async def test_jwt_authentication_failure(self, websocket_authenticator):
        """Test JWT authentication failure."""
        # Mock AuthService.verify_token to raise exception
        with patch.object(
            AuthService, "verify_token", side_effect=Exception("Invalid token")
        ):
            auth_message = WebSocketMessage(
                type=MessageType.AUTH, data={"token": "invalid_jwt_token"}
            )

            is_authenticated, token_data = await websocket_authenticator.authenticate(
                auth_message
            )

            assert is_authenticated is False
            assert token_data is None

    @pytest.mark.asyncio
    async def test_missing_token(self, websocket_authenticator):
        """Test authentication with missing token."""
        auth_message = WebSocketMessage(type=MessageType.AUTH, data={})

        is_authenticated, token_data = await websocket_authenticator.authenticate(
            auth_message
        )

        assert is_authenticated is False
        assert token_data is None

    def test_permission_checking(self, websocket_authenticator):
        """Test permission checking functionality."""
        token_data = TokenData(user_id=123, permissions=["read", "write"])

        # Test with sufficient permissions
        assert websocket_authenticator.check_permissions(token_data, ["read"]) is True
        assert (
            websocket_authenticator.check_permissions(token_data, ["read", "write"])
            is True
        )

        # Test with insufficient permissions
        assert websocket_authenticator.check_permissions(token_data, ["admin"]) is False
        assert (
            websocket_authenticator.check_permissions(token_data, ["read", "admin"])
            is False
        )

    def test_websocket_client_security_fields(self):
        """Test WebSocket client security enhancements."""
        mock_websocket = MagicMock()
        client = WebSocketClient(id="test-client", websocket=mock_websocket)

        # Check security fields are initialized
        assert client.token_data is None
        assert client.user_id is None
        assert client.permissions == []
        assert client.audit_session_id is not None
        assert len(client.audit_session_id) > 0


class TestSecretsManager:
    """Test secrets management functionality."""

    @pytest.mark.asyncio
    async def test_environment_backend(self):
        """Test environment variables backend."""
        backend = EnvironmentSecretsBackend(prefix="TEST_SECRET_")

        # Test setting and getting secret
        await backend.set_secret("api_key", "test_value_123")
        value = await backend.get_secret("api_key")
        assert value == "test_value_123"

        # Test listing secrets
        secrets = await backend.list_secrets()
        assert "api_key" in secrets

        # Test deleting secret
        deleted = await backend.delete_secret("api_key")
        assert deleted is True

        value_after_delete = await backend.get_secret("api_key")
        assert value_after_delete is None

    @pytest.mark.asyncio
    async def test_file_backend(self):
        """Test file-based backend."""
        with tempfile.TemporaryDirectory() as temp_dir:
            backend = FileSecretsBackend(secrets_dir=temp_dir)

            # Test storing and retrieving secret
            metadata = SecretMetadata(
                name="db_password", description="Database password"
            )
            await backend.set_secret("db_password", "secure_password_123", metadata)

            value = await backend.get_secret("db_password")
            assert value == "secure_password_123"

            # Test listing secrets
            secrets = await backend.list_secrets()
            assert "db_password" in secrets

            # Test file permissions (should be 0o600)
            secret_file = os.path.join(temp_dir, "db_password.secret")
            file_mode = oct(os.stat(secret_file).st_mode)[-3:]
            assert file_mode == "600"

            # Test deleting secret
            deleted = await backend.delete_secret("db_password")
            assert deleted is True
            assert not os.path.exists(secret_file)

    @pytest.mark.asyncio
    async def test_secrets_manager_caching(self):
        """Test secrets manager caching functionality."""
        backend = EnvironmentSecretsBackend()
        manager = SecretsManager(backend)

        # Mock backend get_secret method
        backend.get_secret = AsyncMock(return_value="cached_value")

        # First call should hit backend
        value1 = await manager.get_secret("test_key", use_cache=True)
        assert value1 == "cached_value"
        assert backend.get_secret.call_count == 1

        # Second call should use cache
        value2 = await manager.get_secret("test_key", use_cache=True)
        assert value2 == "cached_value"
        assert backend.get_secret.call_count == 1  # No additional call

        # Clear cache and try again
        manager.clear_cache()
        value3 = await manager.get_secret("test_key", use_cache=True)
        assert value3 == "cached_value"
        assert backend.get_secret.call_count == 2  # Additional call after cache clear

    @pytest.mark.asyncio
    async def test_secret_rotation(self):
        """Test secret rotation functionality."""
        backend = EnvironmentSecretsBackend()
        manager = SecretsManager(backend)

        # Set initial secret
        await manager.set_secret("api_token", "old_token_value")

        # Rotate secret
        rotated = await manager.rotate_secret("api_token", "new_token_value")
        assert rotated is True

        # Verify new value
        value = await manager.get_secret("api_token")
        assert value == "new_token_value"


class TestDDoSProtection:
    """Test DDoS protection middleware."""

    @pytest.fixture
    def ddos_middleware(self):
        """Create DDoS protection middleware for testing."""
        app = MagicMock()
        return DDoSProtectionMiddleware(
            app,
            requests_per_minute=120,  # Increased to allow burst_requests_per_second * 60
            burst_requests_per_second=2,  # 2 * 60 = 120, which equals requests_per_minute
            max_concurrent_requests=3,
            suspicious_patterns_threshold=3,
            auto_ban_duration=60,
        )

    def test_client_ip_extraction(self, ddos_middleware):
        """Test client IP extraction from different headers."""
        # Test X-Forwarded-For header
        request = MagicMock()
        request.headers = {"X-Forwarded-For": "192.168.1.100, 10.0.0.1"}
        request.client.host = "127.0.0.1"

        ip = ddos_middleware._get_client_ip(request)
        assert ip == "192.168.1.100"

        # Test X-Real-IP header
        request.headers = {"X-Real-IP": "192.168.1.200"}
        ip = ddos_middleware._get_client_ip(request)
        assert ip == "192.168.1.200"

        # Test fallback to client.host
        request.headers = {}
        ip = ddos_middleware._get_client_ip(request)
        assert ip == "127.0.0.1"

    def test_rate_limiting(self, ddos_middleware):
        """Test rate limiting functionality."""
        ip = "192.168.1.100"
        current_time = time.time()

        # Should not be rate limited initially
        assert ddos_middleware._check_rate_limit(ip, current_time) is False

        # Add requests up to the limit
        for _ in range(120):
            ddos_middleware._record_request(ip, current_time)

        # Should be rate limited now
        assert ddos_middleware._check_rate_limit(ip, current_time) is True

    def test_burst_limiting(self, ddos_middleware):
        """Test burst rate limiting."""
        ip = "192.168.1.100"
        current_time = time.time()

        # Should not be burst limited initially
        assert ddos_middleware._check_burst_limit(ip, current_time) is False

        # Add burst requests
        for _ in range(2):
            ddos_middleware._record_request(ip, current_time)

        # Should be burst limited now
        assert ddos_middleware._check_burst_limit(ip, current_time) is True

    def test_concurrent_limiting(self, ddos_middleware):
        """Test concurrent request limiting."""
        ip = "192.168.1.100"

        # Should not be limited initially
        assert ddos_middleware._check_concurrent_limit(ip) is False

        # Add concurrent requests
        ddos_middleware.concurrent_requests[ip] = 3

        # Should be limited now
        assert ddos_middleware._check_concurrent_limit(ip) is True

    def test_suspicious_pattern_detection(self, ddos_middleware):
        """Test suspicious pattern detection."""
        # Test suspicious user agent with other headers present
        request = MagicMock()
        request.headers = {
            "user-agent": "curl/7.68.0",
            "accept": "text/html",
            "accept-language": "en-US",
        }
        request.url.path = "/api/test"
        request.method = "GET"

        is_suspicious = ddos_middleware._detect_suspicious_patterns(
            request, "192.168.1.100"
        )
        assert is_suspicious is False  # Only one indicator (curl)

        # Test multiple suspicious indicators
        request.headers = {
            "user-agent": "bot crawler"  # 1 indicator for 'bot'
            # Missing accept and accept-language headers = +2 indicators
        }
        request.url.path = "/.env"  # +2 indicators
        request.method = "GET"

        is_suspicious = ddos_middleware._detect_suspicious_patterns(
            request, "192.168.1.100"
        )
        assert is_suspicious is True  # 5 indicators >= 2 threshold

    def test_ip_banning(self, ddos_middleware):
        """Test IP banning functionality."""
        ip = "192.168.1.100"
        current_time = time.time()

        # Should not be banned initially
        assert ddos_middleware._is_banned(ip, current_time) is False

        # Ban the IP
        ddos_middleware._ban_ip(ip, current_time)

        # Should be banned now
        assert ddos_middleware._is_banned(ip, current_time) is True

        # Should not be banned after expiry
        future_time = current_time + ddos_middleware.auto_ban_duration + 1
        assert ddos_middleware._is_banned(ip, future_time) is False

    def test_auth_failure_tracking(self, ddos_middleware):
        """Test authentication failure tracking and auto-banning."""
        ip = "192.168.1.100"
        current_time = time.time()

        # Record multiple auth failures
        for _ in range(5):
            ddos_middleware._record_auth_failure(ip, current_time)

        # Should be auto-banned after 5 failures
        assert ddos_middleware._is_banned(ip, current_time) is True

    def test_statistics(self, ddos_middleware):
        """Test statistics gathering."""
        # Add some test data
        ddos_middleware.concurrent_requests["192.168.1.100"] = 2
        ddos_middleware.concurrent_requests["192.168.1.101"] = 1
        ddos_middleware.suspicious_patterns["192.168.1.102"] = 3
        ddos_middleware._ban_ip("192.168.1.103", time.time())

        stats = ddos_middleware.get_stats()

        assert stats["concurrent_requests"] == 3
        assert stats["suspicious_ips"] == 1
        assert stats["active_bans"] == 1
        assert stats["config"]["requests_per_minute"] == 120


class TestIntegratedSecurity:
    """Test integrated security features."""

    @pytest.mark.asyncio
    async def test_end_to_end_websocket_security(self):
        """Test end-to-end WebSocket security flow."""
        config = WebSocketConfig(require_auth=True)
        authenticator = WebSocketAuthenticator(config)

        # Mock successful JWT verification
        mock_token_data = TokenData(
            user_id=123, permissions=["streaming.events.read", "streaming.metrics.read"]
        )

        with patch.object(AuthService, "verify_token", return_value=mock_token_data):
            # Test authentication
            auth_message = WebSocketMessage(
                type=MessageType.AUTH, data={"token": "valid_jwt_token"}
            )

            is_authenticated, token_data = await authenticator.authenticate(
                auth_message
            )
            assert is_authenticated is True

            # Test permission checking
            has_permission = authenticator.check_permissions(
                token_data, ["streaming.events.read"]
            )
            assert has_permission is True

            # Test insufficient permissions
            has_admin_permission = authenticator.check_permissions(
                token_data, ["streaming.admin.read"]
            )
            assert has_admin_permission is False

    @pytest.mark.asyncio
    async def test_kafka_security_integration(self):
        """Test Kafka security integration."""
        config = KafkaConfig(
            enable_data_encryption=True,
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_username="test_user",
            sasl_password="test_password",
        )

        security_manager = KafkaSecurityManager(config)

        # Test encryption
        test_data = b'{"event": "user_action", "data": {"user_id": 123}}'
        encrypted_data = security_manager.encrypt_data(test_data)
        decrypted_data = security_manager.decrypt_data(encrypted_data)

        assert decrypted_data == test_data

        # Test configuration generation
        security_manager.get_ssl_config()
        sasl_config = security_manager.get_sasl_config()

        assert sasl_config["sasl_mechanism"] == "SCRAM-SHA-256"
        assert sasl_config["sasl_plain_username"] == "test_user"

    def test_security_headers_and_rate_limiting(self):
        """Test that security headers and rate limiting work together."""
        from libs.api_common.middleware import (
            RateLimitMiddleware,
            SecurityHeadersMiddleware,
        )

        # This would typically be tested with actual FastAPI app in integration tests
        # Here we just verify the middleware classes can be instantiated
        app = MagicMock()
        security_middleware = SecurityHeadersMiddleware(app)
        rate_limit_middleware = RateLimitMiddleware(app, requests_per_minute=60)
        ddos_middleware = DDoSProtectionMiddleware(app)

        assert security_middleware is not None
        assert rate_limit_middleware is not None
        assert ddos_middleware is not None
        assert ddos_middleware.requests_per_minute == 600  # Default value
