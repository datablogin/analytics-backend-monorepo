"""Secure secrets management with external provider integration."""

import base64
import os
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import structlog
from pydantic import BaseModel, Field, SecretStr


class SecretProvider(str, Enum):
    """Supported secret management providers."""

    ENVIRONMENT = "environment"
    HASHICORP_VAULT = "hashicorp_vault"
    AWS_SECRETS_MANAGER = "aws_secrets_manager"
    AZURE_KEY_VAULT = "azure_key_vault"
    GOOGLE_SECRET_MANAGER = "google_secret_manager"
    FILE_BASED = "file_based"


class SecretMetadata(BaseModel):
    """Metadata for a secret."""

    key: str
    provider: SecretProvider
    path: str
    version: str | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: datetime | None = None
    tags: dict[str, str] = Field(default_factory=dict)
    rotation_interval: timedelta | None = None
    last_rotated: datetime | None = None


class Secret(BaseModel):
    """A managed secret with metadata."""

    metadata: SecretMetadata
    value: SecretStr

    def is_expired(self) -> bool:
        """Check if the secret has expired."""
        if not self.metadata.expires_at:
            return False
        return datetime.utcnow() > self.metadata.expires_at

    def needs_rotation(self) -> bool:
        """Check if the secret needs rotation."""
        if not self.metadata.rotation_interval or not self.metadata.last_rotated:
            return False

        next_rotation = self.metadata.last_rotated + self.metadata.rotation_interval
        return datetime.utcnow() >= next_rotation


class SecretProviderInterface(ABC):
    """Interface for secret management providers."""

    @abstractmethod
    async def get_secret(self, key: str, path: str) -> str | None:
        """Retrieve a secret value."""
        pass

    @abstractmethod
    async def set_secret(self, key: str, path: str, value: str) -> bool:
        """Store a secret value."""
        pass

    @abstractmethod
    async def delete_secret(self, key: str, path: str) -> bool:
        """Delete a secret."""
        pass

    @abstractmethod
    async def list_secrets(self, path_prefix: str = "") -> list[str]:
        """List available secrets."""
        pass

    @abstractmethod
    async def rotate_secret(self, key: str, path: str) -> str | None:
        """Rotate a secret and return the new value."""
        pass


class EnvironmentSecretProvider(SecretProviderInterface):
    """Environment variable-based secret provider."""

    async def get_secret(self, key: str, path: str) -> str | None:
        """Get secret from environment variables."""
        return os.getenv(key)

    async def set_secret(self, key: str, path: str, value: str) -> bool:
        """Set environment variable (not persistent)."""
        os.environ[key] = value
        return True

    async def delete_secret(self, key: str, path: str) -> bool:
        """Remove environment variable."""
        if key in os.environ:
            del os.environ[key]
            return True
        return False

    async def list_secrets(self, path_prefix: str = "") -> list[str]:
        """List environment variables with prefix."""
        return [k for k in os.environ.keys() if k.startswith(path_prefix)]

    async def rotate_secret(self, key: str, path: str) -> str | None:
        """Environment variables don't support automatic rotation."""
        return None


class FileBasedSecretProvider(SecretProviderInterface):
    """File-based secret provider for development/testing."""

    def __init__(self, secrets_dir: str = ".secrets"):
        self.secrets_dir = secrets_dir
        os.makedirs(secrets_dir, exist_ok=True)

    async def get_secret(self, key: str, path: str) -> str | None:
        """Get secret from file."""
        secret_file = os.path.join(self.secrets_dir, f"{key}.secret")
        if os.path.exists(secret_file):
            try:
                with open(secret_file, encoding="utf-8") as f:
                    encoded_value = f.read().strip()
                    return base64.b64decode(encoded_value).decode("utf-8")
            except (OSError, ValueError) as e:
                logger = structlog.get_logger()
                logger.warning(
                    "Failed to read secret from file",
                    secret_key=key,
                    error=str(e),
                    error_type=type(e).__name__,
                )
        return None

    async def set_secret(self, key: str, path: str, value: str) -> bool:
        """Store secret in file."""
        secret_file = os.path.join(self.secrets_dir, f"{key}.secret")
        try:
            encoded_value = base64.b64encode(value.encode("utf-8")).decode("utf-8")
            with open(secret_file, "w", encoding="utf-8") as f:
                f.write(encoded_value)
            return True
        except (OSError, ValueError) as e:
            logger = structlog.get_logger()
            logger.error(
                "Failed to store secret to file",
                secret_key=key,
                error=str(e),
                error_type=type(e).__name__,
            )
            return False

    async def delete_secret(self, key: str, path: str) -> bool:
        """Delete secret file."""
        secret_file = os.path.join(self.secrets_dir, f"{key}.secret")
        if os.path.exists(secret_file):
            try:
                os.remove(secret_file)
                return True
            except OSError as e:
                logger = structlog.get_logger()
                logger.error(
                    "Failed to delete secret file",
                    secret_key=key,
                    error=str(e),
                    error_type=type(e).__name__,
                )
        return False

    async def list_secrets(self, path_prefix: str = "") -> list[str]:
        """List secret files."""
        secrets = []
        for filename in os.listdir(self.secrets_dir):
            if filename.endswith(".secret"):
                key = filename[:-7]  # Remove .secret extension
                if key.startswith(path_prefix):
                    secrets.append(key)
        return secrets

    async def rotate_secret(self, key: str, path: str) -> str | None:
        """Generate a new secret value."""
        import secrets as secrets_module

        new_value = secrets_module.token_urlsafe(32)
        if await self.set_secret(key, path, new_value):
            return new_value
        return None


class HashiCorpVaultProvider(SecretProviderInterface):
    """HashiCorp Vault secret provider."""

    def __init__(
        self,
        vault_url: str,
        token: str | None = None,
        mount_point: str = "secret",
    ):
        self.vault_url = vault_url.rstrip("/")
        self.token = token or os.getenv("VAULT_TOKEN")
        self.mount_point = mount_point

    async def get_secret(self, key: str, path: str) -> str | None:
        """Get secret from Vault."""
        try:
            import httpx

            headers = {"X-Vault-Token": self.token}
            url = f"{self.vault_url}/v1/{self.mount_point}/data/{path}"

            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers=headers)

                if response.status_code == 200:
                    data = response.json()
                    secret_data = data.get("data", {}).get("data", {})
                    return secret_data.get(key)
                elif response.status_code == 404:
                    return None
                else:
                    logger = structlog.get_logger()
                    logger.error(
                        "Vault API error",
                        status_code=response.status_code,
                        error_type="vault_api_error",
                    )
                    return None
        except Exception as e:
            logger = structlog.get_logger()
            logger.error(
                "Failed to get secret from Vault",
                error=str(e),
                error_type=type(e).__name__,
            )
            return None

    async def set_secret(self, key: str, path: str, value: str) -> bool:
        """Store secret in Vault."""
        try:
            import httpx

            headers = {"X-Vault-Token": self.token}
            url = f"{self.vault_url}/v1/{self.mount_point}/data/{path}"
            payload = {"data": {key: value}}

            async with httpx.AsyncClient() as client:
                response = await client.post(url, headers=headers, json=payload)
                return response.status_code in [200, 204]
        except Exception as e:
            logger = structlog.get_logger()
            logger.error(
                "Failed to store secret in Vault",
                error=str(e),
                error_type=type(e).__name__,
            )
            return False

    async def delete_secret(self, key: str, path: str) -> bool:
        """Delete secret from Vault."""
        try:
            import httpx

            headers = {"X-Vault-Token": self.token}
            url = f"{self.vault_url}/v1/{self.mount_point}/data/{path}"

            async with httpx.AsyncClient() as client:
                response = await client.delete(url, headers=headers)
                return response.status_code in [200, 204]
        except Exception as e:
            logger = structlog.get_logger()
            logger.error(
                "Failed to delete secret from Vault",
                error=str(e),
                error_type=type(e).__name__,
            )
            return False

    async def list_secrets(self, path_prefix: str = "") -> list[str]:
        """List secrets from Vault."""
        try:
            import httpx

            headers = {"X-Vault-Token": self.token}
            url = f"{self.vault_url}/v1/{self.mount_point}/metadata/{path_prefix}?list=true"

            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers=headers)

                if response.status_code == 200:
                    data = response.json()
                    return data.get("data", {}).get("keys", [])
                return []
        except Exception as e:
            logger = structlog.get_logger()
            logger.error(
                "Failed to list secrets from Vault",
                error=str(e),
                error_type=type(e).__name__,
            )
            return []

    async def rotate_secret(self, key: str, path: str) -> str | None:
        """Rotate secret in Vault."""
        import secrets as secrets_module

        new_value = secrets_module.token_urlsafe(32)
        if await self.set_secret(key, path, new_value):
            return new_value
        return None


class SecretsManager:
    """Central secrets management with multiple provider support."""

    def __init__(self):
        self._providers: dict[SecretProvider, SecretProviderInterface] = {}
        self._secrets_cache: dict[str, Secret] = {}
        self._cache_ttl: int = 300  # 5 minutes

    def register_provider(
        self, provider_type: SecretProvider, provider: SecretProviderInterface
    ) -> None:
        """Register a secret provider."""
        self._providers[provider_type] = provider

    async def get_secret(
        self,
        key: str,
        provider: SecretProvider = SecretProvider.ENVIRONMENT,
        path: str = "",
        use_cache: bool = True,
    ) -> Secret | None:
        """Get a secret from the specified provider."""
        cache_key = f"{provider.value}:{path}:{key}"

        # Check cache first
        if use_cache and cache_key in self._secrets_cache:
            secret = self._secrets_cache[cache_key]
            if not secret.is_expired():
                return secret

        # Get from provider
        provider_instance = self._providers.get(provider)
        if not provider_instance:
            logger = structlog.get_logger()
            logger.warning(
                "Secret provider not registered",
                provider=provider.value,
                available_providers=[p.value for p in self._providers.keys()],
            )
            return None

        value = await provider_instance.get_secret(key, path)
        if value is None:
            return None

        # Create secret with metadata
        metadata = SecretMetadata(
            key=key,
            provider=provider,
            path=path,
        )

        secret = Secret(metadata=metadata, value=SecretStr(value))

        # Cache the secret
        if use_cache:
            self._secrets_cache[cache_key] = secret

        return secret

    async def set_secret(
        self,
        key: str,
        value: str,
        provider: SecretProvider = SecretProvider.ENVIRONMENT,
        path: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        """Store a secret using the specified provider."""
        provider_instance = self._providers.get(provider)
        if not provider_instance:
            logger = structlog.get_logger()
            logger.warning(
                "Secret provider not registered for set operation",
                provider=provider.value,
                available_providers=[p.value for p in self._providers.keys()],
            )
            return False

        success = await provider_instance.set_secret(key, path, value)

        if success:
            # Invalidate cache
            cache_key = f"{provider.value}:{path}:{key}"
            if cache_key in self._secrets_cache:
                del self._secrets_cache[cache_key]

        return success

    async def delete_secret(
        self,
        key: str,
        provider: SecretProvider = SecretProvider.ENVIRONMENT,
        path: str = "",
    ) -> bool:
        """Delete a secret from the specified provider."""
        provider_instance = self._providers.get(provider)
        if not provider_instance:
            return False

        success = await provider_instance.delete_secret(key, path)

        if success:
            # Invalidate cache
            cache_key = f"{provider.value}:{path}:{key}"
            if cache_key in self._secrets_cache:
                del self._secrets_cache[cache_key]

        return success

    async def rotate_secret(
        self,
        key: str,
        provider: SecretProvider = SecretProvider.ENVIRONMENT,
        path: str = "",
    ) -> str | None:
        """Rotate a secret and return the new value."""
        provider_instance = self._providers.get(provider)
        if not provider_instance:
            return None

        new_value = await provider_instance.rotate_secret(key, path)

        if new_value:
            # Invalidate cache
            cache_key = f"{provider.value}:{path}:{key}"
            if cache_key in self._secrets_cache:
                del self._secrets_cache[cache_key]

        return new_value

    async def list_secrets(
        self,
        provider: SecretProvider = SecretProvider.ENVIRONMENT,
        path_prefix: str = "",
    ) -> list[str]:
        """List secrets from the specified provider."""
        provider_instance = self._providers.get(provider)
        if not provider_instance:
            return []

        return await provider_instance.list_secrets(path_prefix)

    def clear_cache(self) -> None:
        """Clear the secrets cache."""
        self._secrets_cache.clear()

    async def health_check(self) -> dict[str, bool]:
        """Check the health of all registered providers."""
        health_status = {}

        for provider_type, provider in self._providers.items():
            try:
                # Try to list secrets as a basic health check
                await provider.list_secrets("")
                health_status[provider_type.value] = True
            except Exception:
                health_status[provider_type.value] = False

        return health_status

    def setup_default_providers(self) -> None:
        """Set up default secret providers."""
        # Environment variables (always available)
        self.register_provider(SecretProvider.ENVIRONMENT, EnvironmentSecretProvider())

        # File-based provider for development
        self.register_provider(SecretProvider.FILE_BASED, FileBasedSecretProvider())

        # HashiCorp Vault if configured
        vault_url = os.getenv("VAULT_URL")
        if vault_url:
            vault_provider = HashiCorpVaultProvider(vault_url)
            self.register_provider(SecretProvider.HASHICORP_VAULT, vault_provider)


# Global secrets manager instance
secrets_manager = SecretsManager()
secrets_manager.setup_default_providers()
