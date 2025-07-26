"""Secrets management for streaming analytics with multiple backend support."""

import base64
import json
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

import structlog
from cryptography.fernet import Fernet
from pydantic import BaseModel, Field

logger = structlog.get_logger(__name__)


class SecretMetadata(BaseModel):
    """Metadata for secrets."""

    name: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = 1
    tags: dict[str, str] = Field(default_factory=dict)
    description: str = ""
    rotation_interval_days: int | None = None


class SecretValue(BaseModel):
    """Secret value with metadata."""

    value: str
    metadata: SecretMetadata
    encrypted: bool = True


class SecretsBackend(ABC):
    """Abstract base class for secrets backends."""

    @abstractmethod
    async def get_secret(self, name: str) -> str | None:
        """Retrieve a secret by name."""
        pass

    @abstractmethod
    async def set_secret(
        self, name: str, value: str, metadata: SecretMetadata | None = None
    ) -> bool:
        """Store a secret."""
        pass

    @abstractmethod
    async def delete_secret(self, name: str) -> bool:
        """Delete a secret."""
        pass

    @abstractmethod
    async def list_secrets(self) -> list[str]:
        """List all secret names."""
        pass


class EnvironmentSecretsBackend(SecretsBackend):
    """Environment variables secrets backend."""

    def __init__(self, prefix: str = "SECRET_"):
        self.prefix = prefix
        self.logger = logger.bind(component="env_secrets_backend")

    async def get_secret(self, name: str) -> str | None:
        """Get secret from environment variable."""
        env_var = f"{self.prefix}{name.upper()}"
        value = os.getenv(env_var)

        if value:
            self.logger.debug("Secret retrieved from environment", secret_name=name)
        else:
            self.logger.debug("Secret not found in environment", secret_name=name)

        return value

    async def set_secret(
        self, name: str, value: str, metadata: SecretMetadata | None = None
    ) -> bool:
        """Set secret in environment (not persistent)."""
        env_var = f"{self.prefix}{name.upper()}"
        os.environ[env_var] = value
        self.logger.info("Secret set in environment", secret_name=name)
        return True

    async def delete_secret(self, name: str) -> bool:
        """Delete secret from environment."""
        env_var = f"{self.prefix}{name.upper()}"
        if env_var in os.environ:
            del os.environ[env_var]
            self.logger.info("Secret deleted from environment", secret_name=name)
            return True
        return False

    async def list_secrets(self) -> list[str]:
        """List secrets from environment variables."""
        secrets = []
        for key in os.environ:
            if key.startswith(self.prefix):
                secret_name = key[len(self.prefix) :].lower()
                secrets.append(secret_name)
        return secrets


class FileSecretsBackend(SecretsBackend):
    """File-based encrypted secrets backend."""

    def __init__(
        self, secrets_dir: str = ".secrets", encryption_key: str | None = None
    ):
        self.secrets_dir = secrets_dir
        self.logger = logger.bind(component="file_secrets_backend")

        # Initialize encryption
        if encryption_key:
            key_bytes = base64.urlsafe_b64decode(encryption_key.encode())
            self.cipher = Fernet(key_bytes)
        else:
            # Generate new key (should be stored securely)
            self.cipher = Fernet(Fernet.generate_key())
            self.logger.warning(
                "Generated new encryption key - store securely for production"
            )

        # Ensure secrets directory exists
        os.makedirs(secrets_dir, mode=0o700, exist_ok=True)

    def _get_secret_path(self, name: str) -> str:
        """Get file path for secret."""
        # Validate secret name to prevent path traversal attacks
        if not name or not isinstance(name, str):
            raise ValueError("Secret name must be a non-empty string")

        # Remove any path separators and dangerous characters
        sanitized_name = name.replace("/", "").replace("\\", "").replace("..", "")

        # Ensure the name contains only safe characters
        import re

        if not re.match(r"^[a-zA-Z0-9_-]+$", sanitized_name):
            raise ValueError(
                "Secret name can only contain alphanumeric characters, underscores, and hyphens"
            )

        # Prevent empty names after sanitization
        if not sanitized_name:
            raise ValueError("Secret name cannot be empty after sanitization")

        return os.path.join(self.secrets_dir, f"{sanitized_name}.secret")

    async def get_secret(self, name: str) -> str | None:
        """Get secret from encrypted file."""
        secret_path = self._get_secret_path(name)

        try:
            if not os.path.exists(secret_path):
                return None

            with open(secret_path, "rb") as f:
                encrypted_data = f.read()

            # Decrypt the secret
            decrypted_data = self.cipher.decrypt(encrypted_data)
            secret_obj = json.loads(decrypted_data.decode("utf-8"))

            self.logger.debug("Secret retrieved from file", secret_name=name)
            return secret_obj["value"]

        except Exception as e:
            self.logger.error(
                "Failed to retrieve secret", secret_name=name, error=str(e)
            )
            return None

    async def set_secret(
        self, name: str, value: str, metadata: SecretMetadata | None = None
    ) -> bool:
        """Store secret in encrypted file."""
        if not metadata:
            metadata = SecretMetadata(name=name)

        secret_obj = {
            "value": value,
            "metadata": metadata.model_dump(mode="json"),
        }

        try:
            # Encrypt the secret
            secret_json = json.dumps(secret_obj, default=str)
            encrypted_data = self.cipher.encrypt(secret_json.encode("utf-8"))

            # Write to file with secure permissions
            secret_path = self._get_secret_path(name)
            with open(secret_path, "wb") as f:
                f.write(encrypted_data)

            # Set secure file permissions
            os.chmod(secret_path, 0o600)

            self.logger.info("Secret stored in file", secret_name=name)
            return True

        except Exception as e:
            self.logger.error("Failed to store secret", secret_name=name, error=str(e))
            return False

    async def delete_secret(self, name: str) -> bool:
        """Delete secret file."""
        secret_path = self._get_secret_path(name)

        try:
            if os.path.exists(secret_path):
                os.remove(secret_path)
                self.logger.info("Secret deleted from file", secret_name=name)
                return True
            return False

        except Exception as e:
            self.logger.error("Failed to delete secret", secret_name=name, error=str(e))
            return False

    async def list_secrets(self) -> list[str]:
        """List secrets from files."""
        secrets: list[str] = []

        try:
            if not os.path.exists(self.secrets_dir):
                return secrets

            for filename in os.listdir(self.secrets_dir):
                if filename.endswith(".secret"):
                    secret_name = filename[:-7]  # Remove .secret extension
                    secrets.append(secret_name)

        except Exception as e:
            self.logger.error("Failed to list secrets", error=str(e))

        return secrets

    async def rotate_encryption_key(self, new_encryption_key: str) -> bool:
        """Rotate the encryption key and re-encrypt all secrets."""
        try:
            # Create new cipher with the new key
            key_bytes = base64.urlsafe_b64decode(new_encryption_key.encode())
            new_cipher = Fernet(key_bytes)

            # Get all current secrets
            secret_names = await self.list_secrets()

            # Re-encrypt all secrets with new key
            for secret_name in secret_names:
                # Get secret with old key
                old_secret_value = await self.get_secret(secret_name)
                if old_secret_value is None:
                    continue

                # Get metadata from old file
                secret_path = self._get_secret_path(secret_name)
                if not os.path.exists(secret_path):
                    continue

                with open(secret_path, "rb") as f:
                    encrypted_data = f.read()

                decrypted_data = self.cipher.decrypt(encrypted_data)
                secret_obj = json.loads(decrypted_data.decode("utf-8"))

                # Update metadata with rotation info
                metadata = SecretMetadata(**secret_obj["metadata"])
                metadata.updated_at = datetime.now(timezone.utc)
                metadata.version += 1

                # Encrypt with new key
                new_secret_obj = {
                    "value": old_secret_value,
                    "metadata": metadata.model_dump(mode="json"),
                }

                secret_json = json.dumps(new_secret_obj, default=str)
                new_encrypted_data = new_cipher.encrypt(secret_json.encode("utf-8"))

                # Write with new encryption
                with open(secret_path, "wb") as f:
                    f.write(new_encrypted_data)

                os.chmod(secret_path, 0o600)

            # Update cipher to use new key
            self.cipher = new_cipher

            self.logger.info(
                "Encryption key rotated successfully", secrets_count=len(secret_names)
            )
            return True

        except Exception as e:
            self.logger.error("Failed to rotate encryption key", error=str(e))
            return False


class SecretsManager:
    """Main secrets manager with multiple backend support."""

    def __init__(self, backend: SecretsBackend | None = None):
        if backend:
            self.backend = backend
        else:
            # Default to environment backend
            self.backend = EnvironmentSecretsBackend()

        self.logger = logger.bind(component="secrets_manager")
        self._cache: dict[str, tuple[str, float]] = {}
        self._cache_ttl = 300  # 5 minutes

    async def get_secret(self, name: str, use_cache: bool = True) -> str | None:
        """Get a secret with optional caching."""
        # Check cache first
        if use_cache and name in self._cache:
            value, timestamp = self._cache[name]
            if datetime.now(timezone.utc).timestamp() - timestamp < self._cache_ttl:
                self.logger.debug("Secret retrieved from cache", secret_name=name)
                return value

        # Get from backend
        value = await self.backend.get_secret(name)

        # Cache the value
        if value and use_cache:
            self._cache[name] = (value, datetime.now(timezone.utc).timestamp())

        return value

    async def set_secret(
        self, name: str, value: str, metadata: SecretMetadata | None = None
    ) -> bool:
        """Store a secret."""
        # Clear from cache
        if name in self._cache:
            del self._cache[name]

        # Store in backend
        result = await self.backend.set_secret(name, value, metadata)

        if result:
            self.logger.info("Secret stored successfully", secret_name=name)
        else:
            self.logger.error("Failed to store secret", secret_name=name)

        return result

    async def delete_secret(self, name: str) -> bool:
        """Delete a secret."""
        # Clear from cache
        if name in self._cache:
            del self._cache[name]

        # Delete from backend
        result = await self.backend.delete_secret(name)

        if result:
            self.logger.info("Secret deleted successfully", secret_name=name)
        else:
            self.logger.error("Failed to delete secret", secret_name=name)

        return result

    async def list_secrets(self) -> list[str]:
        """List all secrets."""
        return await self.backend.list_secrets()

    async def rotate_secret(self, name: str, new_value: str) -> bool:
        """Rotate a secret (update with new value)."""
        # Get existing metadata
        existing_value = await self.backend.get_secret(name)
        if not existing_value:
            self.logger.warning("Cannot rotate non-existent secret", secret_name=name)
            return False

        # Create updated metadata
        metadata = SecretMetadata(
            name=name,
            updated_at=datetime.now(timezone.utc),
            version=1,  # Would increment in production
        )

        # Store new value
        result = await self.set_secret(name, new_value, metadata)

        if result:
            self.logger.info("Secret rotated successfully", secret_name=name)

        return result

    def clear_cache(self) -> None:
        """Clear the secrets cache."""
        self._cache.clear()
        self.logger.info("Secrets cache cleared")

    def get_stats(self) -> dict[str, Any]:
        """Get secrets manager statistics."""
        return {
            "backend_type": type(self.backend).__name__,
            "cached_secrets": len(self._cache),
            "cache_ttl_seconds": self._cache_ttl,
        }


# Global secrets manager instance
_secrets_manager: SecretsManager | None = None


def get_secrets_manager() -> SecretsManager:
    """Get the global secrets manager instance."""
    global _secrets_manager

    if _secrets_manager is None:
        # Initialize with default backend based on environment
        if os.getenv("SECRETS_BACKEND") == "file":
            backend = FileSecretsBackend(
                secrets_dir=os.getenv("SECRETS_DIR", ".secrets"),
                encryption_key=os.getenv("SECRETS_ENCRYPTION_KEY"),
            )
        else:
            backend = EnvironmentSecretsBackend(
                prefix=os.getenv("SECRETS_PREFIX", "SECRET_")
            )

        _secrets_manager = SecretsManager(backend)

    return _secrets_manager


async def get_secret(name: str) -> str | None:
    """Convenience function to get a secret."""
    manager = get_secrets_manager()
    return await manager.get_secret(name)


async def set_secret(name: str, value: str) -> bool:
    """Convenience function to set a secret."""
    manager = get_secrets_manager()
    return await manager.set_secret(name, value)
