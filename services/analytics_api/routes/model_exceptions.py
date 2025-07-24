"""Custom exceptions for ML Model Registry and Serving API."""


class ModelRegistryError(Exception):
    """Base exception for model registry operations."""

    pass


class ModelNotFoundError(ModelRegistryError):
    """Exception raised when a model is not found."""

    pass


class ModelLoadError(ModelRegistryError):
    """Exception raised when a model fails to load."""

    pass


class UnsupportedFrameworkError(ModelRegistryError):
    """Exception raised when an unsupported ML framework is used."""

    pass


class InvalidModelFileError(ModelRegistryError):
    """Exception raised when a model file is invalid or corrupted."""

    pass


class SecurityError(ModelRegistryError):
    """Exception raised for security-related issues."""

    pass
