"""Secure model loading utilities for ML Model Registry."""

import asyncio
import os
import re
from pathlib import Path
from typing import Any

import aiofiles
import structlog

from .model_exceptions import (
    InvalidModelFileError,
    SecurityError,
    UnsupportedFrameworkError,
)

logger = structlog.get_logger(__name__)

# Maximum file size for model uploads (100MB)
MAX_MODEL_FILE_SIZE = 100 * 1024 * 1024

# Allowed file extensions by framework
ALLOWED_EXTENSIONS = {
    "sklearn": [".pkl", ".joblib"],
    "lightgbm": [".txt", ".pkl", ".joblib"],
    "xgboost": [".json", ".pkl", ".joblib"],
}


def sanitize_filename(filename: str | None) -> str:
    """Sanitize filename to prevent path traversal and other security issues."""
    if not filename:
        return "model_file"

    # Remove path components
    filename = os.path.basename(filename)

    # Remove or replace dangerous characters
    filename = re.sub(r"[^\w\-_\.]", "_", filename)

    # Limit length
    if len(filename) > 255:
        name_part, ext = os.path.splitext(filename)
        filename = name_part[:250] + ext

    # Ensure it doesn't start with dots (hidden files)
    if filename.startswith("."):
        filename = "model_" + filename

    return filename


def validate_file_extension(filename: str, framework: str) -> None:
    """Validate that the file extension is allowed for the framework."""
    framework_lower = framework.lower()

    if framework_lower not in ALLOWED_EXTENSIONS:
        raise UnsupportedFrameworkError(f"Framework '{framework}' is not supported")

    file_ext = Path(filename).suffix.lower()
    allowed_exts = ALLOWED_EXTENSIONS[framework_lower]

    if file_ext not in allowed_exts:
        raise InvalidModelFileError(
            f"File extension '{file_ext}' not allowed for framework '{framework}'. "
            f"Allowed extensions: {allowed_exts}"
        )


async def validate_file_size(file_path: str) -> None:
    """Validate that the uploaded file is not too large."""
    try:
        stat = await asyncio.to_thread(os.stat, file_path)
        file_size = stat.st_size

        if file_size > MAX_MODEL_FILE_SIZE:
            raise SecurityError(
                f"File size {file_size} bytes exceeds maximum allowed size "
                f"{MAX_MODEL_FILE_SIZE} bytes ({MAX_MODEL_FILE_SIZE // (1024 * 1024)}MB)"
            )

        logger.info("File size validation passed", file_size=file_size)

    except OSError as e:
        raise InvalidModelFileError(f"Could not validate file size: {e}")


async def load_sklearn_model_secure(file_path: str) -> Any:
    """Securely load a scikit-learn model using joblib instead of pickle."""
    try:
        # Import joblib for secure loading
        import joblib

        # Load model in a thread to avoid blocking the event loop
        def _load_model():
            try:
                # Use joblib.load which is safer than pickle for sklearn models
                return joblib.load(file_path)
            except Exception as e:
                logger.error(
                    "Failed to load sklearn model", file_path=file_path, error=str(e)
                )
                raise InvalidModelFileError(f"Failed to load sklearn model: {e}")

        model = await asyncio.to_thread(_load_model)

        # Basic validation that we got a valid sklearn model
        if not hasattr(model, "predict"):
            raise InvalidModelFileError(
                "Loaded object does not appear to be a valid sklearn model (no 'predict' method)"
            )

        logger.info("Successfully loaded sklearn model", file_path=file_path)
        return model

    except ImportError:
        raise UnsupportedFrameworkError(
            "joblib is required for loading sklearn models but is not installed"
        )


async def load_model_secure(file_path: str, framework: str, filename: str) -> Any:
    """Securely load a model file based on the specified framework."""

    # Validate file extension
    validate_file_extension(filename, framework)

    # Validate file size
    await validate_file_size(file_path)

    framework_lower = framework.lower()

    if framework_lower == "sklearn":
        return await load_sklearn_model_secure(file_path)
    else:
        # For future framework support
        raise UnsupportedFrameworkError(
            f"Secure loading for framework '{framework}' is not yet implemented. "
            f"Currently supported: {list(ALLOWED_EXTENSIONS.keys())}"
        )


async def safe_file_write(content: bytes, file_path: str) -> None:
    """Safely write file content using async I/O."""
    try:
        async with aiofiles.open(file_path, "wb") as f:
            await f.write(content)
        logger.debug(
            "File written successfully", file_path=file_path, size=len(content)
        )
    except Exception as e:
        logger.error("Failed to write file", file_path=file_path, error=str(e))
        raise InvalidModelFileError(f"Failed to write model file: {e}")


async def safe_file_cleanup(file_path: str) -> None:
    """Safely remove temporary file."""
    try:
        if os.path.exists(file_path):
            await asyncio.to_thread(os.unlink, file_path)
            logger.debug("Temporary file cleaned up", file_path=file_path)
    except Exception as e:
        logger.warning(
            "Failed to cleanup temporary file", file_path=file_path, error=str(e)
        )
        # Don't raise exception for cleanup failures
