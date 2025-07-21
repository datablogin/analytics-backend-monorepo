"""API versioning utilities and configuration."""

from enum import Enum
from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel


class APIVersion(str, Enum):
    """Supported API versions."""

    V1 = "v1"
    V2 = "v2"  # For future use


class VersionedAPIRouter(APIRouter):
    """API Router with version support."""

    def __init__(self, version: APIVersion, *args, **kwargs):
        # Add version prefix to all routes
        prefix = kwargs.get("prefix", "")
        kwargs["prefix"] = f"/{version.value}{prefix}"

        # Add version tags
        tags = kwargs.get("tags", [])
        if tags:
            kwargs["tags"] = [f"{version.value.upper()} - {tag}" for tag in tags]
        else:
            kwargs["tags"] = [version.value.upper()]

        super().__init__(*args, **kwargs)
        self.version = version


class DeprecationInfo(BaseModel):
    """Information about deprecated API features."""

    deprecated_in: str
    removal_in: str | None = None
    reason: str
    replacement: str | None = None


class APIVersionInfo(BaseModel):
    """Information about an API version."""

    version: str
    status: str  # "stable", "deprecated", "beta"
    release_date: str
    deprecation_info: DeprecationInfo | None = None
    changelog_url: str | None = None


def get_api_version_from_request(request: Request) -> APIVersion:
    """Extract API version from request path."""
    path = request.url.path

    if path.startswith("/v2/"):
        return APIVersion.V2
    elif path.startswith("/v1/"):
        return APIVersion.V1
    else:
        # Default to v1 for non-versioned endpoints
        return APIVersion.V1


def create_version_info() -> dict[str, APIVersionInfo]:
    """Create API version information for documentation."""
    return {
        "v1": APIVersionInfo(
            version="1.0.0",
            status="stable",
            release_date="2025-01-01",
            changelog_url="/v1/changelog",
        ),
        "v2": APIVersionInfo(
            version="2.0.0",
            status="beta",
            release_date="2025-06-01",
            changelog_url="/v2/changelog",
        ),
    }


# Custom OpenAPI generation for versioned APIs
def custom_openapi_for_version(app, version: APIVersion) -> dict[str, Any]:
    """Generate custom OpenAPI spec for a specific version."""
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = {
        "openapi": "3.0.2",
        "info": {
            "title": f"Analytics API {version.value.upper()}",
            "version": "1.0.0" if version == APIVersion.V1 else "2.0.0",
            "description": f"Analytics backend REST API - Version {version.value.upper()}",
            "contact": {
                "name": "Analytics Team",
                "email": "analytics@company.com",
            },
            "license": {
                "name": "MIT",
                "url": "https://opensource.org/licenses/MIT",
            },
        },
        "servers": [
            {
                "url": f"/{version.value}",
                "description": f"Version {version.value.upper()} API",
            }
        ],
    }

    return openapi_schema
