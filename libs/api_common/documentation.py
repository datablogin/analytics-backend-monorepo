"""Enhanced OpenAPI documentation utilities."""

from typing import Any

from fastapi import FastAPI
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from fastapi.responses import HTMLResponse


def create_enhanced_openapi_schema(
    app: FastAPI,
    title: str = "Analytics API",
    version: str = "1.0.0",
    description: str = "Analytics backend REST API",
) -> dict[str, Any]:
    """Create enhanced OpenAPI schema with additional metadata."""

    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title=title,
        version=version,
        description=description,
        routes=app.routes,
    )

    # Add enhanced metadata
    openapi_schema["info"].update(
        {
            "contact": {
                "name": "Analytics Team",
                "email": "analytics@company.com",
                "url": "https://github.com/company/analytics-backend",
            },
            "license": {
                "name": "MIT",
                "url": "https://opensource.org/licenses/MIT",
            },
            "termsOfService": "https://company.com/terms",
            "x-logo": {
                "url": "https://company.com/logo.png",
                "altText": "Analytics API Logo",
            },
        }
    )

    # Add server information
    openapi_schema["servers"] = [
        {
            "url": "/v1",
            "description": "Version 1 API (Stable)",
        },
        {
            "url": "/v2",
            "description": "Version 2 API (Beta)",
        },
        {
            "url": "https://api.analytics.company.com/v1",
            "description": "Production API v1",
        },
        {
            "url": "https://staging-api.analytics.company.com/v1",
            "description": "Staging API v1",
        },
    ]

    # Add security schemes
    openapi_schema["components"]["securitySchemes"] = {
        "BearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
            "description": "JWT token obtained from /v1/auth/token endpoint",
        },
        "ApiKeyAuth": {
            "type": "apiKey",
            "in": "header",
            "name": "X-API-Key",
            "description": "API key for service-to-service authentication",
        },
    }

    # Add global security
    openapi_schema["security"] = [
        {"BearerAuth": []},
        {"ApiKeyAuth": []},
    ]

    # Add custom extensions
    openapi_schema["x-tagGroups"] = [
        {
            "name": "Authentication",
            "tags": ["V1 - authentication"],
        },
        {
            "name": "Administration",
            "tags": ["V1 - admin"],
        },
        {
            "name": "Health & Monitoring",
            "tags": ["health"],
        },
    ]

    # Add rate limiting information
    openapi_schema["x-rateLimit"] = {
        "default": {
            "requests": 100,
            "period": "1 minute",
        },
        "authenticated": {
            "requests": 1000,
            "period": "1 minute",
        },
    }

    # Add response examples
    _add_common_responses(openapi_schema)

    app.openapi_schema = openapi_schema
    return app.openapi_schema


def _add_common_responses(openapi_schema: dict[str, Any]) -> None:
    """Add common response examples to the OpenAPI schema."""

    # Common error responses
    error_responses = {
        "400": {
            "description": "Bad Request",
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/ErrorResponse"},
                    "example": {
                        "success": False,
                        "error": {
                            "code": "BAD_REQUEST",
                            "message": "Invalid request parameters",
                        },
                        "metadata": {
                            "request_id": "12345678-1234-1234-1234-123456789abc",
                            "timestamp": "2025-01-01T12:00:00Z",
                            "version": "v1",
                            "environment": "production",
                        },
                    },
                }
            },
        },
        "401": {
            "description": "Unauthorized",
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/ErrorResponse"},
                    "example": {
                        "success": False,
                        "error": {
                            "code": "UNAUTHORIZED",
                            "message": "Invalid or missing authentication credentials",
                        },
                        "metadata": {
                            "request_id": "12345678-1234-1234-1234-123456789abc",
                            "timestamp": "2025-01-01T12:00:00Z",
                            "version": "v1",
                            "environment": "production",
                        },
                    },
                }
            },
        },
        "403": {
            "description": "Forbidden",
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/ErrorResponse"},
                    "example": {
                        "success": False,
                        "error": {
                            "code": "FORBIDDEN",
                            "message": "Insufficient permissions to access this resource",
                        },
                        "metadata": {
                            "request_id": "12345678-1234-1234-1234-123456789abc",
                            "timestamp": "2025-01-01T12:00:00Z",
                            "version": "v1",
                            "environment": "production",
                        },
                    },
                }
            },
        },
        "422": {
            "description": "Validation Error",
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/ValidationErrorResponse"},
                    "example": {
                        "success": False,
                        "error": {
                            "code": "VALIDATION_ERROR",
                            "message": "Request validation failed",
                        },
                        "validation_errors": [
                            {
                                "field": "email",
                                "message": "Invalid email format",
                                "invalid_value": "not-an-email",
                            }
                        ],
                        "metadata": {
                            "request_id": "12345678-1234-1234-1234-123456789abc",
                            "timestamp": "2025-01-01T12:00:00Z",
                            "version": "v1",
                            "environment": "production",
                        },
                    },
                }
            },
        },
        "429": {
            "description": "Rate Limit Exceeded",
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/ErrorResponse"},
                    "example": {
                        "success": False,
                        "error": {
                            "code": "RATE_LIMIT_EXCEEDED",
                            "message": "Rate limit exceeded. Maximum 100 requests per minute.",
                        },
                        "metadata": {
                            "request_id": "12345678-1234-1234-1234-123456789abc",
                            "timestamp": "2025-01-01T12:00:00Z",
                            "version": "v1",
                            "environment": "production",
                        },
                    },
                }
            },
        },
        "500": {
            "description": "Internal Server Error",
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/ErrorResponse"},
                    "example": {
                        "success": False,
                        "error": {
                            "code": "INTERNAL_SERVER_ERROR",
                            "message": "An unexpected error occurred",
                        },
                        "metadata": {
                            "request_id": "12345678-1234-1234-1234-123456789abc",
                            "timestamp": "2025-01-01T12:00:00Z",
                            "version": "v1",
                            "environment": "production",
                        },
                    },
                }
            },
        },
    }

    # Add these responses to components
    if "components" not in openapi_schema:
        openapi_schema["components"] = {}
    if "responses" not in openapi_schema["components"]:
        openapi_schema["components"]["responses"] = {}

    openapi_schema["components"]["responses"].update(error_responses)


def create_custom_docs_html(
    openapi_url: str,
    title: str,
    swagger_css_url: str = "https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.css",
    swagger_js_url: str = "https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js",
) -> HTMLResponse:
    """Create custom Swagger UI HTML with enhanced styling."""

    return get_swagger_ui_html(
        openapi_url=openapi_url,
        title=title,
        swagger_css_url=swagger_css_url,
        swagger_js_url=swagger_js_url,
        init_oauth={
            "usePkceWithAuthorizationCodeGrant": True,
            "clientId": "swagger-ui",
            "realm": "analytics-api",
            "appName": "Analytics API",
        },
    )


def create_custom_redoc_html(
    openapi_url: str,
    title: str,
    redoc_js_url: str = "https://cdn.jsdelivr.net/npm/redoc@2.1.0/bundles/redoc.standalone.js",
) -> HTMLResponse:
    """Create custom ReDoc HTML with enhanced styling."""

    return get_redoc_html(
        openapi_url=openapi_url,
        title=title,
        redoc_js_url=redoc_js_url,
    )
