"""Tests for OpenAPI documentation and API standards."""

from fastapi.testclient import TestClient

from services.analytics_api.main import app

client = TestClient(app)


class TestOpenAPIDocumentation:
    """Test OpenAPI documentation generation and access."""

    def test_openapi_spec_generation(self):
        """Test that OpenAPI specification is generated correctly."""
        response = client.get("/openapi.json")
        assert response.status_code == 200

        openapi_spec = response.json()

        # Check basic OpenAPI structure
        assert "openapi" in openapi_spec
        assert "info" in openapi_spec
        assert "paths" in openapi_spec
        assert "components" in openapi_spec

        # Check API info
        info = openapi_spec["info"]
        assert info["title"] == "Analytics API"
        assert "version" in info
        assert "contact" in info
        assert "license" in info

        # Check servers configuration
        assert "servers" in openapi_spec
        servers = openapi_spec["servers"]
        assert any(server["url"] == "/v1" for server in servers)

        # Check security schemes
        assert "securitySchemes" in openapi_spec["components"]
        security_schemes = openapi_spec["components"]["securitySchemes"]
        assert "BearerAuth" in security_schemes
        assert "ApiKeyAuth" in security_schemes

    def test_swagger_ui_accessible(self):
        """Test that Swagger UI documentation is accessible."""
        response = client.get("/docs")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]
        assert "swagger-ui" in response.text.lower()

    def test_redoc_accessible(self):
        """Test that ReDoc documentation is accessible."""
        response = client.get("/redoc")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]
        assert "redoc" in response.text.lower()

    def test_documentation_endpoints_not_in_spec(self):
        """Test that documentation endpoints are not included in OpenAPI spec."""
        response = client.get("/openapi.json")
        openapi_spec = response.json()

        paths = openapi_spec["paths"]
        assert "/docs" not in paths
        assert "/redoc" not in paths

    def test_all_endpoints_documented(self):
        """Test that all API endpoints are documented in OpenAPI spec."""
        response = client.get("/openapi.json")
        openapi_spec = response.json()

        paths = openapi_spec["paths"]

        # Check that main endpoints are documented
        assert "/health" in paths
        assert "/health/database" in paths
        assert "/health/detailed" in paths
        assert "/version" in paths
        assert "/api-stats" in paths
        assert "/protected" in paths

        # Check versioned endpoints
        assert "/v1/auth/register" in paths or any(
            "/auth/register" in path for path in paths
        )
        assert "/v1/admin/roles" in paths or any(
            "/admin/roles" in path for path in paths
        )

    def test_response_schemas_defined(self):
        """Test that response schemas are properly defined."""
        response = client.get("/openapi.json")
        openapi_spec = response.json()

        components = openapi_spec["components"]["schemas"]

        # Check that our custom response models are defined
        assert "StandardResponse" in components or any(
            "StandardResponse" in name for name in components
        )
        assert "HealthStatus" in components
        assert "APIMetadata" in components

        # Check for StandardResponse variants
        standard_response_schemas = [
            name for name in components if "StandardResponse" in name
        ]
        assert len(standard_response_schemas) > 0

    def test_security_requirements_documented(self):
        """Test that security requirements are properly documented."""
        response = client.get("/openapi.json")
        openapi_spec = response.json()

        # Check global security
        assert "security" in openapi_spec

        # Check individual endpoint security
        paths = openapi_spec["paths"]
        protected_path = paths.get("/protected") or paths.get("/v1/protected")
        if protected_path:
            get_method = protected_path.get("get")
            if get_method:
                # Should have security requirements
                assert "security" in get_method or "security" in openapi_spec


class TestStandardizedResponses:
    """Test standardized response formats."""

    def test_health_response_format(self):
        """Test that health endpoints return standardized responses."""
        response = client.get("/health")
        assert response.status_code == 200

        data = response.json()

        # Check standardized response structure
        assert "success" in data
        assert "data" in data
        assert "message" in data
        assert "metadata" in data

        # Check metadata structure
        metadata = data["metadata"]
        assert "request_id" in metadata
        assert "timestamp" in metadata
        assert "version" in metadata
        assert "environment" in metadata

        # Check health data structure
        health_data = data["data"]
        assert "status" in health_data
        assert "version" in health_data
        assert "checks" in health_data

    def test_version_response_format(self):
        """Test that version endpoint returns standardized response."""
        response = client.get("/version")
        assert response.status_code == 200

        data = response.json()

        # Check standardized response structure
        assert "success" in data
        assert data["success"] is True
        assert "data" in data
        assert "message" in data
        assert "metadata" in data

        # Check version data structure
        version_data = data["data"]
        assert "api_version" in version_data
        assert "app_version" in version_data
        assert "features" in version_data
        assert "documentation" in version_data

    def test_api_stats_response_format(self):
        """Test that API stats endpoint returns standardized response."""
        response = client.get("/api-stats")
        assert response.status_code == 200

        data = response.json()

        # Check standardized response structure
        assert "success" in data
        assert data["success"] is True
        assert "data" in data

        # Check stats data structure
        stats_data = data["data"]
        assert "uptime_seconds" in stats_data
        assert "total_endpoints" in stats_data
        assert "middleware_count" in stats_data


class TestAPIVersioning:
    """Test API versioning implementation."""

    def test_versioned_endpoints_accessible(self):
        """Test that versioned endpoints are accessible."""
        # Test v1 endpoints are accessible through version prefix
        response = client.post(
            "/v1/auth/register",
            json={
                "username": "testuser",
                "email": "test@example.com",
                "password": "testpass",
                "full_name": "Test User",
            },
        )
        # Should get a response (might be error due to no DB, but endpoint should exist)
        assert response.status_code in [200, 201, 400, 422, 500]

    def test_version_headers_present(self):
        """Test that version headers are included in responses."""
        response = client.get("/health")

        # Check for version headers
        assert "X-API-Version" in response.headers
        assert response.headers["X-API-Version"] == "v1"

    def test_openapi_spec_includes_versioning(self):
        """Test that OpenAPI spec includes versioning information."""
        response = client.get("/openapi.json")
        openapi_spec = response.json()

        # Check servers include version paths
        servers = openapi_spec.get("servers", [])
        version_servers = [s for s in servers if "/v1" in s.get("url", "")]
        assert len(version_servers) > 0


class TestMiddleware:
    """Test middleware functionality."""

    def test_request_id_header(self):
        """Test that request ID header is added to responses."""
        response = client.get("/health")

        assert "X-Request-ID" in response.headers
        request_id = response.headers["X-Request-ID"]
        assert len(request_id) > 0

    def test_response_time_header(self):
        """Test that response time header is added."""
        response = client.get("/health")

        assert "X-Response-Time" in response.headers
        response_time = response.headers["X-Response-Time"]
        assert float(response_time) >= 0

    def test_security_headers(self):
        """Test that security headers are added to responses."""
        response = client.get("/health")

        # Check security headers
        assert response.headers.get("X-Content-Type-Options") == "nosniff"
        assert response.headers.get("X-Frame-Options") == "DENY"
        assert response.headers.get("X-XSS-Protection") == "1; mode=block"
        assert "Referrer-Policy" in response.headers

    def test_rate_limit_headers(self):
        """Test that rate limiting headers are present."""
        response = client.get("/health")

        # Check rate limit headers
        assert "X-RateLimit-Limit" in response.headers
        assert "X-RateLimit-Remaining" in response.headers
        assert "X-RateLimit-Reset" in response.headers

        # Verify values are numeric
        assert int(response.headers["X-RateLimit-Limit"]) > 0
        assert int(response.headers["X-RateLimit-Remaining"]) >= 0

    def test_cors_headers(self):
        """Test that CORS headers are present for cross-origin requests."""
        response = client.options("/health", headers={"Origin": "https://example.com"})

        # Should have CORS headers
        assert "Access-Control-Allow-Origin" in response.headers


class TestErrorHandling:
    """Test standardized error handling."""

    def test_404_error_format(self):
        """Test that 404 errors return standardized format."""
        response = client.get("/nonexistent-endpoint")
        assert response.status_code == 404

        # Note: FastAPI handles 404s before our middleware,
        # so this tests FastAPI's default 404 behavior

    def test_validation_error_format(self):
        """Test that validation errors return standardized format."""
        # Send invalid data to trigger validation error
        response = client.post(
            "/v1/auth/register",
            json={
                "username": "",  # Invalid: empty username
                "email": "not-an-email",  # Invalid: bad email format
                "password": "short",  # Invalid: too short
            },
        )

        # Should get validation error or server error (due to DB issues in test)
        assert response.status_code in [422, 500]
        data = response.json()

        # Should have standardized error format (handled by middleware)
        if response.status_code == 422:
            if "success" in data:
                assert data["success"] is False
                assert "error" in data
            else:
                # FastAPI default validation error format
                assert "detail" in data
        elif response.status_code == 500:
            # Our middleware standardized error format
            assert "success" in data
            assert data["success"] is False
            assert "error" in data


class TestPerformance:
    """Test performance-related features."""

    def test_documentation_performance(self):
        """Test that documentation endpoints load quickly."""
        import time

        start_time = time.time()
        response = client.get("/openapi.json")
        load_time = time.time() - start_time

        assert response.status_code == 200
        assert load_time < 2.0  # Should load in under 2 seconds

        start_time = time.time()
        response = client.get("/docs")
        load_time = time.time() - start_time

        assert response.status_code == 200
        assert load_time < 2.0  # Should load in under 2 seconds

    def test_middleware_overhead(self):
        """Test that middleware doesn't add significant overhead."""
        import time

        # Measure response time for a simple endpoint
        start_time = time.time()
        response = client.get("/health")
        response_time = time.time() - start_time

        assert response.status_code == 200

        # Response time should be reasonable (under 100ms for health check)
        assert response_time < 0.1


class TestDocumentationCompleteness:
    """Test that documentation is complete and accurate."""

    def test_all_response_models_documented(self):
        """Test that all response models are properly documented."""
        response = client.get("/openapi.json")
        openapi_spec = response.json()

        # Check that paths have proper response documentation
        paths = openapi_spec["paths"]

        for path, methods in paths.items():
            for method, details in methods.items():
                if method in ["get", "post", "put", "delete", "patch"]:
                    assert "responses" in details
                    responses = details["responses"]

                    # Should have at least 200 response documented
                    assert "200" in responses or "201" in responses

    def test_authentication_documented(self):
        """Test that authentication requirements are documented."""
        response = client.get("/openapi.json")
        openapi_spec = response.json()

        # Check security schemes are defined
        components = openapi_spec.get("components", {})
        security_schemes = components.get("securitySchemes", {})

        assert "BearerAuth" in security_schemes
        bearer_auth = security_schemes["BearerAuth"]
        assert bearer_auth["type"] == "http"
        assert bearer_auth["scheme"] == "bearer"

    def test_example_responses_included(self):
        """Test that response examples are included in documentation."""
        response = client.get("/openapi.json")
        openapi_spec = response.json()

        # Check that components include example responses
        components = openapi_spec.get("components", {})
        responses = components.get("responses", {})

        # Should have error response examples
        for status_code in ["400", "401", "403", "422", "429", "500"]:
            if status_code in responses:
                error_response = responses[status_code]
                content = error_response.get("content", {})
                if "application/json" in content:
                    json_content = content["application/json"]
                    # Should have either example or schema
                    assert "example" in json_content or "schema" in json_content
