#!/usr/bin/env python3
"""Health check script for streaming analytics containers."""

import os
import sys
from typing import Any

import requests


def check_health_endpoint(port: int = 8080, timeout: int = 3) -> dict[str, Any]:
    """Check the health endpoint of the service."""
    try:
        response = requests.get(
            f"http://localhost:{port}/health",
            timeout=timeout,
            headers={"User-Agent": "healthcheck/1.0"},
        )

        if response.status_code == 200:
            return {
                "status": "healthy",
                "response_time": response.elapsed.total_seconds(),
                "status_code": response.status_code,
            }
        else:
            return {
                "status": "unhealthy",
                "status_code": response.status_code,
                "error": f"HTTP {response.status_code}",
            }
    except requests.exceptions.RequestException as e:
        return {"status": "unhealthy", "error": str(e)}


def check_websocket_port(port: int = 8765, timeout: int = 3) -> dict[str, Any]:
    """Check if WebSocket port is listening."""
    import socket

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex(("localhost", port))
        sock.close()

        if result == 0:
            return {"status": "healthy", "port": port}
        else:
            return {"status": "unhealthy", "error": f"Port {port} not listening"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


def main():
    """Main health check function."""
    # Get service type from environment or default to websocket
    service_type = os.environ.get("SERVICE_TYPE", "websocket")

    checks = []

    # Always check the health endpoint
    health_check = check_health_endpoint()
    checks.append(("health_endpoint", health_check))

    # Additional checks based on service type
    if service_type == "websocket":
        websocket_check = check_websocket_port()
        checks.append(("websocket_port", websocket_check))

    # Evaluate overall health
    all_healthy = all(check[1]["status"] == "healthy" for check in checks)

    if all_healthy:
        print("✓ All health checks passed")
        for check_name, result in checks:
            if "response_time" in result:
                print(
                    f"  {check_name}: {result['status']} ({result['response_time']:.3f}s)"
                )
            else:
                print(f"  {check_name}: {result['status']}")
        sys.exit(0)
    else:
        print("✗ Health check failed")
        for check_name, result in checks:
            status = result["status"]
            if status == "unhealthy":
                error = result.get("error", "Unknown error")
                print(f"  {check_name}: {status} - {error}")
            else:
                print(f"  {check_name}: {status}")
        sys.exit(1)


if __name__ == "__main__":
    main()
