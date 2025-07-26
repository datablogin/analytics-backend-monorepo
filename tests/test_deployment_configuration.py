"""Tests for production deployment configurations."""

from pathlib import Path

import pytest
import yaml


class TestKubernetesManifests:
    """Test Kubernetes manifest files for correctness."""

    @pytest.fixture
    def k8s_manifests_path(self) -> Path:
        """Path to Kubernetes manifests."""
        return Path(__file__).parent.parent / "k8s" / "streaming-analytics"

    def test_namespace_manifest_exists(self, k8s_manifests_path: Path):
        """Test that namespace manifest exists and is valid."""
        namespace_file = k8s_manifests_path / "namespace.yaml"
        assert namespace_file.exists(), "namespace.yaml must exist"

        with open(namespace_file) as f:
            docs = list(yaml.safe_load_all(f))

        # Should have namespace, service account, cluster role, and cluster role binding
        assert len(docs) == 4, "namespace.yaml should contain 4 resources"

        # Check namespace
        namespace = docs[0]
        assert namespace["kind"] == "Namespace"
        assert namespace["metadata"]["name"] == "streaming-analytics"
        assert "pod-security.kubernetes.io/enforce" in namespace["metadata"]["labels"]

        # Check service account
        service_account = docs[1]
        assert service_account["kind"] == "ServiceAccount"
        assert service_account["metadata"]["name"] == "streaming-analytics-sa"

    def test_configmap_manifest_exists(self, k8s_manifests_path: Path):
        """Test that configmap manifest exists and has required configuration."""
        configmap_file = k8s_manifests_path / "configmap.yaml"
        assert configmap_file.exists(), "configmap.yaml must exist"

        with open(configmap_file) as f:
            configmap = yaml.safe_load(f)

        assert configmap["kind"] == "ConfigMap"
        assert configmap["metadata"]["name"] == "streaming-config"

        # Check required configuration keys
        required_keys = [
            "STREAMING_ENVIRONMENT",
            "STREAMING_KAFKA__BOOTSTRAP_SERVERS",
            "STREAMING_WEBSOCKET__HOST",
            "STREAMING_REALTIME_ML__MODEL_CACHE_SIZE",
            "STREAMING_MONITORING__ENABLE_AUTO_SCALING",
        ]

        for key in required_keys:
            assert key in configmap["data"], f"ConfigMap must contain {key}"

    def test_secrets_manifest_exists(self, k8s_manifests_path: Path):
        """Test that secrets manifest exists and has proper structure."""
        secrets_file = k8s_manifests_path / "secrets.yaml"
        assert secrets_file.exists(), "secrets.yaml must exist"

        with open(secrets_file) as f:
            docs = list(yaml.safe_load_all(f))

        # Should have main secrets and TLS secret
        assert len(docs) == 2, "secrets.yaml should contain 2 secrets"

        main_secret = docs[0]
        assert main_secret["kind"] == "Secret"
        assert main_secret["metadata"]["name"] == "streaming-secrets"

        tls_secret = docs[1]
        assert tls_secret["kind"] == "Secret"
        assert tls_secret["metadata"]["name"] == "streaming-tls-secret"
        assert tls_secret["type"] == "kubernetes.io/tls"

    def test_deployment_manifests_exist(self, k8s_manifests_path: Path):
        """Test that all deployment manifests exist and are valid."""
        deployment_files = [
            "websocket-service.yaml",
            "stream-processor.yaml",
            "ml-inference.yaml",
        ]

        for filename in deployment_files:
            file_path = k8s_manifests_path / filename
            assert file_path.exists(), f"{filename} must exist"

            with open(file_path) as f:
                docs = list(yaml.safe_load_all(f))

            # Each file should have deployment and service
            assert len(docs) >= 1, f"{filename} should contain at least 1 resource"

            # Check deployment
            deployment = docs[0]
            assert deployment["kind"] == "Deployment"
            assert "spec" in deployment
            assert "replicas" in deployment["spec"]
            assert "selector" in deployment["spec"]
            assert "template" in deployment["spec"]

    def test_websocket_service_configuration(self, k8s_manifests_path: Path):
        """Test WebSocket service specific configuration."""
        websocket_file = k8s_manifests_path / "websocket-service.yaml"

        with open(websocket_file) as f:
            docs = list(yaml.safe_load_all(f))

        deployment = docs[0]
        service = docs[1]

        # Check deployment configuration
        assert deployment["metadata"]["name"] == "websocket-server"
        assert deployment["spec"]["replicas"] == 3

        # Check container configuration
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        assert container["name"] == "websocket-server"
        assert any(port["containerPort"] == 8765 for port in container["ports"])
        assert any(port["containerPort"] == 8080 for port in container["ports"])

        # Check health checks
        assert "livenessProbe" in container
        assert "readinessProbe" in container
        assert "startupProbe" in container

        # Check service configuration
        assert service["kind"] == "Service"
        assert service["metadata"]["name"] == "websocket-service"
        assert service["spec"]["type"] == "LoadBalancer"

    def test_hpa_configuration(self, k8s_manifests_path: Path):
        """Test Horizontal Pod Autoscaler configuration."""
        hpa_file = k8s_manifests_path / "hpa.yaml"
        assert hpa_file.exists(), "hpa.yaml must exist"

        with open(hpa_file) as f:
            docs = list(yaml.safe_load_all(f))

        # Should have HPA for all three services
        assert len(docs) == 3, "hpa.yaml should contain 3 HPA resources"

        for hpa in docs:
            assert hpa["kind"] == "HorizontalPodAutoscaler"
            assert hpa["apiVersion"] == "autoscaling/v2"
            assert "minReplicas" in hpa["spec"]
            assert "maxReplicas" in hpa["spec"]
            assert "metrics" in hpa["spec"]
            assert "behavior" in hpa["spec"]

    def test_network_policy_configuration(self, k8s_manifests_path: Path):
        """Test network policy configuration."""
        netpol_file = k8s_manifests_path / "network-policy.yaml"
        assert netpol_file.exists(), "network-policy.yaml must exist"

        with open(netpol_file) as f:
            docs = list(yaml.safe_load_all(f))

        assert len(docs) >= 1, "network-policy.yaml should contain network policies"

        for policy in docs:
            assert policy["kind"] == "NetworkPolicy"
            assert "policyTypes" in policy["spec"]
            assert "Ingress" in policy["spec"]["policyTypes"]
            assert "Egress" in policy["spec"]["policyTypes"]

    def test_monitoring_configuration(self, k8s_manifests_path: Path):
        """Test monitoring configuration."""
        monitoring_file = k8s_manifests_path / "monitoring.yaml"
        assert monitoring_file.exists(), "monitoring.yaml must exist"

        with open(monitoring_file) as f:
            docs = list(yaml.safe_load_all(f))

        service_monitors = [doc for doc in docs if doc["kind"] == "ServiceMonitor"]
        prometheus_rules = [doc for doc in docs if doc["kind"] == "PrometheusRule"]

        # Should have service monitors for each service
        assert len(service_monitors) >= 3, (
            "Should have ServiceMonitors for each service"
        )

        # Should have prometheus rules
        assert len(prometheus_rules) >= 1, "Should have PrometheusRule for alerts"


class TestDockerConfiguration:
    """Test Docker configuration files."""

    @pytest.fixture
    def docker_path(self) -> Path:
        """Path to Docker configurations."""
        return Path(__file__).parent.parent / "docker" / "streaming-analytics"

    def test_dockerfile_exists(self, docker_path: Path):
        """Test that production Dockerfile exists."""
        dockerfile = docker_path / "Dockerfile"
        assert dockerfile.exists(), "Production Dockerfile must exist"

        content = dockerfile.read_text()

        # Check multi-stage build
        assert "FROM python:3.11-slim as builder" in content
        assert "FROM python:3.11-slim as production" in content

        # Check security practices
        assert "USER streaming" in content
        assert "RUN useradd" in content or "RUN groupadd" in content

        # Check health check
        assert "HEALTHCHECK" in content

        # Check exposed ports
        assert "EXPOSE 8765 8080 8081" in content

    def test_healthcheck_script_exists(self, docker_path: Path):
        """Test that healthcheck script exists and is valid."""
        healthcheck = docker_path / "healthcheck.py"
        assert healthcheck.exists(), "healthcheck.py must exist"

        content = healthcheck.read_text()

        # Check required functions
        assert "def check_health_endpoint" in content
        assert "def check_websocket_port" in content
        assert "def main" in content

        # Check imports
        assert "import requests" in content
        assert "import socket" in content

    def test_docker_compose_prod_exists(self, docker_path: Path):
        """Test that production docker-compose file exists."""
        compose_file = docker_path / "docker-compose.prod.yml"
        assert compose_file.exists(), "docker-compose.prod.yml must exist"

        with open(compose_file) as f:
            compose_config = yaml.safe_load(f)

        # Check required services
        required_services = [
            "kafka-1",
            "kafka-2",
            "kafka-3",
            "zookeeper",
            "websocket-server",
            "stream-processor",
            "ml-inference",
            "postgres",
            "redis",
        ]

        for service in required_services:
            assert service in compose_config["services"], (
                f"Service {service} must be defined"
            )

        # Check networks and volumes
        assert "networks" in compose_config
        assert "volumes" in compose_config


class TestHelmChart:
    """Test Helm chart configuration."""

    @pytest.fixture
    def helm_path(self) -> Path:
        """Path to Helm chart."""
        return Path(__file__).parent.parent / "helm" / "streaming-analytics"

    def test_chart_yaml_exists(self, helm_path: Path):
        """Test that Chart.yaml exists and is valid."""
        chart_file = helm_path / "Chart.yaml"
        assert chart_file.exists(), "Chart.yaml must exist"

        with open(chart_file) as f:
            chart = yaml.safe_load(f)

        # Check required fields
        required_fields = ["apiVersion", "name", "description", "version", "appVersion"]
        for field in required_fields:
            assert field in chart, f"Chart.yaml must contain {field}"

        assert chart["name"] == "streaming-analytics"
        assert chart["apiVersion"] == "v2"

        # Check dependencies
        if "dependencies" in chart:
            deps = chart["dependencies"]
            assert isinstance(deps, list)
            for dep in deps:
                assert "name" in dep
                assert "version" in dep
                assert "repository" in dep

    def test_values_yaml_exists(self, helm_path: Path):
        """Test that values.yaml exists and has required configuration."""
        values_file = helm_path / "values.yaml"
        assert values_file.exists(), "values.yaml must exist"

        with open(values_file) as f:
            values = yaml.safe_load(f)

        # Check required sections
        required_sections = [
            "image",
            "serviceAccount",
            "websocketServer",
            "streamProcessor",
            "mlInference",
            "config",
        ]

        for section in required_sections:
            assert section in values, f"values.yaml must contain {section}"

        # Check websocket server configuration
        ws_config = values["websocketServer"]
        assert "enabled" in ws_config
        assert "replicaCount" in ws_config
        assert "resources" in ws_config
        assert "autoscaling" in ws_config

        # Check stream processor configuration
        sp_config = values["streamProcessor"]
        assert "enabled" in sp_config
        assert "replicaCount" in sp_config
        assert "resources" in sp_config

        # Check ML inference configuration
        ml_config = values["mlInference"]
        assert "enabled" in ml_config
        assert "replicaCount" in ml_config
        assert "resources" in ml_config


class TestDeploymentScript:
    """Test deployment script."""

    @pytest.fixture
    def script_path(self) -> Path:
        """Path to deployment script."""
        return (
            Path(__file__).parent.parent / "scripts" / "deploy-streaming-analytics.sh"
        )

    def test_deployment_script_exists(self, script_path: Path):
        """Test that deployment script exists and is executable."""
        assert script_path.exists(), "deploy-streaming-analytics.sh must exist"

        # Check if script is executable
        stat = script_path.stat()
        assert stat.st_mode & 0o111, "Script must be executable"

        content = script_path.read_text()

        # Check shebang
        assert content.startswith("#!/bin/bash"), "Script must have proper shebang"

        # Check required functions
        required_functions = [
            "check_prerequisites",
            "create_namespace",
            "deploy_secrets",
            "deploy_helm",
            "verify_deployment",
        ]

        for func in required_functions:
            assert f"{func}()" in content, f"Script must contain {func}() function"

        # Check error handling
        assert "set -euo pipefail" in content, "Script must have proper error handling"


class TestDocumentation:
    """Test deployment documentation."""

    @pytest.fixture
    def readme_path(self) -> Path:
        """Path to Kubernetes README."""
        return (
            Path(__file__).parent.parent / "k8s" / "streaming-analytics" / "README.md"
        )

    def test_readme_exists(self, readme_path: Path):
        """Test that README exists and has required sections."""
        assert readme_path.exists(), "README.md must exist"

        content = readme_path.read_text()

        # Check required sections
        required_sections = [
            "# Streaming Analytics Kubernetes Deployment",
            "## Architecture Overview",
            "## Prerequisites",
            "## Deployment Options",
            "## Configuration",
            "## Scaling Configuration",
            "## Monitoring and Observability",
            "## Security Configuration",
            "## Troubleshooting",
        ]

        for section in required_sections:
            assert section in content, f"README must contain section: {section}"

        # Check for code examples
        assert "```bash" in content, "README should contain bash code examples"
        assert "kubectl" in content, "README should contain kubectl examples"
        assert "helm" in content, "README should contain helm examples"


class TestConfigurationValidation:
    """Test configuration validation and consistency."""

    def test_port_consistency(self):
        """Test that port configurations are consistent across manifests."""
        k8s_path = Path(__file__).parent.parent / "k8s" / "streaming-analytics"

        # Load WebSocket service configuration
        with open(k8s_path / "websocket-service.yaml") as f:
            ws_docs = list(yaml.safe_load_all(f))

        ws_deployment = ws_docs[0]
        ws_service = ws_docs[1]

        # Extract ports from deployment
        container = ws_deployment["spec"]["template"]["spec"]["containers"][0]
        deployment_ports = {
            port["name"]: port["containerPort"] for port in container["ports"]
        }

        # Extract ports from service
        service_ports = {
            port["name"]: port["port"] for port in ws_service["spec"]["ports"]
        }

        # Check consistency
        for port_name in service_ports:
            if port_name in deployment_ports:
                assert service_ports[port_name] == deployment_ports[port_name], (
                    f"Port {port_name} mismatch between deployment and service"
                )

    def test_resource_limits_reasonable(self):
        """Test that resource limits are reasonable."""
        k8s_path = Path(__file__).parent.parent / "k8s" / "streaming-analytics"

        deployment_files = [
            "websocket-service.yaml",
            "stream-processor.yaml",
            "ml-inference.yaml",
        ]

        for filename in deployment_files:
            with open(k8s_path / filename) as f:
                docs = list(yaml.safe_load_all(f))

            deployment = docs[0]
            container = deployment["spec"]["template"]["spec"]["containers"][0]
            resources = container["resources"]

            # Check that limits exist and are reasonable
            assert "limits" in resources, f"{filename} must have resource limits"
            assert "requests" in resources, f"{filename} must have resource requests"

            limits = resources["limits"]
            _requests = resources["requests"]  # Keep reference for future use

            # CPU limits should be reasonable (not too high)
            cpu_limit = limits["cpu"]
            if isinstance(cpu_limit, str):
                if cpu_limit.endswith("m"):
                    cpu_limit_val = int(cpu_limit[:-1])
                    assert cpu_limit_val <= 8000, f"CPU limit too high in {filename}"
                else:
                    cpu_limit_val = int(cpu_limit)
                    assert cpu_limit_val <= 8, f"CPU limit too high in {filename}"
            else:
                # Integer CPU value
                assert cpu_limit <= 8, f"CPU limit too high in {filename}"

            # Memory limits should be reasonable
            memory_limit = limits["memory"]
            if memory_limit.endswith("Gi"):
                memory_limit_val = int(memory_limit[:-2])
                assert memory_limit_val <= 32, f"Memory limit too high in {filename}"

    def test_security_context_configured(self):
        """Test that security contexts are properly configured."""
        k8s_path = Path(__file__).parent.parent / "k8s" / "streaming-analytics"

        deployment_files = [
            "websocket-service.yaml",
            "stream-processor.yaml",
            "ml-inference.yaml",
        ]

        for filename in deployment_files:
            with open(k8s_path / filename) as f:
                docs = list(yaml.safe_load_all(f))

            deployment = docs[0]
            pod_spec = deployment["spec"]["template"]["spec"]

            # Check pod security context
            assert "securityContext" in pod_spec, (
                f"{filename} must have pod security context"
            )
            pod_security = pod_spec["securityContext"]
            assert pod_security["runAsNonRoot"] is True
            assert pod_security["runAsUser"] == 1000

            # Check container security context
            container = pod_spec["containers"][0]
            assert "securityContext" in container, (
                f"{filename} must have container security context"
            )
            container_security = container["securityContext"]
            assert container_security["allowPrivilegeEscalation"] is False
            assert container_security["readOnlyRootFilesystem"] is True
