# Production Deployment Guide

This guide covers deploying the streaming analytics infrastructure to production environments.

## Overview

The streaming analytics system is designed for cloud-native deployment with:

- **Kubernetes Orchestration**: Container orchestration with auto-scaling
- **High Availability**: Multi-zone deployment with failover
- **Security**: TLS encryption, RBAC, and network policies
- **Monitoring**: Comprehensive observability and alerting
- **CI/CD Integration**: Automated testing and deployment pipelines

## Architecture

```
Internet → Load Balancer → Kubernetes Cluster
                              ↓
                         Service Mesh (Istio)
                              ↓
                    ┌─────────────────────────┐
                    │   Streaming Services    │
                    │                         │
                    │ ┌─────────────────────┐ │
                    │ │   WebSocket API     │ │
                    │ │   Stream Processor  │ │
                    │ │   ML Inference      │ │
                    │ └─────────────────────┘ │
                    └─────────────────────────┘
                              ↓
                    ┌─────────────────────────┐
                    │   Infrastructure        │
                    │                         │
                    │ ┌─────────┐ ┌─────────┐ │
                    │ │  Kafka  │ │   DB    │ │
                    │ │ Cluster │ │ Cluster │ │
                    │ └─────────┘ └─────────┘ │
                    └─────────────────────────┘
```

## Prerequisites

### Infrastructure Requirements

#### Kubernetes Cluster
- **Version**: Kubernetes 1.24+
- **Nodes**: Minimum 6 nodes (3 per availability zone)
- **Instance Types**: 
  - Control plane: 3x c5.large (2 vCPU, 4GB RAM)
  - Worker nodes: 6x c5.2xlarge (8 vCPU, 16GB RAM)
- **Storage**: EBS volumes with GP3 (minimum 100 IOPS per GB)
- **Networking**: VPC with private subnets, NAT gateways

#### External Dependencies
- **Kafka Cluster**: MSK (Managed Streaming for Kafka) or self-managed
- **Database**: RDS PostgreSQL with Multi-AZ
- **Redis**: ElastiCache Redis cluster
- **Object Storage**: S3 buckets for artifacts and logs
- **Monitoring**: Prometheus, Grafana, Jaeger

### Software Requirements

#### Container Registry
```bash
# Build and push images
docker build -t your-registry/streaming-analytics:v1.0.0 .
docker push your-registry/streaming-analytics:v1.0.0
```

#### Helm Charts
```bash
# Install Helm
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# Add required repositories
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update
```

## Container Images

### Dockerfile

Create optimized production Dockerfiles:

```dockerfile
# Dockerfile.streaming-analytics
FROM python:3.11-slim as builder

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy dependency files
COPY pyproject.toml uv.lock ./
COPY libs/ ./libs/

# Install dependencies
RUN uv sync --frozen --no-dev

# Production stage
FROM python:3.11-slim as production

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd --create-home --shell /bin/bash streaming
USER streaming
WORKDIR /home/streaming

# Copy virtual environment from builder
COPY --from=builder --chown=streaming:streaming .venv .venv
ENV PATH="/home/streaming/.venv/bin:$PATH"

# Copy application code
COPY --chown=streaming:streaming libs/ ./libs/
COPY --chown=streaming:streaming services/ ./services/

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/health')"

# Default command
CMD ["python", "-m", "services.analytics_api.main"]
```

### Multi-stage Build Script

```bash
#!/bin/bash
# scripts/build-images.sh

set -e

REGISTRY="${DOCKER_REGISTRY:-your-registry.com}"
VERSION="${VERSION:-$(git describe --tags --always)}"

# Build streaming analytics API
docker build \
    -f docker/Dockerfile.streaming-analytics \
    -t "${REGISTRY}/streaming-analytics:${VERSION}" \
    -t "${REGISTRY}/streaming-analytics:latest" \
    .

# Build stream processor
docker build \
    -f docker/Dockerfile.stream-processor \
    -t "${REGISTRY}/stream-processor:${VERSION}" \
    -t "${REGISTRY}/stream-processor:latest" \
    .

# Build ML inference service
docker build \
    -f docker/Dockerfile.ml-inference \
    -t "${REGISTRY}/ml-inference:${VERSION}" \
    -t "${REGISTRY}/ml-inference:latest" \
    .

# Push images
docker push "${REGISTRY}/streaming-analytics:${VERSION}"
docker push "${REGISTRY}/streaming-analytics:latest"
docker push "${REGISTRY}/stream-processor:${VERSION}"
docker push "${REGISTRY}/stream-processor:latest"
docker push "${REGISTRY}/ml-inference:${VERSION}"
docker push "${REGISTRY}/ml-inference:latest"

echo "Built and pushed images for version: ${VERSION}"
```

## Kubernetes Deployment

### Namespace and RBAC

```yaml
# k8s/namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: streaming-analytics
  labels:
    name: streaming-analytics
    environment: production

---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: streaming-analytics-sa
  namespace: streaming-analytics

---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: streaming-analytics-role
rules:
- apiGroups: [""]
  resources: ["pods", "services", "endpoints", "configmaps", "secrets"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["apps"]
  resources: ["deployments", "replicasets"]
  verbs: ["get", "list", "watch"]

---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: streaming-analytics-binding
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: streaming-analytics-role
subjects:
- kind: ServiceAccount
  name: streaming-analytics-sa
  namespace: streaming-analytics
```

### ConfigMaps and Secrets

```yaml
# k8s/configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: streaming-config
  namespace: streaming-analytics
data:
  # Kafka configuration
  STREAMING_KAFKA__BOOTSTRAP_SERVERS: '["kafka-1:9092","kafka-2:9092","kafka-3:9092"]'
  STREAMING_KAFKA__SECURITY_PROTOCOL: "SASL_SSL"
  STREAMING_KAFKA__SASL_MECHANISM: "PLAIN"
  
  # WebSocket configuration
  STREAMING_WEBSOCKET__HOST: "0.0.0.0"
  STREAMING_WEBSOCKET__PORT: "8765"
  STREAMING_WEBSOCKET__MAX_CONNECTIONS: "10000"
  
  # ML configuration
  STREAMING_REALTIME_ML__MODEL_CACHE_SIZE: "20"
  STREAMING_REALTIME_ML__MAX_LATENCY_MS: "50"
  
  # Monitoring configuration
  STREAMING_MONITORING__METRICS_INTERVAL_SECONDS: "10"
  STREAMING_MONITORING__ENABLE_AUTO_SCALING: "true"

---
apiVersion: v1
kind: Secret
metadata:
  name: streaming-secrets
  namespace: streaming-analytics
type: Opaque
data:
  # Base64 encoded secrets
  kafka-username: <base64-encoded-username>
  kafka-password: <base64-encoded-password>
  db-password: <base64-encoded-db-password>
  mlflow-token: <base64-encoded-mlflow-token>
```

### WebSocket Service Deployment

```yaml
# k8s/websocket-service.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: websocket-server
  namespace: streaming-analytics
  labels:
    app: websocket-server
    version: v1
spec:
  replicas: 3
  selector:
    matchLabels:
      app: websocket-server
  template:
    metadata:
      labels:
        app: websocket-server
        version: v1
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: streaming-analytics-sa
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        fsGroup: 1000
      containers:
      - name: websocket-server
        image: your-registry/streaming-analytics:latest
        imagePullPolicy: Always
        command: ["python", "-m", "libs.streaming_analytics.websocket_server"]
        ports:
        - containerPort: 8765
          name: websocket
        - containerPort: 8080
          name: metrics
        env:
        - name: STREAMING_ENVIRONMENT
          value: "production"
        envFrom:
        - configMapRef:
            name: streaming-config
        - secretRef:
            name: streaming-secrets
        resources:
          requests:
            cpu: 500m
            memory: 1Gi
          limits:
            cpu: 2
            memory: 4Gi
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
          timeoutSeconds: 3
          failureThreshold: 3
      terminationGracePeriodSeconds: 60

---
apiVersion: v1
kind: Service
metadata:
  name: websocket-service
  namespace: streaming-analytics
  labels:
    app: websocket-server
spec:
  selector:
    app: websocket-server
  ports:
  - name: websocket
    port: 8765
    targetPort: 8765
    protocol: TCP
  - name: metrics
    port: 8080
    targetPort: 8080
    protocol: TCP
  type: ClusterIP
```

### Stream Processor Deployment

```yaml
# k8s/stream-processor.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: stream-processor
  namespace: streaming-analytics
  labels:
    app: stream-processor
    version: v1
spec:
  replicas: 6
  selector:
    matchLabels:
      app: stream-processor
  template:
    metadata:
      labels:
        app: stream-processor
        version: v1
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
    spec:
      serviceAccountName: streaming-analytics-sa
      containers:
      - name: stream-processor
        image: your-registry/stream-processor:latest
        imagePullPolicy: Always
        env:
        - name: STREAMING_ENVIRONMENT
          value: "production"
        - name: PROCESSOR_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        envFrom:
        - configMapRef:
            name: streaming-config
        - secretRef:
            name: streaming-secrets
        resources:
          requests:
            cpu: 1
            memory: 2Gi
          limits:
            cpu: 4
            memory: 8Gi
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 60
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 10
      terminationGracePeriodSeconds: 120
```

### ML Inference Service Deployment

```yaml
# k8s/ml-inference.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ml-inference
  namespace: streaming-analytics
  labels:
    app: ml-inference
    version: v1
spec:
  replicas: 4
  selector:
    matchLabels:
      app: ml-inference
  template:
    metadata:
      labels:
        app: ml-inference
        version: v1
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
    spec:
      serviceAccountName: streaming-analytics-sa
      containers:
      - name: ml-inference
        image: your-registry/ml-inference:latest
        imagePullPolicy: Always
        env:
        - name: STREAMING_ENVIRONMENT
          value: "production"
        envFrom:
        - configMapRef:
            name: streaming-config
        - secretRef:
            name: streaming-secrets
        resources:
          requests:
            cpu: 2
            memory: 4Gi
          limits:
            cpu: 8
            memory: 16Gi
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 120  # Model loading takes time
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
      terminationGracePeriodSeconds: 180
```

## Auto-scaling Configuration

### Horizontal Pod Autoscaler

```yaml
# k8s/hpa.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: websocket-server-hpa
  namespace: streaming-analytics
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: websocket-server
  minReplicas: 3
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
  - type: Pods
    pods:
      metric:
        name: websocket_connections_active
      target:
        type: AverageValue
        averageValue: "1000"
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Percent
        value: 50
        periodSeconds: 60
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
      - type: Percent
        value: 100
        periodSeconds: 15

---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: stream-processor-hpa
  namespace: streaming-analytics
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: stream-processor
  minReplicas: 6
  maxReplicas: 50
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 80
  - type: Pods
    pods:
      metric:
        name: kafka_consumer_lag
      target:
        type: AverageValue
        averageValue: "1000"
  - type: Pods
    pods:
      metric:
        name: event_processing_duration_ms_p95
      target:
        type: AverageValue
        averageValue: "100"
```

### Vertical Pod Autoscaler

```yaml
# k8s/vpa.yaml
apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: ml-inference-vpa
  namespace: streaming-analytics
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: ml-inference
  updatePolicy:
    updateMode: "Auto"
  resourcePolicy:
    containerPolicies:
    - containerName: ml-inference
      minAllowed:
        cpu: 1
        memory: 2Gi
      maxAllowed:
        cpu: 16
        memory: 32Gi
      mode: Auto
```

## Service Mesh Integration

### Istio Configuration

```yaml
# k8s/istio-gateway.yaml
apiVersion: networking.istio.io/v1beta1
kind: Gateway
metadata:
  name: streaming-gateway
  namespace: streaming-analytics
spec:
  selector:
    istio: ingressgateway
  servers:
  - port:
      number: 443
      name: https
      protocol: HTTPS
    tls:
      mode: SIMPLE
      credentialName: streaming-tls-secret
    hosts:
    - streaming.company.com
  - port:
      number: 80
      name: http
      protocol: HTTP
    hosts:
    - streaming.company.com
    tls:
      httpsRedirect: true

---
apiVersion: networking.istio.io/v1beta1
kind: VirtualService
metadata:
  name: streaming-vs
  namespace: streaming-analytics
spec:
  hosts:
  - streaming.company.com
  gateways:
  - streaming-gateway
  http:
  - match:
    - uri:
        prefix: /ws
    route:
    - destination:
        host: websocket-service
        port:
          number: 8765
  - match:
    - uri:
        prefix: /api
    route:
    - destination:
        host: analytics-api-service
        port:
          number: 8000
    timeout: 30s
    retries:
      attempts: 3
      perTryTimeout: 10s
```

### Traffic Splitting for Canary Deployments

```yaml
# k8s/canary-deployment.yaml
apiVersion: networking.istio.io/v1beta1
kind: VirtualService
metadata:
  name: stream-processor-canary
  namespace: streaming-analytics
spec:
  hosts:
  - stream-processor-service
  http:
  - match:
    - headers:
        canary:
          exact: "true"
    route:
    - destination:
        host: stream-processor-service
        subset: v2
  - route:
    - destination:
        host: stream-processor-service
        subset: v1
      weight: 90
    - destination:
        host: stream-processor-service
        subset: v2
      weight: 10

---
apiVersion: networking.istio.io/v1beta1
kind: DestinationRule
metadata:
  name: stream-processor-dr
  namespace: streaming-analytics
spec:
  host: stream-processor-service
  subsets:
  - name: v1
    labels:
      version: v1
  - name: v2
    labels:
      version: v2
```

## Security Configuration

### Network Policies

```yaml
# k8s/network-policy.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: streaming-analytics-netpol
  namespace: streaming-analytics
spec:
  podSelector:
    matchLabels:
      app: stream-processor
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: streaming-analytics
    - namespaceSelector:
        matchLabels:
          name: istio-system
    ports:
    - protocol: TCP
      port: 8080  # Metrics
  egress:
  - to: []
    ports:
    - protocol: TCP
      port: 9092  # Kafka
    - protocol: TCP
      port: 5432  # PostgreSQL
    - protocol: TCP
      port: 6379  # Redis
    - protocol: TCP
      port: 443   # HTTPS
    - protocol: TCP
      port: 53    # DNS
    - protocol: UDP
      port: 53    # DNS
```

### Pod Security Standards

```yaml
# k8s/pod-security-policy.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: streaming-analytics
  labels:
    pod-security.kubernetes.io/enforce: restricted
    pod-security.kubernetes.io/audit: restricted
    pod-security.kubernetes.io/warn: restricted
```

### TLS Configuration

```yaml
# k8s/tls-secret.yaml
apiVersion: v1
kind: Secret
metadata:
  name: streaming-tls-secret
  namespace: streaming-analytics
type: kubernetes.io/tls
data:
  tls.crt: <base64-encoded-certificate>
  tls.key: <base64-encoded-private-key>
```

## Monitoring and Observability

### Prometheus ServiceMonitor

```yaml
# k8s/service-monitor.yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: streaming-analytics-metrics
  namespace: streaming-analytics
  labels:
    app: streaming-analytics
spec:
  selector:
    matchLabels:
      app: websocket-server
  endpoints:
  - port: metrics
    interval: 15s
    path: /metrics
    honorLabels: true

---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: stream-processor-metrics
  namespace: streaming-analytics
spec:
  selector:
    matchLabels:
      app: stream-processor
  endpoints:
  - port: metrics
    interval: 15s
    path: /metrics
```

### Jaeger Tracing

```yaml
# k8s/jaeger-config.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: jaeger-config
  namespace: streaming-analytics
data:
  JAEGER_AGENT_HOST: jaeger-agent.observability
  JAEGER_AGENT_PORT: "6831"
  JAEGER_SAMPLER_TYPE: probabilistic
  JAEGER_SAMPLER_PARAM: "0.1"
  JAEGER_SERVICE_NAME: streaming-analytics
```

## CI/CD Pipeline

### GitHub Actions Workflow

```yaml
# .github/workflows/deploy-production.yml
name: Deploy to Production

on:
  push:
    tags:
      - 'v*'

env:
  REGISTRY: your-registry.com
  K8S_CLUSTER: production-cluster
  K8S_NAMESPACE: streaming-analytics

jobs:
  build-and-push:
    runs-on: ubuntu-latest
    outputs:
      image-tag: ${{ steps.meta.outputs.tags }}
      image-digest: ${{ steps.build.outputs.digest }}
    steps:
    - name: Checkout
      uses: actions/checkout@v4

    - name: Set up Docker Buildx
      uses: docker/setup-buildx-action@v3

    - name: Login to Registry
      uses: docker/login-action@v3
      with:
        registry: ${{ env.REGISTRY }}
        username: ${{ secrets.REGISTRY_USERNAME }}
        password: ${{ secrets.REGISTRY_PASSWORD }}

    - name: Extract metadata
      id: meta
      uses: docker/metadata-action@v5
      with:
        images: ${{ env.REGISTRY }}/streaming-analytics
        tags: |
          type=ref,event=tag
          type=sha

    - name: Build and push
      id: build
      uses: docker/build-push-action@v5
      with:
        context: .
        file: docker/Dockerfile.streaming-analytics
        push: true
        tags: ${{ steps.meta.outputs.tags }}
        labels: ${{ steps.meta.outputs.labels }}
        cache-from: type=gha
        cache-to: type=gha,mode=max

  security-scan:
    runs-on: ubuntu-latest
    needs: build-and-push
    steps:
    - name: Run Trivy vulnerability scanner
      uses: aquasecurity/trivy-action@master
      with:
        image-ref: ${{ needs.build-and-push.outputs.image-tag }}
        format: 'sarif'
        output: 'trivy-results.sarif'

    - name: Upload Trivy scan results
      uses: github/codeql-action/upload-sarif@v2
      with:
        sarif_file: 'trivy-results.sarif'

  deploy:
    runs-on: ubuntu-latest
    needs: [build-and-push, security-scan]
    environment: production
    steps:
    - name: Checkout
      uses: actions/checkout@v4

    - name: Configure kubectl
      uses: azure/k8s-set-context@v3
      with:
        method: kubeconfig
        kubeconfig: ${{ secrets.KUBE_CONFIG }}

    - name: Deploy to Kubernetes
      run: |
        # Update image tags in manifests
        sed -i "s|your-registry/streaming-analytics:latest|${{ needs.build-and-push.outputs.image-tag }}|g" k8s/*.yaml
        
        # Apply manifests
        kubectl apply -f k8s/namespace.yaml
        kubectl apply -f k8s/configmap.yaml
        kubectl apply -f k8s/secrets.yaml
        kubectl apply -f k8s/
        
        # Wait for rollout
        kubectl rollout status deployment/websocket-server -n ${{ env.K8S_NAMESPACE }} --timeout=600s
        kubectl rollout status deployment/stream-processor -n ${{ env.K8S_NAMESPACE }} --timeout=600s
        kubectl rollout status deployment/ml-inference -n ${{ env.K8S_NAMESPACE }} --timeout=600s

    - name: Run smoke tests
      run: |
        kubectl run smoke-test \
          --image=${{ needs.build-and-push.outputs.image-tag }} \
          --rm -i --restart=Never \
          --namespace=${{ env.K8S_NAMESPACE }} \
          -- python -m tests.smoke_tests
```

### Helm Chart Deployment

```yaml
# helm/streaming-analytics/Chart.yaml
apiVersion: v2
name: streaming-analytics
description: Streaming Analytics Platform
version: 1.0.0
appVersion: "1.0.0"
dependencies:
- name: postgresql
  version: 12.x.x
  repository: https://charts.bitnami.com/bitnami
  condition: postgresql.enabled
- name: redis
  version: 17.x.x
  repository: https://charts.bitnami.com/bitnami
  condition: redis.enabled
```

```yaml
# helm/streaming-analytics/values.yaml
replicaCount:
  websocketServer: 3
  streamProcessor: 6
  mlInference: 4

image:
  repository: your-registry/streaming-analytics
  tag: "latest"
  pullPolicy: Always

autoscaling:
  websocketServer:
    enabled: true
    minReplicas: 3
    maxReplicas: 20
    targetCPUUtilizationPercentage: 70
  streamProcessor:
    enabled: true
    minReplicas: 6
    maxReplicas: 50
    targetCPUUtilizationPercentage: 80

resources:
  websocketServer:
    requests:
      cpu: 500m
      memory: 1Gi
    limits:
      cpu: 2
      memory: 4Gi
  streamProcessor:
    requests:
      cpu: 1
      memory: 2Gi
    limits:
      cpu: 4
      memory: 8Gi
  mlInference:
    requests:
      cpu: 2
      memory: 4Gi
    limits:
      cpu: 8
      memory: 16Gi

kafka:
  bootstrapServers: "kafka-1:9092,kafka-2:9092,kafka-3:9092"
  securityProtocol: SASL_SSL
  saslMechanism: PLAIN

postgresql:
  enabled: false  # Use external RDS
  host: your-rds-endpoint.amazonaws.com
  port: 5432
  database: streaming_analytics

redis:
  enabled: false  # Use external ElastiCache
  host: your-redis-cluster.cache.amazonaws.com
  port: 6379

monitoring:
  prometheus:
    enabled: true
  grafana:
    enabled: true
  jaeger:
    enabled: true

istio:
  enabled: true
  gateway:
    hosts:
    - streaming.company.com
```

### Deploy with Helm

```bash
#!/bin/bash
# scripts/deploy-helm.sh

set -e

NAMESPACE="streaming-analytics"
RELEASE_NAME="streaming-analytics"
CHART_PATH="helm/streaming-analytics"
VALUES_FILE="helm/values-production.yaml"

# Create namespace
kubectl create namespace ${NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -

# Deploy with Helm
helm upgrade --install ${RELEASE_NAME} ${CHART_PATH} \
  --namespace ${NAMESPACE} \
  --values ${VALUES_FILE} \
  --wait \
  --timeout 10m

# Verify deployment
kubectl rollout status deployment/websocket-server -n ${NAMESPACE}
kubectl rollout status deployment/stream-processor -n ${NAMESPACE}
kubectl rollout status deployment/ml-inference -n ${NAMESPACE}

echo "Deployment completed successfully!"
```

## Best Practices

### Container Security
- Use minimal base images (distroless, alpine)
- Run containers as non-root users
- Implement proper resource limits
- Regular security scanning with Trivy/Snyk

### Kubernetes Configuration
- Use namespaces for isolation
- Implement network policies
- Enable pod security standards
- Regular cluster updates

### Monitoring and Alerting
- Comprehensive metrics collection
- Proactive alerting on SLO violations
- Distributed tracing for debugging
- Regular runbook testing

### Disaster Recovery
- Multi-zone deployment
- Regular backup testing
- Automated failover procedures
- RTO/RPO monitoring

### Performance Optimization
- Resource right-sizing
- Auto-scaling configuration
- Connection pooling
- Caching strategies