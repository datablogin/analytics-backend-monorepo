# Streaming Analytics Kubernetes Deployment

This directory contains production-ready Kubernetes manifests for deploying the streaming analytics infrastructure.

## Architecture Overview

The streaming analytics platform consists of three main components:

1. **WebSocket Server** - Handles real-time client connections and live data streaming
2. **Stream Processor** - Processes events from Kafka with windowing and aggregations  
3. **ML Inference** - Real-time machine learning inference with model caching

## Prerequisites

### Kubernetes Cluster Requirements

- **Kubernetes Version**: 1.24+
- **Minimum Nodes**: 6 nodes across 3 availability zones
- **Node Resources**: 
  - Control plane: 3x c5.large (2 vCPU, 4GB RAM)
  - Worker nodes: 6x c5.2xlarge (8 vCPU, 16GB RAM)
- **Storage**: GP3 EBS volumes (100+ IOPS/GB)
- **Networking**: VPC with private subnets and NAT gateways

### External Dependencies

- **Kafka Cluster**: MSK or self-managed Kafka with 3+ brokers
- **PostgreSQL**: RDS PostgreSQL with Multi-AZ deployment
- **Redis**: ElastiCache Redis cluster
- **Container Registry**: ECR, Docker Hub, or private registry
- **Observability**: Prometheus, Grafana, Jaeger

### Required Tools

```bash
# kubectl
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"

# helm
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

## Deployment Options

### Option 1: Direct Kubernetes Manifests

```bash
# Create namespace and RBAC
kubectl apply -f namespace.yaml

# Deploy configuration
kubectl apply -f configmap.yaml
kubectl apply -f secrets.yaml

# Deploy services
kubectl apply -f websocket-service.yaml
kubectl apply -f stream-processor.yaml  
kubectl apply -f ml-inference.yaml

# Deploy auto-scaling and monitoring
kubectl apply -f hpa.yaml
kubectl apply -f monitoring.yaml
kubectl apply -f network-policy.yaml

# Verify deployment
kubectl get all -n streaming-analytics
```

### Option 2: Helm Chart (Recommended)

```bash
# Using the deployment script
./scripts/deploy-streaming-analytics.sh --build-images -t v1.0.0

# Or manually with Helm
helm upgrade --install streaming-analytics helm/streaming-analytics \
  --namespace streaming-analytics --create-namespace \
  --set image.tag=v1.0.0 \
  --wait --timeout 10m
```

## Configuration

### Environment Variables

Set these environment variables before deployment:

```bash
# Kafka credentials
export KAFKA_USERNAME="your-kafka-user"
export KAFKA_PASSWORD="your-kafka-password"

# Database credentials  
export DATABASE_USER="streaming_user"
export DATABASE_PASSWORD="your-database-password"

# Redis password
export REDIS_PASSWORD="your-redis-password"

# MLflow token
export MLFLOW_TOKEN="your-mlflow-token"

# JWT secret for WebSocket authentication
export JWT_SECRET_KEY="your-jwt-secret-key"

# TLS certificates (optional)
export TLS_CERT_PATH="/path/to/certificate.crt"
export TLS_KEY_PATH="/path/to/private.key"
```

### Secrets Management

Update the base64-encoded values in `secrets.yaml`:

```bash
# Encode secrets
echo -n "your-kafka-username" | base64
echo -n "your-kafka-password" | base64
echo -n "your-database-password" | base64
```

### ConfigMap Customization

Modify `configmap.yaml` to adjust:

- Kafka bootstrap servers and security settings
- Stream processing parallelism and batch sizes
- WebSocket connection limits and timeouts
- ML model cache sizes and refresh intervals
- Monitoring thresholds and auto-scaling policies

## Scaling Configuration

### Horizontal Pod Autoscaler (HPA)

The deployment includes HPA configurations for automatic scaling:

- **WebSocket Server**: 3-20 replicas based on CPU, memory, and active connections
- **Stream Processor**: 6-50 replicas based on CPU, memory, and Kafka consumer lag
- **ML Inference**: 4-20 replicas based on CPU, memory, and inference queue size

### Manual Scaling

```bash
# Scale WebSocket servers
kubectl scale deployment websocket-server --replicas=10 -n streaming-analytics

# Scale stream processors  
kubectl scale deployment stream-processor --replicas=20 -n streaming-analytics

# Scale ML inference services
kubectl scale deployment ml-inference --replicas=8 -n streaming-analytics
```

## Monitoring and Observability

### ServiceMonitor

Prometheus ServiceMonitor configurations are included for:

- Application metrics (WebSocket connections, processing latency, ML inference metrics)
- System metrics (CPU, memory, network)
- Custom business metrics

### Prometheus Alerts

The `monitoring.yaml` includes critical alerts:

- High latency warnings (>100ms p95)
- High error rates (>5%)
- Service availability
- Resource utilization
- Kafka consumer lag

### Grafana Dashboards

Deploy pre-built dashboards for:

- Real-time streaming metrics
- ML inference performance
- Kafka consumer monitoring
- System resource utilization

## Security Configuration

### Network Policies

Network policies restrict traffic to:

- Allow internal namespace communication
- Allow metrics scraping from observability namespace
- Allow external traffic only to WebSocket service
- Restrict egress to required services (Kafka, DB, Redis)

### Pod Security Standards

All pods run with:

- Non-root user (UID 1000)
- Read-only root filesystem
- Dropped capabilities
- Security context enforcement

### TLS Configuration

WebSocket services support TLS with:

- Kubernetes TLS secrets
- Cert-manager integration (optional)
- Custom certificate management

## Troubleshooting

### Common Issues

1. **Pods stuck in Pending**
   ```bash
   kubectl describe pod <pod-name> -n streaming-analytics
   # Check resource requests, node capacity, and scheduling constraints
   ```

2. **Services not accessible**
   ```bash
   kubectl get svc -n streaming-analytics
   kubectl describe svc websocket-service -n streaming-analytics
   # Verify service selectors and endpoint availability
   ```

3. **High Kafka consumer lag**
   ```bash
   kubectl logs deployment/stream-processor -n streaming-analytics
   # Check processing performance and scaling
   ```

4. **ML inference high latency**
   ```bash
   kubectl logs deployment/ml-inference -n streaming-analytics
   # Check model loading and inference queue sizes
   ```

### Debug Commands

```bash
# Check pod status and events
kubectl get pods -n streaming-analytics -o wide
kubectl describe pod <pod-name> -n streaming-analytics

# View application logs
kubectl logs -f deployment/websocket-server -n streaming-analytics
kubectl logs -f deployment/stream-processor -n streaming-analytics  
kubectl logs -f deployment/ml-inference -n streaming-analytics

# Check resource usage
kubectl top pods -n streaming-analytics
kubectl top nodes

# Verify service endpoints
kubectl get endpoints -n streaming-analytics

# Test internal connectivity
kubectl run debug --image=busybox -it --rm --restart=Never -- sh
```

### Performance Tuning

1. **Adjust resource limits** based on actual usage
2. **Tune Kafka consumer** configurations for throughput
3. **Optimize ML model** cache sizes and refresh intervals
4. **Configure node affinity** for compute/memory-intensive workloads
5. **Use persistent volumes** for RocksDB state in stream processors

## Rollback Procedures

```bash
# Rollback to previous release
helm rollback streaming-analytics -n streaming-analytics

# Or rollback specific deployments
kubectl rollout undo deployment/websocket-server -n streaming-analytics
kubectl rollout undo deployment/stream-processor -n streaming-analytics
kubectl rollout undo deployment/ml-inference -n streaming-analytics

# Check rollout status
kubectl rollout status deployment/websocket-server -n streaming-analytics
```

## Maintenance

### Regular Tasks

1. **Monitor resource usage** and adjust limits
2. **Review and update** auto-scaling policies
3. **Rotate secrets** periodically
4. **Update container images** for security patches
5. **Review logs** for errors and performance issues

### Backup Considerations

- **Database backups**: Automated RDS backups
- **Kafka topics**: Topic-level backup/restore procedures
- **Configuration**: Git-based configuration management
- **Persistent volumes**: Snapshot EBS volumes for state

## Support and Contacts

- **Team**: Streaming Analytics Team
- **Email**: streaming-analytics@company.com
- **Slack**: #streaming-analytics
- **Documentation**: Internal wiki
- **Runbooks**: Incident response procedures