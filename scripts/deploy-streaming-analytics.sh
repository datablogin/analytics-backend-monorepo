#!/bin/bash

# Production deployment script for Streaming Analytics
# This script deploys the streaming analytics platform to Kubernetes

set -euo pipefail

# Configuration
NAMESPACE="${NAMESPACE:-streaming-analytics}"
RELEASE_NAME="${RELEASE_NAME:-streaming-analytics}"
CHART_PATH="${CHART_PATH:-helm/streaming-analytics}"
VALUES_FILE="${VALUES_FILE:-helm/streaming-analytics/values.yaml}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
REGISTRY="${REGISTRY:-your-registry.com}"
ENVIRONMENT="${ENVIRONMENT:-production}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if kubectl is available and configured
    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl is not installed or not in PATH"
        exit 1
    fi
    
    # Check if helm is available
    if ! command -v helm &> /dev/null; then
        log_error "helm is not installed or not in PATH"
        exit 1
    fi
    
    # Check if we can connect to the cluster
    if ! kubectl cluster-info &> /dev/null; then
        log_error "Cannot connect to Kubernetes cluster"
        exit 1
    fi
    
    # Check if the chart path exists
    if [ ! -d "$CHART_PATH" ]; then
        log_error "Chart path $CHART_PATH does not exist"
        exit 1
    fi
    
    # Check if values file exists
    if [ ! -f "$VALUES_FILE" ]; then
        log_error "Values file $VALUES_FILE does not exist"
        exit 1
    fi
    
    log_success "Prerequisites check passed"
}

# Function to create namespace
create_namespace() {
    log_info "Creating namespace $NAMESPACE..."
    
    if kubectl get namespace "$NAMESPACE" &> /dev/null; then
        log_warning "Namespace $NAMESPACE already exists"
    else
        kubectl create namespace "$NAMESPACE"
        kubectl label namespace "$NAMESPACE" name="$NAMESPACE" environment="$ENVIRONMENT"
        log_success "Namespace $NAMESPACE created"
    fi
}

# Function to build and push Docker images
build_and_push_images() {
    log_info "Building and pushing Docker images..."
    
    local image_name="${REGISTRY}/streaming-analytics:${IMAGE_TAG}"
    
    # Build the image
    log_info "Building image $image_name..."
    docker build -f docker/streaming-analytics/Dockerfile -t "$image_name" .
    
    # Tag as latest if not already
    if [ "$IMAGE_TAG" != "latest" ]; then
        docker tag "$image_name" "${REGISTRY}/streaming-analytics:latest"
    fi
    
    # Push images
    log_info "Pushing images to registry..."
    docker push "$image_name"
    if [ "$IMAGE_TAG" != "latest" ]; then
        docker push "${REGISTRY}/streaming-analytics:latest"
    fi
    
    log_success "Images built and pushed successfully"
}

# Function to validate Helm chart
validate_chart() {
    log_info "Validating Helm chart..."
    
    # Lint the chart
    if helm lint "$CHART_PATH"; then
        log_success "Chart validation passed"
    else
        log_error "Chart validation failed"
        exit 1
    fi
    
    # Template the chart to check for issues
    if helm template "$RELEASE_NAME" "$CHART_PATH" --values "$VALUES_FILE" > /dev/null; then
        log_success "Chart templating successful"
    else
        log_error "Chart templating failed"
        exit 1
    fi
}

# Function to deploy secrets
deploy_secrets() {
    log_info "Deploying secrets..."
    
    # Check if secrets already exist
    if kubectl get secret streaming-secrets -n "$NAMESPACE" &> /dev/null; then
        log_warning "Secrets already exist, skipping creation"
    else
        log_info "Creating secrets from environment variables..."
        
        # Create secrets from environment variables
        kubectl create secret generic streaming-secrets \
            --from-literal=STREAMING_KAFKA__SASL_USERNAME="${KAFKA_USERNAME:-}" \
            --from-literal=STREAMING_KAFKA__SASL_PASSWORD="${KAFKA_PASSWORD:-}" \
            --from-literal=DATABASE_USER="${DATABASE_USER:-}" \
            --from-literal=DATABASE_PASSWORD="${DATABASE_PASSWORD:-}" \
            --from-literal=REDIS_PASSWORD="${REDIS_PASSWORD:-}" \
            --from-literal=STREAMING_MLFLOW_TRACKING_TOKEN="${MLFLOW_TOKEN:-}" \
            --from-literal=JWT_SECRET_KEY="${JWT_SECRET_KEY:-}" \
            --namespace "$NAMESPACE"
        
        log_success "Secrets created"
    fi
    
    # Create TLS secret if certificates are provided
    if [ -n "${TLS_CERT_PATH:-}" ] && [ -n "${TLS_KEY_PATH:-}" ]; then
        log_info "Creating TLS secret..."
        kubectl create secret tls streaming-tls-secret \
            --cert="$TLS_CERT_PATH" \
            --key="$TLS_KEY_PATH" \
            --namespace "$NAMESPACE" \
            --dry-run=client -o yaml | kubectl apply -f -
        log_success "TLS secret created"
    fi
}

# Function to deploy with Helm
deploy_helm() {
    log_info "Deploying with Helm..."
    
    # Prepare Helm values
    local helm_args=(
        "upgrade" "--install" "$RELEASE_NAME" "$CHART_PATH"
        "--namespace" "$NAMESPACE"
        "--values" "$VALUES_FILE"
        "--set" "image.registry=$REGISTRY"
        "--set" "image.tag=$IMAGE_TAG"
        "--set" "config.environment=$ENVIRONMENT"
        "--wait"
        "--timeout" "10m"
        "--create-namespace"
    )
    
    # Add additional values if provided
    if [ -n "${EXTRA_VALUES:-}" ]; then
        helm_args+=("--values" "$EXTRA_VALUES")
    fi
    
    # Deploy
    if helm "${helm_args[@]}"; then
        log_success "Helm deployment successful"
    else
        log_error "Helm deployment failed"
        exit 1
    fi
}

# Function to verify deployment
verify_deployment() {
    log_info "Verifying deployment..."
    
    # Check if deployments are ready
    local deployments=("websocket-server" "stream-processor" "ml-inference")
    
    for deployment in "${deployments[@]}"; do
        log_info "Checking deployment: $deployment"
        if kubectl rollout status deployment/"$deployment" -n "$NAMESPACE" --timeout=600s; then
            log_success "Deployment $deployment is ready"
        else
            log_error "Deployment $deployment failed to become ready"
            exit 1
        fi
    done
    
    # Check if services are available
    log_info "Checking services..."
    kubectl get services -n "$NAMESPACE"
    
    # Check if pods are running
    log_info "Checking pods..."
    kubectl get pods -n "$NAMESPACE"
    
    log_success "Deployment verification completed"
}

# Function to run smoke tests
run_smoke_tests() {
    log_info "Running smoke tests..."
    
    # Create a test pod to run smoke tests
    local test_image="${REGISTRY}/streaming-analytics:${IMAGE_TAG}"
    
    kubectl run smoke-test \
        --image="$test_image" \
        --rm -i --restart=Never \
        --namespace="$NAMESPACE" \
        --command -- python -c "
import requests
import sys
import time

def test_websocket_service():
    try:
        # Test health endpoint
        response = requests.get('http://websocket-service:8080/health', timeout=5)
        return response.status_code == 200
    except:
        return False

def test_ml_inference_service():
    try:
        response = requests.get('http://ml-inference-service:8080/health', timeout=5)
        return response.status_code == 200
    except:
        return False

# Wait a bit for services to be ready
time.sleep(10)

tests = [
    ('WebSocket Service', test_websocket_service),
    ('ML Inference Service', test_ml_inference_service),
]

failed_tests = []
for name, test_func in tests:
    print(f'Testing {name}...', end=' ')
    if test_func():
        print('✓ PASSED')
    else:
        print('✗ FAILED')
        failed_tests.append(name)

if failed_tests:
    print(f'Failed tests: {failed_tests}')
    sys.exit(1)
else:
    print('All smoke tests passed!')
    sys.exit(0)
"
    
    if [ $? -eq 0 ]; then
        log_success "Smoke tests passed"
    else
        log_error "Smoke tests failed"
        exit 1
    fi
}

# Function to display deployment information
display_info() {
    log_info "Deployment Information:"
    echo "=========================="
    echo "Namespace: $NAMESPACE"
    echo "Release: $RELEASE_NAME"
    echo "Image Tag: $IMAGE_TAG"
    echo "Environment: $ENVIRONMENT"
    echo ""
    
    log_info "Service endpoints:"
    kubectl get services -n "$NAMESPACE" -o wide
    
    echo ""
    log_info "To view logs:"
    echo "kubectl logs -f deployment/websocket-server -n $NAMESPACE"
    echo "kubectl logs -f deployment/stream-processor -n $NAMESPACE"
    echo "kubectl logs -f deployment/ml-inference -n $NAMESPACE"
    
    echo ""
    log_info "To scale deployments:"
    echo "kubectl scale deployment websocket-server --replicas=5 -n $NAMESPACE"
    echo "kubectl scale deployment stream-processor --replicas=10 -n $NAMESPACE"
    echo "kubectl scale deployment ml-inference --replicas=6 -n $NAMESPACE"
}

# Function for cleanup
cleanup() {
    if [ "${CLEANUP_ON_ERROR:-false}" = "true" ]; then
        log_warning "Cleaning up due to error..."
        helm uninstall "$RELEASE_NAME" -n "$NAMESPACE" || true
    fi
}

# Main execution
main() {
    log_info "Starting Streaming Analytics deployment..."
    log_info "Environment: $ENVIRONMENT"
    log_info "Namespace: $NAMESPACE"
    log_info "Image Tag: $IMAGE_TAG"
    
    # Set up error handling
    trap cleanup ERR
    
    # Execute deployment steps
    check_prerequisites
    create_namespace
    
    # Build and push images if requested
    if [ "${BUILD_IMAGES:-false}" = "true" ]; then
        build_and_push_images
    fi
    
    validate_chart
    deploy_secrets
    deploy_helm
    verify_deployment
    
    # Run smoke tests if requested
    if [ "${RUN_SMOKE_TESTS:-true}" = "true" ]; then
        run_smoke_tests
    fi
    
    display_info
    
    log_success "🎉 Streaming Analytics deployment completed successfully!"
}

# Script usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help              Show this help message"
    echo "  -n, --namespace NAME    Kubernetes namespace (default: streaming-analytics)"
    echo "  -r, --release NAME      Helm release name (default: streaming-analytics)"
    echo "  -t, --tag TAG           Docker image tag (default: latest)"
    echo "  -e, --environment ENV   Environment (default: production)"
    echo "  --build-images          Build and push Docker images"
    echo "  --skip-smoke-tests      Skip smoke tests"
    echo "  --cleanup-on-error      Cleanup on deployment error"
    echo ""
    echo "Environment variables:"
    echo "  KAFKA_USERNAME          Kafka SASL username"
    echo "  KAFKA_PASSWORD          Kafka SASL password"
    echo "  DATABASE_USER           Database username"
    echo "  DATABASE_PASSWORD       Database password"
    echo "  REDIS_PASSWORD          Redis password"
    echo "  MLFLOW_TOKEN           MLflow tracking token"
    echo "  JWT_SECRET_KEY         JWT secret key"
    echo "  TLS_CERT_PATH          Path to TLS certificate"
    echo "  TLS_KEY_PATH           Path to TLS private key"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Deploy with defaults"
    echo "  $0 --build-images -t v1.2.3          # Build and deploy specific version"
    echo "  $0 -n staging -e staging             # Deploy to staging environment"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            exit 0
            ;;
        -n|--namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        -r|--release)
            RELEASE_NAME="$2"
            shift 2
            ;;
        -t|--tag)
            IMAGE_TAG="$2"
            shift 2
            ;;
        -e|--environment)
            ENVIRONMENT="$2"
            shift 2
            ;;
        --build-images)
            BUILD_IMAGES="true"
            shift
            ;;
        --skip-smoke-tests)
            RUN_SMOKE_TESTS="false"
            shift
            ;;
        --cleanup-on-error)
            CLEANUP_ON_ERROR="true"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Run main function
main