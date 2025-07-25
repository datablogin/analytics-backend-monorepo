#!/bin/bash

# End-to-End Integration Tests Runner for Streaming Analytics
# This script sets up the test environment and runs comprehensive integration tests

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Starting Streaming Analytics Integration Tests${NC}"

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DOCKER_DIR="$PROJECT_ROOT/docker"
TEST_DIR="$PROJECT_ROOT/tests"

# Default values
RUN_PERFORMANCE_TESTS=${RUN_PERFORMANCE_TESTS:-false}
CLEANUP_AFTER=${CLEANUP_AFTER:-true}
VERBOSE=${VERBOSE:-false}
TEST_TIMEOUT=${TEST_TIMEOUT:-300}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --performance)
            RUN_PERFORMANCE_TESTS=true
            shift
            ;;
        --no-cleanup)
            CLEANUP_AFTER=false
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --timeout)
            TEST_TIMEOUT="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  --performance    Run performance tests (slower)"
            echo "  --no-cleanup     Don't cleanup containers after tests"
            echo "  --verbose        Enable verbose output"
            echo "  --timeout SEC    Test timeout in seconds (default: 300)"
            echo "  --help           Show this help message"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Function to check if docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        echo -e "${RED}❌ Docker is not running. Please start Docker and try again.${NC}"
        exit 1
    fi
}

# Function to cleanup containers
cleanup_containers() {
    if [ "$CLEANUP_AFTER" = true ]; then
        echo -e "${YELLOW}🧹 Cleaning up test containers...${NC}"
        cd "$DOCKER_DIR"
        docker-compose -f docker-compose.test.yml down -v --remove-orphans || true
        
        # Remove any dangling test containers
        docker ps -a --filter "name=streaming_test" --format "{{.ID}}" | xargs -r docker rm -f || true
        docker network ls --filter "name=streaming_test" --format "{{.ID}}" | xargs -r docker network rm || true
    fi
}

# Function to wait for services to be ready
wait_for_services() {
    echo -e "${YELLOW}⏳ Waiting for services to be ready...${NC}"
    
    # Wait for Kafka
    echo "Waiting for Kafka..."
    timeout 60 bash -c 'until docker exec $(docker-compose -f docker/docker-compose.test.yml ps -q kafka) kafka-topics --bootstrap-server localhost:9092 --list &>/dev/null; do sleep 1; done' || {
        echo -e "${RED}❌ Kafka failed to start${NC}"
        return 1
    }
    
    # Wait for Redis
    echo "Waiting for Redis..."
    timeout 30 bash -c 'until docker exec $(docker-compose -f docker/docker-compose.test.yml ps -q redis) redis-cli ping &>/dev/null; do sleep 1; done' || {
        echo -e "${RED}❌ Redis failed to start${NC}"
        return 1
    }
    
    # Wait for PostgreSQL
    echo "Waiting for PostgreSQL..."
    timeout 30 bash -c 'until docker exec $(docker-compose -f docker/docker-compose.test.yml ps -q postgres) pg_isready -U postgres &>/dev/null; do sleep 1; done' || {
        echo -e "${RED}❌ PostgreSQL failed to start${NC}"
        return 1
    }
    
    echo -e "${GREEN}✅ All services are ready${NC}"
}

# Function to run tests
run_tests() {
    echo -e "${GREEN}🧪 Running integration tests...${NC}"
    
    cd "$PROJECT_ROOT"
    
    # Set environment variables for integration tests
    export RUN_INTEGRATION_TESTS=1
    export GITHUB_ACTIONS=${GITHUB_ACTIONS:-false}
    export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
    export REDIS_URL=redis://localhost:6379
    export DATABASE_URL=postgresql://postgres:password@localhost:5433/analytics_test
    export MLFLOW_TRACKING_URI=http://localhost:5001
    
    # Pytest arguments
    PYTEST_ARGS=("-v" "--tb=short" "--durations=10")
    
    if [ "$VERBOSE" = true ]; then
        PYTEST_ARGS+=("-s" "--log-cli-level=INFO")
    fi
    
    if [ "$RUN_PERFORMANCE_TESTS" = true ]; then
        PYTEST_ARGS+=("-m" "integration")
        echo -e "${YELLOW}🏃 Including performance tests${NC}"
    else
        PYTEST_ARGS+=("-m" "integration and not performance")
        echo -e "${YELLOW}⚡ Skipping performance tests (use --performance to include)${NC}"
    fi
    
    # Add timeout
    PYTEST_ARGS+=("--timeout=$TEST_TIMEOUT")
    
    # Run specific test files
    TEST_FILES=(
        "tests/integration/test_streaming_e2e.py"
        "tests/integration/test_kafka_integration.py"
    )
    
    echo -e "${GREEN}Running tests with timeout: ${TEST_TIMEOUT}s${NC}"
    
    # Run tests with timeout and proper error handling
    if timeout "${TEST_TIMEOUT}" python -m pytest "${PYTEST_ARGS[@]}" "${TEST_FILES[@]}"; then
        echo -e "${GREEN}✅ All integration tests passed!${NC}"
        return 0
    else
        TEST_EXIT_CODE=$?
        if [ $TEST_EXIT_CODE -eq 124 ]; then
            echo -e "${RED}❌ Tests timed out after ${TEST_TIMEOUT} seconds${NC}"
        else
            echo -e "${RED}❌ Integration tests failed with exit code: $TEST_EXIT_CODE${NC}"
        fi
        return $TEST_EXIT_CODE
    fi
}

# Function to generate test report
generate_report() {
    echo -e "${GREEN}📊 Generating test report...${NC}"
    
    # Create reports directory
    mkdir -p "$PROJECT_ROOT/reports"
    
    # Run tests with coverage and HTML report
    cd "$PROJECT_ROOT"
    export RUN_INTEGRATION_TESTS=1
    
    python -m pytest \
        tests/integration/ \
        --cov=libs/streaming_analytics \
        --cov-report=html:reports/coverage_integration \
        --cov-report=term-missing \
        --html=reports/integration_test_report.html \
        --self-contained-html \
        -v || true
    
    echo -e "${GREEN}📄 Reports generated in: reports/${NC}"
    echo -e "  - Coverage: reports/coverage_integration/index.html"
    echo -e "  - Test Report: reports/integration_test_report.html"
}

# Main execution
main() {
    echo -e "${GREEN}🔧 Configuration:${NC}"
    echo -e "  Performance Tests: $RUN_PERFORMANCE_TESTS"
    echo -e "  Cleanup After: $CLEANUP_AFTER"
    echo -e "  Verbose: $VERBOSE"
    echo -e "  Timeout: ${TEST_TIMEOUT}s"
    echo ""
    
    # Check prerequisites
    check_docker
    
    # Trap to ensure cleanup happens
    trap cleanup_containers EXIT
    
    # Start test environment
    echo -e "${YELLOW}🐳 Starting test environment...${NC}"
    cd "$DOCKER_DIR"
    
    # Stop any existing containers
    docker-compose -f docker-compose.test.yml down -v --remove-orphans || true
    
    # Start test infrastructure
    if ! docker-compose -f docker-compose.test.yml up -d; then
        echo -e "${RED}❌ Failed to start test environment${NC}"
        exit 1
    fi
    
    # Wait for services to be ready
    if ! wait_for_services; then
        echo -e "${RED}❌ Services failed to start properly${NC}"
        exit 1
    fi
    
    # Run the tests
    if run_tests; then
        echo -e "${GREEN}🎉 Integration tests completed successfully!${NC}"
        
        # Generate report if requested
        if [ "$VERBOSE" = true ]; then
            generate_report
        fi
        
        exit 0
    else
        echo -e "${RED}💥 Integration tests failed${NC}"
        exit 1
    fi
}

# Run main function
main "$@"