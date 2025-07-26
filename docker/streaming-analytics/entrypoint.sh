#!/bin/bash
set -e

# Default service type if not specified
SERVICE_TYPE=${SERVICE_TYPE:-websocket}

# Log startup information
echo "Starting Streaming Analytics service: $SERVICE_TYPE"
echo "Environment: ${STREAMING_ENVIRONMENT:-development}"

# Start the appropriate service based on SERVICE_TYPE
case "$SERVICE_TYPE" in
    "websocket"|"websocket-server")
        echo "Starting WebSocket server..."
        exec python -m libs.streaming_analytics.websocket_server "$@"
        ;;
    "processor"|"stream-processor")
        echo "Starting stream processor..."
        exec python -m libs.streaming_analytics.processor "$@"
        ;;
    "ml-inference"|"ml_inference")
        echo "Starting ML inference service..."
        exec python -m libs.streaming_analytics.realtime_ml "$@"
        ;;
    "metrics"|"metrics-server")
        echo "Starting metrics server..."
        exec python -m libs.streaming_analytics.metrics "$@"
        ;;
    *)
        echo "ERROR: Unknown service type: $SERVICE_TYPE"
        echo "Available service types: websocket, processor, ml-inference, metrics"
        exit 1
        ;;
esac