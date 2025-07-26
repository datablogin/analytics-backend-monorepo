#!/usr/bin/env python3
"""Validation script to test the fixes implemented for PR review."""

import os
import sys

# Add libs to Python path
sys.path.insert(0, "libs")


def test_performance_testing_constants():
    """Test that magic numbers were replaced with named constants."""
    try:
        from streaming_analytics.performance_testing import (
            P95_QUANTILE_INDEX,
            P99_PERCENTILE,
            QUANTILES_COUNT,
        )

        # Verify the constants have expected values
        assert QUANTILES_COUNT == 20, (
            f"Expected QUANTILES_COUNT=20, got {QUANTILES_COUNT}"
        )
        assert P95_QUANTILE_INDEX == 18, (
            f"Expected P95_QUANTILE_INDEX=18, got {P95_QUANTILE_INDEX}"
        )
        assert P99_PERCENTILE == 0.99, (
            f"Expected P99_PERCENTILE=0.99, got {P99_PERCENTILE}"
        )

        print("✓ Performance testing constants validated")
        return True
    except Exception as e:
        print(f"✗ Performance testing constants validation failed: {e}")
        return False


def test_encryption_key_validation():
    """Test that encryption key validation works in production."""
    try:
        from streaming_analytics.config import KafkaConfig
        from streaming_analytics.kafka_manager import KafkaSecurityManager

        # Test that production environment requires explicit key
        config = KafkaConfig(
            bootstrap_servers=["localhost:9092"],
            enable_data_encryption=True,
            encryption_key=None,
        )

        # Mock production environment
        original_env = os.environ.get("ENVIRONMENT")
        os.environ["ENVIRONMENT"] = "production"

        try:
            KafkaSecurityManager(config)
            print("✗ Should have raised ValueError for missing production key")
            return False
        except ValueError as e:
            if "Encryption key is required in production environment" in str(e):
                print("✓ Production encryption key validation works")
                return True
            else:
                print(f"✗ Unexpected error: {e}")
                return False
        finally:
            # Restore original environment
            if original_env:
                os.environ["ENVIRONMENT"] = original_env
            else:
                os.environ.pop("ENVIRONMENT", None)

    except Exception as e:
        print(f"✗ Encryption key validation test failed: {e}")
        return False


def test_proactive_cleanup_logic():
    """Test that proactive window cleanup logic is implemented."""
    try:
        # Read the processor file to check for proactive cleanup logic
        with open("libs/streaming_analytics/processor.py") as f:
            content = f.read()

        # Check for proactive cleanup implementation
        if "proactive_limit = int(self._window_count_limit * 0.8)" in content:
            print("✓ Proactive window cleanup logic implemented")
            return True
        else:
            print("✗ Proactive window cleanup logic not found")
            return False

    except Exception as e:
        print(f"✗ Proactive cleanup validation failed: {e}")
        return False


def test_exception_handling_improvements():
    """Test that specific exception handling was implemented."""
    try:
        # Read the kafka_manager file to check for specific exception handling
        with open("libs/streaming_analytics/kafka_manager.py") as f:
            content = f.read()

        # Check for specific exception types
        if (
            "except (ValueError, UnicodeDecodeError)" in content
            and "except json.JSONDecodeError" in content
        ):
            print("✓ Specific exception handling implemented")
            return True
        else:
            print("✗ Specific exception handling not found")
            return False

    except Exception as e:
        print(f"✗ Exception handling validation failed: {e}")
        return False


def test_realtime_ml_refactoring():
    """Test that realtime ML methods were refactored."""
    try:
        # Read the realtime_ml file to check for method refactoring
        with open("libs/streaming_analytics/realtime_ml.py") as f:
            content = f.read()

        # Check for new helper methods
        helper_methods = [
            "_get_or_load_model",
            "_prepare_features",
            "_execute_inference",
            "_create_success_result",
            "_create_error_result",
        ]

        found_methods = sum(
            1 for method in helper_methods if f"def {method}(" in content
        )

        if found_methods >= 4:  # Should have most of the helper methods
            print(
                f"✓ Realtime ML refactoring implemented ({found_methods}/{len(helper_methods)} methods found)"
            )
            return True
        else:
            print(
                f"✗ Realtime ML refactoring incomplete ({found_methods}/{len(helper_methods)} methods found)"
            )
            return False

    except Exception as e:
        print(f"✗ Realtime ML refactoring validation failed: {e}")
        return False


def main():
    """Run all validation tests."""
    print("Validating Claude PR review fixes...\n")

    tests = [
        test_performance_testing_constants,
        test_encryption_key_validation,
        test_proactive_cleanup_logic,
        test_exception_handling_improvements,
        test_realtime_ml_refactoring,
    ]

    passed = 0
    total = len(tests)

    for test in tests:
        if test():
            passed += 1
        print()  # Add spacing between tests

    print(f"Validation Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All fixes validated successfully!")
        return 0
    else:
        print("❌ Some fixes need attention")
        return 1


if __name__ == "__main__":
    sys.exit(main())
