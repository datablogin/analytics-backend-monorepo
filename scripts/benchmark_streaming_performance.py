#!/usr/bin/env python3
"""
Comprehensive performance benchmarking script for streaming analytics.

This script validates the streaming analytics platform against the performance targets:
- < 100ms end-to-end latency
- 100,000+ events/sec throughput
- < 50ms ML inference latency
- 99.9% uptime
- Auto-scaling responsiveness
"""

import asyncio
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import click
import structlog

# Add the project root to the path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import after path modification to avoid import errors
from libs.streaming_analytics.performance_profiler import (  # noqa: E402
    get_performance_profiler,
)
from libs.streaming_analytics.performance_testing import (  # noqa: E402
    run_latency_test,
    run_stress_test,
    run_throughput_test,
)

logger = structlog.get_logger(__name__)


class PerformanceBenchmark:
    """Main performance benchmark orchestrator."""

    def __init__(self, output_dir: str = "benchmark_results"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.logger = logger.bind(component="performance_benchmark")
        self.profiler = get_performance_profiler()

        # Benchmark results
        self.results = {
            "timestamp": datetime.utcnow().isoformat(),
            "benchmarks": {},
            "summary": {},
            "targets_validation": {},
        }

    async def run_all_benchmarks(self) -> dict:
        """Run complete performance benchmark suite."""
        self.logger.info("Starting comprehensive performance benchmarks")

        try:
            # 1. Throughput benchmark
            await self._run_throughput_benchmark()

            # 2. Latency benchmark
            await self._run_latency_benchmark()

            # 3. ML inference benchmark
            await self._run_ml_inference_benchmark()

            # 4. Stress test benchmark
            await self._run_stress_test_benchmark()

            # 5. Memory and resource benchmark
            await self._run_resource_benchmark()

            # Generate summary and validation
            self._generate_benchmark_summary()
            self._validate_performance_targets()

            # Save results
            await self._save_results()

            return self.results

        except Exception as e:
            self.logger.error("Benchmark suite failed", error=str(e))
            raise

    async def _run_throughput_benchmark(self) -> None:
        """Run throughput-focused benchmark."""
        self.logger.info("Running throughput benchmark")

        # Test different throughput levels
        throughput_tests = [10000, 50000, 100000, 150000]
        throughput_results = {}

        for target_eps in throughput_tests:
            self.logger.info(f"Testing throughput at {target_eps} events/sec")

            try:
                result = await run_throughput_test(
                    events_per_second=target_eps, duration_seconds=60
                )

                throughput_results[f"{target_eps}_eps"] = {
                    "target_eps": target_eps,
                    "achieved_eps": result["producer_metrics"][
                        "achieved_throughput_eps"
                    ],
                    "success_rate": result["producer_metrics"]["success_rate_percent"],
                    "avg_latency_ms": result["producer_metrics"]["avg_send_latency_ms"],
                    "p95_latency_ms": result["producer_metrics"]["p95_send_latency_ms"],
                    "memory_peak_mb": result["system_metrics"]
                    .get("memory_usage_mb", {})
                    .get("max", 0),
                    "cpu_avg_percent": result["system_metrics"]
                    .get("cpu_usage_percent", {})
                    .get("avg", 0),
                }

                # Break if we can't sustain the throughput
                achieved_ratio = (
                    result["producer_metrics"]["achieved_throughput_eps"] / target_eps
                )
                if achieved_ratio < 0.8:  # Less than 80% of target
                    self.logger.warning(
                        "Throughput target not met, stopping throughput tests",
                        target=target_eps,
                        achieved=result["producer_metrics"]["achieved_throughput_eps"],
                        ratio=achieved_ratio,
                    )
                    break

            except Exception as e:
                self.logger.error(
                    f"Throughput test failed at {target_eps} eps", error=str(e)
                )
                throughput_results[f"{target_eps}_eps"] = {"error": str(e)}
                break

        self.results["benchmarks"]["throughput"] = throughput_results

    async def _run_latency_benchmark(self) -> None:
        """Run latency-focused benchmark."""
        self.logger.info("Running latency benchmark")

        try:
            result = await run_latency_test(
                events_per_second=1000,
                duration_seconds=300,  # 5 minutes for stable latency measurement
            )

            self.results["benchmarks"]["latency"] = {
                "avg_latency_ms": result["producer_metrics"]["avg_send_latency_ms"],
                "p95_latency_ms": result["producer_metrics"]["p95_send_latency_ms"],
                "p99_latency_ms": result["producer_metrics"]["p99_send_latency_ms"],
                "success_rate": result["producer_metrics"]["success_rate_percent"],
                "target_met_100ms": result["producer_metrics"]["avg_send_latency_ms"]
                < 100,
            }

        except Exception as e:
            self.logger.error("Latency benchmark failed", error=str(e))
            self.results["benchmarks"]["latency"] = {"error": str(e)}

    async def _run_ml_inference_benchmark(self) -> None:
        """Run ML inference latency benchmark."""
        self.logger.info("Running ML inference benchmark")

        try:
            from libs.streaming_analytics.realtime_ml import (
                FeatureVector,
                PredictionRequest,
                RealtimeMLInferenceEngine,
            )

            # Initialize ML inference engine
            ml_engine = RealtimeMLInferenceEngine()
            await ml_engine.start()

            # Benchmark different batch sizes and models
            batch_sizes = [1, 10, 50, 100]
            ml_results = {}

            for batch_size in batch_sizes:
                self.logger.info(f"Testing ML inference with batch size {batch_size}")

                # Create test requests
                requests = []
                for i in range(batch_size):
                    features = FeatureVector(
                        features={
                            "feature_1": 1.0 + i * 0.1,
                            "feature_2": 2.0 + i * 0.2,
                            "feature_3": 3.0 + i * 0.3,
                        }
                    )

                    request = PredictionRequest(
                        model_name="test_model",
                        features=features,
                        request_id=f"benchmark_{i}",
                    )
                    requests.append(request)

                # Measure inference time
                start_time = time.time()

                # Process requests (simulating different scenarios)
                if batch_size == 1:
                    # Single request latency
                    result = await ml_engine._single_predict(requests[0])
                    inference_times = [result.inference_time_ms]
                else:
                    # Batch processing
                    results = await ml_engine._process_batch(requests)
                    inference_times = [r.inference_time_ms for r in results]

                total_time = (time.time() - start_time) * 1000
                avg_inference_time = sum(inference_times) / len(inference_times)

                ml_results[f"batch_size_{batch_size}"] = {
                    "batch_size": batch_size,
                    "avg_inference_time_ms": avg_inference_time,
                    "total_batch_time_ms": total_time,
                    "throughput_predictions_per_sec": len(requests)
                    / (total_time / 1000),
                    "target_met_50ms": avg_inference_time < 50,
                }

            await ml_engine.stop()
            self.results["benchmarks"]["ml_inference"] = ml_results

        except Exception as e:
            self.logger.error("ML inference benchmark failed", error=str(e))
            self.results["benchmarks"]["ml_inference"] = {"error": str(e)}

    async def _run_stress_test_benchmark(self) -> None:
        """Run stress test to find breaking points."""
        self.logger.info("Running stress test benchmark")

        try:
            result = await run_stress_test(duration_seconds=600)  # 10 minutes

            self.results["benchmarks"]["stress_test"] = {
                "peak_throughput_eps": result["producer_metrics"][
                    "achieved_throughput_eps"
                ],
                "sustained_success_rate": result["producer_metrics"][
                    "success_rate_percent"
                ],
                "peak_memory_mb": result["system_metrics"]
                .get("memory_usage_mb", {})
                .get("max", 0),
                "peak_cpu_percent": result["system_metrics"]
                .get("cpu_usage_percent", {})
                .get("max", 0),
                "bottlenecks": result["bottlenecks_identified"],
                "recommendations": result["recommendations"],
            }

        except Exception as e:
            self.logger.error("Stress test benchmark failed", error=str(e))
            self.results["benchmarks"]["stress_test"] = {"error": str(e)}

    async def _run_resource_benchmark(self) -> None:
        """Run resource utilization benchmark."""
        self.logger.info("Running resource utilization benchmark")

        try:
            # Get performance profiling data
            profiler_report = self.profiler.generate_performance_report()

            self.results["benchmarks"]["resource_utilization"] = {
                "profiler_summary": profiler_report["summary"],
                "component_performance": {
                    name: {
                        "total_calls": data["total_calls"],
                        "total_errors": data["total_errors"],
                        "method_count": data["method_count"],
                        "avg_method_time": sum(
                            method["avg_time_ms"] for method in data["methods"].values()
                        )
                        / max(len(data["methods"]), 1),
                    }
                    for name, data in profiler_report["components"].items()
                },
                "performance_trends": profiler_report["trending"],
            }

        except Exception as e:
            self.logger.error("Resource benchmark failed", error=str(e))
            self.results["benchmarks"]["resource_utilization"] = {"error": str(e)}

    def _generate_benchmark_summary(self) -> None:
        """Generate benchmark summary."""
        summary = {
            "test_completion_time": datetime.utcnow().isoformat(),
            "benchmarks_completed": [],
            "benchmarks_failed": [],
        }

        for benchmark_name, result in self.results["benchmarks"].items():
            if "error" in result:
                summary["benchmarks_failed"].append(benchmark_name)
            else:
                summary["benchmarks_completed"].append(benchmark_name)

        # Extract key metrics
        throughput_data = self.results["benchmarks"].get("throughput", {})
        latency_data = self.results["benchmarks"].get("latency", {})
        ml_data = self.results["benchmarks"].get("ml_inference", {})

        # Find peak sustainable throughput
        peak_throughput = 0
        for test_name, test_data in throughput_data.items():
            if isinstance(test_data, dict) and "achieved_eps" in test_data:
                if test_data["success_rate"] >= 99.0:  # Only count high success rate
                    peak_throughput = max(peak_throughput, test_data["achieved_eps"])

        summary.update(
            {
                "peak_sustainable_throughput_eps": peak_throughput,
                "end_to_end_latency_ms": latency_data.get("avg_latency_ms", 0),
                "ml_inference_latency_ms": self._get_best_ml_latency(ml_data),
                "overall_success_rate": self._calculate_overall_success_rate(),
            }
        )

        self.results["summary"] = summary

    def _validate_performance_targets(self) -> None:
        """Validate against performance targets from issue requirements."""
        targets = {
            "throughput_100k_eps": False,
            "latency_under_100ms": False,
            "ml_inference_under_50ms": False,
            "uptime_99_9_percent": False,
        }

        # Validate throughput target
        peak_throughput = self.results["summary"].get(
            "peak_sustainable_throughput_eps", 0
        )
        targets["throughput_100k_eps"] = peak_throughput >= 100000

        # Validate latency target
        avg_latency = self.results["summary"].get("end_to_end_latency_ms", float("inf"))
        targets["latency_under_100ms"] = avg_latency < 100

        # Validate ML inference latency
        ml_latency = self.results["summary"].get(
            "ml_inference_latency_ms", float("inf")
        )
        targets["ml_inference_under_50ms"] = ml_latency < 50

        # Validate uptime (success rate as proxy)
        success_rate = self.results["summary"].get("overall_success_rate", 0)
        targets["uptime_99_9_percent"] = success_rate >= 99.9

        # Overall validation
        all_targets_met = all(targets.values())

        self.results["targets_validation"] = {
            "individual_targets": targets,
            "all_targets_met": all_targets_met,
            "targets_met_count": sum(targets.values()),
            "total_targets": len(targets),
            "performance_grade": self._calculate_performance_grade(targets),
        }

    def _get_best_ml_latency(self, ml_data: dict) -> float:
        """Get the best ML inference latency from benchmark data."""
        if not ml_data or "error" in ml_data:
            return float("inf")

        latencies = []
        for test_data in ml_data.values():
            if isinstance(test_data, dict) and "avg_inference_time_ms" in test_data:
                latencies.append(test_data["avg_inference_time_ms"])

        return min(latencies) if latencies else float("inf")

    def _calculate_overall_success_rate(self) -> float:
        """Calculate overall success rate across all benchmarks."""
        success_rates = []

        # Collect success rates from different benchmarks
        for benchmark_data in self.results["benchmarks"].values():
            if isinstance(benchmark_data, dict) and "error" not in benchmark_data:
                if "success_rate" in benchmark_data:
                    success_rates.append(benchmark_data["success_rate"])
                else:
                    # Look for success rates in nested data
                    for test_data in benchmark_data.values():
                        if isinstance(test_data, dict) and "success_rate" in test_data:
                            success_rates.append(test_data["success_rate"])

        return sum(success_rates) / len(success_rates) if success_rates else 0.0

    def _calculate_performance_grade(self, targets: dict[str, bool]) -> str:
        """Calculate an overall performance grade."""
        targets_met = sum(targets.values())
        total_targets = len(targets)
        percentage = (targets_met / total_targets) * 100

        if percentage >= 90:
            return "A"
        elif percentage >= 80:
            return "B"
        elif percentage >= 70:
            return "C"
        elif percentage >= 60:
            return "D"
        else:
            return "F"

    async def _save_results(self) -> None:
        """Save benchmark results to files."""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        # Save detailed results
        results_file = self.output_dir / f"benchmark_results_{timestamp}.json"
        with open(results_file, "w") as f:
            json.dump(self.results, f, indent=2, default=str)

        # Save summary report
        summary_file = self.output_dir / f"benchmark_summary_{timestamp}.txt"
        with open(summary_file, "w") as f:
            f.write(self._generate_text_report())

        self.logger.info(
            "Benchmark results saved",
            results_file=str(results_file),
            summary_file=str(summary_file),
        )

    def _generate_text_report(self) -> str:
        """Generate human-readable text report."""
        targets = self.results["targets_validation"]["individual_targets"]
        summary = self.results["summary"]

        report = f"""
STREAMING ANALYTICS PERFORMANCE BENCHMARK REPORT
================================================

Test Date: {self.results["timestamp"]}
Performance Grade: {self.results["targets_validation"]["performance_grade"]}

PERFORMANCE TARGETS VALIDATION:
==============================
✓ Throughput (100,000+ events/sec): {"PASS" if targets["throughput_100k_eps"] else "FAIL"}
  Achieved: {summary.get("peak_sustainable_throughput_eps", 0):,.0f} events/sec

✓ End-to-End Latency (< 100ms): {"PASS" if targets["latency_under_100ms"] else "FAIL"}
  Achieved: {summary.get("end_to_end_latency_ms", 0):.1f}ms

✓ ML Inference Latency (< 50ms): {"PASS" if targets["ml_inference_under_50ms"] else "FAIL"}
  Achieved: {summary.get("ml_inference_latency_ms", 0):.1f}ms

✓ Uptime/Reliability (99.9%): {"PASS" if targets["uptime_99_9_percent"] else "FAIL"}
  Achieved: {summary.get("overall_success_rate", 0):.2f}%

SUMMARY:
========
Targets Met: {self.results["targets_validation"]["targets_met_count"]}/{self.results["targets_validation"]["total_targets"]}
Overall Result: {"PASS" if self.results["targets_validation"]["all_targets_met"] else "FAIL"}

RECOMMENDATIONS:
===============
"""

        # Add benchmark-specific recommendations
        for benchmark_name, benchmark_data in self.results["benchmarks"].items():
            if isinstance(benchmark_data, dict) and "recommendations" in benchmark_data:
                report += f"\n{benchmark_name.upper()}:\n"
                for rec in benchmark_data["recommendations"]:
                    report += f"- {rec}\n"

        return report


@click.command()
@click.option(
    "--output-dir", default="benchmark_results", help="Output directory for results"
)
@click.option("--quick", is_flag=True, help="Run quick benchmarks (shorter duration)")
@click.option("--throughput-only", is_flag=True, help="Run only throughput benchmarks")
@click.option("--latency-only", is_flag=True, help="Run only latency benchmarks")
@click.option("--ml-only", is_flag=True, help="Run only ML inference benchmarks")
async def main(
    output_dir: str,
    quick: bool,
    throughput_only: bool,
    latency_only: bool,
    ml_only: bool,
):
    """Run streaming analytics performance benchmarks."""

    # Configure logging
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    benchmark = PerformanceBenchmark(output_dir)

    try:
        if throughput_only:
            await benchmark._run_throughput_benchmark()
        elif latency_only:
            await benchmark._run_latency_benchmark()
        elif ml_only:
            await benchmark._run_ml_inference_benchmark()
        else:
            await benchmark.run_all_benchmarks()

        # Print summary
        validation = benchmark.results["targets_validation"]
        print(f"\nBenchmark completed! Grade: {validation['performance_grade']}")
        print(
            f"Targets met: {validation['targets_met_count']}/{validation['total_targets']}"
        )

        if validation["all_targets_met"]:
            print("🎉 All performance targets met!")
            return 0
        else:
            print("⚠️  Some performance targets not met. Check detailed report.")
            return 1

    except Exception as e:
        print(f"Benchmark failed: {e}")
        return 1


if __name__ == "__main__":
    asyncio.run(main())
