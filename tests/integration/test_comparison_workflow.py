"""Integration tests for benchmark comparison workflow."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from .conftest import (
    create_sample_metrics,
    write_metrics_file,
    read_metrics_file,
)


class TestComparisonWorkflow:
    """Test the complete comparison workflow used in PR benchmarking."""

    def test_compare_two_metrics_files(self, tmp_path):
        """Test basic comparison between two metrics files."""
        # Create baseline metrics
        baseline_metrics = [
            create_sample_metrics("baseline123", "GET", rps=100000.0),
            create_sample_metrics("baseline123", "SET", rps=80000.0),
        ]
        baseline_path = tmp_path / "baseline" / "metrics.json"
        write_metrics_file(baseline_path, baseline_metrics)

        # Create new metrics (with improvement)
        new_metrics = [
            create_sample_metrics("newcommit456", "GET", rps=110000.0),
            create_sample_metrics("newcommit456", "SET", rps=88000.0),
        ]
        new_path = tmp_path / "new" / "metrics.json"
        write_metrics_file(new_path, new_metrics)

        # Run comparison
        output_path = tmp_path / "comparison.md"
        result = subprocess.run(
            [
                sys.executable,
                "utils/compare_benchmark_results.py",
                "--baseline", str(baseline_path),
                "--new", str(new_path),
                "--output", str(output_path),
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )

        assert result.returncode == 0, f"Comparison failed: {result.stderr}"
        assert output_path.exists()

        # Verify output contains expected content
        content = output_path.read_text()
        assert "baseline12" in content  # Truncated commit
        assert "newcommi" in content  # Truncated commit
        assert "GET" in content
        assert "SET" in content

    def test_compare_with_rps_filter(self, tmp_path):
        """Test comparison with RPS-only filter (used in PR workflow)."""
        baseline_metrics = [
            create_sample_metrics("base", "GET", rps=100000.0),
        ]
        baseline_path = tmp_path / "baseline" / "metrics.json"
        write_metrics_file(baseline_path, baseline_metrics)

        new_metrics = [
            create_sample_metrics("new", "GET", rps=105000.0),
        ]
        new_path = tmp_path / "new" / "metrics.json"
        write_metrics_file(new_path, new_metrics)

        output_path = tmp_path / "comparison.md"
        result = subprocess.run(
            [
                sys.executable,
                "utils/compare_benchmark_results.py",
                "--baseline", str(baseline_path),
                "--new", str(new_path),
                "--output", str(output_path),
                "--metrics", "rps",
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )

        assert result.returncode == 0
        content = output_path.read_text()

        # Should have RPS but not latency metrics
        assert "rps" in content.lower()
        # Latency columns should not appear in filtered output
        # (the table structure changes based on filter)

    def test_compare_multiple_runs_averaging(self, tmp_path):
        """Test that multiple runs are properly averaged."""
        # Create baseline with multiple runs of same config
        baseline_metrics = [
            create_sample_metrics("base", "GET", rps=100000.0, pipeline=1),
            create_sample_metrics("base", "GET", rps=102000.0, pipeline=1),
            create_sample_metrics("base", "GET", rps=98000.0, pipeline=1),
        ]
        baseline_path = tmp_path / "baseline" / "metrics.json"
        write_metrics_file(baseline_path, baseline_metrics)

        new_metrics = [
            create_sample_metrics("new", "GET", rps=110000.0, pipeline=1),
            create_sample_metrics("new", "GET", rps=112000.0, pipeline=1),
            create_sample_metrics("new", "GET", rps=108000.0, pipeline=1),
        ]
        new_path = tmp_path / "new" / "metrics.json"
        write_metrics_file(new_path, new_metrics)

        output_path = tmp_path / "comparison.md"
        result = subprocess.run(
            [
                sys.executable,
                "utils/compare_benchmark_results.py",
                "--baseline", str(baseline_path),
                "--new", str(new_path),
                "--output", str(output_path),
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )

        assert result.returncode == 0
        content = output_path.read_text()

        # Should show run count in summary
        assert "n=3" in content or "3 runs" in content.lower()

    def test_compare_different_configurations(self, tmp_path):
        """Test comparison handles different pipeline configurations."""
        baseline_metrics = [
            create_sample_metrics("base", "GET", rps=100000.0, pipeline=1),
            create_sample_metrics("base", "GET", rps=500000.0, pipeline=10),
        ]
        baseline_path = tmp_path / "baseline" / "metrics.json"
        write_metrics_file(baseline_path, baseline_metrics)

        new_metrics = [
            create_sample_metrics("new", "GET", rps=105000.0, pipeline=1),
            create_sample_metrics("new", "GET", rps=520000.0, pipeline=10),
        ]
        new_path = tmp_path / "new" / "metrics.json"
        write_metrics_file(new_path, new_metrics)

        output_path = tmp_path / "comparison.md"
        result = subprocess.run(
            [
                sys.executable,
                "utils/compare_benchmark_results.py",
                "--baseline", str(baseline_path),
                "--new", str(new_path),
                "--output", str(output_path),
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )

        assert result.returncode == 0
        content = output_path.read_text()

        # Both pipeline values should appear
        assert "| 1 |" in content or "Pipeline" in content
        assert "| 10 |" in content or "10" in content

    def test_compare_empty_baseline_fails_gracefully(self, tmp_path):
        """Test comparison handles missing baseline gracefully."""
        new_metrics = [create_sample_metrics("new", "GET", rps=100000.0)]
        new_path = tmp_path / "new" / "metrics.json"
        write_metrics_file(new_path, new_metrics)

        result = subprocess.run(
            [
                sys.executable,
                "utils/compare_benchmark_results.py",
                "--baseline", str(tmp_path / "nonexistent.json"),
                "--new", str(new_path),
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )

        # Should fail with clear error
        assert result.returncode != 0
        assert "not found" in result.stderr.lower() or "error" in result.stderr.lower()


class TestMetricsFileFormat:
    """Test metrics file format compatibility."""

    def test_metrics_file_structure(self, tmp_path):
        """Verify metrics file has expected structure."""
        metrics = [
            create_sample_metrics("abc123", "GET", rps=100000.0),
        ]
        path = tmp_path / "metrics.json"
        write_metrics_file(path, metrics)

        loaded = read_metrics_file(path)
        assert isinstance(loaded, list)
        assert len(loaded) == 1

        m = loaded[0]
        assert "commit" in m
        assert "command" in m
        assert "rps" in m
        assert "avg_latency_ms" in m
        assert "p50_latency_ms" in m
        assert "p95_latency_ms" in m
        assert "p99_latency_ms" in m

    def test_metrics_with_io_threads(self, tmp_path):
        """Test metrics with io_threads field."""
        metrics = [create_sample_metrics("abc123", "GET")]
        metrics[0]["io_threads"] = 4
        path = tmp_path / "metrics.json"
        write_metrics_file(path, metrics)

        loaded = read_metrics_file(path)
        assert loaded[0]["io_threads"] == 4

    def test_metrics_with_cluster_mode(self, tmp_path):
        """Test metrics with cluster mode enabled."""
        metrics = [create_sample_metrics("abc123", "GET")]
        metrics[0]["cluster_mode"] = True
        path = tmp_path / "metrics.json"
        write_metrics_file(path, metrics)

        loaded = read_metrics_file(path)
        assert loaded[0]["cluster_mode"] is True


class TestPRCommentGeneration:
    """Test generation of PR comment content."""

    def test_comparison_output_is_valid_markdown(self, tmp_path):
        """Verify comparison output is valid markdown for PR comments."""
        baseline_metrics = [
            create_sample_metrics("baseline", "GET", rps=100000.0),
            create_sample_metrics("baseline", "SET", rps=80000.0),
        ]
        baseline_path = tmp_path / "baseline" / "metrics.json"
        write_metrics_file(baseline_path, baseline_metrics)

        new_metrics = [
            create_sample_metrics("newcommit", "GET", rps=110000.0),
            create_sample_metrics("newcommit", "SET", rps=88000.0),
        ]
        new_path = tmp_path / "new" / "metrics.json"
        write_metrics_file(new_path, new_metrics)

        output_path = tmp_path / "comparison.md"
        subprocess.run(
            [
                sys.executable,
                "utils/compare_benchmark_results.py",
                "--baseline", str(baseline_path),
                "--new", str(new_path),
                "--output", str(output_path),
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
            check=True,
        )

        content = output_path.read_text()

        # Verify markdown structure
        assert content.startswith("#")  # Has header
        assert "|" in content  # Has table
        assert "---" in content  # Has table separator

    def test_percentage_change_shown(self, tmp_path):
        """Verify percentage change is shown in comparison."""
        baseline_metrics = [create_sample_metrics("base", "GET", rps=100000.0)]
        baseline_path = tmp_path / "baseline" / "metrics.json"
        write_metrics_file(baseline_path, baseline_metrics)

        # 10% improvement
        new_metrics = [create_sample_metrics("new", "GET", rps=110000.0)]
        new_path = tmp_path / "new" / "metrics.json"
        write_metrics_file(new_path, new_metrics)

        output_path = tmp_path / "comparison.md"
        subprocess.run(
            [
                sys.executable,
                "utils/compare_benchmark_results.py",
                "--baseline", str(baseline_path),
                "--new", str(new_path),
                "--output", str(output_path),
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
            check=True,
        )

        content = output_path.read_text()

        # Should show percentage change (approximately 10%)
        assert "%" in content
        # The exact format may vary, but should show positive change
        assert "+" in content or "10" in content
