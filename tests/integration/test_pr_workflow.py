"""Integration tests simulating the complete PR benchmark workflow."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from .conftest import (
    GitRepoFixture,
    MockBenchmarkBinary,
    create_sample_metrics,
    write_metrics_file,
)


class TestPRWorkflowSimulation:
    """Simulate the PR benchmark workflow end-to-end."""

    def test_simulate_pr_workflow_comparison_phase(self, tmp_path):
        """Simulate the comparison phase of PR workflow.

        This tests the workflow from having benchmark results to generating
        the PR comment, without actually running benchmarks.
        """
        # Setup: Create directory structure like workflow produces
        pr_results = tmp_path / "results" / "pr"
        baseline_results = tmp_path / "results" / "baseline"

        # Create mock benchmark results for PR
        pr_metrics = [
            create_sample_metrics("pr_commit_abc", "GET", rps=110000.0, pipeline=1),
            create_sample_metrics("pr_commit_abc", "GET", rps=550000.0, pipeline=10),
            create_sample_metrics("pr_commit_abc", "SET", rps=95000.0, pipeline=1),
            create_sample_metrics("pr_commit_abc", "SET", rps=480000.0, pipeline=10),
        ]
        write_metrics_file(pr_results / "metrics.json", pr_metrics)

        # Create mock benchmark results for baseline
        baseline_metrics = [
            create_sample_metrics("baseline_xyz", "GET", rps=100000.0, pipeline=1),
            create_sample_metrics("baseline_xyz", "GET", rps=500000.0, pipeline=10),
            create_sample_metrics("baseline_xyz", "SET", rps=90000.0, pipeline=1),
            create_sample_metrics("baseline_xyz", "SET", rps=450000.0, pipeline=10),
        ]
        write_metrics_file(baseline_results / "metrics.json", baseline_metrics)

        # Run comparison (like workflow does)
        comparison_output = tmp_path / "comparison.md"
        result = subprocess.run(
            [
                sys.executable,
                "utils/compare_benchmark_results.py",
                "--baseline", str(baseline_results / "metrics.json"),
                "--new", str(pr_results / "metrics.json"),
                "--output", str(comparison_output),
                "--metrics", "rps",  # PR workflow uses RPS filter
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )

        assert result.returncode == 0, f"Comparison failed: {result.stderr}"
        assert comparison_output.exists()

        # Verify comparison content is suitable for PR comment
        content = comparison_output.read_text()

        # Should be valid markdown
        assert content.startswith("#")

        # Should show both commits
        assert "pr_commi" in content or "pr_commit" in content
        assert "baseline" in content

        # Should show improvements (PR has higher RPS)
        assert "+" in content  # Positive change indicator

        # Should have table structure
        assert "|" in content
        assert "GET" in content
        assert "SET" in content

    def test_simulate_pr_workflow_with_multiple_runs(self, tmp_path):
        """Simulate workflow with multiple benchmark runs for statistical analysis."""
        pr_results = tmp_path / "results" / "pr"
        baseline_results = tmp_path / "results" / "baseline"

        # Create multiple runs for PR (simulating --runs 3)
        pr_metrics = []
        for rps_variance in [0, 2000, -1500]:
            pr_metrics.append(
                create_sample_metrics("pr_abc", "GET", rps=110000.0 + rps_variance)
            )

        write_metrics_file(pr_results / "metrics.json", pr_metrics)

        # Create multiple runs for baseline
        baseline_metrics = []
        for rps_variance in [0, 1500, -1000]:
            baseline_metrics.append(
                create_sample_metrics("base_xyz", "GET", rps=100000.0 + rps_variance)
            )

        write_metrics_file(baseline_results / "metrics.json", baseline_metrics)

        # Run comparison
        comparison_output = tmp_path / "comparison.md"
        result = subprocess.run(
            [
                sys.executable,
                "utils/compare_benchmark_results.py",
                "--baseline", str(baseline_results / "metrics.json"),
                "--new", str(pr_results / "metrics.json"),
                "--output", str(comparison_output),
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )

        assert result.returncode == 0
        content = comparison_output.read_text()

        # Should show statistical info (n=3 runs)
        assert "n=3" in content
        # Should show standard deviation
        assert "σ=" in content or "stdev" in content.lower()

    def test_simulate_regression_detection(self, tmp_path):
        """Simulate detecting a performance regression in PR."""
        pr_results = tmp_path / "results" / "pr"
        baseline_results = tmp_path / "results" / "baseline"

        # PR has LOWER performance (regression)
        pr_metrics = [
            create_sample_metrics("pr_abc", "GET", rps=85000.0),  # 15% regression
        ]
        write_metrics_file(pr_results / "metrics.json", pr_metrics)

        baseline_metrics = [
            create_sample_metrics("base_xyz", "GET", rps=100000.0),
        ]
        write_metrics_file(baseline_results / "metrics.json", baseline_metrics)

        comparison_output = tmp_path / "comparison.md"
        result = subprocess.run(
            [
                sys.executable,
                "utils/compare_benchmark_results.py",
                "--baseline", str(baseline_results / "metrics.json"),
                "--new", str(pr_results / "metrics.json"),
                "--output", str(comparison_output),
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )

        assert result.returncode == 0
        content = comparison_output.read_text()

        # Should show negative change (regression)
        assert "-" in content  # Negative percentage
        # Should show approximately -15% change
        assert "15" in content or "14" in content or "16" in content


class TestWorkflowArtifacts:
    """Test workflow artifact generation."""

    def test_results_directory_structure(self, tmp_path):
        """Verify expected results directory structure."""
        # Simulate what benchmark.py creates
        results_dir = tmp_path / "results" / "abc123"
        results_dir.mkdir(parents=True)

        # Create expected files
        (results_dir / "logs.txt").write_text("benchmark logs here")
        (results_dir / "metrics.json").write_text("[]")

        # Verify structure
        assert (results_dir / "logs.txt").exists()
        assert (results_dir / "metrics.json").exists()

    def test_comparison_output_format_for_github(self, tmp_path):
        """Verify comparison output is GitHub-compatible markdown."""
        pr_metrics = [create_sample_metrics("pr", "GET", rps=100000.0)]
        baseline_metrics = [create_sample_metrics("base", "GET", rps=95000.0)]

        write_metrics_file(tmp_path / "pr.json", pr_metrics)
        write_metrics_file(tmp_path / "base.json", baseline_metrics)

        output = tmp_path / "comparison.md"
        subprocess.run(
            [
                sys.executable,
                "utils/compare_benchmark_results.py",
                "--baseline", str(tmp_path / "base.json"),
                "--new", str(tmp_path / "pr.json"),
                "--output", str(output),
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
            check=True,
        )

        content = output.read_text()

        # GitHub markdown requirements
        # 1. Tables must have header separator
        assert "| ---" in content or "|---" in content

        # 2. No raw HTML that might be stripped
        assert "<script" not in content.lower()
        assert "<style" not in content.lower()

        # 3. Reasonable length (GitHub has limits)
        assert len(content) < 65000  # GitHub comment limit


class TestGitIntegrationForPR:
    """Test git operations specific to PR workflow."""

    def test_checkout_pr_and_baseline_branches(self, git_repo: GitRepoFixture):
        """Simulate checking out PR and baseline branches."""
        # Create baseline branch (like 'unstable')
        baseline_sha = git_repo.create_commit(
            "Baseline version",
            files={"version.txt": "1.0.0"},
        )

        # Create PR branch with changes
        git_repo.create_branch("feature/improvement")
        pr_sha = git_repo.create_commit(
            "Performance improvement",
            files={"version.txt": "1.0.1", "optimization.c": "// faster code"},
        )

        # Simulate workflow: checkout baseline, run benchmarks
        git_repo.checkout(baseline_sha)
        assert git_repo.get_current_commit() == baseline_sha
        assert (git_repo.path / "version.txt").read_text() == "1.0.0"

        # Simulate workflow: checkout PR, run benchmarks
        git_repo.checkout(pr_sha)
        assert git_repo.get_current_commit() == pr_sha
        assert (git_repo.path / "version.txt").read_text() == "1.0.1"
        assert (git_repo.path / "optimization.c").exists()

    def test_get_merge_base(self, git_repo: GitRepoFixture):
        """Test finding merge base between PR and baseline."""
        # Create some history
        base_sha = git_repo.create_commit("Base")

        # Create diverging branches
        git_repo.create_branch("main-branch")
        main_sha = git_repo.create_commit("Main progress")

        git_repo.checkout(base_sha)
        git_repo.create_branch("pr-branch")
        pr_sha = git_repo.create_commit("PR changes")

        # Find merge base
        result = subprocess.run(
            ["git", "merge-base", main_sha, pr_sha],
            cwd=git_repo.path,
            capture_output=True,
            text=True,
        )

        merge_base = result.stdout.strip()
        assert merge_base == base_sha


class TestModuleBenchmarkWorkflow:
    """Test module-specific benchmark workflow."""

    def test_module_results_directory_structure(self, tmp_path):
        """Verify module benchmark results use correct directory."""
        # Module benchmarks use {module}_tests/ subdirectory
        module_results = tmp_path / "results" / "search_tests"
        module_results.mkdir(parents=True)

        metrics = [create_sample_metrics("abc123", "FT.SEARCH", rps=50000.0)]
        write_metrics_file(module_results / "metrics.json", metrics)

        assert (module_results / "metrics.json").exists()
        loaded = json.loads((module_results / "metrics.json").read_text())
        assert loaded[0]["command"] == "FT.SEARCH"

    def test_compare_module_results(self, tmp_path):
        """Test comparing module benchmark results."""
        pr_results = tmp_path / "pr" / "search_tests"
        baseline_results = tmp_path / "baseline" / "search_tests"

        pr_metrics = [
            create_sample_metrics("pr", "FT.SEARCH idx query", rps=55000.0),
            create_sample_metrics("pr", "FT.AGGREGATE idx query", rps=45000.0),
        ]
        write_metrics_file(pr_results / "metrics.json", pr_metrics)

        baseline_metrics = [
            create_sample_metrics("base", "FT.SEARCH idx query", rps=50000.0),
            create_sample_metrics("base", "FT.AGGREGATE idx query", rps=42000.0),
        ]
        write_metrics_file(baseline_results / "metrics.json", baseline_metrics)

        output = tmp_path / "comparison.md"
        result = subprocess.run(
            [
                sys.executable,
                "utils/compare_benchmark_results.py",
                "--baseline", str(baseline_results / "metrics.json"),
                "--new", str(pr_results / "metrics.json"),
                "--output", str(output),
                "--metrics", "rps",
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )

        assert result.returncode == 0
        content = output.read_text()
        assert "FT.SEARCH" in content or "SEARCH" in content
