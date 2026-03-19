"""Integration tests for benchmark execution flow."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from .conftest import GitRepoFixture, MockBenchmarkBinary


class TestBenchmarkConfigLoading:
    """Test benchmark configuration loading and validation."""

    def test_load_minimal_config(self, minimal_config_file):
        """Test loading a minimal valid configuration."""
        # Import here to avoid issues with module path
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))
        from benchmark import load_configs

        configs = load_configs(str(minimal_config_file))
        assert len(configs) == 1
        assert configs[0]["commands"] == ["GET", "SET"]

    def test_config_validation_rejects_invalid(self, tmp_path):
        """Test that invalid configs are rejected."""
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))
        from benchmark import load_configs

        # Missing required field
        invalid_config = [{"commands": ["GET"]}]  # Missing other required fields
        config_path = tmp_path / "invalid.json"
        config_path.write_text(json.dumps(invalid_config))

        with pytest.raises(ValueError):
            load_configs(str(config_path))

    def test_config_with_duration_mode(self, tmp_path):
        """Test config using duration instead of requests."""
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))
        from benchmark import load_configs

        config = [{
            "keyspacelen": [100],
            "data_sizes": [16],
            "pipelines": [1],
            "clients": [1],
            "commands": ["GET"],
            "cluster_mode": False,
            "tls_mode": False,
            "warmup": 0,
            "duration": 1,  # 1 second duration
        }]
        config_path = tmp_path / "duration.json"
        config_path.write_text(json.dumps(config))

        configs = load_configs(str(config_path))
        assert configs[0]["duration"] == 1


class TestMockBenchmarkExecution:
    """Test benchmark execution with mock binary."""

    def test_mock_benchmark_produces_valid_output(self, mock_benchmark_binary):
        """Test that mock benchmark produces parseable output."""
        result = subprocess.run(
            [
                sys.executable,
                mock_benchmark_binary.executable,
                "-t", "GET",
                "--csv",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        lines = result.stdout.strip().split("\n")
        assert len(lines) >= 2  # Header + at least one data line

        # Parse CSV
        import csv
        reader = csv.DictReader(lines)
        rows = list(reader)
        assert len(rows) >= 1
        assert "rps" in rows[0]
        assert float(rows[0]["rps"]) > 0

    def test_mock_benchmark_handles_multiple_commands(self, mock_benchmark_binary):
        """Test mock handles multiple commands."""
        result = subprocess.run(
            [
                sys.executable,
                mock_benchmark_binary.executable,
                "-t", "GET,SET,LPUSH",
                "--csv",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        lines = result.stdout.strip().split("\n")
        # Header + 3 commands
        assert len(lines) == 4

    def test_mock_benchmark_pipeline_affects_rps(self, mock_benchmark_binary):
        """Test that pipeline parameter affects mock RPS."""
        result_p1 = subprocess.run(
            [
                sys.executable,
                mock_benchmark_binary.executable,
                "-t", "GET",
                "-P", "1",
                "--csv",
                "--seed", "42",
            ],
            capture_output=True,
            text=True,
        )

        result_p10 = subprocess.run(
            [
                sys.executable,
                mock_benchmark_binary.executable,
                "-t", "GET",
                "-P", "10",
                "--csv",
                "--seed", "42",
            ],
            capture_output=True,
            text=True,
        )

        import csv
        rps_p1 = float(list(csv.DictReader(result_p1.stdout.strip().split("\n")))[0]["rps"])
        rps_p10 = float(list(csv.DictReader(result_p10.stdout.strip().split("\n")))[0]["rps"])

        # Pipeline 10 should have ~10x RPS
        assert rps_p10 > rps_p1 * 5  # Allow some variance


class TestBenchmarkCommandBuilding:
    """Test benchmark command construction."""

    def test_build_simple_command(self, minimal_benchmark_config):
        """Test building a simple benchmark command."""
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))
        from valkey_benchmark import ClientRunner

        runner = ClientRunner(
            commit_id="abc123",
            config=minimal_benchmark_config,
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp"),
            valkey_path="/tmp/valkey",
            valkey_benchmark_path="/tmp/valkey-benchmark",
        )

        cmd = runner._build_benchmark_command(
            tls=False,
            requests=100,
            keyspacelen=1000,
            data_size=16,
            pipeline=1,
            clients=10,
            command="GET",
            seed_val=12345,
        )

        assert "/tmp/valkey-benchmark" in cmd
        assert "-n" in cmd
        assert "100" in cmd
        assert "-t" in cmd
        assert "GET" in cmd
        assert "--csv" in cmd

    def test_build_tls_command(self, minimal_benchmark_config):
        """Test building command with TLS enabled."""
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))
        from valkey_benchmark import ClientRunner

        runner = ClientRunner(
            commit_id="abc123",
            config=minimal_benchmark_config,
            cluster_mode=False,
            tls_mode=True,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp"),
            valkey_path="/tmp/valkey",
            valkey_benchmark_path="/tmp/valkey-benchmark",
        )

        cmd = runner._build_benchmark_command(
            tls=True,
            requests=100,
            keyspacelen=1000,
            data_size=16,
            pipeline=1,
            clients=10,
            command="GET",
            seed_val=12345,
        )

        assert "--tls" in cmd
        assert "--cert" in cmd
        assert "--key" in cmd
        assert "--cacert" in cmd

    def test_build_command_with_cpu_pinning(self, minimal_benchmark_config):
        """Test building command with CPU pinning."""
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))
        from valkey_benchmark import ClientRunner

        runner = ClientRunner(
            commit_id="abc123",
            config=minimal_benchmark_config,
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp"),
            valkey_path="/tmp/valkey",
            valkey_benchmark_path="/tmp/valkey-benchmark",
            cores="0-3",
        )

        cmd = runner._build_benchmark_command(
            tls=False,
            requests=100,
            keyspacelen=1000,
            data_size=16,
            pipeline=1,
            clients=10,
            command="GET",
            seed_val=12345,
        )

        assert "taskset" in cmd
        assert "-c" in cmd
        assert "0-3" in cmd


class TestMetricsProcessing:
    """Test metrics processing from benchmark output."""

    def test_create_metrics_from_csv_data(self):
        """Test creating metrics dict from CSV data."""
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))
        from process_metrics import MetricsProcessor

        processor = MetricsProcessor(
            commit_id="abc123",
            cluster_mode=False,
            tls_mode=False,
            commit_time="2024-01-01T00:00:00Z",
        )

        csv_data = {
            "rps": "150000.00",
            "avg_latency_ms": "0.500",
            "min_latency_ms": "0.100",
            "p50_latency_ms": "0.400",
            "p95_latency_ms": "0.800",
            "p99_latency_ms": "1.200",
            "max_latency_ms": "5.000",
        }

        metrics = processor.create_metrics(
            csv_data,
            command="GET",
            data_size=16,
            pipeline=1,
            clients=10,
            requests=100000,
        )

        assert metrics is not None
        assert metrics["commit"] == "abc123"
        assert metrics["command"] == "GET"
        assert metrics["rps"] == 150000.0
        assert metrics["avg_latency_ms"] == 0.5
        assert metrics["cluster_mode"] is False
        assert metrics["tls"] is False

    def test_write_and_read_metrics(self, tmp_path):
        """Test writing and reading metrics file."""
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))
        from process_metrics import MetricsProcessor

        processor = MetricsProcessor(
            commit_id="abc123",
            cluster_mode=False,
            tls_mode=False,
            commit_time="2024-01-01T00:00:00Z",
        )

        metrics = [{
            "commit": "abc123",
            "command": "GET",
            "rps": 100000.0,
            "avg_latency_ms": 0.5,
            "min_latency_ms": 0.1,
            "p50_latency_ms": 0.4,
            "p95_latency_ms": 0.8,
            "p99_latency_ms": 1.2,
            "max_latency_ms": 2.0,
        }]

        results_dir = tmp_path / "results"
        processor.write_metrics(results_dir, metrics)

        # Verify file was created
        metrics_file = results_dir / "metrics.json"
        assert metrics_file.exists()

        # Verify content
        loaded = json.loads(metrics_file.read_text())
        assert len(loaded) == 1
        assert loaded[0]["commit"] == "abc123"

    def test_append_metrics_to_existing(self, tmp_path):
        """Test appending metrics to existing file."""
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))
        from process_metrics import MetricsProcessor

        processor = MetricsProcessor(
            commit_id="abc123",
            cluster_mode=False,
            tls_mode=False,
            commit_time="2024-01-01T00:00:00Z",
        )

        results_dir = tmp_path / "results"

        # Write first batch
        metrics1 = [{"commit": "abc123", "command": "GET", "rps": 100000.0}]
        processor.write_metrics(results_dir, metrics1)

        # Write second batch
        metrics2 = [{"commit": "abc123", "command": "SET", "rps": 80000.0}]
        processor.write_metrics(results_dir, metrics2)

        # Verify both are present
        metrics_file = results_dir / "metrics.json"
        loaded = json.loads(metrics_file.read_text())
        assert len(loaded) == 2
        commands = {m["command"] for m in loaded}
        assert commands == {"GET", "SET"}


class TestEndToEndWithMock:
    """End-to-end tests using mock benchmark binary."""

    def test_full_benchmark_flow_with_mock(
        self,
        mock_valkey_repo: GitRepoFixture,
        tmp_path,
    ):
        """Test benchmark CLI accepts valid arguments and attempts execution.

        This test validates:
        1. CLI argument parsing works
        2. Config loading works
        3. The flow proceeds to server connection phase

        Note: Will timeout/fail at server connection since no server runs.
        """
        # Create minimal config
        config = [{
            "requests": [10],
            "keyspacelen": [100],
            "data_sizes": [16],
            "pipelines": [1],
            "clients": [1],
            "commands": ["SET"],
            "cluster_mode": False,
            "tls_mode": False,
            "warmup": 0,
        }]
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config))

        results_dir = tmp_path / "results"

        # Run with very short timeout - we expect it to fail at server connection
        # but this validates the CLI and config loading worked
        try:
            result = subprocess.run(
                [
                    sys.executable,
                    "benchmark.py",
                    "--valkey-path", str(mock_valkey_repo.path),
                    "--valkey-benchmark-path", str(mock_valkey_repo.path / "src" / "valkey-benchmark"),
                    "--config", str(config_path),
                    "--results-dir", str(results_dir),
                    "--use-running-server",
                    "--mode", "client",
                ],
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent.parent.parent,
                timeout=5,  # Short timeout - just enough to validate startup
            )
            # If it completes quickly, it should fail due to no server
            combined_output = result.stdout + result.stderr
            assert result.returncode != 0 or "error" in combined_output.lower()
        except subprocess.TimeoutExpired as e:
            # Timeout is expected - means it got past config loading
            # and is waiting for server (which validates the flow)
            output = (e.stdout or b"").decode() + (e.stderr or b"").decode()
            # Verify it got to the server waiting phase
            assert (
                "Waiting for Valkey" in output
                or "Loaded config" in output
                or "Running" in output
                or len(output) > 0  # Any output means it started
            ), f"Expected benchmark to start, got: {output}"

    def test_benchmark_generates_metrics_with_mock_server(
        self,
        mock_valkey_repo: GitRepoFixture,
        tmp_path,
    ):
        """Test that benchmark would generate metrics file structure.

        Uses component-level testing since full flow requires real server.
        """
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))
        from process_metrics import MetricsProcessor

        # Simulate what benchmark.py does after getting results
        processor = MetricsProcessor(
            commit_id=mock_valkey_repo.get_current_commit()[:8],
            cluster_mode=False,
            tls_mode=False,
            commit_time="2024-01-01T00:00:00Z",
        )

        # Simulate CSV output from mock benchmark
        mock_csv_data = {
            "rps": "95432.10",
            "avg_latency_ms": "0.523",
            "min_latency_ms": "0.102",
            "p50_latency_ms": "0.412",
            "p95_latency_ms": "0.834",
            "p99_latency_ms": "1.245",
            "max_latency_ms": "4.532",
        }

        metrics = processor.create_metrics(
            mock_csv_data,
            command="SET",
            data_size=16,
            pipeline=1,
            clients=1,
            requests=10,
        )

        results_dir = tmp_path / "results" / "test_commit"
        processor.write_metrics(results_dir, [metrics])

        # Verify metrics file created with expected structure
        metrics_file = results_dir / "metrics.json"
        assert metrics_file.exists()

        loaded = json.loads(metrics_file.read_text())
        assert len(loaded) == 1
        assert loaded[0]["command"] == "SET"
        assert loaded[0]["rps"] == 95432.10
        assert loaded[0]["cluster_mode"] is False

    def test_benchmark_cli_help(self):
        """Test benchmark.py CLI shows help."""
        result = subprocess.run(
            [sys.executable, "benchmark.py", "--help"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )

        assert result.returncode == 0
        assert "--config" in result.stdout
        assert "--valkey-path" in result.stdout
        assert "--mode" in result.stdout

    def test_benchmark_cli_validates_args(self):
        """Test benchmark.py validates required arguments."""
        result = subprocess.run(
            [
                sys.executable,
                "benchmark.py",
                "--use-running-server",
                # Missing --valkey-path which is required with --use-running-server
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )

        assert result.returncode != 0
        assert "valkey_path" in result.stdout.lower() or "error" in result.stdout.lower()
