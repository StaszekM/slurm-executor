"""Unit tests for SubmitSbatchScript step."""

from unittest.mock import Mock

import pytest

from slurm_executor.models.ConnectionConfig import ConnectionConfig
from slurm_executor.models.Context import Context
from slurm_executor.pipeline.SubmitSbatchScript import SubmitSbatchScript


class TestSubmitSbatchScript:
    """Test suite for SubmitSbatchScript step."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock fabric connection."""
        conn = Mock()
        mock_result = Mock()
        mock_result.stdout = ""
        mock_result.stderr = ""
        mock_result.return_code = 0

        conn.run.return_value = mock_result
        conn.local.return_value = mock_result
        return conn

    @pytest.fixture
    def connection_config(self):
        """Create a test connection configuration."""
        return ConnectionConfig(host="test-host.com", user="test-user", port=2222)

    @pytest.fixture
    def base_context(self, mock_connection, connection_config):
        """Create a basic context for testing."""
        ctx = Context(
            function=lambda x: x * 2,
            args=(5,),
            kwargs={},
            connection_config=connection_config,
        )
        ctx._connection = mock_connection
        ctx.remote_workspace_path = "/remote/workspace/"
        ctx.remote_sbatch_path = "/remote/workspace/job.sh"
        return ctx

    def test_provides_and_requires(self):
        """Test that SubmitSbatchScript declares correct provides and requires."""
        # Arrange
        step = SubmitSbatchScript(output_file_location="/output/job.out")

        # Act & Assert
        assert step.provides == ["job_id", "job_output_file_location"]
        assert step.requires == ["remote_sbatch_path", "remote_workspace_path"]

    def test_initialization(self):
        """Test SubmitSbatchScript initialization."""
        # Arrange & Act
        output_location = "/custom/output/job.out"
        step = SubmitSbatchScript(output_file_location=output_location)

        # Assert
        assert step.output_file_location == output_location

    def test_run_successful_job_submission(self, base_context):
        """Test successful SLURM job submission."""
        # Arrange
        step = SubmitSbatchScript(output_file_location="/output/job.out")

        # Mock sbatch command output
        mock_result = Mock()
        mock_result.stdout = "12345;cluster_name"
        base_context._connection.run.return_value = mock_result

        # Act
        result_ctx = step.run(base_context)

        # Assert
        expected_command = (
            "cd /remote/workspace/ && "
            "sbatch --parsable --output=/output/job.out /remote/workspace/job.sh"
        )
        base_context._connection.run.assert_called_once_with(
            expected_command, pty=False, hide=None
        )

        # Verify context updates - job_id should be the last part after split
        assert result_ctx.job_id == "cluster_name"
        assert result_ctx.job_output_file_location == "/output/job.out"
        assert result_ctx is base_context

    def test_run_job_submission_with_different_output_formats(self, base_context):
        """Test job submission with different sbatch output formats."""
        # Arrange
        step = SubmitSbatchScript(output_file_location="/output/job.out")

        test_cases = [
            ("12345", "12345"),  # Simple job ID
            ("67890;cluster", "cluster"),  # Job ID with cluster info - takes last part
            ("999;cluster;extra", "extra"),  # Multiple semicolons - takes last part
            ("  54321  ", "54321"),  # With whitespace
            ("11111;cluster\n", "cluster"),  # With newline - takes last part
        ]

        for stdout_output, expected_job_id in test_cases:
            mock_result = Mock()
            mock_result.stdout = stdout_output
            base_context._connection.run.return_value = mock_result

            # Act
            result_ctx = step.run(base_context)

            # Assert
            assert result_ctx.job_id == expected_job_id

    def test_run_fails_without_remote_workspace_path(self, base_context):
        """Test that run fails when remote_workspace_path is not set."""
        # Arrange
        step = SubmitSbatchScript(output_file_location="/output/job.out")
        base_context.remote_workspace_path = None

        # Act & Assert
        error_msg = "Remote workspace location must be set"
        with pytest.raises(AssertionError, match=error_msg):
            step.run(base_context)

    def test_run_fails_without_remote_sbatch_path(self, base_context):
        """Test that run fails when remote_sbatch_path is not set."""
        # Arrange
        step = SubmitSbatchScript(output_file_location="/output/job.out")
        base_context.remote_sbatch_path = None

        # Act & Assert
        error_msg = "Remote sbatch script location must be set"
        with pytest.raises(AssertionError, match=error_msg):
            step.run(base_context)

    def test_run_handles_sbatch_command_failure(self, base_context):
        """Test handling of sbatch command failure."""
        # Arrange
        step = SubmitSbatchScript(output_file_location="/output/job.out")
        base_context._connection.run.side_effect = Exception("sbatch failed")

        # Act & Assert
        with pytest.raises(Exception, match="sbatch failed"):
            step.run(base_context)

    @pytest.mark.parametrize(
        "workspace_path,sbatch_path,output_path",
        [
            ("/remote/workspace/", "/remote/workspace/job.sh", "/output/job1.out"),
            ("/custom/path", "/custom/path/script.sh", "/logs/job2.out"),
            ("/home/user/work/", "/home/user/work/run.sh", "/tmp/job3.out"),
        ],
    )
    def test_run_with_different_paths(
        self, base_context, workspace_path, sbatch_path, output_path
    ):
        """Test run with different workspace and script paths."""
        # Arrange
        step = SubmitSbatchScript(output_file_location=output_path)
        base_context.remote_workspace_path = workspace_path
        base_context.remote_sbatch_path = sbatch_path

        mock_result = Mock()
        mock_result.stdout = "54321"
        base_context._connection.run.return_value = mock_result

        # Act
        result_ctx = step.run(base_context)

        # Assert
        expected_command = (
            f"cd {workspace_path} && "
            f"sbatch --parsable --output={output_path} {sbatch_path}"
        )
        base_context._connection.run.assert_called_once_with(
            expected_command, pty=False, hide=None
        )
        assert result_ctx.job_id == "54321"
        assert result_ctx.job_output_file_location == output_path

    def test_run_command_construction(self, base_context):
        """Test that the sbatch command is constructed correctly."""
        # Arrange
        step = SubmitSbatchScript(output_file_location="/custom/logs/job.out")
        base_context.remote_workspace_path = "/work/dir/"
        base_context.remote_sbatch_path = "/work/dir/custom_job.sh"

        mock_result = Mock()
        mock_result.stdout = "99999"
        base_context._connection.run.return_value = mock_result

        # Act
        step.run(base_context)

        # Assert
        expected_command = (
            "cd /work/dir/ && "
            "sbatch --parsable --output=/custom/logs/job.out /work/dir/custom_job.sh"
        )
        base_context._connection.run.assert_called_once_with(
            expected_command, pty=False, hide=None
        )

    def test_run_preserves_context_object(self, base_context):
        """Test that run returns the same context object."""
        # Arrange
        step = SubmitSbatchScript(output_file_location="/output/job.out")

        mock_result = Mock()
        mock_result.stdout = "12345"
        base_context._connection.run.return_value = mock_result

        # Act
        result_ctx = step.run(base_context)

        # Assert
        assert result_ctx is base_context
        assert hasattr(result_ctx, "job_id")
        assert hasattr(result_ctx, "job_output_file_location")

    @pytest.mark.parametrize(
        "job_output",
        [
            "123",
            "456;cluster",
            "789;cluster;extra;info",
            "\n999\n",
            "  111  ",
        ],
    )
    def test_run_job_id_parsing(self, base_context, job_output):
        """Test job ID parsing from various sbatch output formats."""
        # Arrange
        step = SubmitSbatchScript(output_file_location="/output/job.out")

        mock_result = Mock()
        mock_result.stdout = job_output
        base_context._connection.run.return_value = mock_result

        # Act
        result_ctx = step.run(base_context)

        # Assert
        expected_job_id = job_output.split(";")[-1].strip()
        assert result_ctx.job_id == expected_job_id
