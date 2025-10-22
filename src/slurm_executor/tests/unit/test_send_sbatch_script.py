"""Unit tests for SendSbatchScript step."""

import pytest
from unittest.mock import Mock, patch, mock_open, MagicMock
from pathlib import Path

from slurm_executor.models.Context import Context
from slurm_executor.models.ConnectionConfig import ConnectionConfig
from slurm_executor.pipeline.SendSbatchScript import (
    SendSbatchScript,
    compose_sbatch_script,
)


class TestSendSbatchScript:
    """Test suite for SendSbatchScript step."""

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
        ctx.remote_call_path = "/remote/workspace/call.pkl"
        return ctx

    def test_provides_and_requires(self):
        """Test that SendSbatchScript declares correct provides and requires."""
        # Arrange
        step = SendSbatchScript(partition="cpu", time="01:00:00")

        # Act & Assert
        assert step.provides == ["remote_sbatch_path"]
        assert step.requires == ["remote_workspace_path", "remote_call_path"]

    def test_initialization(self):
        """Test SendSbatchScript initialization."""
        # Arrange & Act
        step = SendSbatchScript(partition="gpu", time="02:30:00")

        # Assert
        assert step.partition == "gpu"
        assert step.time == "02:30:00"

    def test_compose_sbatch_script(self):
        """Test compose_sbatch_script function."""
        # Arrange
        partition = "cpu"
        time = "01:00:00"
        workspace_location = "/remote/workspace/"
        remote_call_location = "/remote/workspace/call.pkl"

        # Act
        result = compose_sbatch_script(
            partition=partition,
            time=time,
            workspace_location=workspace_location,
            remote_call_location=remote_call_location,
        )

        # Assert
        assert isinstance(result, str)
        assert partition in result
        assert time in result
        assert workspace_location in result
        assert remote_call_location in result

    @patch("slurm_executor.pipeline.SendSbatchScript.compose_rsync_command")
    @patch("slurm_executor.pipeline.SendSbatchScript.compose_sbatch_script")
    @patch("slurm_executor.pipeline.SendSbatchScript.tempfile.TemporaryDirectory")
    def test_run_successful_script_creation_and_upload(
        self, mock_tempdir, mock_compose_sbatch, mock_compose_rsync, base_context
    ):
        """Test successful sbatch script creation and upload."""
        # Arrange
        step = SendSbatchScript(partition="cpu", time="01:00:00")

        # Mock temporary directory
        mock_tempdir_instance = MagicMock()
        mock_tempdir_instance.__enter__.return_value = "/tmp/test_dir"
        mock_tempdir_instance.__exit__.return_value = None
        mock_tempdir.return_value = mock_tempdir_instance

        mock_compose_sbatch.return_value = "#!/bin/bash\n#SBATCH --partition=cpu\n"
        mock_compose_rsync.return_value = "rsync command"

        with patch("builtins.open", mock_open()) as mock_file:
            # Act
            result_ctx = step.run(base_context)

            # Assert
            # Verify sbatch script composition
            mock_compose_sbatch.assert_called_once_with(
                partition="cpu",
                time="01:00:00",
                workspace_location="/remote/workspace/",
                remote_call_location="/remote/workspace/call.pkl",
            )

            # Verify file operations
            mock_file.assert_called_once_with(Path("/tmp/test_dir/job.sh"), "w")
            mock_file().write.assert_called_once_with(
                "#!/bin/bash\n#SBATCH --partition=cpu\n"
            )

            # Verify rsync command composition
            mock_compose_rsync.assert_called_once_with(
                port=2222,
                user="test-user",
                host="test-host.com",
                local_root=str(Path("/tmp/test_dir/job.sh")),
                remote_root="/remote/workspace/job.sh",
                exclusion_file=None,
                direction="to_remote",
            )

            # Verify rsync execution
            base_context._connection.local.assert_called_once_with(
                "rsync command", pty=False
            )

            # Verify context update
            assert result_ctx.remote_sbatch_path == "/remote/workspace/job.sh"
            assert result_ctx is base_context

    def test_run_fails_without_remote_workspace_path(self, base_context):
        """Test that run fails when remote_workspace_path is not set."""
        # Arrange
        step = SendSbatchScript(partition="cpu", time="01:00:00")
        base_context.remote_workspace_path = None

        # Act & Assert
        error_msg = "Remote workspace location must be set"
        with pytest.raises(AssertionError, match=error_msg):
            step.run(base_context)

    def test_run_fails_without_remote_call_path(self, base_context):
        """Test that run fails when remote_call_path is not set."""
        # Arrange
        step = SendSbatchScript(partition="cpu", time="01:00:00")
        base_context.remote_call_path = None

        # Act & Assert
        error_msg = "Remote call location must be set"
        with pytest.raises(AssertionError, match=error_msg):
            step.run(base_context)

    @patch("slurm_executor.pipeline.SendSbatchScript.compose_rsync_command")
    @patch("slurm_executor.pipeline.SendSbatchScript.compose_sbatch_script")
    @patch("slurm_executor.pipeline.SendSbatchScript.tempfile.TemporaryDirectory")
    def test_run_handles_script_generation_failure(
        self, mock_tempdir, mock_compose_sbatch, mock_compose_rsync, base_context
    ):
        """Test handling of sbatch script generation failure."""
        # Arrange
        step = SendSbatchScript(partition="cpu", time="01:00:00")

        mock_tempdir_instance = MagicMock()
        mock_tempdir_instance.__enter__.return_value = "/tmp/test_dir"
        mock_tempdir_instance.__exit__.return_value = None
        mock_tempdir.return_value = mock_tempdir_instance

        mock_compose_sbatch.side_effect = Exception("Template error")

        # Act & Assert
        with pytest.raises(Exception, match="Template error"):
            step.run(base_context)

    @patch("slurm_executor.pipeline.SendSbatchScript.compose_rsync_command")
    @patch("slurm_executor.pipeline.SendSbatchScript.compose_sbatch_script")
    @patch("slurm_executor.pipeline.SendSbatchScript.tempfile.TemporaryDirectory")
    def test_run_handles_rsync_failure(
        self, mock_tempdir, mock_compose_sbatch, mock_compose_rsync, base_context
    ):
        """Test handling of rsync upload failure."""
        # Arrange
        step = SendSbatchScript(partition="cpu", time="01:00:00")

        mock_tempdir_instance = MagicMock()
        mock_tempdir_instance.__enter__.return_value = "/tmp/test_dir"
        mock_tempdir_instance.__exit__.return_value = None
        mock_tempdir.return_value = mock_tempdir_instance

        mock_compose_sbatch.return_value = "script content"
        mock_compose_rsync.return_value = "rsync command"
        base_context._connection.local.side_effect = Exception("rsync failed")

        with patch("builtins.open", mock_open()):
            # Act & Assert
            with pytest.raises(Exception, match="rsync failed"):
                step.run(base_context)

    @patch("slurm_executor.pipeline.SendSbatchScript.compose_rsync_command")
    @patch("slurm_executor.pipeline.SendSbatchScript.compose_sbatch_script")
    @patch("slurm_executor.pipeline.SendSbatchScript.tempfile.TemporaryDirectory")
    def test_run_uses_connection_config_values(
        self, mock_tempdir, mock_compose_sbatch, mock_compose_rsync, base_context
    ):
        """Test that run method uses values from connection config correctly."""
        # Arrange
        step = SendSbatchScript(partition="gpu", time="02:00:00")

        # Modify connection config to test different values
        base_context.connection_config.host = "custom-host.example.com"
        base_context.connection_config.user = "custom-user"
        base_context.connection_config.port = 9999

        mock_tempdir_instance = MagicMock()
        mock_tempdir_instance.__enter__.return_value = "/tmp/custom_dir"
        mock_tempdir_instance.__exit__.return_value = None
        mock_tempdir.return_value = mock_tempdir_instance

        mock_compose_sbatch.return_value = "script content"
        mock_compose_rsync.return_value = "rsync command"

        with patch("builtins.open", mock_open()):
            # Act
            step.run(base_context)

            # Assert
            mock_compose_rsync.assert_called_once_with(
                port=9999,
                user="custom-user",
                host="custom-host.example.com",
                local_root=str(Path("/tmp/custom_dir/job.sh")),
                remote_root="/remote/workspace/job.sh",
                exclusion_file=None,
                direction="to_remote",
            )

    @pytest.mark.parametrize(
        "workspace_path,expected_remote_path",
        [
            ("/remote/workspace/", "/remote/workspace/job.sh"),
            ("/remote/workspace", "/remote/workspace/job.sh"),
            ("/custom/path/", "/custom/path/job.sh"),
        ],
    )
    @patch("slurm_executor.pipeline.SendSbatchScript.compose_rsync_command")
    @patch("slurm_executor.pipeline.SendSbatchScript.compose_sbatch_script")
    @patch("slurm_executor.pipeline.SendSbatchScript.tempfile.TemporaryDirectory")
    def test_run_remote_sbatch_path_construction(
        self,
        mock_tempdir,
        mock_compose_sbatch,
        mock_compose_rsync,
        base_context,
        workspace_path,
        expected_remote_path,
    ):
        """Test remote sbatch path construction with different workspace paths."""
        # Arrange
        step = SendSbatchScript(partition="cpu", time="01:00:00")
        base_context.remote_workspace_path = workspace_path

        mock_tempdir_instance = MagicMock()
        mock_tempdir_instance.__enter__.return_value = "/tmp/test_dir"
        mock_tempdir_instance.__exit__.return_value = None
        mock_tempdir.return_value = mock_tempdir_instance

        mock_compose_sbatch.return_value = "script content"
        mock_compose_rsync.return_value = "rsync command"

        with patch("builtins.open", mock_open()):
            # Act
            result_ctx = step.run(base_context)

            # Assert
            assert result_ctx.remote_sbatch_path == expected_remote_path
            mock_compose_rsync.assert_called_once()
            call_args = mock_compose_rsync.call_args[1]
            assert call_args["remote_root"] == expected_remote_path

    @pytest.mark.parametrize(
        "partition,time",
        [
            ("cpu", "01:00:00"),
            ("gpu", "04:30:00"),
            ("high-mem", "12:00:00"),
            ("debug", "00:15:00"),
        ],
    )
    @patch("slurm_executor.pipeline.SendSbatchScript.compose_rsync_command")
    @patch("slurm_executor.pipeline.SendSbatchScript.compose_sbatch_script")
    @patch("slurm_executor.pipeline.SendSbatchScript.tempfile.TemporaryDirectory")
    def test_run_with_different_partition_and_time_values(
        self,
        mock_tempdir,
        mock_compose_sbatch,
        mock_compose_rsync,
        base_context,
        partition,
        time,
    ):
        """Test script generation with different partition and time values."""
        # Arrange
        step = SendSbatchScript(partition=partition, time=time)

        mock_tempdir_instance = MagicMock()
        mock_tempdir_instance.__enter__.return_value = "/tmp/test_dir"
        mock_tempdir_instance.__exit__.return_value = None
        mock_tempdir.return_value = mock_tempdir_instance

        mock_compose_sbatch.return_value = f"script for {partition}"
        mock_compose_rsync.return_value = "rsync command"

        with patch("builtins.open", mock_open()):
            # Act
            step.run(base_context)

            # Assert
            mock_compose_sbatch.assert_called_once_with(
                partition=partition,
                time=time,
                workspace_location="/remote/workspace/",
                remote_call_location="/remote/workspace/call.pkl",
            )
