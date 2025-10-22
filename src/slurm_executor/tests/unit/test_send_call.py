"""Unit tests for SendCall step."""

import pytest
from unittest.mock import Mock, patch, mock_open, MagicMock
from pathlib import Path

from slurm_executor.models.Context import Context
from slurm_executor.models.ConnectionConfig import ConnectionConfig
from slurm_executor.models.SerializableCallData import SerializableCallData
from slurm_executor.pipeline.SendCall import SendCall


class TestSendCall:
    """Test suite for SendCall step."""

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
            kwargs={"multiplier": 3},
            connection_config=connection_config,
        )
        ctx._connection = mock_connection
        ctx.remote_workspace_path = "/remote/workspace/"
        return ctx

    def test_provides_and_requires(self):
        """Test that SendCall declares correct provides and requires."""
        # Arrange
        step = SendCall()

        # Act & Assert
        assert step.provides == ["remote_call_path"]
        assert step.requires == ["remote_workspace_path"]

    def test_initialization(self):
        """Test SendCall initialization."""
        # Arrange & Act
        step = SendCall()

        # Assert
        assert isinstance(step, SendCall)

    @patch("slurm_executor.pipeline.SendCall.compose_rsync_command")
    @patch("slurm_executor.pipeline.SendCall.cloudpickle")
    @patch("slurm_executor.pipeline.SendCall.tempfile.TemporaryDirectory")
    def test_run_successful_serialization_and_upload(
        self, mock_tempdir, mock_cloudpickle, mock_compose_rsync, base_context
    ):
        """Test successful call serialization and upload."""
        # Arrange
        step = SendCall()

        # Mock temporary directory
        mock_tempdir_instance = MagicMock()
        mock_tempdir_instance.__enter__.return_value = "/tmp/test_dir"
        mock_tempdir_instance.__exit__.return_value = None
        mock_tempdir.return_value = mock_tempdir_instance

        mock_compose_rsync.return_value = "rsync command"

        with patch("builtins.open", mock_open()) as mock_file:
            # Act
            result_ctx = step.run(base_context)

            # Assert
            # Verify SerializableCallData creation and serialization
            mock_cloudpickle.dump.assert_called_once()
            call_data = mock_cloudpickle.dump.call_args[0][0]
            assert isinstance(call_data, SerializableCallData)
            assert call_data.func == base_context.function
            assert call_data.args == base_context.args
            assert call_data.kwargs == base_context.kwargs

            # Verify file operations
            mock_file.assert_called_once_with(Path("/tmp/test_dir/call.pkl"), "wb")

            # Verify rsync command composition
            mock_compose_rsync.assert_called_once_with(
                port=2222,
                user="test-user",
                host="test-host.com",
                local_root=str(Path("/tmp/test_dir/call.pkl")),
                remote_root="/remote/workspace/call.pkl",
                direction="to_remote",
            )

            # Verify rsync execution
            base_context._connection.local.assert_called_once_with(
                "rsync command", pty=False
            )

            # Verify context update
            assert result_ctx.remote_call_path == "/remote/workspace/call.pkl"
            assert result_ctx is base_context

    def test_run_fails_without_remote_workspace_path(self, base_context):
        """Test that run fails when remote_workspace_path is not set."""
        # Arrange
        step = SendCall()
        base_context.remote_workspace_path = None

        # Act & Assert
        with pytest.raises(
            AssertionError, match="Remote workspace location must be set"
        ):
            step.run(base_context)

    @patch("slurm_executor.pipeline.SendCall.compose_rsync_command")
    @patch("slurm_executor.pipeline.SendCall.cloudpickle")
    @patch("slurm_executor.pipeline.SendCall.tempfile.TemporaryDirectory")
    def test_run_handles_cloudpickle_failure(
        self, mock_tempdir, mock_cloudpickle, mock_compose_rsync, base_context
    ):
        """Test handling of cloudpickle serialization failure."""
        # Arrange
        step = SendCall()

        mock_tempdir_instance = MagicMock()
        mock_tempdir_instance.__enter__.return_value = "/tmp/test_dir"
        mock_tempdir_instance.__exit__.return_value = None
        mock_tempdir.return_value = mock_tempdir_instance

        mock_cloudpickle.dump.side_effect = Exception("Serialization failed")

        with patch("builtins.open", mock_open()):
            # Act & Assert
            with pytest.raises(Exception, match="Serialization failed"):
                step.run(base_context)

    @patch("slurm_executor.pipeline.SendCall.compose_rsync_command")
    @patch("slurm_executor.pipeline.SendCall.cloudpickle")
    @patch("slurm_executor.pipeline.SendCall.tempfile.TemporaryDirectory")
    def test_run_handles_rsync_failure(
        self, mock_tempdir, mock_cloudpickle, mock_compose_rsync, base_context
    ):
        """Test handling of rsync upload failure."""
        # Arrange
        step = SendCall()

        mock_tempdir_instance = MagicMock()
        mock_tempdir_instance.__enter__.return_value = "/tmp/test_dir"
        mock_tempdir_instance.__exit__.return_value = None
        mock_tempdir.return_value = mock_tempdir_instance

        mock_compose_rsync.return_value = "rsync command"
        base_context._connection.local.side_effect = Exception("rsync failed")

        with patch("builtins.open", mock_open()):
            # Act & Assert
            with pytest.raises(Exception, match="rsync failed"):
                step.run(base_context)

    @patch("slurm_executor.pipeline.SendCall.compose_rsync_command")
    @patch("slurm_executor.pipeline.SendCall.cloudpickle")
    @patch("slurm_executor.pipeline.SendCall.tempfile.TemporaryDirectory")
    def test_run_uses_connection_config_values(
        self, mock_tempdir, mock_cloudpickle, mock_compose_rsync, base_context
    ):
        """Test that run method uses values from connection config correctly."""
        # Arrange
        step = SendCall()

        # Modify connection config to test different values
        base_context.connection_config.host = "custom-host.example.com"
        base_context.connection_config.user = "custom-user"
        base_context.connection_config.port = 9999

        mock_tempdir_instance = MagicMock()
        mock_tempdir_instance.__enter__.return_value = "/tmp/custom_dir"
        mock_tempdir_instance.__exit__.return_value = None
        mock_tempdir.return_value = mock_tempdir_instance

        mock_compose_rsync.return_value = "rsync command"

        with patch("builtins.open", mock_open()):
            # Act
            step.run(base_context)

            # Assert
            mock_compose_rsync.assert_called_once_with(
                port=9999,
                user="custom-user",
                host="custom-host.example.com",
                local_root=str(Path("/tmp/custom_dir/call.pkl")),
                remote_root="/remote/workspace/call.pkl",
                direction="to_remote",
            )

    @pytest.mark.parametrize(
        "workspace_path,expected_remote_path",
        [
            ("/remote/workspace/", "/remote/workspace/call.pkl"),
            ("/remote/workspace", "/remote/workspace/call.pkl"),
            ("/custom/path/", "/custom/path/call.pkl"),
        ],
    )
    @patch("slurm_executor.pipeline.SendCall.compose_rsync_command")
    @patch("slurm_executor.pipeline.SendCall.cloudpickle")
    @patch("slurm_executor.pipeline.SendCall.tempfile.TemporaryDirectory")
    def test_run_remote_call_path_construction(
        self,
        mock_tempdir,
        mock_cloudpickle,
        mock_compose_rsync,
        base_context,
        workspace_path,
        expected_remote_path,
    ):
        """Test remote call path construction with different workspace paths."""
        # Arrange
        step = SendCall()
        base_context.remote_workspace_path = workspace_path

        mock_tempdir_instance = MagicMock()
        mock_tempdir_instance.__enter__.return_value = "/tmp/test_dir"
        mock_tempdir_instance.__exit__.return_value = None
        mock_tempdir.return_value = mock_tempdir_instance

        mock_compose_rsync.return_value = "rsync command"

        with patch("builtins.open", mock_open()):
            # Act
            result_ctx = step.run(base_context)

            # Assert
            assert result_ctx.remote_call_path == expected_remote_path
            mock_compose_rsync.assert_called_once()
            call_args = mock_compose_rsync.call_args[1]
            assert call_args["remote_root"] == expected_remote_path

    @patch("slurm_executor.pipeline.SendCall.compose_rsync_command")
    @patch("slurm_executor.pipeline.SendCall.cloudpickle")
    @patch("slurm_executor.pipeline.SendCall.tempfile.TemporaryDirectory")
    def test_run_with_complex_function_and_data(
        self, mock_tempdir, mock_cloudpickle, mock_compose_rsync, base_context
    ):
        """Test serialization with complex function and data structures."""
        # Arrange
        step = SendCall()

        def complex_function(data, **kwargs):
            return {"processed": data, "options": kwargs}

        complex_args = ([1, 2, 3], {"nested": {"data": True}})
        complex_kwargs = {"option1": "value1", "option2": [4, 5, 6]}

        base_context.function = complex_function
        base_context.args = complex_args
        base_context.kwargs = complex_kwargs

        mock_tempdir_instance = MagicMock()
        mock_tempdir_instance.__enter__.return_value = "/tmp/test_dir"
        mock_tempdir_instance.__exit__.return_value = None
        mock_tempdir.return_value = mock_tempdir_instance

        mock_compose_rsync.return_value = "rsync command"

        with patch("builtins.open", mock_open()):
            # Act
            result_ctx = step.run(base_context)

            # Assert
            mock_cloudpickle.dump.assert_called_once()
            call_data = mock_cloudpickle.dump.call_args[0][0]
            assert call_data.func == complex_function
            assert call_data.args == complex_args
            assert call_data.kwargs == complex_kwargs
            assert result_ctx.remote_call_path == "/remote/workspace/call.pkl"
