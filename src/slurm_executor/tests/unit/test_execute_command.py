"""Unit tests for ExecuteCommand step."""

from unittest.mock import Mock

import pytest

from slurm_executor.models.ConnectionConfig import ConnectionConfig
from slurm_executor.models.Context import Context
from slurm_executor.pipeline.ExecuteCommand import ExecuteCommand


class TestExecuteCommand:
    """Test suite for ExecuteCommand step."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock fabric connection."""
        conn = Mock()
        mock_result = Mock()
        mock_result.stdout = ""
        mock_result.stderr = ""
        mock_result.return_code = 0
        mock_result.ok = True

        conn.run.return_value = mock_result
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
        return ctx

    def test_provides_and_requires(self):
        """Test that ExecuteCommand declares correct provides and requires."""
        # Arrange
        step = ExecuteCommand(remote_command="echo 'test'")

        # Act & Assert
        assert step.provides == []
        assert step.requires == []

    def test_initialization_with_command_only(self):
        """Test ExecuteCommand initialization with command only."""
        # Arrange & Act
        command = "ls -la /home"
        step = ExecuteCommand(remote_command=command)

        # Assert
        assert step.remote_command == command
        assert step.on_success is None
        assert step.on_failure is None

    def test_initialization_with_callbacks(self):
        """Test ExecuteCommand initialization with success and failure callbacks."""
        # Arrange
        command = "echo 'test'"
        success_callback = Mock()
        failure_callback = Mock()

        # Act
        step = ExecuteCommand(
            remote_command=command,
            on_success=success_callback,
            on_failure=failure_callback,
        )

        # Assert
        assert step.remote_command == command
        assert step.on_success == success_callback
        assert step.on_failure == failure_callback

    def test_run_successful_command_without_callbacks(self, base_context):
        """Test successful command execution without callbacks."""
        # Arrange
        step = ExecuteCommand(remote_command="echo 'hello world'")

        mock_result = Mock()
        mock_result.ok = True
        mock_result.stdout = "hello world\n"
        base_context._connection.run.return_value = mock_result

        # Act
        result_ctx = step.run(base_context)

        # Assert
        base_context._connection.run.assert_called_once_with("echo 'hello world'")
        assert result_ctx is base_context

    def test_run_successful_command_with_success_callback(self, base_context):
        """Test successful command execution with success callback."""
        # Arrange
        success_callback = Mock()
        step = ExecuteCommand(remote_command="ls /tmp", on_success=success_callback)

        mock_result = Mock()
        mock_result.ok = True
        mock_result.stdout = "file1.txt\nfile2.txt\n"
        base_context._connection.run.return_value = mock_result

        # Act
        result_ctx = step.run(base_context)

        # Assert
        base_context._connection.run.assert_called_once_with("ls /tmp")
        success_callback.assert_called_once_with(mock_result)
        assert result_ctx is base_context

    def test_run_failed_command_without_callbacks(self, base_context):
        """Test failed command execution without callbacks."""
        # Arrange
        step = ExecuteCommand(remote_command="cat /nonexistent/file")

        mock_result = Mock()
        mock_result.ok = False
        mock_result.return_code = 1
        mock_result.stderr = "cat: /nonexistent/file: No such file or directory\n"
        base_context._connection.run.return_value = mock_result

        # Act
        result_ctx = step.run(base_context)

        # Assert
        base_context._connection.run.assert_called_once_with("cat /nonexistent/file")
        assert result_ctx is base_context

    def test_run_failed_command_with_failure_callback(self, base_context):
        """Test failed command execution with failure callback."""
        # Arrange
        failure_callback = Mock()
        step = ExecuteCommand(
            remote_command="rm /protected/file", on_failure=failure_callback
        )

        mock_result = Mock()
        mock_result.ok = False
        mock_result.return_code = 1
        mock_result.stderr = "rm: cannot remove '/protected/file': Permission denied\n"
        base_context._connection.run.return_value = mock_result

        # Act
        result_ctx = step.run(base_context)

        # Assert
        base_context._connection.run.assert_called_once_with("rm /protected/file")
        failure_callback.assert_called_once_with(mock_result)
        assert result_ctx is base_context

    def test_run_with_both_callbacks_success_path(self, base_context):
        """Test command execution with both callbacks when command succeeds."""
        # Arrange
        success_callback = Mock()
        failure_callback = Mock()
        step = ExecuteCommand(
            remote_command="echo 'success'",
            on_success=success_callback,
            on_failure=failure_callback,
        )

        mock_result = Mock()
        mock_result.ok = True
        mock_result.stdout = "success\n"
        base_context._connection.run.return_value = mock_result

        # Act
        step.run(base_context)

        # Assert
        success_callback.assert_called_once_with(mock_result)
        failure_callback.assert_not_called()

    def test_run_with_both_callbacks_failure_path(self, base_context):
        """Test command execution with both callbacks when command fails."""
        # Arrange
        success_callback = Mock()
        failure_callback = Mock()
        step = ExecuteCommand(
            remote_command="exit 1",
            on_success=success_callback,
            on_failure=failure_callback,
        )

        mock_result = Mock()
        mock_result.ok = False
        mock_result.return_code = 1
        base_context._connection.run.return_value = mock_result

        # Act
        step.run(base_context)

        # Assert
        success_callback.assert_not_called()
        failure_callback.assert_called_once_with(mock_result)

    @pytest.mark.parametrize(
        "command",
        [
            "ls -la",
            "cat /etc/hostname",
            "mkdir -p /tmp/test",
            "chmod 755 /tmp/script.sh",
            "ps aux | grep python",
            "find /home -name '*.txt'",
        ],
    )
    def test_run_with_different_commands(self, base_context, command):
        """Test execution with various command formats."""
        # Arrange
        step = ExecuteCommand(remote_command=command)

        mock_result = Mock()
        mock_result.ok = True
        base_context._connection.run.return_value = mock_result

        # Act
        step.run(base_context)

        # Assert
        base_context._connection.run.assert_called_once_with(command)

    def test_run_preserves_context_state(self, base_context):
        """Test that run method preserves all context state."""
        # Arrange
        step = ExecuteCommand(remote_command="echo 'test'")

        # Set some context state
        base_context.remote_workspace_path = "/some/path"
        base_context.job_id = "12345"

        mock_result = Mock()
        mock_result.ok = True
        base_context._connection.run.return_value = mock_result

        # Act
        result_ctx = step.run(base_context)

        # Assert
        assert result_ctx is base_context
        assert result_ctx.remote_workspace_path == "/some/path"
        assert result_ctx.job_id == "12345"

    def test_callback_receives_full_result_object(self, base_context):
        """Test that callbacks receive the complete result object."""
        # Arrange
        callback_result = None

        def capture_result(result):
            nonlocal callback_result
            callback_result = result

        step = ExecuteCommand(remote_command="echo 'test'", on_success=capture_result)

        mock_result = Mock()
        mock_result.ok = True
        mock_result.stdout = "test\n"
        mock_result.stderr = ""
        mock_result.return_code = 0
        base_context._connection.run.return_value = mock_result

        # Act
        step.run(base_context)

        # Assert
        assert callback_result is mock_result
        assert callback_result.stdout == "test\n"  # type: ignore[attr-defined]
        assert callback_result.return_code == 0  # type: ignore[attr-defined]

    def test_callback_exception_handling(self, base_context):
        """Test behavior when callback raises an exception."""

        # Arrange
        def failing_callback(result):
            raise ValueError("Callback failed")

        step = ExecuteCommand(remote_command="echo 'test'", on_success=failing_callback)

        mock_result = Mock()
        mock_result.ok = True
        base_context._connection.run.return_value = mock_result

        # Act & Assert
        with pytest.raises(ValueError, match="Callback failed"):
            step.run(base_context)

    @pytest.mark.parametrize(
        "return_code,expected_ok",
        [
            (0, True),
            (1, False),
            (127, False),
            (255, False),
        ],
    )
    def test_run_with_different_return_codes(
        self, base_context, return_code, expected_ok
    ):
        """Test command execution with different return codes."""
        # Arrange
        success_callback = Mock()
        failure_callback = Mock()
        step = ExecuteCommand(
            remote_command="exit " + str(return_code),
            on_success=success_callback,
            on_failure=failure_callback,
        )

        mock_result = Mock()
        mock_result.ok = expected_ok
        mock_result.return_code = return_code
        base_context._connection.run.return_value = mock_result

        # Act
        step.run(base_context)

        # Assert
        if expected_ok:
            success_callback.assert_called_once()
            failure_callback.assert_not_called()
        else:
            success_callback.assert_not_called()
            failure_callback.assert_called_once()

    def test_run_command_execution_exception(self, base_context):
        """Test handling of command execution exceptions."""
        # Arrange
        step = ExecuteCommand(remote_command="echo 'test'")
        base_context._connection.run.side_effect = Exception("Connection failed")

        # Act & Assert
        with pytest.raises(Exception, match="Connection failed"):
            step.run(base_context)

    def test_complex_callback_interaction(self, base_context):
        """Test complex interaction between callbacks and result processing."""
        # Arrange
        results = []

        def success_handler(result):
            results.append(("success", result.stdout.strip()))

        def failure_handler(result):
            results.append(("failure", result.stderr.strip()))

        step = ExecuteCommand(
            remote_command="echo 'Complex test output'",
            on_success=success_handler,
            on_failure=failure_handler,
        )

        mock_result = Mock()
        mock_result.ok = True
        mock_result.stdout = "Complex test output\n"
        mock_result.stderr = ""
        base_context._connection.run.return_value = mock_result

        # Act
        step.run(base_context)

        # Assert
        assert len(results) == 1
        assert results[0] == ("success", "Complex test output")

    def test_step_isolation_multiple_runs(self, base_context):
        """Test that multiple runs of the same step don't interfere."""
        # Arrange
        call_count = 0

        def counter_callback(result):
            nonlocal call_count
            call_count += 1

        step = ExecuteCommand(remote_command="echo 'test'", on_success=counter_callback)

        mock_result = Mock()
        mock_result.ok = True
        base_context._connection.run.return_value = mock_result

        # Act
        step.run(base_context)
        step.run(base_context)
        step.run(base_context)

        # Assert
        assert call_count == 3
        assert base_context._connection.run.call_count == 3
