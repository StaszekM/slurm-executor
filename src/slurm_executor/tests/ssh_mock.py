"""
SSH Connection Mock for testing slurm-executor pipeline steps.

This module provides a mock implementation of fabric.Connection that can be used
to test SSH-based operations without needing an actual remote server.
"""

import re
from dataclasses import dataclass, field
from typing import Any, List, Optional, Pattern, Union
from unittest.mock import MagicMock, Mock

from fabric.runners import Result


@dataclass
class SSHCommandExpectation:
    """
    Represents an expected SSH command execution and its response.

    Attributes:
        command: The command pattern to match (can be string or regex pattern)
        stdout: Standard output to return
        stderr: Standard error to return
        exit_code: Exit code to return (default: 0)
        is_regex: Whether the command is a regex pattern (default: False)
        call_count: Number of times this command should be called (None = unlimited)
    """

    command: Union[str, Pattern[str]]
    stdout: str = ""
    stderr: str = ""
    exit_code: int = 0
    is_regex: bool = False
    call_count: Optional[int] = None
    _actual_calls: int = field(default=0, init=False)

    def matches(self, command: str) -> bool:
        """Check if the given command matches this expectation."""
        if self.is_regex:
            if isinstance(self.command, str):
                return bool(re.search(self.command, command))
            else:
                return bool(self.command.search(command))
        else:
            return str(self.command) == command

    def can_execute(self) -> bool:
        """Check if this expectation can still be executed."""
        if self.call_count is None:
            return True
        return self._actual_calls < self.call_count

    def execute(self) -> Result:
        """Execute this expectation and return the mocked result."""
        if not self.can_execute():
            raise AssertionError(
                f"Command '{self.command}' was called more times than expected "
                f"(expected: {self.call_count}, actual: {self._actual_calls + 1})"
            )

        self._actual_calls += 1

        # Create a mock Result object
        result = Mock(spec=Result)
        result.stdout = self.stdout
        result.stderr = self.stderr
        result.return_code = self.exit_code
        result.exited = self.exit_code
        result.ok = self.exit_code == 0
        result.failed = self.exit_code != 0

        return result


class MockSSHConnection:
    """
    Mock implementation of fabric.Connection for testing.

    This class simulates SSH connections and command execution by matching
    commands against predefined expectations and returning configured responses.

    Usage:
        mock_conn = MockSSHConnection()
        mock_conn.expect_command("ls -la", stdout="file1\\nfile2\\n")
        mock_conn.expect_command(
            r"sacct -j \\d+",
            stdout="12345 COMPLETED",
            is_regex=True
        )

        # Use mock_conn as you would use fabric.Connection
        result = mock_conn.run("ls -la")
        print(result.stdout)  # "file1\\nfile2\\n"
    """

    def __init__(
        self, host: str = "mock-host", user: str = "mock-user", port: int = 22
    ):
        """
        Initialize the mock SSH connection.

        Args:
            host: Mock hostname
            user: Mock username
            port: Mock port number
        """
        self.host = host
        self.user = user
        self.port = port
        self._expectations: List[SSHCommandExpectation] = []
        self._executed_commands: List[str] = []
        self._strict_mode = False

        # Mock connection state
        self._connected = False
        self.transport = MagicMock()
        self.transport.active = True

    def expect_command(
        self,
        command: Union[str, Pattern[str]],
        stdout: str = "",
        stderr: str = "",
        exit_code: int = 0,
        is_regex: bool = False,
        call_count: Optional[int] = None,
    ) -> "MockSSHConnection":
        """
        Add an expected command execution.

        Args:
            command: Command string or regex pattern to match
            stdout: Standard output to return
            stderr: Standard error to return
            exit_code: Exit code to return
            is_regex: Whether command is a regex pattern
            call_count: Maximum number of times this command can be called

        Returns:
            Self for method chaining
        """
        expectation = SSHCommandExpectation(
            command=command,
            stdout=stdout,
            stderr=stderr,
            exit_code=exit_code,
            is_regex=is_regex,
            call_count=call_count,
        )
        self._expectations.append(expectation)
        return self

    def expect_commands(
        self, expectations: List[SSHCommandExpectation]
    ) -> "MockSSHConnection":
        """
        Add multiple command expectations at once.

        Args:
            expectations: List of SSHCommandExpectation objects

        Returns:
            Self for method chaining
        """
        self._expectations.extend(expectations)
        return self

    def set_strict_mode(self, strict: bool = True) -> "MockSSHConnection":
        """
        Enable/disable strict mode.

        In strict mode, any command that doesn't match an expectation
        will raise an error.
        In non-strict mode, unknown commands return empty output with exit code 0.

        Args:
            strict: Whether to enable strict mode

        Returns:
            Self for method chaining
        """
        self._strict_mode = strict
        return self

    def run(self, command: str, **kwargs: Any) -> Result:
        """
        Mock implementation of fabric.Connection.run().

        Args:
            command: Command to execute
            **kwargs: Additional arguments (ignored in mock)

        Returns:
            Mocked Result object

        Raises:
            AssertionError: If command doesn't match any expectation in strict mode
        """
        self._executed_commands.append(command)

        # Find matching expectation
        for expectation in self._expectations:
            if expectation.matches(command) and expectation.can_execute():
                return expectation.execute()

        # No matching expectation found
        if self._strict_mode:
            expectations_str = [str(exp.command) for exp in self._expectations]
            raise AssertionError(
                f"Unexpected command: '{command}'. "
                f"Available expectations: {expectations_str}"
            )

        # Return default empty result in non-strict mode
        result = Mock(spec=Result)
        result.stdout = ""
        result.stderr = ""
        result.return_code = 0
        result.exited = 0
        result.ok = True
        result.failed = False
        return result

    def local(self, command: str, **kwargs: Any) -> Result:
        """
        Mock implementation of fabric.Connection.local().

        For simplicity, this delegates to run() as if it were a remote command.
        In real usage, you might want to handle local commands differently.
        """
        return self.run(command, **kwargs)

    def open(self):
        """Mock connection opening."""
        self._connected = True

    def close(self):
        """Mock connection closing."""
        self._connected = False

    def is_connected(self) -> bool:
        """Check if the mock connection is open."""
        return self._connected

    def __enter__(self):
        """Context manager entry."""
        self.open()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any):
        """Context manager exit."""
        self.close()

    # Test assertion methods

    def assert_command_executed(
        self, command: str, count: Optional[int] = None
    ) -> None:
        """
        Assert that a specific command was executed.

        Args:
            command: Command that should have been executed
            count: Expected number of times (None = at least once)

        Raises:
            AssertionError: If command was not executed as expected
        """
        actual_count = self._executed_commands.count(command)

        if count is None:
            if actual_count == 0:
                raise AssertionError(
                    f"Command '{command}' was never executed. "
                    f"Executed commands: {self._executed_commands}"
                )
        else:
            if actual_count != count:
                raise AssertionError(
                    f"Command '{command}' was executed {actual_count} times, "
                    f"expected {count} times"
                )

    def assert_command_not_executed(self, command: str) -> None:
        """
        Assert that a specific command was NOT executed.

        Args:
            command: Command that should not have been executed

        Raises:
            AssertionError: If command was executed
        """
        if command in self._executed_commands:
            count = self._executed_commands.count(command)
            raise AssertionError(
                f"Command '{command}' was executed {count} times, "
                f"but should not have been executed"
            )

    def assert_no_unexpected_commands(self) -> None:
        """
        Assert that all executed commands matched expectations.

        Raises:
            AssertionError: If any command was executed without matching expectation
        """
        for command in self._executed_commands:
            found_match = False
            for expectation in self._expectations:
                if expectation.matches(command):
                    found_match = True
                    break

            if not found_match:
                raise AssertionError(f"Unexpected command executed: '{command}'")

    def get_executed_commands(self) -> List[str]:
        """
        Get list of all executed commands in order.

        Returns:
            List of executed command strings
        """
        return self._executed_commands.copy()

    def reset(self) -> None:
        """
        Reset the mock connection state.

        Clears all executed commands and resets expectation call counts.
        """
        self._executed_commands.clear()
        for expectation in self._expectations:
            expectation._actual_calls = 0


def create_mock_connection(**kwargs: Any) -> MockSSHConnection:
    """
    Factory function to create a MockSSHConnection.

    This is useful for compatibility with existing code that expects
    a Connection factory function.

    Args:
        **kwargs: Arguments passed to MockSSHConnection constructor

    Returns:
        MockSSHConnection instance
    """
    return MockSSHConnection(**kwargs)
