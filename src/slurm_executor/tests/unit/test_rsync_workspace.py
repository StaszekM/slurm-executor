"""Unit tests for RSyncWorkspace step."""

from unittest.mock import Mock, call, patch

import pytest

from slurm_executor.models.ConnectionConfig import ConnectionConfig
from slurm_executor.models.Context import Context
from slurm_executor.pipeline.RSyncWorkspaceToRemote import RSyncWorkspace


class TestRSyncWorkspace:
    """Test suite for RSyncWorkspace step."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock fabric connection."""
        conn = Mock()
        # Mock the result objects without using the actual Result class
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
            function=lambda: None,
            args=(),
            kwargs={},
            connection_config=connection_config,
        )
        ctx._connection = mock_connection
        return ctx

    def test_provides_and_requires(self):
        """Test that RSyncWorkspace declares correct provides and requires."""
        # Arrange
        step = RSyncWorkspace(
            local_root="/local/",
            remote_root="/remote/",
            direction="to_remote",
            exclude_from="exclude.txt",
        )

        # Act & Assert
        assert step.provides == ["remote_workspace_path"]
        assert step.requires == []

    @pytest.mark.parametrize(
        "direction,exclude_from,include_only",
        [
            ("to_remote", "exclude.txt", None),
            ("from_remote", "exclude.txt", None),
            ("to_remote", None, "include.txt"),
            ("from_remote", None, "include.txt"),
        ],
    )
    def test_valid_initialization(self, direction, exclude_from, include_only):
        """Test valid RSyncWorkspace initialization scenarios."""
        # Arrange & Act
        step = RSyncWorkspace(
            local_root="/local/",
            remote_root="/remote/",
            direction=direction,
            exclude_from=exclude_from,
            include_only=include_only,
        )

        # Assert
        assert step.direction == direction
        assert step.exclude_from == exclude_from
        assert step.include_only == include_only

    def test_initialization_adds_trailing_slash_to_paths(self):
        """Test that initialization adds trailing slashes to paths."""
        # Arrange & Act
        with patch(
            "slurm_executor.pipeline.RSyncWorkspaceToRemote.logger"
        ) as mock_logger:
            step = RSyncWorkspace(
                local_root="/local",  # No trailing slash
                remote_root="/remote",  # No trailing slash
                direction="to_remote",
                exclude_from="exclude.txt",
            )

        # Assert
        assert step.local_root == "/local/"
        assert step.remote_root == "/remote/"
        assert mock_logger.warning.call_count == 2
        expected_calls = [
            call(
                "Local root '/local' does not end with '/' which may lead to "
                "unexpected behavior. Adding '/' to the end."
            ),
            call(
                "Remote root '/remote' does not end with '/' which may lead to "
                "unexpected behavior. Adding '/' to the end."
            ),
        ]
        mock_logger.warning.assert_has_calls(expected_calls)

    def test_initialization_preserves_trailing_slashes(self):
        """Test that initialization preserves existing trailing slashes."""
        # Arrange & Act
        with patch(
            "slurm_executor.pipeline.RSyncWorkspaceToRemote.logger"
        ) as mock_logger:
            step = RSyncWorkspace(
                local_root="/local/",
                remote_root="/remote/",
                direction="to_remote",
                exclude_from="exclude.txt",
            )

        # Assert
        assert step.local_root == "/local/"
        assert step.remote_root == "/remote/"
        assert mock_logger.warning.call_count == 0

    def test_initialization_fails_with_both_include_and_exclude(self):
        """Test initialization fails when both include_only and exclude_from are provided."""
        # Arrange, Act & Assert
        error_msg = "Cannot specify both include_only and exclude_from files"
        with pytest.raises(AssertionError, match=error_msg):
            RSyncWorkspace(
                local_root="/local/",
                remote_root="/remote/",
                direction="to_remote",
                exclude_from="exclude.txt",
                include_only="include.txt",
            )

    def test_initialization_fails_with_neither_include_nor_exclude(self):
        """Test initialization fails when neither include_only nor exclude_from are provided."""
        # Arrange, Act & Assert
        error_msg = "Must specify either include_only or exclude_from file"
        with pytest.raises(AssertionError, match=error_msg):
            RSyncWorkspace(
                local_root="/local/", remote_root="/remote/", direction="to_remote"
            )

    @patch("slurm_executor.pipeline.RSyncWorkspaceToRemote.compose_rsync_command")
    def test_run_to_remote_with_exclude_file(self, mock_compose_rsync, base_context):
        """Test successful run with to_remote direction and exclude file."""
        # Arrange
        step = RSyncWorkspace(
            local_root="/local/",
            remote_root="/remote/",
            direction="to_remote",
            exclude_from="exclude.txt",
        )
        expected_rsync_cmd = (
            "rsync -e 'ssh -p 2222' --delete --info=progress2 -az "
            "--exclude-from=exclude.txt /local/ test-user@test-host.com:/remote/"
        )
        mock_compose_rsync.return_value = expected_rsync_cmd

        # Act
        result_ctx = step.run(base_context)

        # Assert
        base_context._connection.run.assert_called_once_with(
            "mkdir -p /remote/", pty=False
        )
        mock_compose_rsync.assert_called_once_with(
            port=2222,
            user="test-user",
            host="test-host.com",
            local_root="/local/",
            remote_root="/remote/",
            exclusion_file="exclude.txt",
            inclusion_file=None,
            direction="to_remote",
        )
        base_context._connection.local.assert_called_once_with(
            expected_rsync_cmd, pty=False
        )
        assert result_ctx.remote_workspace_path == "/remote/"

    @patch("slurm_executor.pipeline.RSyncWorkspaceToRemote.compose_rsync_command")
    def test_run_from_remote_with_include_file(self, mock_compose_rsync, base_context):
        """Test successful run with from_remote direction and include file."""
        # Arrange
        step = RSyncWorkspace(
            local_root="/local/",
            remote_root="/remote/",
            direction="from_remote",
            include_only="include.txt",
        )
        expected_rsync_cmd = (
            "rsync -e 'ssh -p 2222' --delete --info=progress2 -az "
            "--files-from=include.txt test-user@test-host.com:/remote/ /local/"
        )
        mock_compose_rsync.return_value = expected_rsync_cmd

        # Act
        result_ctx = step.run(base_context)

        # Assert
        base_context._connection.run.assert_called_once_with(
            "mkdir -p /remote/", pty=False
        )
        mock_compose_rsync.assert_called_once_with(
            port=2222,
            user="test-user",
            host="test-host.com",
            local_root="/local/",
            remote_root="/remote/",
            exclusion_file=None,
            inclusion_file="include.txt",
            direction="from_remote",
        )
        base_context._connection.local.assert_called_once_with(
            expected_rsync_cmd, pty=False
        )
        assert result_ctx.remote_workspace_path == "/remote/"

    @patch("slurm_executor.pipeline.RSyncWorkspaceToRemote.compose_rsync_command")
    def test_run_mkdir_failure(self, mock_compose_rsync, base_context):
        """Test handling of mkdir failure."""
        # Arrange
        step = RSyncWorkspace(
            local_root="/local/",
            remote_root="/remote/",
            direction="to_remote",
            exclude_from="exclude.txt",
        )
        base_context._connection.run.side_effect = Exception("Permission denied")

        # Act & Assert
        with pytest.raises(Exception, match="Permission denied"):
            step.run(base_context)

        base_context._connection.run.assert_called_once_with(
            "mkdir -p /remote/", pty=False
        )
        mock_compose_rsync.assert_not_called()
        base_context._connection.local.assert_not_called()

    @patch("slurm_executor.pipeline.RSyncWorkspaceToRemote.compose_rsync_command")
    def test_run_rsync_failure(self, mock_compose_rsync, base_context):
        """Test handling of rsync failure."""
        # Arrange
        step = RSyncWorkspace(
            local_root="/local/",
            remote_root="/remote/",
            direction="to_remote",
            exclude_from="exclude.txt",
        )
        mock_compose_rsync.return_value = "rsync command"
        base_context._connection.local.side_effect = Exception("rsync failed")

        # Act & Assert
        with pytest.raises(Exception, match="rsync failed"):
            step.run(base_context)

        base_context._connection.run.assert_called_once_with(
            "mkdir -p /remote/", pty=False
        )
        base_context._connection.local.assert_called_once_with(
            "rsync command", pty=False
        )

    @patch("slurm_executor.pipeline.RSyncWorkspaceToRemote.compose_rsync_command")
    def test_run_sets_context_remote_workspace_path(
        self, mock_compose_rsync, base_context
    ):
        """Test that run method correctly sets remote_workspace_path in context."""
        # Arrange
        step = RSyncWorkspace(
            local_root="/some/local/path/",
            remote_root="/some/remote/path/",
            direction="to_remote",
            exclude_from="exclude.txt",
        )
        mock_compose_rsync.return_value = "rsync command"

        # Act
        result_ctx = step.run(base_context)

        # Assert
        assert result_ctx.remote_workspace_path == "/some/remote/path/"
        assert result_ctx is base_context  # Should return the same context object

    def test_run_uses_connection_config_values(self, base_context):
        """Test that run method uses values from connection config correctly."""
        # Arrange
        step = RSyncWorkspace(
            local_root="/local/",
            remote_root="/remote/",
            direction="to_remote",
            exclude_from="exclude.txt",
        )

        # Modify connection config to test different values
        base_context.connection_config.host = "custom-host.example.com"
        base_context.connection_config.user = "custom-user"
        base_context.connection_config.port = 9999

        mock_module = (
            "slurm_executor.pipeline.RSyncWorkspaceToRemote.compose_rsync_command"
        )
        with patch(mock_module) as mock_compose_rsync:
            mock_compose_rsync.return_value = "rsync command"

            # Act
            step.run(base_context)

            # Assert
            mock_compose_rsync.assert_called_once_with(
                port=9999,
                user="custom-user",
                host="custom-host.example.com",
                local_root="/local/",
                remote_root="/remote/",
                exclusion_file="exclude.txt",
                inclusion_file=None,
                direction="to_remote",
            )

    @pytest.mark.parametrize(
        "local_root,remote_root,expected_local,expected_remote",
        [
            (
                "/path/without/slash",
                "/remote/without/slash",
                "/path/without/slash/",
                "/remote/without/slash/",
            ),
            (
                "/path/with/slash/",
                "/remote/with/slash/",
                "/path/with/slash/",
                "/remote/with/slash/",
            ),
            ("relative/path", "remote/relative", "relative/path/", "remote/relative/"),
        ],
    )
    def test_path_normalization_scenarios(
        self, local_root, remote_root, expected_local, expected_remote
    ):
        """Test various path normalization scenarios."""
        # Arrange & Act
        with patch("slurm_executor.pipeline.RSyncWorkspaceToRemote.logger"):
            step = RSyncWorkspace(
                local_root=local_root,
                remote_root=remote_root,
                direction="to_remote",
                exclude_from="exclude.txt",
            )

        # Assert
        assert step.local_root == expected_local
        assert step.remote_root == expected_remote
