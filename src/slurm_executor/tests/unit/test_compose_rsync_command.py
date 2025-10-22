"""Unit tests for compose_rsync_command function."""

import pytest

from slurm_executor.utils.compose_rsync_command import compose_rsync_command


class TestComposeRsyncCommand:
    """Test suite for compose_rsync_command function."""

    def test_compose_rsync_command_to_remote_with_exclusion(self):
        """Test rsync command composition for to_remote with exclusion file."""
        # Arrange
        port = 22
        user = "testuser"
        host = "example.com"
        local_root = "/local/path/"
        remote_root = "/remote/path/"
        direction = "to_remote"
        exclusion_file = "exclude.txt"

        # Act
        result = compose_rsync_command(
            port=port,
            user=user,
            host=host,
            local_root=local_root,
            remote_root=remote_root,
            direction=direction,
            exclusion_file=exclusion_file,
        )

        # Assert
        expected_parts = [
            "rsync -e 'ssh -p 22'",
            "--delete",
            "--info=progress2",
            "-az",
            "--exclude-from=exclude.txt",
            "/local/path/",
            "testuser@example.com:/remote/path/",
        ]
        for part in expected_parts:
            assert part in result

    def test_compose_rsync_command_from_remote_with_inclusion(self):
        """Test rsync command composition for from_remote with inclusion file."""
        # Arrange
        port = 2222
        user = "remoteuser"
        host = "server.example.org"
        local_root = "/local/dest/"
        remote_root = "/remote/source/"
        direction = "from_remote"
        inclusion_file = "include.txt"

        # Act
        result = compose_rsync_command(
            port=port,
            user=user,
            host=host,
            local_root=local_root,
            remote_root=remote_root,
            direction=direction,
            inclusion_file=inclusion_file,
        )

        # Assert
        expected_parts = [
            "rsync -e 'ssh -p 2222'",
            "--delete",
            "--info=progress2",
            "-az",
            "--files-from=include.txt",
            "remoteuser@server.example.org:/remote/source/",
            "/local/dest/",
        ]
        for part in expected_parts:
            assert part in result

    def test_compose_rsync_command_to_remote_no_filters(self):
        """Test rsync command composition to_remote without filter files."""
        # Arrange
        port = 22
        user = "user"
        host = "host.com"
        local_root = "/src/"
        remote_root = "/dst/"
        direction = "to_remote"

        # Act
        result = compose_rsync_command(
            port=port,
            user=user,
            host=host,
            local_root=local_root,
            remote_root=remote_root,
            direction=direction,
        )

        # Assert
        expected_parts = [
            "rsync -e 'ssh -p 22'",
            "--delete",
            "--info=progress2",
            "-az",
            "/src/",
            "user@host.com:/dst/",
        ]
        for part in expected_parts:
            assert part in result

        # Should not contain filter options
        assert "--exclude-from=" not in result
        assert "--files-from=" not in result

    def test_compose_rsync_command_from_remote_no_filters(self):
        """Test rsync command composition from_remote without filter files."""
        # Arrange
        port = 443
        user = "sshuser"
        host = "secure.example.net"
        local_root = "/backup/"
        remote_root = "/data/"
        direction = "from_remote"

        # Act
        result = compose_rsync_command(
            port=port,
            user=user,
            host=host,
            local_root=local_root,
            remote_root=remote_root,
            direction=direction,
        )

        # Assert
        expected_parts = [
            "rsync -e 'ssh -p 443'",
            "--delete",
            "--info=progress2",
            "-az",
            "sshuser@secure.example.net:/data/",
            "/backup/",
        ]
        for part in expected_parts:
            assert part in result

    @pytest.mark.parametrize(
        "direction,expected_order",
        [
            ("to_remote", ["local", "remote"]),
            ("from_remote", ["remote", "local"]),
        ],
    )
    def test_compose_rsync_command_direction_affects_path_order(
        self, direction, expected_order
    ):
        """Test that direction parameter affects the order of source and destination."""
        # Arrange
        local_root = "/local/path/"
        remote_root = "/remote/path/"

        # Act
        result = compose_rsync_command(
            port=22,
            user="user",
            host="host.com",
            local_root=local_root,
            remote_root=remote_root,
            direction=direction,
        )

        # Assert
        if expected_order == ["local", "remote"]:
            # to_remote: local comes first
            local_pos = result.find("/local/path/")
            remote_pos = result.find("user@host.com:/remote/path/")
            assert local_pos < remote_pos
        else:
            # from_remote: remote comes first
            local_pos = result.find("/local/path/")
            remote_pos = result.find("user@host.com:/remote/path/")
            assert remote_pos < local_pos

    @pytest.mark.parametrize("port", [22, 443, 2222, 8022])
    def test_compose_rsync_command_with_different_ports(self, port):
        """Test rsync command composition with different SSH ports."""
        # Arrange & Act
        result = compose_rsync_command(
            port=port,
            user="user",
            host="example.com",
            local_root="/local/",
            remote_root="/remote/",
            direction="to_remote",
        )

        # Assert
        assert f"ssh -p {port}" in result

    def test_compose_rsync_command_with_special_characters_in_paths(self):
        """Test rsync command with paths containing special characters."""
        # Arrange
        local_root = "/path with spaces/"
        remote_root = "/remote-path_with-symbols/"

        # Act
        result = compose_rsync_command(
            port=22,
            user="user",
            host="example.com",
            local_root=local_root,
            remote_root=remote_root,
            direction="to_remote",
        )

        # Assert
        assert local_root in result
        assert f"user@example.com:{remote_root}" in result

    def test_compose_rsync_command_with_both_exclusion_and_inclusion_none(self):
        """Test rsync command when both exclusion and inclusion are None."""
        # Act
        result = compose_rsync_command(
            port=22,
            user="user",
            host="example.com",
            local_root="/local/",
            remote_root="/remote/",
            direction="to_remote",
            exclusion_file=None,
            inclusion_file=None,
        )

        # Assert
        assert "--exclude-from=" not in result
        assert "--files-from=" not in result

    @pytest.mark.parametrize(
        "exclusion_file,inclusion_file",
        [
            ("exclude.txt", None),
            (None, "include.txt"),
            ("filters/exclude.list", None),
            (None, "filters/include.list"),
        ],
    )
    def test_compose_rsync_command_with_different_filter_files(
        self, exclusion_file, inclusion_file
    ):
        """Test rsync command with different filter file configurations."""
        # Act
        result = compose_rsync_command(
            port=22,
            user="user",
            host="example.com",
            local_root="/local/",
            remote_root="/remote/",
            direction="to_remote",
            exclusion_file=exclusion_file,
            inclusion_file=inclusion_file,
        )

        # Assert
        if exclusion_file:
            assert f"--exclude-from={exclusion_file}" in result
            assert "--files-from=" not in result
        if inclusion_file:
            assert f"--files-from={inclusion_file}" in result
            assert "--exclude-from=" not in result

    def test_compose_rsync_command_basic_structure(self):
        """Test that the rsync command has the expected basic structure."""
        # Act
        result = compose_rsync_command(
            port=22,
            user="user",
            host="example.com",
            local_root="/local/",
            remote_root="/remote/",
            direction="to_remote",
        )

        # Assert
        # Should start with rsync
        assert result.startswith("rsync")

        # Should contain required flags
        required_flags = ["-e", "--delete", "--info=progress2", "-az"]
        for flag in required_flags:
            assert flag in result

    def test_compose_rsync_command_returns_string(self):
        """Test that compose_rsync_command returns a string."""
        # Act
        result = compose_rsync_command(
            port=22,
            user="user",
            host="example.com",
            local_root="/local/",
            remote_root="/remote/",
            direction="to_remote",
        )

        # Assert
        assert isinstance(result, str)
        assert len(result) > 0
