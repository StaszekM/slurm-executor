import importlib.resources as pkg_resources
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest

from slurm_executor.cli.commands.init import run_init


class TestRunInit:
    """Test suite for run_init function."""

    @pytest.fixture
    def mock_git_root(self):
        """Mock git root directory."""
        return Path("/mock/git/root")

    @pytest.fixture
    def app_repo(self, git_repo):
        path = git_repo.workspace
        file = path / "hello.txt"
        file.write_text("hello world!")

        # We can run commands relative to the working directory
        git_repo.run("git add hello.txt")

        # It's better to use the GitPython api directly - the 'api' attribute is
        # a handle to the repository object.

        git_repo.api.index.commit("Initial commit")

        return git_repo

    @pytest.fixture
    def template_content(self):
        """Sample template content for testing."""
        return """#!/bin/bash
#SBATCH --partition={{partition}}
#SBATCH --time={{time}}

# Test template content
uv sync
uv run - <<'EOF'
from slurm_executor.executor.CloudpickleExecutor import CloudpickleExecutor
executor = CloudpickleExecutor(deserialize_from="{{remote_call_location}}")
executor.run()
EOF
"""

    def test_run_init_creates_template_file_when_not_exists(
        self,
        app_repo,
        capsys,
        monkeypatch,
    ):
        """Test that run_init creates template file when it doesn't exist."""
        # Arrange - change to the git repo directory
        git_root = Path(app_repo.workspace)
        monkeypatch.chdir(git_root)

        destination_file = git_root / "sbatch_script.jinja"

        # Ensure the destination file doesn't exist initially
        assert not destination_file.exists()

        # Act
        run_init(force=False)

        # Assert
        # Verify the file was created
        assert destination_file.exists()

        # Verify the file content contains expected SLURM directives
        content = destination_file.read_text()
        assert "#!/bin/bash" in content
        assert "#SBATCH --partition={{partition}}" in content
        assert "#SBATCH --time={{time}}" in content
        assert "CloudpickleExecutor" in content
        assert "{{remote_call_location}}" in content
        assert "uv sync" in content

        # Verify output messages
        captured = capsys.readouterr()
        assert f"Creating SBATCH script template at {destination_file}." in captured.out
        assert f"SBATCH script template created at {destination_file}." in captured.out

    @patch("slurm_executor.cli.commands.init.get_git_root")
    @patch("slurm_executor.cli.commands.init.pkg_resources")
    @patch("builtins.open", new_callable=mock_open)
    def test_run_init_overwrites_when_force_true(
        self,
        mock_file_open,
        mock_pkg_resources,
        mock_get_git_root,
        mock_git_root,
        template_content,
        capsys,
    ):
        """Test that run_init overwrites existing file when force=True."""
        # Arrange
        mock_get_git_root.return_value = mock_git_root

        # Mock the template file reading using mock_open context manager
        template_mock = mock_open(read_data=template_content)
        mock_pkg_resources.files.return_value.joinpath.return_value.open = template_mock

        destination_file = mock_git_root / "sbatch_script.jinja"

        # Mock that file exists
        with patch.object(Path, "exists", return_value=True):
            # Act
            run_init(force=True)

        # Assert
        mock_get_git_root.assert_called_once()
        mock_pkg_resources.files.assert_called_once_with("slurm_executor.templates")

        # Verify file was written with correct content
        mock_file_open.assert_called_once_with(destination_file, "w")
        mock_file_open().write.assert_called_once_with(template_content)

        # Verify output messages (should still create the file)
        captured = capsys.readouterr()
        assert f"Creating SBATCH script template at {destination_file}." in captured.out
        assert f"SBATCH script template created at {destination_file}." in captured.out

    @patch("slurm_executor.cli.commands.init.get_git_root")
    @patch("slurm_executor.cli.commands.init.pkg_resources")
    @patch("builtins.open", new_callable=mock_open)
    def test_run_init_warns_when_file_exists_and_force_false(
        self,
        mock_file_open,
        mock_pkg_resources,
        mock_get_git_root,
        mock_git_root,
        template_content,
        capsys,
    ):
        """Test that run_init shows warning and doesn't overwrite when file exists and force=False."""
        # Arrange
        mock_get_git_root.return_value = mock_git_root

        # Mock the template file reading using mock_open context manager
        template_mock = mock_open(read_data=template_content)
        mock_pkg_resources.files.return_value.joinpath.return_value.open = template_mock

        destination_file = mock_git_root / "sbatch_script.jinja"

        # Mock that file exists
        with patch.object(Path, "exists", return_value=True):
            # Act
            run_init(force=False)

        # Assert
        mock_get_git_root.assert_called_once()
        mock_pkg_resources.files.assert_called_once_with("slurm_executor.templates")

        # Verify file was NOT written
        mock_file_open.assert_not_called()

        # Verify warning message
        captured = capsys.readouterr()
        assert f"Warning: {destination_file} already exists" in captured.out
        assert "--force" in captured.out and "overwrite" in captured.out

    @patch("slurm_executor.cli.commands.init.get_git_root")
    @patch("slurm_executor.cli.commands.init.pkg_resources")
    def test_run_init_reads_correct_template_file(
        self, mock_pkg_resources, mock_get_git_root, mock_git_root
    ):
        """Test that run_init reads the correct template file from package resources."""
        # Arrange
        mock_get_git_root.return_value = mock_git_root

        template_mock = mock_open(read_data="template content")
        mock_pkg_resources.files.return_value.joinpath.return_value.open = template_mock

        # Mock that file doesn't exist
        with (
            patch.object(Path, "exists", return_value=False),
            patch("builtins.open", mock_open()),
        ):
            # Act
            run_init(force=False)

        # Assert - verify correct package and template file are accessed
        mock_pkg_resources.files.assert_called_once_with("slurm_executor.templates")
        mock_pkg_resources.files.return_value.joinpath.assert_called_once_with(
            "basic_sbatch_template.jinja"
        )

    @patch("slurm_executor.cli.commands.init.get_git_root")
    def test_run_init_uses_git_root_for_destination(
        self, mock_get_git_root, mock_git_root
    ):
        """Test that run_init uses git root directory for destination file."""
        # Arrange
        mock_get_git_root.return_value = mock_git_root

        with (
            patch(
                "slurm_executor.cli.commands.init.pkg_resources"
            ) as mock_pkg_resources,
            patch.object(Path, "exists", return_value=False),
            patch("builtins.open", mock_open()) as mock_file_open,
        ):
            # Mock template file
            template_mock = mock_open(read_data="content")
            mock_pkg_resources.files.return_value.joinpath.return_value.open = (
                template_mock
            )

            # Act
            run_init(force=False)

        # Assert
        mock_get_git_root.assert_called_once()
        expected_destination = mock_git_root / "sbatch_script.jinja"
        mock_file_open.assert_called_once_with(expected_destination, "w")

    def test_run_init_with_real_template_content(self):
        """Integration test that verifies the actual template content can be read."""
        # This test verifies that the template file actually exists and can be read
        # It's more of an integration test but helps ensure the package structure is correct

        # Act - try to read the actual template file
        with (
            pkg_resources.files("slurm_executor.templates")
            .joinpath("basic_sbatch_template.jinja")
            .open("r") as f
        ):
            content = f.read()

        # Assert - verify template contains expected SLURM directives and structure
        assert "#!/bin/bash" in content
        assert "#SBATCH --partition={{partition}}" in content
        assert "#SBATCH --time={{time}}" in content
        assert "CloudpickleExecutor" in content
        assert "{{remote_call_location}}" in content
        assert "uv sync" in content

    @patch("slurm_executor.cli.commands.init.Path")
    @patch("slurm_executor.cli.commands.init.get_git_root")
    def test_run_init_uses_current_file_location_for_git_root(
        self, mock_get_git_root, mock_path_class
    ):
        """Test that run_init uses the current file's location to find git root."""
        # Arrange
        mock_cli_location = MagicMock()
        mock_path_class.return_value = mock_cli_location
        mock_get_git_root.return_value = Path("/some/git/root")

        with (
            patch(
                "slurm_executor.cli.commands.init.pkg_resources"
            ) as mock_pkg_resources,
            patch.object(Path, "exists", return_value=False),
            patch("builtins.open", mock_open()),
        ):
            # Mock template file
            template_mock = mock_open(read_data="content")
            mock_pkg_resources.files.return_value.joinpath.return_value.open = (
                template_mock
            )

            # Act
            run_init(force=False)

        # Assert
        # Verify that Path.cwd() was called to get the CLI location
        mock_path_class.cwd.assert_called_once()
        # Verify that get_git_root was called with the CLI location
        mock_get_git_root.assert_called_once_with(mock_path_class.cwd())
