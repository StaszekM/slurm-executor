import importlib.resources as pkg_resources
from pathlib import Path

import pytest

import slurm_executor.cli.commands.init
from slurm_executor.cli.commands.init import run_init
from slurm_executor.utils import get_git_root


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

    def test_run_init_overwrites_when_force_true(
        self,
        app_repo,
        capsys,
        monkeypatch,
    ):
        """Test that run_init overwrites existing file with basic_sbatch_template.jinja
        when force=True."""
        # Arrange
        git_root = Path(app_repo.workspace)
        monkeypatch.chdir(git_root)

        destination_file = git_root / "sbatch_script.jinja"

        # Ensure the destination file doesn't exist initially
        assert not destination_file.exists()
        destination_file.write_text("Old content")
        assert destination_file.exists()

        with (
            pkg_resources.files("slurm_executor.templates")
            .joinpath("basic_sbatch_template.jinja")
            .open("r") as template_file
        ):
            expected_template_content = template_file.read()

        # Act
        run_init(force=True)

        # Assert
        # Verify the file was overwritten with new content
        assert destination_file.exists()
        content = destination_file.read_text()
        assert content == expected_template_content

        # Verify output messages (should still create the file)
        captured = capsys.readouterr()
        assert f"Creating SBATCH script template at {destination_file}." in captured.out
        assert f"SBATCH script template created at {destination_file}." in captured.out

    def test_run_init_warns_when_file_exists_and_force_false(
        self,
        app_repo,
        capsys,
        monkeypatch,
    ):
        """Test that run_init shows warning and doesn't overwrite
        when file exists and force=False."""
        # Arrange
        git_root = Path(app_repo.workspace)
        monkeypatch.chdir(git_root)

        destination_file = git_root / "sbatch_script.jinja"

        # Ensure the destination file doesn't exist initially
        assert not destination_file.exists()
        destination_file.write_text("Old content")
        assert destination_file.exists()

        run_init(force=False)

        assert destination_file.read_text() == "Old content"

        # Verify warning message
        captured = capsys.readouterr()
        assert f"Warning: {destination_file} already exists" in captured.out
        assert "--force" in captured.out and "overwrite" in captured.out

    def test_get_git_root_identifies_git_root(
        self,
        app_repo,
        monkeypatch,
    ):
        """Test that get_git_root correctly identifies git root for destination file,
        when called from subfolder."""
        # Arrange
        git_root = Path(app_repo.workspace)
        git_subdir = git_root / "subdir"
        git_subdir.mkdir()
        monkeypatch.chdir(git_subdir)

        result = get_git_root(Path.cwd())

        assert result == git_root

    def test_get_git_root_raises_error_when_not_in_git_repo(
        self, app_repo, monkeypatch, capsys
    ):
        """Test that run_init prints error when not in a git repository."""
        # Arrange
        git_root = Path(app_repo.workspace)
        git_subdir = git_root / "subdir"
        git_subdir.mkdir()
        git_parentdir = git_root.parent
        monkeypatch.chdir(git_parentdir)

        # Act
        run_init(force=False)

        # Verify error message
        captured = capsys.readouterr()
        assert (
            "Error: Current directory is not inside a git repository." in captured.out
        )

    def test_run_init_with_real_template_content(self):
        """Test that the actual template content can be read."""
        # This test verifies that the template file actually exists and can be read
        # It's more of an integration test but helps ensure the package structure
        # is correct

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

    def test_run_init_uses_current_file_location_for_git_root(
        self, app_repo, monkeypatch, mocker
    ):
        """Test that run_init uses the Path.cwd() when calling get_git_root"""
        spy = mocker.spy(slurm_executor.cli.commands.init, "get_git_root")
        git_root = Path(app_repo.workspace)
        monkeypatch.chdir(git_root)

        # check that the run_init called Path.cwd()
        run_init(force=False)

        spy.assert_called_once_with(Path.cwd())
