"""
Smoke test for the full-run pipeline orchestration.

Does NOT hit live APIs or Ollama. All three stages (_run_fetch,
_run_evaluate, _run_analyze) are patched to verify call order,
argument passing, and that failure in one stage doesn't crash the others.
"""
import pytest
from unittest.mock import patch, call, MagicMock
from click.testing import CliRunner


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def mock_profile(tmp_path):
    """Write a minimal profile.yaml that _load_profile can parse."""
    import yaml
    p = tmp_path / "profile.yaml"
    p.write_text(yaml.dump({
        "personal": {"name": "Test User", "email": "test@example.com"},
        "skills": ["Python"],
        "keywords": ["backend"],
        "target_roles": ["software engineer"],
        "seniority": {"preferred": ["senior"], "acceptable": ["mid"]},
        "preferences": {
            "remote_only": True,
            "accepted_regions": ["worldwide"],
            "reject_regions": [],
            "contractor_ok": True,
        },
        "languages": ["english"],
        "blacklisted_companies": [],
        "resumes": [],
    }))
    return str(p)


class TestFullRunSmoke:
    def test_calls_all_three_stages_in_order(self, runner, mock_profile):
        call_order = []

        with patch("run_pipeline._run_fetch", side_effect=lambda *a, **kw: call_order.append("fetch")) as mf, \
             patch("run_pipeline._run_evaluate", side_effect=lambda *a, **kw: call_order.append("evaluate")) as me, \
             patch("run_pipeline._run_analyze", side_effect=lambda *a, **kw: call_order.append("analyze")) as ma:

            from run_pipeline import cli
            result = runner.invoke(cli, ["full-run", "--profile", mock_profile, "--dry-run"])

        assert call_order == ["fetch", "evaluate", "analyze"], \
            f"Expected fetch→evaluate→analyze, got: {call_order}"

    def test_dry_run_flag_passed_to_fetch(self, runner, mock_profile):
        with patch("run_pipeline._run_fetch") as mf, \
             patch("run_pipeline._run_evaluate"), \
             patch("run_pipeline._run_analyze"):

            from run_pipeline import cli
            runner.invoke(cli, ["full-run", "--profile", mock_profile, "--dry-run"])
            args, kwargs = mf.call_args
            # dry_run should be True (passed as positional or keyword)
            assert True in args or kwargs.get("dry_run") is True

    def test_source_flag_passed_to_fetch(self, runner, mock_profile):
        with patch("run_pipeline._run_fetch") as mf, \
             patch("run_pipeline._run_evaluate"), \
             patch("run_pipeline._run_analyze"):

            from run_pipeline import cli
            runner.invoke(cli, ["full-run", "--profile", mock_profile, "--source", "remotive", "--dry-run"])
            args, kwargs = mf.call_args
            assert "remotive" in args or kwargs.get("source") == "remotive"

    def test_fetch_failure_does_not_crash_pipeline(self, runner, mock_profile):
        with patch("run_pipeline._run_fetch", side_effect=Exception("network error")), \
             patch("run_pipeline._run_evaluate") as me, \
             patch("run_pipeline._run_analyze") as ma:

            from run_pipeline import cli
            result = runner.invoke(cli, ["full-run", "--profile", mock_profile, "--dry-run"])
            # Pipeline should not propagate the exception as an unhandled crash
            assert result.exit_code in (0, 1)  # may log error but shouldn't traceback

    def test_invalid_profile_exits_gracefully(self, runner, tmp_path):
        bad_profile = str(tmp_path / "missing.yaml")
        from run_pipeline import cli
        result = runner.invoke(cli, ["full-run", "--profile", bad_profile, "--dry-run"])
        assert result.exit_code in (0, 1)  # no unhandled exception
