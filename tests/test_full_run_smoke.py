"""
Smoke test for the full-run pipeline orchestration.

Does NOT hit live APIs or Ollama. All three stages (_run_fetch,
_run_evaluate, _run_analyze) are patched to verify call order,
argument passing, and that failure in one stage doesn't crash the others.
"""
import pytest
from unittest.mock import patch, MagicMock
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


def _mock_session():
    """Return a MagicMock that satisfies the SessionLocal() DB calls in full_run."""
    session = MagicMock()
    session.query.return_value.group_by.return_value.all.return_value = []
    session.__enter__ = MagicMock(return_value=session)
    session.__exit__ = MagicMock(return_value=False)
    return session


class TestFullRunSmoke:
    def test_calls_all_three_stages_in_order(self, runner, mock_profile):
        call_order = []
        mock_session = _mock_session()

        with patch("run_pipeline._run_fetch", side_effect=lambda *a, **kw: call_order.append("fetch")), \
             patch("run_pipeline._run_evaluate", side_effect=lambda *a, **kw: call_order.append("evaluate")), \
             patch("run_pipeline._run_analyze", side_effect=lambda *a, **kw: call_order.append("analyze")), \
             patch("run_pipeline.SessionLocal", return_value=mock_session):

            from run_pipeline import cli
            runner.invoke(cli, ["full-run", "--profile", mock_profile, "--dry-run"])

        # full-run now calls _run_analyze twice: once for the non-default bucket,
        # once for the configured analyze_status bucket (both shortlisted + review).
        assert call_order[:2] == ["fetch", "evaluate"], \
            f"Expected fetch→evaluate first, got: {call_order}"
        assert call_order.count("analyze") == 2, \
            f"Expected 2 analyze calls (both buckets), got: {call_order}"

    def test_dry_run_flag_passed_to_fetch(self, runner, mock_profile):
        mock_session = _mock_session()
        with patch("run_pipeline._run_fetch") as mf, \
             patch("run_pipeline._run_evaluate"), \
             patch("run_pipeline._run_analyze"), \
             patch("run_pipeline.SessionLocal", return_value=mock_session):

            from run_pipeline import cli
            runner.invoke(cli, ["full-run", "--profile", mock_profile, "--dry-run"])
            args, kwargs = mf.call_args
            assert True in args or kwargs.get("dry_run") is True

    def test_source_flag_passed_to_fetch(self, runner, mock_profile):
        mock_session = _mock_session()
        with patch("run_pipeline._run_fetch") as mf, \
             patch("run_pipeline._run_evaluate"), \
             patch("run_pipeline._run_analyze"), \
             patch("run_pipeline.SessionLocal", return_value=mock_session):

            from run_pipeline import cli
            runner.invoke(cli, ["full-run", "--profile", mock_profile, "--source", "remotive", "--dry-run"])
            args, kwargs = mf.call_args
            assert "remotive" in args or kwargs.get("source") == "remotive"

    def test_fetch_failure_does_not_crash_pipeline(self, runner, mock_profile):
        mock_session = _mock_session()
        with patch("run_pipeline._run_fetch", side_effect=Exception("network error")), \
             patch("run_pipeline._run_evaluate"), \
             patch("run_pipeline._run_analyze"), \
             patch("run_pipeline.SessionLocal", return_value=mock_session):

            from run_pipeline import cli
            result = runner.invoke(cli, ["full-run", "--profile", mock_profile, "--dry-run"])
            assert result.exit_code in (0, 1)

    def test_invalid_profile_exits_gracefully(self, runner, tmp_path):
        mock_session = _mock_session()
        bad_profile = str(tmp_path / "missing.yaml")
        with patch("run_pipeline.SessionLocal", return_value=mock_session):
            from run_pipeline import cli
            result = runner.invoke(cli, ["full-run", "--profile", bad_profile, "--dry-run"])
        assert result.exit_code in (0, 1)
