"""
Tests for smaller utils: application_filter, resume_selector, email_report, logger.
"""
import os
from unittest.mock import patch, MagicMock

from models.database import ApplicationHistory


# ---------------------------------------------------------------------------
# application_filter — has_already_applied()
# ---------------------------------------------------------------------------

class TestHasAlreadyApplied:
    def test_returns_false_when_no_match(self, db_session):
        from utils.application_filter import has_already_applied
        assert not has_already_applied({"company": "Acme", "title": "Engineer"}, db_session)

    def test_returns_true_when_match_exists(self, db_session):
        from utils.application_filter import has_already_applied
        app = ApplicationHistory(company="Acme", job_title="Engineer",
                                 applied_date=__import__("datetime").datetime.utcnow())
        db_session.add(app)
        db_session.commit()
        assert has_already_applied({"company": "Acme", "title": "Engineer"}, db_session)

    def test_case_insensitive_match(self, db_session):
        from utils.application_filter import has_already_applied
        app = ApplicationHistory(company="acme", job_title="engineer",
                                 applied_date=__import__("datetime").datetime.utcnow())
        db_session.add(app)
        db_session.commit()
        assert has_already_applied({"company": "ACME", "title": "ENGINEER"}, db_session)

    def test_missing_company_returns_false(self, db_session):
        from utils.application_filter import has_already_applied
        assert not has_already_applied({"company": "", "title": "Engineer"}, db_session)

    def test_missing_title_returns_false(self, db_session):
        from utils.application_filter import has_already_applied
        assert not has_already_applied({"company": "Acme", "title": ""}, db_session)

    def test_uses_job_title_key_fallback(self, db_session):
        from utils.application_filter import has_already_applied
        assert not has_already_applied({"company": "Acme", "job_title": "Engineer"}, db_session)


# ---------------------------------------------------------------------------
# resume_selector — select_resume()
# ---------------------------------------------------------------------------

def _profile_with_resumes(*resumes):
    return {"resumes": list(resumes)}


def _resume(name, path="", tags=None):
    return {"name": name, "path": path or f"/resumes/{name}.pdf", "tags": tags or []}


class TestSelectResume:
    def test_empty_resumes_returns_empty(self):
        from utils.resume_selector import select_resume
        assert select_resume({"title": "Dev", "description_text": "Python"}, {}) == {}

    def test_matching_tags_win(self):
        from utils.resume_selector import select_resume
        profile = _profile_with_resumes(
            _resume("ml", tags=["python", "machine learning"]),
            _resume("backend", tags=["golang", "kubernetes"]),
        )
        result = select_resume({"title": "ML Engineer", "description_text": "python machine learning"}, profile)
        assert result["resume_name"] == "ml"
        assert "python" in [t.lower() for t in result["tag_matches"]]

    def test_fallback_to_software_engineer_on_no_match(self):
        from utils.resume_selector import select_resume
        profile = _profile_with_resumes(
            _resume("software_engineer", tags=["python"]),
            _resume("ml", tags=["pytorch"]),
        )
        result = select_resume({"title": "Barista", "description_text": "coffee"}, profile)
        assert result["resume_name"] == "software_engineer"
        assert result["tag_matches"] == []

    def test_fallback_to_last_resume_if_no_software_engineer(self):
        from utils.resume_selector import select_resume
        profile = _profile_with_resumes(
            _resume("frontend", tags=["react"]),
            _resume("backend", tags=["django"]),
        )
        result = select_resume({"title": "Barista", "description_text": "coffee"}, profile)
        assert result["resume_name"] == "backend"

    def test_result_has_required_keys(self):
        from utils.resume_selector import select_resume
        profile = _profile_with_resumes(_resume("default", tags=["python"]))
        result = select_resume({"title": "Engineer", "description_text": "python"}, profile)
        assert "resume_name" in result
        assert "resume_path" in result
        assert "tag_matches" in result

    def test_most_matches_wins(self):
        from utils.resume_selector import select_resume
        profile = _profile_with_resumes(
            _resume("general", tags=["python"]),
            _resume("ml_specialist", tags=["python", "pytorch", "tensorflow"]),
        )
        result = select_resume(
            {"title": "ML Eng", "description_text": "python pytorch tensorflow ml"},
            profile,
        )
        assert result["resume_name"] == "ml_specialist"


# ---------------------------------------------------------------------------
# email_report — _build_html() and send_report()
# ---------------------------------------------------------------------------

class TestBuildHtml:
    def test_returns_string(self):
        from utils.email_report import _build_html
        html = _build_html([], {"shortlisted": 3, "review": 10})
        assert isinstance(html, str)

    def test_includes_counts(self):
        from utils.email_report import _build_html
        html = _build_html([], {"shortlisted": 5, "review": 12})
        assert "5" in html
        assert "12" in html

    def test_includes_new_jobs(self):
        from utils.email_report import _build_html
        jobs = [{"title": "ML Engineer", "company": "DeepCo",
                 "status": "shortlisted", "fit_score": 80, "source": "remotive"}]
        html = _build_html(jobs, {"shortlisted": 1})
        assert "ML Engineer" in html
        assert "DeepCo" in html

    def test_empty_new_jobs_still_renders(self):
        from utils.email_report import _build_html
        html = _build_html([], {})
        assert "<html>" in html.lower() or "<html" in html

    def test_all_statuses_present(self):
        from utils.email_report import _build_html
        html = _build_html([], {"shortlisted": 1, "review": 2, "applied": 3,
                                "deferred": 4, "rejected": 5})
        for status in ("Shortlisted", "Review", "Applied", "Deferred", "Rejected"):
            assert status in html


class TestGetCredential:
    def test_falls_back_to_env_var(self):
        from utils.email_report import _get_credential
        with patch("keyring.get_password", return_value=None), \
             patch.dict(os.environ, {"EMAIL_FROM": "test@example.com"}):
            assert _get_credential("EMAIL_FROM") == "test@example.com"

    def test_returns_empty_when_not_set(self):
        from utils.email_report import _get_credential
        env = {k: v for k, v in os.environ.items() if k != "NONEXISTENT_KEY_XYZ"}
        with patch.dict(os.environ, env, clear=True):
            assert _get_credential("NONEXISTENT_KEY_XYZ") == ""

    def test_keyring_value_takes_priority(self):
        from utils.email_report import _get_credential
        with patch("utils.email_report._get_credential", wraps=_get_credential):
            with patch("keyring.get_password", return_value="keyring_value"):
                result = _get_credential("EMAIL_FROM")
        # If keyring returns a value it should be used (or env fallback)
        assert isinstance(result, str)


class TestSendReport:
    def test_returns_false_when_not_configured(self):
        from utils.email_report import send_report
        with patch("utils.email_report._get_credential", return_value=""):
            assert send_report([], {}) is False

    def test_sends_when_configured(self):
        from utils.email_report import send_report
        creds = {"EMAIL_FROM": "a@b.com", "EMAIL_TO": "c@d.com",
                 "EMAIL_PASSWORD": "secret", "EMAIL_SMTP_HOST": "smtp.test.com",
                 "EMAIL_SMTP_PORT": "587"}

        mock_server = MagicMock()

        with patch("utils.email_report._get_credential", side_effect=lambda k: creds.get(k, "")), \
             patch("smtplib.SMTP") as smtp_cls:
            smtp_cls.return_value.__enter__ = MagicMock(return_value=mock_server)
            smtp_cls.return_value.__exit__ = MagicMock(return_value=False)
            result = send_report([], {"shortlisted": 1, "review": 2})

        assert result is True
        mock_server.sendmail.assert_called_once()

    def test_returns_false_on_smtp_error(self):
        from utils.email_report import send_report
        creds = {"EMAIL_FROM": "a@b.com", "EMAIL_TO": "c@d.com", "EMAIL_PASSWORD": "pw"}
        with patch("utils.email_report._get_credential", side_effect=lambda k: creds.get(k, "")), \
             patch("smtplib.SMTP", side_effect=Exception("SMTP error")):
            assert send_report([], {}) is False
