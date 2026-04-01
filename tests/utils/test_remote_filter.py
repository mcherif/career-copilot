"""
Tests for utils/remote_filter.py — classify_remote_eligibility()

This is the highest-risk logic layer: wrong rejections lose real jobs,
wrong accepts waste LLM quota on ineligible jobs.
"""
import pytest
from utils.remote_filter import classify_remote_eligibility


def _job(location="", description=""):
    return {
        "raw_location_text": location,
        "location": location,
        "description_text": description,
        "description": description,
    }


def _profile(accepted=None, rejected=None, remote_only=True, work_auth=None):
    return {
        "preferences": {
            "remote_only": remote_only,
            "accepted_regions": accepted or ["worldwide", "global", "emea", "europe", "canada"],
            "reject_regions": rejected or ["us only"],
        },
        "work_authorization": work_auth or {"canada": True},
    }


PROFILE = _profile()


# ---------------------------------------------------------------------------
# Hard rejects — US-only
# ---------------------------------------------------------------------------

class TestUSOnlyRejects:
    def test_exact_us_location(self):
        assert classify_remote_eligibility(_job("us"), PROFILE) == "reject"

    def test_exact_usa_location(self):
        assert classify_remote_eligibility(_job("usa"), PROFILE) == "reject"

    def test_exact_united_states(self):
        assert classify_remote_eligibility(_job("united states"), PROFILE) == "reject"

    def test_greenhouse_us_remote(self):
        assert classify_remote_eligibility(_job("us-remote"), PROFILE) == "reject"

    def test_greenhouse_us_east(self):
        assert classify_remote_eligibility(_job("us-east"), PROFILE) == "reject"

    def test_greenhouse_us_west(self):
        assert classify_remote_eligibility(_job("us-west"), PROFILE) == "reject"

    def test_remote_united_states(self):
        assert classify_remote_eligibility(_job("remote - united states"), PROFILE) == "reject"

    def test_remote_us_parenthetical(self):
        assert classify_remote_eligibility(_job("remote (us)"), PROFILE) == "reject"

    def test_remote_usdot_parenthetical(self):
        assert classify_remote_eligibility(_job("remote (u.s.)"), PROFILE) == "reject"

    def test_description_us_only_keyword(self):
        assert classify_remote_eligibility(_job("remote", "must reside in the us"), PROFILE) == "reject"

    def test_description_security_clearance(self):
        assert classify_remote_eligibility(_job("remote", "security clearance required"), PROFILE) == "reject"

    def test_description_north_america_only(self):
        assert classify_remote_eligibility(_job("remote", "north america only"), PROFILE) == "reject"

    def test_profile_reject_region_in_description(self):
        profile = _profile(rejected=["latam only"])
        assert classify_remote_eligibility(_job("remote", "latam only"), profile) == "reject"

    def test_hybrid_with_remote_only_profile(self):
        assert classify_remote_eligibility(_job("hybrid"), PROFILE) == "reject"


# ---------------------------------------------------------------------------
# Hard accepts — worldwide / accepted regions
# ---------------------------------------------------------------------------

class TestAccepts:
    def test_worldwide_location(self):
        assert classify_remote_eligibility(_job("worldwide"), PROFILE) == "accept"

    def test_global_location(self):
        assert classify_remote_eligibility(_job("global"), PROFILE) == "accept"

    def test_anywhere_location(self):
        assert classify_remote_eligibility(_job("anywhere"), PROFILE) == "accept"

    def test_work_from_anywhere_in_description(self):
        assert classify_remote_eligibility(_job("remote", "work from anywhere"), PROFILE) == "accept"

    def test_remote_worldwide_in_description(self):
        assert classify_remote_eligibility(_job("remote", "remote worldwide"), PROFILE) == "accept"

    def test_emea_location(self):
        assert classify_remote_eligibility(_job("emea"), PROFILE) == "accept"

    def test_europe_location(self):
        assert classify_remote_eligibility(_job("europe"), PROFILE) == "accept"

    def test_canada_location(self):
        assert classify_remote_eligibility(_job("canada"), PROFILE) == "accept"

    def test_remote_canada_dash_pattern(self):
        assert classify_remote_eligibility(_job("remote - canada"), PROFILE) == "accept"


# ---------------------------------------------------------------------------
# Review — ambiguous cases
# ---------------------------------------------------------------------------

class TestReview:
    def test_plain_remote_is_review(self):
        assert classify_remote_eligibility(_job("remote"), PROFILE) == "review"

    def test_remote_us_or_canada(self):
        # Contains US substring but also an accepted region — should be review not reject
        result = classify_remote_eligibility(_job("remote (us or canada)"), PROFILE)
        assert result == "review"

    def test_americas_mixed_region(self):
        # "americas" is a mixed region hint
        result = classify_remote_eligibility(_job("americas"), PROFILE)
        assert result in ("review", "reject")  # acceptable either way, not accept

    def test_no_location_no_description(self):
        result = classify_remote_eligibility(_job("", ""), PROFILE)
        assert result == "review"


# ---------------------------------------------------------------------------
# Remote - [Country] pattern
# ---------------------------------------------------------------------------

class TestRemoteCountryPattern:
    def test_remote_india_rejected(self):
        assert classify_remote_eligibility(_job("remote - india"), PROFILE) == "reject"

    def test_remote_brazil_rejected(self):
        assert classify_remote_eligibility(_job("remote - brazil"), PROFILE) == "reject"

    def test_remote_europe_accepted(self):
        assert classify_remote_eligibility(_job("remote - europe"), PROFILE) == "accept"

    def test_remote_worldwide_accepted(self):
        assert classify_remote_eligibility(_job("remote - worldwide"), PROFILE) == "accept"


# ---------------------------------------------------------------------------
# No profile (profile=None)
# ---------------------------------------------------------------------------

class TestNoProfile:
    def test_us_still_rejected_without_profile(self):
        assert classify_remote_eligibility(_job("us"), None) == "reject"

    def test_worldwide_still_accepted_without_profile(self):
        assert classify_remote_eligibility(_job("worldwide"), None) == "accept"

    def test_plain_remote_review_without_profile(self):
        assert classify_remote_eligibility(_job("remote"), None) == "review"


# ---------------------------------------------------------------------------
# Purely geographic locations (no remote hint)
# ---------------------------------------------------------------------------

class TestGeographicOnly:
    def test_city_only_rejected(self):
        assert classify_remote_eligibility(_job("seoul"), PROFILE) == "reject"

    def test_country_not_in_accepted_rejected(self):
        assert classify_remote_eligibility(_job("south africa"), PROFILE) == "reject"
