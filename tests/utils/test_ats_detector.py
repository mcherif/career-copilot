import pytest
from utils.ats_detector import detect_ats


@pytest.mark.parametrize("url,expected", [
    ("https://boards.greenhouse.io/company/jobs/123", "greenhouse"),
    ("https://motional.com/open-positions#/6608351003/apply", "greenhouse"),
    ("https://motional.com/open-positions#/6608351003", "greenhouse"),
    ("https://lever.co/company/job/456", "lever"),
    ("https://yellowcard.bamboohr.com/careers/385", "bamboohr"),
    ("https://jobs.ashbyhq.com/company/job", "ashby"),
    ("https://careers.tether.io/o/some-slug/c/new", "comeet"),
    ("https://canonical.com/careers/5792361/application", "canonical"),
    ("https://canonical.com/careers/5792361", "unknown"),
    ("https://example.com/jobs/123", "unknown"),
])
def test_detect_ats(url, expected):
    assert detect_ats(url) == expected
