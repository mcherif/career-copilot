"""
Tests for utils/text_cleaning.py

clean_description() is called on every job before scoring and LLM analysis,
so regressions here silently corrupt the entire pipeline.
"""
import pytest
from utils.text_cleaning import strip_html, normalize_whitespace, clean_description


class TestStripHtml:
    def test_removes_basic_tags(self):
        assert "hello world" in strip_html("<p>hello world</p>")

    def test_removes_nested_tags(self):
        result = strip_html("<div><p><strong>text</strong></p></div>")
        assert "text" in result
        assert "<" not in result

    def test_unescapes_named_entities(self):
        assert "&" in strip_html("AT&amp;T")
        assert "©" in strip_html("&copy;")
        # &lt;code&gt; unescapes to <code> which regex strips as a tag — correct behavior.
        # A lone < that doesn't form a valid tag (no closing >) is preserved:
        assert "<" in strip_html("5 &lt; 10")

    def test_unescapes_numeric_entities(self):
        assert "'" in strip_html("&#39;it&#39;s")
        assert "é" in strip_html("&#233;")

    def test_prevents_word_merging(self):
        # Tags replaced with space so adjacent words don't merge
        result = strip_html("<p>Python</p><p>Developer</p>")
        assert "Python" in result
        assert "Developer" in result
        assert "PythonDeveloper" not in result

    def test_empty_string_returns_empty(self):
        assert strip_html("") == ""

    def test_none_returns_empty(self):
        assert strip_html(None) == ""

    def test_plain_text_unchanged(self):
        assert strip_html("no html here") == "no html here"

    def test_malformed_html_does_not_crash(self):
        # Unclosed tag, mangled attribute — must not raise
        result = strip_html("<div class='x>broken<p>text")
        assert isinstance(result, str)

    def test_self_closing_tags(self):
        result = strip_html("line1<br/>line2<hr />end")
        assert "line1" in result
        assert "line2" in result
        assert "<br" not in result

    def test_script_and_style_tags_removed(self):
        result = strip_html("<style>.foo{color:red}</style><p>content</p><script>alert(1)</script>")
        assert "content" in result
        assert "<style" not in result
        assert "<script" not in result


class TestNormalizeWhitespace:
    def test_collapses_multiple_spaces(self):
        assert normalize_whitespace("a  b   c") == "a b c"

    def test_collapses_tabs(self):
        assert normalize_whitespace("a\t\tb") == "a b"

    def test_collapses_newlines(self):
        assert normalize_whitespace("a\n\nb") == "a b"

    def test_strips_leading_trailing(self):
        assert normalize_whitespace("  hello  ") == "hello"

    def test_empty_string_returns_empty(self):
        assert normalize_whitespace("") == ""

    def test_none_returns_empty(self):
        assert normalize_whitespace(None) == ""

    def test_only_whitespace_returns_empty(self):
        assert normalize_whitespace("   \t\n  ") == ""


class TestCleanDescription:
    def test_full_pipeline_html_to_plaintext(self):
        html = "<h2>About the role</h2><p>We need a <strong>Python</strong> engineer.</p>"
        result = clean_description(html)
        assert "About the role" in result
        assert "Python" in result
        assert "<" not in result
        assert "  " not in result  # no double spaces

    def test_empty_returns_empty(self):
        assert clean_description("") == ""

    def test_none_returns_empty(self):
        assert clean_description(None) == ""

    def test_entities_and_whitespace_combined(self):
        html = "<p>Salt &amp; Pepper   Co.</p>"
        result = clean_description(html)
        assert result == "Salt & Pepper Co."

    def test_realistic_job_description(self):
        html = """
        <div>
          <h3>Requirements</h3>
          <ul>
            <li>5+ years Python</li>
            <li>Experience with AWS &amp; Docker</li>
          </ul>
        </div>
        """
        result = clean_description(html)
        assert "5+ years Python" in result
        assert "AWS & Docker" in result
        assert "<" not in result
        # Should be a single clean line or short multi-word result
        assert "\n" not in result

    def test_malformed_html_does_not_crash(self):
        result = clean_description("<<<div>broken>>text</p")
        assert isinstance(result, str)
