"""Messages rendered from templates: what escapes, what does not, what fails."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from jinja2 import TemplateNotFound, UndefinedError

from keel.exceptions import ConfigurationError, InvalidMessageError
from keel.mail.templates import MailTemplates


@pytest.fixture
def templates(tmp_path: Path) -> MailTemplates:
    (tmp_path / "layout.html.j2").write_text(
        "<html><body><h1>{{ app_name }}</h1>{% block body %}{% endblock %}</body></html>\n"
    )
    (tmp_path / "welcome.txt.j2").write_text(
        "Hello {{ user.full_name }},\n\nWelcome to {{ app_name }}. Sign in with {{ user.email }}.\n"
    )
    (tmp_path / "welcome.html.j2").write_text(
        '{% extends "layout.html.j2" %}{% block body %}'
        "<p>Hello {{ user.full_name }},</p><p>Sign in with <b>{{ user.email }}</b>.</p>"
        "{% endblock %}\n"
    )
    (tmp_path / "plain.txt.j2").write_text("Only text: {{ note }}\n")
    return MailTemplates(tmp_path, globals={"app_name": "Invoices"})


class Person:
    full_name = "Ada <Lovelace>"
    email = "ada@example.com"


def test_html_escapes_and_text_does_not(templates: MailTemplates) -> None:
    rendered = templates.render("welcome", user=Person())

    assert "Hello Ada <Lovelace>," in rendered.text
    assert rendered.html is not None
    assert "Hello Ada &lt;Lovelace&gt;," in rendered.html
    assert "<h1>Invoices</h1>" in rendered.html, "the layout and the global reached the page"
    assert "Welcome to Invoices." in rendered.text


def test_a_message_without_an_html_template_has_no_html(templates: MailTemplates) -> None:
    rendered = templates.render("plain", note="x")

    assert rendered.text == "Only text: x\n"
    assert rendered.html is None


def test_an_undefined_variable_is_an_error_not_blank_text(templates: MailTemplates) -> None:
    """A reset message with an empty code would be worse than no message."""
    with pytest.raises(UndefinedError):
        templates.render("plain")


def test_a_missing_text_template_names_the_directory(templates: MailTemplates) -> None:
    with pytest.raises(ConfigurationError, match=re.escape("goodbye.txt.j2")):
        templates.render("goodbye")


def test_message_builds_the_whole_message(templates: MailTemplates) -> None:
    message = templates.message(
        "welcome", to="ada@example.com", subject="Welcome", cc="grace@example.com", user=Person()
    )

    assert message.to == ("ada@example.com",)
    assert message.cc == ("grace@example.com",)
    assert message.subject == "Welcome"
    assert "Sign in with ada@example.com" in message.text
    assert message.html is not None and "<b>ada@example.com</b>" in message.html


def test_message_still_refuses_what_a_message_refuses(templates: MailTemplates) -> None:
    with pytest.raises(InvalidMessageError):
        templates.message("plain", to="ada@example.com\nBcc: x@example.com", subject="s", note="n")


def test_a_broken_extends_is_an_error_not_a_missing_html_body(tmp_path: Path) -> None:
    """A layout renamed under every message must not turn them all into text-only mail."""
    (tmp_path / "note.txt.j2").write_text("text\n")
    (tmp_path / "note.html.j2").write_text('{% extends "layuot.html.j2" %}')

    with pytest.raises(TemplateNotFound, match="layuot"):
        MailTemplates(tmp_path).render("note")


def test_a_missing_include_does_not_claim_the_message_itself_is_missing(tmp_path: Path) -> None:
    (tmp_path / "note.txt.j2").write_text('{% include "footer.txt.j2" %}')

    with pytest.raises(TemplateNotFound, match="footer"):
        MailTemplates(tmp_path).render("note")
