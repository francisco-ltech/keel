"""Messages rendered from Jinja templates.

ADR 0015 declined a template engine until a second message needed the layout
of the first. The starter template's password reset was that message. This
is the smallest thing that answers it: a directory of templates, a text
variant per message and an optional HTML one, rendered into a
:class:`~keel.mail.message.Message`.

Two rules are load-bearing. **HTML templates autoescape and text templates do
not**, decided by the file name, so ``{{ user.full_name }}`` is safe in
``welcome.html.j2`` and readable in ``welcome.txt.j2``. **An undefined
variable is an error**, not blank text: a reset message with an empty code
is worse than no message.

Jinja is the ``templates`` extra, imported here and nowhere else in the
package, so a service that builds its messages by hand carries no engine.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from os import PathLike
from pathlib import Path
from typing import Any, cast

from keel.exceptions import ConfigurationError
from keel.mail.message import Message

try:
    from jinja2 import Environment, FileSystemLoader, StrictUndefined, Template, TemplateNotFound
except ImportError as exc:  # pragma: no cover - exercised only without the extra
    raise ConfigurationError(
        "keel.mail.templates needs Jinja: install the `templates` extra, `keel[templates]`"
    ) from exc

TEXT_SUFFIX = ".txt.j2"
"""The mandatory variant. A client that cannot show HTML still reads the mail."""

HTML_SUFFIX = ".html.j2"
"""The optional variant, rendered with autoescaping on."""


def _autoescape(name: str | None) -> bool:
    return name is not None and name.endswith(HTML_SUFFIX)


@dataclass(frozen=True, slots=True)
class Rendered:
    """A message's bodies, rendered.

    Attributes:
        text: The plain-text body.
        html: The HTML alternative, or ``None`` when the message has no HTML
            template.
    """

    text: str
    html: str | None


class MailTemplates:
    """A directory of message templates.

    ``name`` names a pair: ``<name>.txt.j2``, which must exist, and
    ``<name>.html.j2``, which may. Both see the same context, plus whatever
    ``globals`` carries, which is where an application name or a public URL
    goes once rather than at every call.

    Args:
        directory: Where the templates are.
        globals: Variables every template sees.
    """

    __slots__ = ("_directory", "_environment")

    def __init__(
        self,
        directory: str | PathLike[str],
        *,
        globals: Mapping[str, object] | None = None,  # noqa: A002 - Jinja's own word for it
    ) -> None:
        self._directory = Path(directory)
        self._environment = Environment(
            loader=FileSystemLoader(self._directory),
            autoescape=_autoescape,
            undefined=StrictUndefined,
            trim_blocks=True,
            lstrip_blocks=True,
            keep_trailing_newline=True,
        )
        if globals:
            # Jinja types its globals by their defaults; ours are the caller's.
            cast("dict[str, Any]", self._environment.globals).update(globals)

    def render(self, name: str, /, **context: Any) -> Rendered:
        """Render a message's bodies.

        Args:
            name: The message, without a suffix.
            **context: What the templates see.

        Returns:
            The text body and, if ``<name>.html.j2`` exists, the HTML one.

        Raises:
            ConfigurationError: If there is no text template by that name.
            jinja2.TemplateNotFound: If a template extends or includes one
                that does not exist. Not the same as a missing HTML variant:
                a layout renamed under every message must not turn them all
                into text-only mail with a green suite.
            jinja2.UndefinedError: If a template names a variable the context
                does not carry.
        """
        text_template = self._load(name + TEXT_SUFFIX)
        if text_template is None:
            raise ConfigurationError(
                f"no mail template {name + TEXT_SUFFIX!r} under {self._directory}"
            )
        html_template = self._load(name + HTML_SUFFIX)
        # Rendering is outside the lookups on purpose: a TemplateNotFound
        # raised here names a missing extends or include, and propagates.
        text = text_template.render(**context)
        html = None if html_template is None else html_template.render(**context)
        return Rendered(text=text, html=html)

    def _load(self, filename: str) -> Template | None:
        """Look one template up, or answer None when that file does not exist."""
        try:
            return self._environment.get_template(filename)
        except TemplateNotFound as exc:
            if exc.name == filename:
                return None
            raise

    def message(
        self,
        name: str,
        /,
        *,
        to: str | Sequence[str],
        subject: str,
        sender: str | None = None,
        reply_to: str | None = None,
        cc: str | Sequence[str] | None = None,
        bcc: str | Sequence[str] | None = None,
        headers: Mapping[str, str] | None = None,
        **context: Any,
    ) -> Message:
        """Render a message's bodies and build the message around them.

        Args:
            name: The message, without a suffix.
            to: The recipients.
            subject: The subject line. A string rather than a template, so
                the call site reads as the whole message.
            sender: The ``From`` address, or ``None`` for the configured one.
            reply_to: Where replies go.
            cc: Carbon copies.
            bcc: Blind copies.
            headers: Extra headers.
            **context: What the templates see.

        Returns:
            The message, ready for :func:`keel.mail.send`.
        """
        rendered = self.render(name, **context)
        return Message(
            to=to,
            subject=subject,
            text=rendered.text,
            html=rendered.html,
            sender=sender,
            reply_to=reply_to,
            cc=cc,
            bcc=bcc,
            headers=headers,
        )


__all__ = ["HTML_SUFFIX", "TEXT_SUFFIX", "MailTemplates", "Rendered"]
