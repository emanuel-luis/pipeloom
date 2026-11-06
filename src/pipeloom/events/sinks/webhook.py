"""Post selected events as JSON to a webhook: Slack, Teams or a pager (ADR-0047).

The URL is a secret. It is never logged, printed or put in an error message: a failed post
names only the HTTP status or the exception type. Run the sink with `buffered: true`.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from collections.abc import Collection
from typing import Annotated, Literal

from pydantic import Field

from pipeloom.config.types import Duration
from pipeloom.events.model import Event
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec
from pipeloom.secrets.resolve import contains_secret

_RANK = {"debug": 0, "info": 1, "warning": 2, "error": 3}
_SHOWN_FAILURES = 3


class WebhookOptions(OptionsModel):
    secret_url: str
    """The webhook URL, as ${secret:provider/path}. Only http and https; never logged."""
    events: tuple[str, ...] = ("run.completed",)
    """Event types to post."""
    statuses: tuple[Literal["succeeded", "failed", "cancelled"], ...] = ("failed",)
    """A run.completed event is posted only with one of these statuses."""
    min_level: Literal["warning", "error"] | None = None
    """Also post every event at or above this level. Off by default."""
    timeout: Annotated[Duration, Field(gt=0)] = 10
    """Seconds to wait for each post, as 10 or 30s."""
    max_failures: int = Field(3, ge=1)
    """Failed posts in a row before the sink stops posting."""


def summary(event: Event) -> str:
    """The one-line `text` that Slack and Teams incoming webhooks render."""
    run = event.run_id or "-"
    if event.type == "run.completed":
        failures = event.data.get("failures")
        units = [
            f"{f.get('unit_id')} {f.get('code')}"
            for f in (failures if isinstance(failures, list) else [])[:_SHOWN_FAILURES]
            if isinstance(f, dict)
        ]
        head = f"pipeloom run {run} {event.data.get('status')}"
        return head + (": " + ", ".join(units) if units else "")
    message = event.data.get("message")
    tail = f": {message}" if isinstance(message, str) and message else ""
    return f"pipeloom {event.level.value} {event.type} in run {run}{tail}"


class WebhookSink:
    """POSTs `{"text": ..., "event": ...}` for the events it selects.

    After `max_failures` failed posts in a row it stops posting and fails every later delivery,
    so the bus disables it and emits one sink.disabled warning. Built without secret resolution
    (the URL is still a reference), it posts nothing.
    """

    name = "webhook"

    def __init__(
        self,
        url: str,
        *,
        events: Collection[str] = ("run.completed",),
        statuses: Collection[str] = ("failed",),
        min_level: str | None = None,
        timeout: float = 10,
        max_failures: int = 3,
    ) -> None:
        self._url = None if contains_secret(url) else url
        if self._url is not None and not self._url.lower().startswith(("http://", "https://")):
            raise ValueError("secret_url must be an http or https URL")
        self._events = frozenset(events)
        self._statuses = frozenset(statuses)
        self._min = None if min_level is None else _RANK[min_level]
        self._timeout = timeout
        self._max = max_failures
        self.failures = 0
        """Failed posts in a row."""

    def __repr__(self) -> str:  # never show the URL
        return "WebhookSink()"

    def wanted(self, event: Event) -> bool:
        if event.type == "run.completed" and event.data.get("replay") is True:
            return False  # a retried finalize: the first one already posted
        if event.type in self._events and (
            event.type != "run.completed" or event.data.get("status") in self._statuses
        ):
            return True
        return self._min is not None and _RANK[event.level.value] >= self._min

    def emit(self, event: Event) -> None:
        if self.failures >= self._max:
            raise RuntimeError(f"webhook stopped after {self._max} failed posts in a row")
        if self._url is None or not self.wanted(event):
            return
        body = json.dumps({"text": summary(event), "event": event.to_json()}, ensure_ascii=True)
        request = urllib.request.Request(
            self._url,
            data=body.encode("utf-8"),
            method="POST",
            headers={"Content-Type": "application/json", "User-Agent": "pipeloom"},
        )
        reason = ""
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                if not 200 <= response.status < 300:
                    reason = f"HTTP {response.status}"
        except urllib.error.HTTPError as err:
            err.close()
            reason = f"HTTP {err.code}"
        except Exception as err:  # noqa: BLE001 - its text may hold the URL: keep the type only
            reason = type(err).__name__
        if not reason:
            self.failures = 0
            return
        self.failures += 1
        raise RuntimeError(f"webhook post failed: {reason}")

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


def _build(opts: WebhookOptions) -> WebhookSink:
    return WebhookSink(
        opts.secret_url,
        events=opts.events,
        statuses=opts.statuses,
        min_level=opts.min_level,
        timeout=opts.timeout,
        max_failures=opts.max_failures,
    )


SPEC: PluginSpec[WebhookSink] = PluginSpec(
    "webhook",
    "webhook",
    "sinks",
    API_VERSION,
    _build,
    WebhookOptions,
    description="Posts selected events as JSON to a webhook URL given as a secret.",
)
