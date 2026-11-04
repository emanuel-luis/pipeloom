"""A sink plugin that always fails, for the PLE401 case and the CLI tests."""

from __future__ import annotations

from pipeloom.events.model import Event
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec


class BoomOptions(OptionsModel):
    pass


class BoomSink:
    name = "boom"

    def emit(self, event: Event) -> None:
        raise RuntimeError("boom")

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


BOOM_SINK: PluginSpec[BoomSink] = PluginSpec(
    "boom", "boom", "sinks", API_VERSION, lambda _opts: BoomSink(), BoomOptions
)
