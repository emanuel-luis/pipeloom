"""Contract suites that plugin authors subclass to prove their plugin."""

KIT_VERSION = "1.0"
"""Version of the conformance kit (`pipeloom.testing.kit`), printed in the pytest report header.

Within pipeloom 1.x the kit changes only additively. A minor kit version may add cells, for a new
capability or for behavior the docs already describe, and new optional `EngineHarness` fields. A
required `EngineHarness` field, a removed cell or a cell that changes meaning needs a pipeloom major
(and so a kit major). The kit version is independent of the pipeloom version.
"""
