"""Contract suites that plugin authors subclass to prove their plugin."""

KIT_VERSION = "1.0"
"""Version of the conformance kit (`pipeloom.testing.kit`), printed in the pytest report header.

The major number changes when a cell changes meaning or `EngineHarness` gains a required field.
The minor number changes when cells are added. Both are independent of the pipeloom version.
"""
