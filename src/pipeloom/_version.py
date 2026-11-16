"""The installed pipeloom version. Lower layers import it from here, because the package root
imports the authoring API, which sits above them."""

from importlib.metadata import version

__version__ = version("pipeloom")
