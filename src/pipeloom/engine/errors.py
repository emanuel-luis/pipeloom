"""Errors engines and user code raise to tell the core how to react."""

from __future__ import annotations


class TransientError(Exception):
    """Raise from user code or a plugin to force a retryable failure."""


class CommitConflict(TransientError):
    """A table changed between the read and the commit (expected-version mismatch)."""


class WriteRefusedError(RuntimeError):
    """The engine's idempotency guard refused a write that no earlier attempt applied: a later
    transaction version of the same writer already exists (a newer run, an overlapping run, or
    clock skew). An engine raises it before it changes any data. The core reports it as PLM207."""


class WriteKeyError(ValueError):
    """A write key is unusable: a `unique_key` or `partition_by` column is missing, or a merge
    batch repeats a key. The core reports it as PLM206."""
