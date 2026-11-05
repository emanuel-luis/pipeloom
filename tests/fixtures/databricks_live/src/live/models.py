def flaky(ctx, a):
    """Fail on the first attempt, succeed on every later one (a repair is a later attempt)."""
    if ctx.attempt == 1:
        raise RuntimeError("live fixture: planned failure on attempt 1")
    return a
