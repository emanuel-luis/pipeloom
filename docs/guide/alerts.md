# Alerts with the webhook sink

The built-in `webhook` sink posts selected events as JSON to a URL, which is enough for a Slack or Teams incoming webhook or a pager. By default it posts once when a run fails. It uses the Python standard library for HTTP and adds no dependency. Decided in [ADR-0047](../adr/0047-run-history-and-webhook.md).

## Configure it

```yaml
events:
  sinks:
    - kind: webhook
      secret_url: ${secret:env/PIPELOOM_ALERT_URL}
      buffered: true
```

The URL carries its own credentials, so it is a secret: give it as `${secret:provider/path}` (here the `env` provider reads the variable `PIPELOOM_ALERT_URL`). A literal URL is rejected with PLS104. pipeloom never prints or logs the URL, and an error names only the HTTP status or the exception type. Only `http` and `https` URLs are accepted.

Keep `buffered: true`: the posts then run on a background thread, so a slow endpoint never holds up the run.

| Option | Default | Meaning |
|---|---|---|
| `secret_url` | required | the webhook URL, as a secret reference |
| `events` | `[run.completed]` | event types to post |
| `statuses` | `[failed]` | a `run.completed` event is posted only with one of these statuses (`succeeded`, `failed`, `cancelled`) |
| `min_level` | none | also post every event at or above `warning` or `error`; off by default, so a failed run posts exactly once |
| `timeout` | `10` | seconds to wait for each post, as `10` or `30s` |
| `max_failures` | `3` | failed posts in a row before the sink stops posting |

`failure_budget`, `buffered` and `buffer_size` work as for every sink.

## What it sends

One POST per selected event, with `Content-Type: application/json`:

```json
{
  "text": "pipeloom run 20261106T101500Z-1a2b failed: fct_orders PLM201",
  "event": {"schema": "pipeloom.event/1", "type": "run.completed", "...": "..."}
}
```

`text` is a one-line summary that Slack and Teams render as the message. `event` is the whole event envelope, with secrets masked, for anything that wants more. Any 2xx answer counts as delivered.

## When the endpoint is down

A failed post is a sink failure. After `max_failures` failed posts in a row, the sink stops posting for the rest of the invocation, and the bus disables it and reports one `sink.disabled` warning ([PLE401](../reference/errors.md#ple401)). The run itself is never affected.

Commands that do not run anything (`validate`, `compile`) build the sink without resolving secrets, and then it posts nothing.
