"""Render a plan, and optionally a `run_results.json`, as one self-contained HTML page.

No templates and no embedded JSON: the HTML is the rendering, and `manifest.json` stays the data
contract. The output depends only on its inputs, so the same inputs give the same bytes.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from html import escape
from typing import Any, Final

from pipeloom.docs.layout import NODE_H, NODE_W, LayoutNode, layout_graph
from pipeloom.plan.model import Plan, PlanNode, PlanSource

CSP: Final = (
    "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; img-src data:"
)
STATUSES: Final = ("succeeded", "failed", "skipped", "cancelled", "timed_out")
_LABEL_MAX = 24

_CSS = """
:root{--bg:#fafaf9;--fg:#1c1917;--muted:#57534e;--line:#d6d3d1;--card:#fff;--link:#1d4ed8;
--model:#dbeafe;--source:#e7e5e4;--ok:#bbf7d0;--bad:#fecaca;--skip:#fef08a;--cancel:#fed7aa;
--code:#f5f5f4}
@media (prefers-color-scheme:dark){:root{--bg:#1c1917;--fg:#f5f5f4;--muted:#a8a29e;--line:#44403c;
--card:#292524;--link:#93c5fd;--model:#1e3a5f;--source:#44403c;--ok:#14532d;--bad:#7f1d1d;
--skip:#713f12;--cancel:#7c2d12;--code:#0c0a09}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.5 system-ui,sans-serif}
main{max-width:1100px;margin:0 auto;padding:16px}
a{color:var(--link)}
h1{margin:0 0 4px;font-size:1.6rem}
h2{margin:32px 0 8px;font-size:1.2rem}
h3{margin:16px 0 4px;font-size:1rem}
.meta{color:var(--muted);margin:0 0 8px}
.note{border:1px solid var(--line);border-left:4px solid #d97706;background:var(--card);
padding:8px 12px;margin:12px 0}
.scroll{overflow:auto;border:1px solid var(--line);background:var(--card);max-height:70vh}
.tools{margin:8px 0}
.tools button{font:inherit;padding:2px 10px;margin-right:4px}
#filter{font:inherit;padding:4px 8px;width:100%;max-width:360px;margin:4px 0 8px}
table{border-collapse:collapse;width:100%;background:var(--card)}
th,td{border-bottom:1px solid var(--line);padding:4px 8px;text-align:left;vertical-align:top}
th{color:var(--muted);font-weight:600}
section{border-top:1px solid var(--line);padding:8px 0}
pre{background:var(--code);border:1px solid var(--line);padding:8px;overflow:auto;margin:4px 0}
code,pre{font:13px/1.4 ui-monospace,Consolas,monospace}
dl{display:grid;grid-template-columns:max-content 1fr;gap:2px 16px;margin:4px 0}
dt{color:var(--muted)}
dd{margin:0}
.pill{display:inline-block;border:1px solid var(--line);border-radius:10px;padding:0 8px;
margin-right:4px;font-size:.85rem}
svg{display:block}
.edge{fill:none;stroke:var(--muted);stroke-width:1.2;opacity:.7}
.node rect{stroke:var(--muted);stroke-width:1;fill:var(--model)}
.node.source rect{fill:var(--source)}
.node.ephemeral rect{stroke-dasharray:4 3}
.node.st-succeeded rect{fill:var(--ok)}
.node.st-failed rect,.node.st-timed_out rect{fill:var(--bad)}
.node.st-skipped rect{fill:var(--skip)}
.node.st-cancelled rect{fill:var(--cancel)}
.node text{fill:var(--fg);font:12px system-ui,sans-serif;pointer-events:none}
.node:hover rect{stroke:var(--link);stroke-width:2}
.legend span{margin-right:12px;white-space:nowrap}
"""

_JS = """
(function(){
var f=document.getElementById('filter');
var rows=document.querySelectorAll('#models tbody tr');
if(f){f.hidden=false;f.addEventListener('input',function(){
var q=f.value.toLowerCase();
rows.forEach(function(r){r.hidden=q!==''&&r.textContent.toLowerCase().indexOf(q)<0;});});}
var s=document.getElementById('graph-svg');var t=document.getElementById('graph-tools');
if(s&&t){var z=1;var w=+s.getAttribute('width');var h=+s.getAttribute('height');
[['-',0.8],['+',1.25],['100%',0]].forEach(function(p){var b=document.createElement('button');
b.type='button';b.textContent=p[0];b.addEventListener('click',function(){
z=p[1]?z*p[1]:1;s.setAttribute('width',Math.round(w*z));s.setAttribute('height',Math.round(h*z));});
t.appendChild(b);});}
})();
"""


def _e(value: object) -> str:
    return escape(str(value), quote=True)


def _dash(value: object) -> str:
    return "-" if value is None or value == "" else _e(value)


def _ms(value: int | None) -> str:
    if value is None:
        return "-"
    return f"{value} ms" if value < 1000 else f"{value / 1000:.1f} s"


def _num(value: int | None) -> str:
    return "-" if value is None else f"{value:,}"


def _pre(text: str, code: bool = False) -> str:
    body = _e(text)
    return f"<pre><code>{body}</code></pre>" if code else f"<pre>{body}</pre>"


def _json(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False, default=str)


def _anchor(uid: str, plan: Plan) -> str:
    return ("model-" + plan.nodes[uid].name) if uid in plan.nodes else ("source-" + plan.sources[uid].name)


def _name(uid: str, plan: Plan) -> str:
    return plan.nodes[uid].name if uid in plan.nodes else plan.sources[uid].name


def _link(uid: str, plan: Plan) -> str:
    if uid not in plan.nodes and uid not in plan.sources:
        return _e(uid)
    return f'<a href="#{_e(_anchor(uid, plan))}">{_e(_name(uid, plan))}</a>'


def _links(uids: tuple[str, ...], plan: Plan) -> str:
    known = sorted((u for u in uids if u in plan.nodes or u in plan.sources), key=lambda u: _name(u, plan))
    return ", ".join(_link(u, plan) for u in known) or "-"


def _relation(node: PlanNode) -> str:
    return node.relation.relation_name if node.relation else ""


def _status(rows: Mapping[str, Mapping[str, Any]], uid: str) -> str:
    row = rows.get(uid)
    status = row.get("status") if row else None
    return status if status in STATUSES else "none"


def _graph(plan: Plan, rows: Mapping[str, Mapping[str, Any]]) -> str:
    layout_nodes = [LayoutNode(u, (n.file, n.name)) for u, n in plan.nodes.items()]
    layout_nodes += [LayoutNode(u, (s.file, s.name)) for u, s in plan.sources.items()]
    edges = [(p, c) for c, parents in plan.parent_map.items() for p in parents]
    layout = layout_graph(layout_nodes, edges)
    out = [
        f'<svg id="graph-svg" role="img" aria-label="Lineage graph" width="{layout.width}" '
        f'height="{layout.height}" viewBox="0 0 {layout.width} {layout.height}">'
    ]
    out += [f'<path class="edge" d="{e.path}"/>' for e in layout.edges]
    for placed in layout.nodes:
        uid = placed.id
        node = plan.nodes.get(uid)
        if node is not None:
            kind = "ephemeral" if node.materialized == "ephemeral" else "model"
            tip = _relation(node) or "ephemeral (inlined)"
        else:
            source = plan.sources[uid]
            kind = "source"
            tip = source.location or source.table or source.name
        name = _name(uid, plan)
        label = name if len(name) <= _LABEL_MAX else name[: _LABEL_MAX - 3] + "..."
        out.append(
            f'<a href="#{_e(_anchor(uid, plan))}"><g class="node {kind} st-{_status(rows, uid)}" '
            f'data-id="{_e(uid)}"><title>{_e(name)}: {_e(tip)}</title>'
            f'<rect x="{placed.x}" y="{placed.y}" width="{NODE_W}" height="{NODE_H}" rx="6"/>'
            f'<text x="{placed.x + 10}" y="{placed.y + NODE_H // 2 + 4}">{_e(label)}</text></g></a>'
        )
    out.append("</svg>")
    return "".join(out)


def _header(plan: Plan, results: Mapping[str, Any] | None) -> str:
    m = plan.metadata
    parts = [
        f"<h1>{_e(m.project)}</h1>",
        f'<p class="meta">target {_e(m.target)}, pipeloom {_e(m.pipeloom_version)}, '
        f"plan {_e(m.plan_hash[:12])}</p>",
    ]
    if results is not None:
        run = results["run"]
        counts = run.get("counts", {})
        shown = ", ".join(f"{k} {counts[k]}" for k in ("total", *STATUSES) if counts.get(k))
        parts.append(
            f'<p class="meta">last run {_e(run.get("run_id"))}: {_e(run.get("status"))}, '
            f'ended {_dash(run.get("ended_at"))}, {_e(shown)}</p>'
        )
        if run.get("plan_hash") != m.plan_hash:
            parts.append(
                '<p class="note">The plan changed since this run. Statuses are matched by model '
                "id and may not describe the current code.</p>"
            )
    return "".join(parts)


def _legend() -> str:
    items = [("model", "model"), ("source", "source"), ("ephemeral", "ephemeral (dashed)")]
    items += [(f"model st-{s}", s) for s in STATUSES]
    keys = "".join(
        f'<span><svg width="14" height="10" style="display:inline"><g class="node {c}">'
        f'<rect x="1" y="1" width="12" height="8"/></g></svg> {_e(t)}</span>'
        for c, t in items
    )
    return f'<p class="meta legend">{keys}</p>'


def _table(plan: Plan, rows: Mapping[str, Mapping[str, Any]]) -> str:
    head =["name", "path", "materialized", "strategy", "relation", "tags", "status", "rows written", "duration"]
    out = [
        '<input id="filter" type="search" placeholder="Filter models" aria-label="Filter models" hidden>',
        '<table id="models"><thead><tr>' + "".join(f"<th>{h}</th>" for h in head) + "</tr></thead><tbody>",
    ]
    for node in sorted(plan.nodes.values(), key=lambda n: n.name):
        row = rows.get(node.unique_id, {})
        strategy = node.config.write.strategy or ("-" if node.materialized == "ephemeral" else "")
        out.append(
            "<tr>"
            f"<td>{_link(node.unique_id, plan)}</td><td>{_e(node.file)}</td>"
            f"<td>{_e(node.materialized)}</td><td>{_dash(strategy)}</td>"
            f"<td>{_dash(_relation(node))}</td><td>{_dash(', '.join(node.tags))}</td>"
            f"<td>{_e(_status(rows, node.unique_id))}</td>"
            f"<td>{_num(row.get('rows_written'))}</td><td>{_ms(row.get('duration_ms'))}</td></tr>"
        )
    out.append("</tbody></table>")
    return "".join(out)


def _dl(pairs: list[tuple[str, str]]) -> str:
    return "<dl>" + "".join(f"<dt>{k}</dt><dd>{v}</dd>" for k, v in pairs) + "</dl>"


def _result(row: Mapping[str, Any]) -> str:
    pairs = [
        ("status", _dash(row.get("status"))),
        ("rows written", _num(row.get("rows_written"))),
        ("bytes written", _num(row.get("bytes_written"))),
        ("duration", _ms(row.get("duration_ms"))),
        ("attempt", _dash(row.get("attempt"))),
    ]
    err = row.get("error")
    if err:
        pairs.append(("error", f"{_e(err.get('code'))}: {_e(err.get('message'))}"))
        if err.get("code"):
            pairs.append(("explain", f"<code>pipeloom explain {_e(err['code'])}</code>"))
    return "<h3>Last result</h3>" + _dl(pairs)


def _model_section(node: PlanNode, plan: Plan, rows: Mapping[str, Mapping[str, Any]]) -> str:
    uid = node.unique_id
    pairs = [
        ("path", _e(node.file) + (f":{node.line}" if node.line else "")),
        ("relation", _dash(_relation(node)) if node.relation else "none (ephemeral, inlined)"),
        ("materialized", _e(node.materialized)),
        ("strategy", _dash(node.config.write.strategy)),
        ("tags", "".join(f'<span class="pill">{_e(t)}</span>' for t in node.tags) or "-"),
        (
            "inputs",
            "".join(f"{_e(a)} &rarr; {_link(t, plan)}<br>" for a, t in sorted(node.inputs.items()))
            or "-",
        ),
        ("parents", _links(plan.parent_map.get(uid, ()), plan)),
        ("children", _links(plan.child_map.get(uid, ()), plan)),
    ]
    parts = [f'<section id="model-{_e(node.name)}"><h2>{_e(node.name)}</h2>', _dl(pairs)]
    if node.config.expectations:
        items = []
        for exp in node.config.expectations:
            spec = exp.model_dump(mode="json", exclude_none=True, exclude={"name"})
            items.append(f"<li>{_e(exp.name)}: <code>{_e(json.dumps(spec, sort_keys=True))}</code></li>")
        parts.append("<h3>Expectations</h3><ul>" + "".join(items) + "</ul>")
    parts.append("<h3>Resolved config</h3>")
    parts.append(_pre(_json(node.config.model_dump(mode="json", by_alias=True))))
    if node.body.kind == "sql":
        parts.append("<h3>SQL</h3>" + _pre(node.body.sql, code=True))
    else:
        parts.append(f"<h3>Python</h3><p><code>{_e(node.body.callable)}</code> in {_e(node.body.file)}</p>")
    if uid in rows:
        parts.append(_result(rows[uid]))
    parts.append("</section>")
    return "".join(parts)


def _source_section(source: PlanSource, plan: Plan) -> str:
    uid = source.unique_id
    pairs = [
        ("path", _e(source.file)),
        ("format", _dash(source.format)),
        ("location", _dash(source.location)),
        ("table", _dash(source.table)),
        ("tags", "".join(f'<span class="pill">{_e(t)}</span>' for t in source.tags) or "-"),
        ("read by", _links(plan.child_map.get(uid, ()), plan)),
    ]
    out = f'<section id="source-{_e(source.name)}"><h2>{_e(source.name)}</h2>' + _dl(pairs)
    if source.options:
        out += "<h3>Options</h3>" + _pre(_json(source.options))
    return out + "</section>"


def render_site(plan: Plan, run_results: Mapping[str, Any] | None = None) -> str:
    """The whole page. `run_results` is a parsed `pipeloom.run_results/1` document or None."""
    rows = {r["unique_id"]: r for r in (run_results or {}).get("results", []) if r.get("unique_id")}
    body = [
        _header(plan, run_results),
        "<h2>Graph</h2>",
        _legend(),
        '<div id="graph-tools" class="tools"></div>',
        f'<div class="scroll">{_graph(plan, rows)}</div>',
        "<h2>Models</h2>",
        _table(plan, rows),
    ]
    body += [_model_section(n, plan, rows) for _, n in sorted(plan.nodes.items(), key=lambda i: i[1].name)]
    if plan.sources:
        body.append("<h2>Sources</h2>")
        body += [_source_section(s, plan) for s in sorted(plan.sources.values(), key=lambda s: s.name)]
    title = f"{_e(plan.metadata.project)} docs"
    return (
        '<!doctype html>\n<html lang="en"><head><meta charset="utf-8">'
        f'<meta http-equiv="Content-Security-Policy" content="{CSP}">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        f"<title>{title}</title><style>{_CSS}</style></head><body><main>"
        + "".join(body)
        + f"</main><script>{_JS}</script></body></html>\n"
    )
