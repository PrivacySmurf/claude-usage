"""
dashboard.py - Local web dashboard served on localhost:9123.
"""

import json
import os
import sqlite3
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from datetime import datetime

DB_PATH = Path.home() / ".claude" / "usage.db"


def get_codex_status(db_path=DB_PATH):
    """Get Codex rate limit status, merging JSONL data and usage-monitor state."""
    import time

    result = {"state": "unavailable"}
    best_ts = 0

    # Source 1: codex_rate_limits table (from JSONL scanner)
    if db_path.exists():
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM codex_rate_limits WHERE id=1").fetchone()
            conn.close()
            if row:
                best_ts = row["scraped_at"] or 0
                result = {
                    "state": "green",
                    "primary_pct": row["primary_pct"],
                    "primary_resets_at": row["primary_resets_at"],
                    "secondary_pct": row["secondary_pct"],
                    "secondary_resets_at": row["secondary_resets_at"],
                    "plan_type": (row["plan_type"] or "").title() or None,
                    "credits_has": bool(row["credits_has"]),
                    "freshness_s": int(time.time()) - best_ts,
                    "auth_ok": True,
                    "source": "jsonl",
                }
        except Exception:
            pass

    # Source 2: usage-monitor-state.json (from wham poll)
    state_file = Path.home() / ".cc-agents" / "scripts" / "usage-monitor-state.json"
    if state_file.exists():
        try:
            with open(state_file, encoding="utf-8", errors="replace") as f:
                mon = json.load(f)
            # usage-monitor stores codex data under codex_five_hour / codex_seven_day
            mon_ts_str = mon.get("timestamp") or mon.get("ts") or ""
            mon_ts = 0
            if mon_ts_str:
                try:
                    dt = datetime.fromisoformat(mon_ts_str.replace("Z", "+00:00"))
                    mon_ts = int(dt.timestamp())
                except Exception:
                    pass
            if mon_ts > best_ts:
                best_ts = mon_ts
                fh = mon.get("codex_five_hour", {})
                sd = mon.get("codex_seven_day", {})
                result = {
                    "state": "green",
                    "primary_pct": fh.get("pct"),
                    "primary_resets_at": None,
                    "secondary_pct": sd.get("pct"),
                    "secondary_resets_at": None,
                    "plan_type": None,
                    "credits_has": False,
                    "freshness_s": int(time.time()) - mon_ts,
                    "auth_ok": True,
                    "source": "monitor",
                }
        except Exception:
            pass

    # Format reset times as human strings
    def _fmt_resets(ts):
        if not ts:
            return None
        import time as time_mod
        delta = ts - int(time_mod.time())
        if delta <= 0:
            return "Resets soon"
        h = delta // 3600
        m = (delta % 3600) // 60
        if h > 0:
            return f"in {h}h {m}m"
        return f"in {m}m"

    if "primary_resets_at" in result:
        result["primary_resets_str"] = _fmt_resets(result.get("primary_resets_at"))
        result["secondary_resets_str"] = _fmt_resets(result.get("secondary_resets_at"))

    return result


def get_dashboard_data(db_path=DB_PATH):
    if not db_path.exists():
        return {"error": "Database not found. Run: python cli.py scan"}

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    turns_cols = {r["name"] for r in conn.execute("PRAGMA table_info(turns)").fetchall()}
    sessions_cols = {r["name"] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()}
    turns_provider_pred = "COALESCE(provider, 'claude') = 'claude'" if "provider" in turns_cols else "1=1"
    sessions_provider_pred = "COALESCE(provider, 'claude') = 'claude'" if "provider" in sessions_cols else "1=1"

    # Claude tab data
    model_rows = conn.execute(f"""
        SELECT COALESCE(model, 'unknown') as model
        FROM turns
        WHERE {turns_provider_pred}
        GROUP BY model
        ORDER BY SUM(input_tokens + output_tokens) DESC
    """).fetchall()
    all_models = [r["model"] for r in model_rows]

    daily_rows = conn.execute(f"""
        SELECT
            substr(timestamp, 1, 10)   as day,
            COALESCE(model, 'unknown') as model,
            SUM(input_tokens)          as input,
            SUM(output_tokens)         as output,
            SUM(cache_read_tokens)     as cache_read,
            SUM(cache_creation_tokens) as cache_creation,
            COUNT(*)                   as turns
        FROM turns
        WHERE {turns_provider_pred}
        GROUP BY day, model
        ORDER BY day, model
    """).fetchall()

    daily_by_model = [{
        "day":            r["day"],
        "model":          r["model"],
        "input":          r["input"] or 0,
        "output":         r["output"] or 0,
        "cache_read":     r["cache_read"] or 0,
        "cache_creation": r["cache_creation"] or 0,
        "turns":          r["turns"] or 0,
    } for r in daily_rows]

    session_rows = conn.execute(f"""
        SELECT
            session_id, project_name, first_timestamp, last_timestamp,
            total_input_tokens, total_output_tokens,
            total_cache_read, total_cache_creation, model, turn_count
        FROM sessions
        WHERE {sessions_provider_pred}
        ORDER BY last_timestamp DESC
    """).fetchall()

    sessions_all = []
    for r in session_rows:
        try:
            t1 = datetime.fromisoformat((r["first_timestamp"] or "").replace("Z", "+00:00"))
            t2 = datetime.fromisoformat((r["last_timestamp"] or "").replace("Z", "+00:00"))
            duration_min = round((t2 - t1).total_seconds() / 60, 1)
        except Exception:
            duration_min = 0
        sessions_all.append({
            "session_id":    (r["session_id"] or "")[:8],
            "project":       r["project_name"] or "unknown",
            "last":          (r["last_timestamp"] or "")[:16].replace("T", " "),
            "last_date":     (r["last_timestamp"] or "")[:10],
            "duration_min":  duration_min,
            "model":         r["model"] or "unknown",
            "turns":         r["turn_count"] or 0,
            "input":         r["total_input_tokens"] or 0,
            "output":        r["total_output_tokens"] or 0,
            "cache_read":    r["total_cache_read"] or 0,
            "cache_creation": r["total_cache_creation"] or 0,
        })

    conn.close()

    # --- Codex data ---
    codex_daily_rows = []
    codex_sessions_all = []
    combined_daily_rows = []

    if db_path.exists():
        conn2 = sqlite3.connect(db_path)
        conn2.row_factory = sqlite3.Row
        turns_cols2 = {r["name"] for r in conn2.execute("PRAGMA table_info(turns)").fetchall()}
        sessions_cols2 = {r["name"] for r in conn2.execute("PRAGMA table_info(sessions)").fetchall()}
        has_turns_provider = "provider" in turns_cols2
        has_sessions_provider = "provider" in sessions_cols2

        # Codex daily
        if has_turns_provider:
            cdrows = conn2.execute("""
                SELECT substr(timestamp,1,10) as day,
                       COALESCE(model,'gpt-5') as model,
                       SUM(input_tokens) as input, SUM(output_tokens) as output,
                       SUM(cache_read_tokens) as cache_read, COUNT(*) as turns
                FROM turns WHERE provider='codex'
                GROUP BY day, model ORDER BY day, model
            """).fetchall()
            codex_daily_rows = [{
                "day": r["day"], "model": r["model"],
                "input": r["input"] or 0, "output": r["output"] or 0,
                "cache_read": r["cache_read"] or 0, "turns": r["turns"] or 0,
            } for r in cdrows]

        # Codex sessions
        if has_sessions_provider:
            csrows = conn2.execute("""
                SELECT session_id, project_name, first_timestamp, last_timestamp,
                       total_input_tokens, total_output_tokens,
                       total_cache_read, model, turn_count
                FROM sessions WHERE provider='codex'
                ORDER BY last_timestamp DESC
            """).fetchall()
            for r in csrows:
                try:
                    t1 = datetime.fromisoformat((r["first_timestamp"] or "").replace("Z", "+00:00"))
                    t2 = datetime.fromisoformat((r["last_timestamp"] or "").replace("Z", "+00:00"))
                    dur = round((t2 - t1).total_seconds() / 60, 1)
                except Exception:
                    dur = 0
                codex_sessions_all.append({
                    "session_id": (r["session_id"] or "")[:8],
                    "project": r["project_name"] or "unknown",
                    "last": (r["last_timestamp"] or "")[:16].replace("T", " "),
                    "last_date": (r["last_timestamp"] or "")[:10],
                    "duration_min": dur,
                    "model": r["model"] or "gpt-5",
                    "turns": r["turn_count"] or 0,
                    "input": r["total_input_tokens"] or 0,
                    "output": r["total_output_tokens"] or 0,
                    "cache_read": r["total_cache_read"] or 0,
                })

        # Combined daily (both providers)
        if has_turns_provider:
            combrows = conn2.execute("""
                SELECT substr(timestamp,1,10) as day,
                       COALESCE(provider,'claude') as provider,
                       SUM(input_tokens) as input, SUM(output_tokens) as output,
                       SUM(cache_read_tokens) as cache_read, COUNT(*) as turns
                FROM turns GROUP BY day, provider ORDER BY day, provider
            """).fetchall()
        else:
            combrows = conn2.execute("""
                SELECT substr(timestamp,1,10) as day,
                       'claude' as provider,
                       SUM(input_tokens) as input, SUM(output_tokens) as output,
                       SUM(cache_read_tokens) as cache_read, COUNT(*) as turns
                FROM turns GROUP BY day ORDER BY day
            """).fetchall()
        combined_daily_rows = [{
            "day": r["day"], "provider": r["provider"],
            "input": r["input"] or 0, "output": r["output"] or 0,
            "cache_read": r["cache_read"] or 0, "turns": r["turns"] or 0,
        } for r in combrows]

        conn2.close()

    codex_strip = get_codex_status(db_path)

    return {
        "all_models":          all_models,
        "daily_by_model":      daily_by_model,
        "sessions_all":        sessions_all,
        "generated_at":        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "codex_strip":         codex_strip,
        "codex_daily":         codex_daily_rows,
        "codex_sessions":      codex_sessions_all,
        "combined_daily":      combined_daily_rows,
    }


HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AI Usage Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  :root {
    --bg: #0f1117;
    --card: #1a1d27;
    --border: #2a2d3a;
    --text: #e2e8f0;
    --muted: #8892a4;
    --accent: #d97757;
    --blue: #4f8ef7;
    --green: #4ade80;
    --codex: #22c55e;
    --combined: #60a5fa;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; font-size: 14px; }

  header { background: var(--card); border-bottom: 1px solid var(--border); padding: 16px 24px; display: flex; align-items: center; justify-content: space-between; gap: 12px; }
  header h1 { font-size: 18px; font-weight: 600; color: var(--accent); }
  header .meta { color: var(--muted); font-size: 12px; }
  #rescan-btn { background: var(--card); border: 1px solid var(--border); color: var(--muted); padding: 4px 12px; border-radius: 6px; cursor: pointer; font-size: 12px; }
  #rescan-btn:hover { color: var(--text); border-color: var(--accent); }
  #rescan-btn:disabled { opacity: 0.5; cursor: not-allowed; }

  .tab-bar {
    background: var(--card);
    border-bottom: 1px solid var(--border);
    padding: 0 24px;
    display: flex;
    gap: 24px;
  }
  .tab-btn {
    background: transparent;
    color: var(--muted);
    border: none;
    border-bottom: 2px solid transparent;
    padding: 12px 0 10px;
    font-size: 13px;
    font-weight: 600;
    letter-spacing: 0.02em;
    cursor: pointer;
  }
  .tab-btn:hover { color: var(--text); }
  .tab-btn.active { color: var(--text); border-bottom-color: var(--accent); }

  .status-strip {
    max-width: 1400px;
    margin: 16px auto 0;
    padding: 0 24px;
    display: grid;
    gap: 16px;
    grid-template-columns: 1fr 1fr;
  }
  .status-card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 14px 16px;
  }
  .status-title {
    color: var(--muted);
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-bottom: 10px;
  }
  .status-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 14px;
    align-items: center;
  }
  .gauge-wrap { display: flex; align-items: center; gap: 10px; }
  .gauge {
    width: 96px;
    height: 48px;
    border-radius: 96px 96px 0 0;
    overflow: hidden;
    position: relative;
    background: #2a2d3a;
    flex-shrink: 0;
  }
  .gauge::after {
    content: '';
    position: absolute;
    left: 8px;
    right: 8px;
    bottom: 0;
    height: 40px;
    border-radius: 88px 88px 0 0;
    background: var(--card);
  }
  .gauge-value {
    position: absolute;
    left: 0;
    right: 0;
    bottom: 7px;
    text-align: center;
    font-size: 11px;
    font-family: monospace;
    z-index: 1;
    color: var(--text);
  }
  .gauge-meta .glabel { color: var(--muted); font-size: 11px; margin-bottom: 2px; }
  .gauge-meta .greset { color: var(--text); font-size: 12px; }
  .chips { margin-top: 10px; display: flex; gap: 8px; flex-wrap: wrap; }
  .chip {
    border: 1px solid var(--border);
    border-radius: 999px;
    padding: 2px 8px;
    font-size: 11px;
    color: var(--muted);
  }
  .chip.ok { border-color: rgba(74,222,128,0.35); color: #86efac; }
  .status-placeholder {
    color: #6b7280;
    font-size: 13px;
    border: 1px dashed #374151;
    border-radius: 6px;
    padding: 10px 12px;
    line-height: 1.5;
  }

  .tab-content { display: none; }
  .tab-content.active { display: block; }

  #filter-bar { background: var(--card); border-bottom: 1px solid var(--border); padding: 10px 24px; display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
  .filter-label { font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted); white-space: nowrap; }
  .filter-sep { width: 1px; height: 22px; background: var(--border); flex-shrink: 0; }
  #model-checkboxes { display: flex; flex-wrap: wrap; gap: 6px; }
  .model-cb-label { display: flex; align-items: center; gap: 5px; padding: 3px 10px; border-radius: 20px; border: 1px solid var(--border); cursor: pointer; font-size: 12px; color: var(--muted); transition: border-color 0.15s, color 0.15s, background 0.15s; user-select: none; }
  .model-cb-label:hover { border-color: var(--accent); color: var(--text); }
  .model-cb-label.checked { background: rgba(217,119,87,0.12); border-color: var(--accent); color: var(--text); }
  .model-cb-label input { display: none; }
  .filter-btn { padding: 3px 10px; border-radius: 4px; border: 1px solid var(--border); background: transparent; color: var(--muted); font-size: 11px; cursor: pointer; white-space: nowrap; }
  .filter-btn:hover { border-color: var(--accent); color: var(--text); }
  .range-group { display: flex; border: 1px solid var(--border); border-radius: 6px; overflow: hidden; flex-shrink: 0; }
  .range-btn { padding: 4px 13px; background: transparent; border: none; border-right: 1px solid var(--border); color: var(--muted); font-size: 12px; cursor: pointer; transition: background 0.15s, color 0.15s; }
  .range-btn:last-child { border-right: none; }
  .range-btn:hover { background: rgba(255,255,255,0.04); color: var(--text); }
  .range-btn.active { background: rgba(217,119,87,0.15); color: var(--accent); font-weight: 600; }

  .container { max-width: 1400px; margin: 0 auto; padding: 24px; }
  .stats-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 16px; margin-bottom: 24px; }
  .stat-card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 16px; }
  .stat-card .label { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 6px; }
  .stat-card .value { font-size: 22px; font-weight: 700; }
  .stat-card .sub { color: var(--muted); font-size: 11px; margin-top: 4px; }

  .charts-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 24px; }
  .chart-card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 20px; }
  .chart-card.wide { grid-column: 1 / -1; }
  .chart-card h2 { font-size: 13px; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 16px; }
  .chart-wrap { position: relative; height: 240px; }
  .chart-wrap.tall { height: 300px; }

  table { width: 100%; border-collapse: collapse; }
  th { text-align: left; padding: 8px 12px; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted); border-bottom: 1px solid var(--border); white-space: nowrap; }
  th.sortable { cursor: pointer; user-select: none; }
  th.sortable:hover { color: var(--text); }
  .sort-icon { font-size: 9px; opacity: 0.8; }
  td { padding: 10px 12px; border-bottom: 1px solid var(--border); font-size: 13px; }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: rgba(255,255,255,0.02); }
  .model-tag { display: inline-block; padding: 2px 7px; border-radius: 4px; font-size: 11px; background: rgba(79,142,247,0.15); color: var(--blue); }
  .cost { color: var(--green); font-family: monospace; }
  .cost-na { color: var(--muted); font-family: monospace; font-size: 11px; }
  .num { font-family: monospace; }
  .muted { color: var(--muted); }
  .section-title { font-size: 13px; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 12px; }
  .section-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; }
  .section-header .section-title { margin-bottom: 0; }
  .export-btn { background: var(--card); border: 1px solid var(--border); color: var(--muted); padding: 3px 10px; border-radius: 5px; cursor: pointer; font-size: 11px; }
  .export-btn:hover { color: var(--text); border-color: var(--accent); }
  .table-card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 20px; margin-bottom: 24px; overflow-x: auto; }
  .list-table td:first-child { max-width: 320px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }

  footer { border-top: 1px solid var(--border); padding: 20px 24px; margin-top: 8px; }
  .footer-content { max-width: 1400px; margin: 0 auto; }
  .footer-content p { color: var(--muted); font-size: 12px; line-height: 1.7; margin-bottom: 4px; }
  .footer-content p:last-child { margin-bottom: 0; }
  .footer-content a { color: var(--blue); text-decoration: none; }
  .footer-content a:hover { text-decoration: underline; }

  @media (max-width: 1024px) { .status-strip { grid-template-columns: 1fr; } }
  @media (max-width: 768px) {
    .charts-grid { grid-template-columns: 1fr; }
    .chart-card.wide { grid-column: 1; }
    .tab-bar { gap: 14px; overflow-x: auto; }
    header { flex-wrap: wrap; }
  }
</style>
</head>
<body>
<header>
  <h1>AI Usage Dashboard</h1>
  <div class="meta" id="meta">Loading...</div>
  <button id="rescan-btn" onclick="triggerRescan()" title="Rebuild the database from scratch by re-scanning all JSONL files. Use if data looks stale or costs seem wrong.">&#x21bb; Rescan</button>
</header>

<div class="tab-bar">
  <button class="tab-btn active" data-tab="claude" onclick="setTab('claude')">Claude</button>
  <button class="tab-btn" data-tab="codex" onclick="setTab('codex')">Codex</button>
  <button class="tab-btn" data-tab="combined" onclick="setTab('combined')">Combined</button>
</div>

<div class="status-strip">
  <div class="status-card">
    <div class="status-title">Codex Status</div>
    <div id="codex-strip-body" class="status-grid"></div>
    <div class="chips" id="codex-strip-chips"></div>
  </div>
  <div class="status-card">
    <div class="status-title">Gemini Status</div>
    <div class="status-placeholder">Gemini: not configured - Run <code>gemini auth login</code></div>
  </div>
</div>

<div id="tab-claude" class="tab-content active">
  <div id="filter-bar">
    <div class="filter-label">Models</div>
    <div id="model-checkboxes"></div>
    <button class="filter-btn" onclick="selectAllModels()">All</button>
    <button class="filter-btn" onclick="clearAllModels()">None</button>
    <div class="filter-sep"></div>
    <div class="filter-label">Range</div>
    <div class="range-group">
      <button class="range-btn" data-range="7d" onclick="setRange('7d')">7d</button>
      <button class="range-btn" data-range="30d" onclick="setRange('30d')">30d</button>
      <button class="range-btn" data-range="90d" onclick="setRange('90d')">90d</button>
      <button class="range-btn" data-range="all" onclick="setRange('all')">All</button>
    </div>
  </div>

  <div class="container">
    <div class="stats-row" id="stats-row"></div>
    <div class="charts-grid">
      <div class="chart-card wide">
        <h2 id="daily-chart-title">Daily Token Usage</h2>
        <div class="chart-wrap tall"><canvas id="chart-daily"></canvas></div>
      </div>
      <div class="chart-card">
        <h2>By Model</h2>
        <div class="chart-wrap"><canvas id="chart-model"></canvas></div>
      </div>
      <div class="chart-card">
        <h2>Top Projects by Tokens</h2>
        <div class="chart-wrap"><canvas id="chart-project"></canvas></div>
      </div>
    </div>
    <div class="table-card">
      <div class="section-title">Cost by Model</div>
      <table>
        <thead><tr>
          <th>Model</th>
          <th class="sortable" onclick="setModelSort('turns')">Turns <span class="sort-icon" id="msort-turns"></span></th>
          <th class="sortable" onclick="setModelSort('input')">Input <span class="sort-icon" id="msort-input"></span></th>
          <th class="sortable" onclick="setModelSort('output')">Output <span class="sort-icon" id="msort-output"></span></th>
          <th class="sortable" onclick="setModelSort('cache_read')">Cache Read <span class="sort-icon" id="msort-cache_read"></span></th>
          <th class="sortable" onclick="setModelSort('cache_creation')">Cache Creation <span class="sort-icon" id="msort-cache_creation"></span></th>
          <th class="sortable" onclick="setModelSort('cost')">Est. Cost <span class="sort-icon" id="msort-cost"></span></th>
        </tr></thead>
        <tbody id="model-cost-body"></tbody>
      </table>
    </div>
    <div class="table-card">
      <div class="section-header"><div class="section-title">Recent Sessions</div><button class="export-btn" onclick="exportSessionsCSV()" title="Export all filtered sessions to CSV">&#x2913; CSV</button></div>
      <table>
        <thead><tr>
          <th>Session</th>
          <th>Project</th>
          <th class="sortable" onclick="setSessionSort('last')">Last Active <span class="sort-icon" id="sort-icon-last"></span></th>
          <th class="sortable" onclick="setSessionSort('duration_min')">Duration <span class="sort-icon" id="sort-icon-duration_min"></span></th>
          <th>Model</th>
          <th class="sortable" onclick="setSessionSort('turns')">Turns <span class="sort-icon" id="sort-icon-turns"></span></th>
          <th class="sortable" onclick="setSessionSort('input')">Input <span class="sort-icon" id="sort-icon-input"></span></th>
          <th class="sortable" onclick="setSessionSort('output')">Output <span class="sort-icon" id="sort-icon-output"></span></th>
          <th class="sortable" onclick="setSessionSort('cost')">Est. Cost <span class="sort-icon" id="sort-icon-cost"></span></th>
        </tr></thead>
        <tbody id="sessions-body"></tbody>
      </table>
    </div>
    <div class="table-card">
      <div class="section-header"><div class="section-title">Cost by Project</div><button class="export-btn" onclick="exportProjectsCSV()" title="Export all projects to CSV">&#x2913; CSV</button></div>
      <table>
        <thead><tr>
          <th>Project</th>
          <th class="sortable" onclick="setProjectSort('sessions')">Sessions <span class="sort-icon" id="psort-sessions"></span></th>
          <th class="sortable" onclick="setProjectSort('turns')">Turns <span class="sort-icon" id="psort-turns"></span></th>
          <th class="sortable" onclick="setProjectSort('input')">Input <span class="sort-icon" id="psort-input"></span></th>
          <th class="sortable" onclick="setProjectSort('output')">Output <span class="sort-icon" id="psort-output"></span></th>
          <th class="sortable" onclick="setProjectSort('cost')">Est. Cost <span class="sort-icon" id="psort-cost"></span></th>
        </tr></thead>
        <tbody id="project-cost-body"></tbody>
      </table>
    </div>
  </div>
</div>

<div id="tab-codex" class="tab-content">
  <div class="container">
    <div class="stats-row" id="codex-stats-row"></div>
    <div class="charts-grid">
      <div class="chart-card wide">
        <h2>Codex Daily Usage by Model</h2>
        <div class="chart-wrap tall"><canvas id="chart-codex-daily"></canvas></div>
      </div>
      <div class="chart-card">
        <h2>Token Distribution</h2>
        <div class="chart-wrap"><canvas id="chart-codex-token"></canvas></div>
      </div>
      <div class="chart-card">
        <h2>Top Projects by Tokens</h2>
        <table class="list-table">
          <thead><tr><th>Project</th><th>Tokens</th><th>Cost</th></tr></thead>
          <tbody id="codex-top-projects"></tbody>
        </table>
      </div>
    </div>
    <div class="table-card">
      <div class="section-title">Codex Sessions</div>
      <table>
        <thead><tr>
          <th>Session</th>
          <th>Project</th>
          <th class="sortable" onclick="setCodexSessionSort('last')">Last Active <span class="sort-icon" id="csort-icon-last"></span></th>
          <th class="sortable" onclick="setCodexSessionSort('duration_min')">Duration <span class="sort-icon" id="csort-icon-duration_min"></span></th>
          <th>Model</th>
          <th class="sortable" onclick="setCodexSessionSort('turns')">Turns <span class="sort-icon" id="csort-icon-turns"></span></th>
          <th class="sortable" onclick="setCodexSessionSort('input')">Input <span class="sort-icon" id="csort-icon-input"></span></th>
          <th class="sortable" onclick="setCodexSessionSort('output')">Output <span class="sort-icon" id="csort-icon-output"></span></th>
          <th class="sortable" onclick="setCodexSessionSort('cost')">Est. Cost <span class="sort-icon" id="csort-icon-cost"></span></th>
        </tr></thead>
        <tbody id="codex-sessions-body"></tbody>
      </table>
    </div>
    <div class="table-card">
      <div class="section-title">Codex Cost by Model</div>
      <table>
        <thead><tr>
          <th>Model</th>
          <th>Turns</th>
          <th>Input</th>
          <th>Output</th>
          <th>Cache Read</th>
          <th>Est. Cost</th>
        </tr></thead>
        <tbody id="codex-model-cost-body"></tbody>
      </table>
    </div>
  </div>
</div>

<div id="tab-combined" class="tab-content">
  <div class="container">
    <div class="stats-row" id="combined-stats-row"></div>
    <div class="charts-grid">
      <div class="chart-card wide">
        <h2>Daily Usage by Provider</h2>
        <div class="chart-wrap tall"><canvas id="chart-combined-daily"></canvas></div>
      </div>
      <div class="chart-card">
        <h2>Token Distribution (All Providers)</h2>
        <div class="chart-wrap"><canvas id="chart-combined-token"></canvas></div>
      </div>
      <div class="chart-card">
        <h2>Top Projects (Union)</h2>
        <table class="list-table">
          <thead><tr><th>Project</th><th>Sessions</th><th>Turns</th><th>Cost</th></tr></thead>
          <tbody id="combined-projects-body"></tbody>
        </table>
      </div>
    </div>
  </div>
</div>

<footer>
  <div class="footer-content">
    <p>Cost estimates use Anthropic and OpenAI API rates (April 2026). Subscription plans may bill differently than API token pricing.</p>
    <p>
      GitHub: <a href="https://github.com/phuryn/claude-usage" target="_blank">https://github.com/phuryn/claude-usage</a>
      &nbsp;&middot;&nbsp;
      Created by: <a href="https://www.productcompass.pm" target="_blank">The Product Compass Newsletter</a>
      &nbsp;&middot;&nbsp;
      License: MIT
    </p>
  </div>
</footer>

<script>
function esc(s) {
  const d = document.createElement('div');
  d.textContent = String(s);
  return d.innerHTML;
}

let rawData = null;
window._data = null;
let selectedModels = new Set();
let selectedRange = '30d';
let selectedTab = 'claude';
let charts = {};
let sessionSortCol = 'last';
let sessionSortDir = 'desc';
let modelSortCol = 'cost';
let modelSortDir = 'desc';
let projectSortCol = 'cost';
let projectSortDir = 'desc';
let codexSessionSortCol = 'last';
let codexSessionSortDir = 'desc';
let lastFilteredSessions = [];
let lastByProject = [];

const PRICING = {
  'claude-opus-4-6': { input: 5.00, output: 25.00, cache_write: 6.25, cache_read: 0.50 },
  'claude-opus-4-5': { input: 5.00, output: 25.00, cache_write: 6.25, cache_read: 0.50 },
  'claude-sonnet-4-6': { input: 3.00, output: 15.00, cache_write: 3.75, cache_read: 0.30 },
  'claude-sonnet-4-5': { input: 3.00, output: 15.00, cache_write: 3.75, cache_read: 0.30 },
  'claude-haiku-4-5': { input: 1.00, output: 5.00, cache_write: 1.25, cache_read: 0.10 },
  'claude-haiku-4-6': { input: 1.00, output: 5.00, cache_write: 1.25, cache_read: 0.10 }
};

const CODEX_PRICING = {
  'gpt-5': { in: 1.25, cached_in: 0.125, out: 10.0 },
  'gpt-5.2-codex': { in: 1.25, cached_in: 0.125, out: 10.0 },
  'gpt-5.3-codex': { in: 1.25, cached_in: 0.125, out: 10.0 }
};
function codexCost(model, inp, cached, out) {
  const p = CODEX_PRICING[model] || CODEX_PRICING['gpt-5'];
  // inp is already net (uncached) from DB; cached is cache_read_tokens
  return (inp * p.in + cached * p.cached_in + out * p.out) / 1e6;
}

const TOKEN_COLORS = {
  input: 'rgba(79,142,247,0.8)',
  output: 'rgba(167,139,250,0.8)',
  cache_read: 'rgba(74,222,128,0.6)',
  cache_creation: 'rgba(251,191,36,0.6)',
  codex: 'rgba(34,197,94,0.8)',
  claude: 'rgba(217,119,87,0.8)'
};
const MODEL_COLORS = ['#d97757','#4f8ef7','#4ade80','#a78bfa','#fbbf24','#f472b6','#34d399','#60a5fa'];
const RANGE_LABELS = { '7d': 'Last 7 Days', '30d': 'Last 30 Days', '90d': 'Last 90 Days', 'all': 'All Time' };
const RANGE_TICKS = { '7d': 7, '30d': 15, '90d': 13, 'all': 12 };

function fmt(n) {
  n = Number(n || 0);
  if (n >= 1e9) return (n/1e9).toFixed(2) + 'B';
  if (n >= 1e6) return (n/1e6).toFixed(2) + 'M';
  if (n >= 1e3) return (n/1e3).toFixed(1) + 'K';
  return n.toLocaleString();
}
function fmtCost(c) { return '$' + Number(c || 0).toFixed(4); }
function fmtCostBig(c) { return '$' + Number(c || 0).toFixed(2); }

function isBillable(model) {
  if (!model) return false;
  const m = model.toLowerCase();
  return m.includes('opus') || m.includes('sonnet') || m.includes('haiku');
}

function getPricing(model) {
  if (!model) return null;
  if (PRICING[model]) return PRICING[model];
  for (const key of Object.keys(PRICING)) {
    if (model.startsWith(key)) return PRICING[key];
  }
  const m = model.toLowerCase();
  if (m.includes('opus')) return PRICING['claude-opus-4-6'];
  if (m.includes('sonnet')) return PRICING['claude-sonnet-4-6'];
  if (m.includes('haiku')) return PRICING['claude-haiku-4-5'];
  return null;
}

function calcCost(model, inp, out, cacheRead, cacheCreation) {
  if (!isBillable(model)) return 0;
  const p = getPricing(model);
  if (!p) return 0;
  return (
    inp * p.input / 1e6 +
    out * p.output / 1e6 +
    cacheRead * p.cache_read / 1e6 +
    cacheCreation * p.cache_write / 1e6
  );
}

function readURLRange() {
  const p = new URLSearchParams(window.location.search).get('range');
  return ['7d', '30d', '90d', 'all'].includes(p) ? p : '30d';
}
function getRangeCutoff(range) {
  if (range === 'all') return null;
  const days = range === '7d' ? 7 : range === '30d' ? 30 : 90;
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d.toISOString().slice(0, 10);
}
function getRangeCutoffDays(days) {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d.toISOString().slice(0, 10);
}

function setRange(range) {
  selectedRange = range;
  document.querySelectorAll('.range-btn').forEach(btn => btn.classList.toggle('active', btn.dataset.range === range));
  updateURL();
  applyFilter();
}

function setTab(tab, persist=true) {
  selectedTab = tab;
  document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.toggle('active', btn.dataset.tab === tab));
  document.querySelectorAll('.tab-content').forEach(div => div.classList.toggle('active', div.id === 'tab-' + tab));
  if (persist) localStorage.setItem('aidash_tab', tab);
}

function initTabs() {
  const saved = localStorage.getItem('aidash_tab');
  setTab(saved || 'claude', false);
}

function modelPriority(m) {
  const ml = m.toLowerCase();
  if (ml.includes('opus')) return 0;
  if (ml.includes('sonnet')) return 1;
  if (ml.includes('haiku')) return 2;
  return 3;
}

function readURLModels(allModels) {
  const param = new URLSearchParams(window.location.search).get('models');
  if (!param) return new Set(allModels.filter(m => isBillable(m)));
  const fromURL = new Set(param.split(',').map(s => s.trim()).filter(Boolean));
  return new Set(allModels.filter(m => fromURL.has(m)));
}

function isDefaultModelSelection(allModels) {
  const billable = allModels.filter(m => isBillable(m));
  if (selectedModels.size !== billable.length) return false;
  return billable.every(m => selectedModels.has(m));
}

function buildFilterUI(allModels) {
  const sorted = [...allModels].sort((a, b) => {
    const pa = modelPriority(a), pb = modelPriority(b);
    return pa !== pb ? pa - pb : a.localeCompare(b);
  });
  selectedModels = readURLModels(allModels);
  const container = document.getElementById('model-checkboxes');
  container.innerHTML = sorted.map(m => {
    const checked = selectedModels.has(m);
    return `<label class="model-cb-label ${checked ? 'checked' : ''}" data-model="${esc(m)}">
      <input type="checkbox" value="${esc(m)}" ${checked ? 'checked' : ''} onchange="onModelToggle(this)">
      ${esc(m)}
    </label>`;
  }).join('');
}

function onModelToggle(cb) {
  const label = cb.closest('label');
  if (cb.checked) { selectedModels.add(cb.value); label.classList.add('checked'); }
  else { selectedModels.delete(cb.value); label.classList.remove('checked'); }
  updateURL();
  applyFilter();
}

function selectAllModels() {
  document.querySelectorAll('#model-checkboxes input').forEach(cb => {
    cb.checked = true;
    selectedModels.add(cb.value);
    cb.closest('label').classList.add('checked');
  });
  updateURL();
  applyFilter();
}

function clearAllModels() {
  document.querySelectorAll('#model-checkboxes input').forEach(cb => {
    cb.checked = false;
    selectedModels.delete(cb.value);
    cb.closest('label').classList.remove('checked');
  });
  updateURL();
  applyFilter();
}

function updateURL() {
  const allModels = Array.from(document.querySelectorAll('#model-checkboxes input')).map(cb => cb.value);
  const params = new URLSearchParams();
  if (selectedRange !== '30d') params.set('range', selectedRange);
  if (!isDefaultModelSelection(allModels)) params.set('models', Array.from(selectedModels).join(','));
  const search = params.toString() ? '?' + params.toString() : '';
  history.replaceState(null, '', window.location.pathname + search);
}

function setSessionSort(col) {
  if (sessionSortCol === col) sessionSortDir = sessionSortDir === 'desc' ? 'asc' : 'desc';
  else { sessionSortCol = col; sessionSortDir = 'desc'; }
  updateSortIcons();
  applyFilter();
}

function updateSortIcons() {
  document.querySelectorAll('[id^="sort-icon-"]').forEach(el => { el.textContent = ''; });
  const icon = document.getElementById('sort-icon-' + sessionSortCol);
  if (icon) icon.textContent = sessionSortDir === 'desc' ? ' \u25bc' : ' \u25b2';
}

function sortSessions(sessions) {
  return [...sessions].sort((a, b) => {
    let av, bv;
    if (sessionSortCol === 'cost') {
      av = calcCost(a.model, a.input, a.output, a.cache_read, a.cache_creation);
      bv = calcCost(b.model, b.input, b.output, b.cache_read, b.cache_creation);
    } else if (sessionSortCol === 'duration_min') {
      av = parseFloat(a.duration_min) || 0;
      bv = parseFloat(b.duration_min) || 0;
    } else {
      av = a[sessionSortCol] ?? 0;
      bv = b[sessionSortCol] ?? 0;
    }
    if (av < bv) return sessionSortDir === 'desc' ? 1 : -1;
    if (av > bv) return sessionSortDir === 'desc' ? -1 : 1;
    return 0;
  });
}

function setModelSort(col) {
  if (modelSortCol === col) modelSortDir = modelSortDir === 'desc' ? 'asc' : 'desc';
  else { modelSortCol = col; modelSortDir = 'desc'; }
  updateModelSortIcons();
  applyFilter();
}

function updateModelSortIcons() {
  document.querySelectorAll('[id^="msort-"]').forEach(el => { el.textContent = ''; });
  const icon = document.getElementById('msort-' + modelSortCol);
  if (icon) icon.textContent = modelSortDir === 'desc' ? ' \u25bc' : ' \u25b2';
}

function sortModels(byModel) {
  return [...byModel].sort((a, b) => {
    let av, bv;
    if (modelSortCol === 'cost') {
      av = calcCost(a.model, a.input, a.output, a.cache_read, a.cache_creation);
      bv = calcCost(b.model, b.input, b.output, b.cache_read, b.cache_creation);
    } else {
      av = a[modelSortCol] ?? 0;
      bv = b[modelSortCol] ?? 0;
    }
    if (av < bv) return modelSortDir === 'desc' ? 1 : -1;
    if (av > bv) return modelSortDir === 'desc' ? -1 : 1;
    return 0;
  });
}

function setProjectSort(col) {
  if (projectSortCol === col) projectSortDir = projectSortDir === 'desc' ? 'asc' : 'desc';
  else { projectSortCol = col; projectSortDir = 'desc'; }
  updateProjectSortIcons();
  applyFilter();
}

function updateProjectSortIcons() {
  document.querySelectorAll('[id^="psort-"]').forEach(el => { el.textContent = ''; });
  const icon = document.getElementById('psort-' + projectSortCol);
  if (icon) icon.textContent = projectSortDir === 'desc' ? ' \u25bc' : ' \u25b2';
}

function sortProjects(byProject) {
  return [...byProject].sort((a, b) => {
    const av = a[projectSortCol] ?? 0;
    const bv = b[projectSortCol] ?? 0;
    if (av < bv) return projectSortDir === 'desc' ? 1 : -1;
    if (av > bv) return projectSortDir === 'desc' ? -1 : 1;
    return 0;
  });
}

function applyFilter() {
  if (!rawData) return;
  const cutoff = getRangeCutoff(selectedRange);

  const filteredDaily = (rawData.daily_by_model || []).filter(r => selectedModels.has(r.model) && (!cutoff || r.day >= cutoff));
  const dailyMap = {};
  for (const r of filteredDaily) {
    if (!dailyMap[r.day]) dailyMap[r.day] = { day: r.day, input: 0, output: 0, cache_read: 0, cache_creation: 0 };
    const d = dailyMap[r.day];
    d.input += r.input || 0;
    d.output += r.output || 0;
    d.cache_read += r.cache_read || 0;
    d.cache_creation += r.cache_creation || 0;
  }
  const daily = Object.values(dailyMap).sort((a, b) => a.day.localeCompare(b.day));

  const modelMap = {};
  for (const r of filteredDaily) {
    if (!modelMap[r.model]) modelMap[r.model] = { model: r.model, input: 0, output: 0, cache_read: 0, cache_creation: 0, turns: 0, sessions: 0 };
    const m = modelMap[r.model];
    m.input += r.input || 0;
    m.output += r.output || 0;
    m.cache_read += r.cache_read || 0;
    m.cache_creation += r.cache_creation || 0;
    m.turns += r.turns || 0;
  }

  const filteredSessions = (rawData.sessions_all || []).filter(s => selectedModels.has(s.model) && (!cutoff || s.last_date >= cutoff));
  for (const s of filteredSessions) if (modelMap[s.model]) modelMap[s.model].sessions++;
  const byModel = Object.values(modelMap).sort((a, b) => (b.input + b.output) - (a.input + a.output));

  const projMap = {};
  for (const s of filteredSessions) {
    if (!projMap[s.project]) projMap[s.project] = { project: s.project, input: 0, output: 0, cache_read: 0, cache_creation: 0, turns: 0, sessions: 0, cost: 0 };
    const p = projMap[s.project];
    p.input += s.input || 0;
    p.output += s.output || 0;
    p.cache_read += s.cache_read || 0;
    p.cache_creation += s.cache_creation || 0;
    p.turns += s.turns || 0;
    p.sessions++;
    p.cost += calcCost(s.model, s.input, s.output, s.cache_read, s.cache_creation);
  }
  const byProject = Object.values(projMap).sort((a, b) => (b.input + b.output) - (a.input + a.output));

  const totals = {
    sessions: filteredSessions.length,
    turns: byModel.reduce((s, m) => s + m.turns, 0),
    input: byModel.reduce((s, m) => s + m.input, 0),
    output: byModel.reduce((s, m) => s + m.output, 0),
    cache_read: byModel.reduce((s, m) => s + m.cache_read, 0),
    cache_creation: byModel.reduce((s, m) => s + m.cache_creation, 0),
    cost: byModel.reduce((s, m) => s + calcCost(m.model, m.input, m.output, m.cache_read, m.cache_creation), 0)
  };

  document.getElementById('daily-chart-title').textContent = 'Daily Token Usage - ' + RANGE_LABELS[selectedRange];
  renderClaudeStats(totals);
  renderDailyChart(daily);
  renderModelChart(byModel);
  renderProjectChart(byProject);
  lastFilteredSessions = sortSessions(filteredSessions);
  lastByProject = sortProjects(byProject);
  renderSessionsTable(lastFilteredSessions.slice(0, 20));
  renderModelCostTable(byModel);
  renderProjectCostTable(lastByProject.slice(0, 20));

  renderCodexTab();
  renderCombinedTab();
}

function renderClaudeStats(t) {
  const rangeLabel = RANGE_LABELS[selectedRange].toLowerCase();
  const stats = [
    { label: 'Sessions', value: t.sessions.toLocaleString(), sub: rangeLabel },
    { label: 'Turns', value: fmt(t.turns), sub: rangeLabel },
    { label: 'Input Tokens', value: fmt(t.input), sub: rangeLabel },
    { label: 'Output Tokens', value: fmt(t.output), sub: rangeLabel },
    { label: 'Cache Read', value: fmt(t.cache_read), sub: 'from prompt cache' },
    { label: 'Cache Creation', value: fmt(t.cache_creation), sub: 'writes to prompt cache' },
    { label: 'Est. Cost', value: fmtCostBig(t.cost), sub: 'API pricing, Apr 2026', color: '#4ade80' }
  ];
  document.getElementById('stats-row').innerHTML = stats.map(s => `
    <div class="stat-card">
      <div class="label">${s.label}</div>
      <div class="value" style="${s.color ? 'color:' + s.color : ''}">${esc(s.value)}</div>
      ${s.sub ? `<div class="sub">${esc(s.sub)}</div>` : ''}
    </div>
  `).join('');
}

function renderDailyChart(daily) {
  const ctx = document.getElementById('chart-daily').getContext('2d');
  if (charts.daily) charts.daily.destroy();
  charts.daily = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: daily.map(d => d.day),
      datasets: [
        { label: 'Input', data: daily.map(d => d.input), backgroundColor: TOKEN_COLORS.input, stack: 'tokens' },
        { label: 'Output', data: daily.map(d => d.output), backgroundColor: TOKEN_COLORS.output, stack: 'tokens' },
        { label: 'Cache Read', data: daily.map(d => d.cache_read), backgroundColor: TOKEN_COLORS.cache_read, stack: 'tokens' },
        { label: 'Cache Creation', data: daily.map(d => d.cache_creation), backgroundColor: TOKEN_COLORS.cache_creation, stack: 'tokens' }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { labels: { color: '#8892a4', boxWidth: 12 } } },
      scales: {
        x: { ticks: { color: '#8892a4', maxTicksLimit: RANGE_TICKS[selectedRange] }, grid: { color: '#2a2d3a' } },
        y: { ticks: { color: '#8892a4', callback: v => fmt(v) }, grid: { color: '#2a2d3a' } }
      }
    }
  });
}

function renderModelChart(byModel) {
  const ctx = document.getElementById('chart-model').getContext('2d');
  if (charts.model) charts.model.destroy();
  if (!byModel.length) return;
  charts.model = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: byModel.map(m => m.model),
      datasets: [{ data: byModel.map(m => (m.input || 0) + (m.output || 0)), backgroundColor: MODEL_COLORS, borderWidth: 2, borderColor: '#1a1d27' }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { position: 'bottom', labels: { color: '#8892a4', boxWidth: 12, font: { size: 11 } } },
        tooltip: { callbacks: { label: ctx => ` ${ctx.label}: ${fmt(ctx.raw)} tokens` } }
      }
    }
  });
}

function renderProjectChart(byProject) {
  const top = byProject.slice(0, 10);
  const ctx = document.getElementById('chart-project').getContext('2d');
  if (charts.project) charts.project.destroy();
  if (!top.length) return;
  charts.project = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: top.map(p => p.project.length > 22 ? '...' + p.project.slice(-20) : p.project),
      datasets: [
        { label: 'Input', data: top.map(p => p.input), backgroundColor: TOKEN_COLORS.input },
        { label: 'Output', data: top.map(p => p.output), backgroundColor: TOKEN_COLORS.output }
      ]
    },
    options: {
      indexAxis: 'y', responsive: true, maintainAspectRatio: false,
      plugins: { legend: { labels: { color: '#8892a4', boxWidth: 12 } } },
      scales: {
        x: { ticks: { color: '#8892a4', callback: v => fmt(v) }, grid: { color: '#2a2d3a' } },
        y: { ticks: { color: '#8892a4', font: { size: 11 } }, grid: { color: '#2a2d3a' } }
      }
    }
  });
}

function renderSessionsTable(sessions) {
  document.getElementById('sessions-body').innerHTML = sessions.map(s => {
    const cost = calcCost(s.model, s.input, s.output, s.cache_read, s.cache_creation);
    const costCell = isBillable(s.model) ? `<td class="cost">${fmtCost(cost)}</td>` : `<td class="cost-na">n/a</td>`;
    return `<tr>
      <td class="muted" style="font-family:monospace">${esc(s.session_id)}&hellip;</td>
      <td>${esc(s.project)}</td>
      <td class="muted">${esc(s.last)}</td>
      <td class="muted">${esc(s.duration_min)}m</td>
      <td><span class="model-tag">${esc(s.model)}</span></td>
      <td class="num">${s.turns}</td>
      <td class="num">${fmt(s.input)}</td>
      <td class="num">${fmt(s.output)}</td>
      ${costCell}
    </tr>`;
  }).join('');
}

function renderModelCostTable(byModel) {
  document.getElementById('model-cost-body').innerHTML = sortModels(byModel).map(m => {
    const cost = calcCost(m.model, m.input, m.output, m.cache_read, m.cache_creation);
    const costCell = isBillable(m.model) ? `<td class="cost">${fmtCost(cost)}</td>` : `<td class="cost-na">n/a</td>`;
    return `<tr>
      <td><span class="model-tag">${esc(m.model)}</span></td>
      <td class="num">${fmt(m.turns)}</td>
      <td class="num">${fmt(m.input)}</td>
      <td class="num">${fmt(m.output)}</td>
      <td class="num">${fmt(m.cache_read)}</td>
      <td class="num">${fmt(m.cache_creation)}</td>
      ${costCell}
    </tr>`;
  }).join('');
}

function renderProjectCostTable(byProject) {
  document.getElementById('project-cost-body').innerHTML = sortProjects(byProject).map(p => `<tr>
      <td>${esc(p.project)}</td>
      <td class="num">${p.sessions}</td>
      <td class="num">${fmt(p.turns)}</td>
      <td class="num">${fmt(p.input)}</td>
      <td class="num">${fmt(p.output)}</td>
      <td class="cost">${fmtCost(p.cost)}</td>
    </tr>`).join('');
}

function freshnessLabel(sec) {
  sec = Number(sec || 0);
  if (sec < 60) return 'fresh now';
  if (sec < 3600) return `${Math.floor(sec/60)}m old`;
  return `${Math.floor(sec/3600)}h old`;
}

function gaugeHTML(label, pct, resets) {
  const p = Math.max(0, Math.min(100, Number(pct || 0)));
  const deg = Math.round(p * 1.8);
  const bg = `conic-gradient(from 180deg, #22c55e 0deg ${deg}deg, #2a2d3a ${deg}deg 180deg)`;
  return `<div class="gauge-wrap">
    <div class="gauge" style="background:${bg}">
      <div class="gauge-value">${p.toFixed(0)}%</div>
    </div>
    <div class="gauge-meta">
      <div class="glabel">${esc(label)}</div>
      <div class="greset">${esc(resets || 'n/a')}</div>
    </div>
  </div>`;
}

function renderCodexStrip(strip) {
  const body = document.getElementById('codex-strip-body');
  const chips = document.getElementById('codex-strip-chips');
  if (!strip || strip.state === 'unavailable') {
    body.innerHTML = '<div class="muted">No Codex rate-limit data yet. Run `python cli.py scan --provider codex` first.</div>';
    chips.innerHTML = '<span class="chip">source: none</span>';
    return;
  }
  body.innerHTML = [
    gaugeHTML('5hr Window', strip.primary_pct, strip.primary_resets_str),
    gaugeHTML('7d Window', strip.secondary_pct, strip.secondary_resets_str)
  ].join('');
  chips.innerHTML = [
    `<span class="chip ok">${esc(freshnessLabel(strip.freshness_s))}</span>`,
    `<span class="chip">${esc(strip.plan_type || 'Plan: unknown')}</span>`,
    `<span class="chip">${esc(strip.source || 'source')}</span>`
  ].join('');
}

function aggregateCodexByModel(rows, cutoff=null) {
  const map = {};
  for (const r of rows || []) {
    if (cutoff && r.day < cutoff) continue;
    if (!map[r.model]) map[r.model] = { model: r.model, input: 0, output: 0, cache_read: 0, turns: 0 };
    const m = map[r.model];
    m.input += r.input || 0;
    m.output += r.output || 0;
    m.cache_read += r.cache_read || 0;
    m.turns += r.turns || 0;
  }
  return Object.values(map).sort((a, b) => (b.input + b.output + b.cache_read) - (a.input + a.output + a.cache_read));
}

function setCodexSessionSort(col) {
  if (codexSessionSortCol === col) codexSessionSortDir = codexSessionSortDir === 'desc' ? 'asc' : 'desc';
  else { codexSessionSortCol = col; codexSessionSortDir = 'desc'; }
  updateCodexSortIcons();
  renderCodexTab();
}

function updateCodexSortIcons() {
  document.querySelectorAll('[id^="csort-icon-"]').forEach(el => { el.textContent = ''; });
  const icon = document.getElementById('csort-icon-' + codexSessionSortCol);
  if (icon) icon.textContent = codexSessionSortDir === 'desc' ? ' \u25bc' : ' \u25b2';
}

function sortCodexSessions(rows) {
  return [...rows].sort((a, b) => {
    let av, bv;
    if (codexSessionSortCol === 'cost') {
      av = codexCost(a.model, a.input, a.cache_read, a.output);
      bv = codexCost(b.model, b.input, b.cache_read, b.output);
    } else if (codexSessionSortCol === 'duration_min') {
      av = parseFloat(a.duration_min) || 0;
      bv = parseFloat(b.duration_min) || 0;
    } else {
      av = a[codexSessionSortCol] ?? 0;
      bv = b[codexSessionSortCol] ?? 0;
    }
    if (av < bv) return codexSessionSortDir === 'desc' ? 1 : -1;
    if (av > bv) return codexSessionSortDir === 'desc' ? -1 : 1;
    return 0;
  });
}

function renderCodexTab() {
  const daily = rawData.codex_daily || [];
  const sessions = rawData.codex_sessions || [];
  const cutoff = getRangeCutoff(selectedRange);
  const cutoff7 = getRangeCutoffDays(7);
  const byModelAll = aggregateCodexByModel(daily, null);
  const byModelRange = aggregateCodexByModel(daily, cutoff);
  const byModel7 = aggregateCodexByModel(daily, cutoff7);
  const allCost = byModelAll.reduce((s, m) => s + codexCost(m.model, m.input, m.cache_read, m.output), 0);
  const sevenCost = byModel7.reduce((s, m) => s + codexCost(m.model, m.input, m.cache_read, m.output), 0);
  const topModel = byModelAll.length ? byModelAll[0].model : 'n/a';
  const turns = byModelAll.reduce((s, m) => s + (m.turns || 0), 0);

  const stats = [
    { label: 'Sessions', value: sessions.length.toLocaleString(), sub: 'all-time' },
    { label: 'Turns', value: fmt(turns), sub: 'all-time' },
    { label: 'Cost (7d)', value: fmtCostBig(sevenCost), sub: 'OpenAI API pricing', color: '#4ade80' },
    { label: 'Cost (All)', value: fmtCostBig(allCost), sub: 'OpenAI API pricing', color: '#4ade80' },
    { label: 'Top Model', value: topModel, sub: 'by token volume' }
  ];
  document.getElementById('codex-stats-row').innerHTML = stats.map(s => `
    <div class="stat-card">
      <div class="label">${s.label}</div>
      <div class="value" style="${s.color ? 'color:' + s.color : ''}">${esc(s.value)}</div>
      ${s.sub ? `<div class="sub">${esc(s.sub)}</div>` : ''}
    </div>
  `).join('');

  const dayMap = {};
  for (const r of daily) {
    if (cutoff && r.day < cutoff) continue;
    if (!dayMap[r.day]) dayMap[r.day] = {};
    dayMap[r.day][r.model] = (dayMap[r.day][r.model] || 0) + (r.input || 0) + (r.output || 0) + (r.cache_read || 0);
  }
  const days = Object.keys(dayMap).sort();
  const models = [...new Set(byModelRange.map(m => m.model))];
  const codexDatasets = models.map((m, i) => ({
    label: m,
    data: days.map(day => dayMap[day][m] || 0),
    backgroundColor: MODEL_COLORS[i % MODEL_COLORS.length],
    stack: 'codex'
  }));
  if (charts.codexDaily) charts.codexDaily.destroy();
  charts.codexDaily = new Chart(document.getElementById('chart-codex-daily').getContext('2d'), {
    type: 'bar',
    data: { labels: days, datasets: codexDatasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { labels: { color: '#8892a4', boxWidth: 12 } } },
      scales: {
        x: { ticks: { color: '#8892a4', maxTicksLimit: RANGE_TICKS[selectedRange] }, grid: { color: '#2a2d3a' } },
        y: { ticks: { color: '#8892a4', callback: v => fmt(v) }, grid: { color: '#2a2d3a' } }
      }
    }
  });

  const rangeTotals = byModelRange.reduce((acc, m) => {
    acc.input += m.input || 0;
    acc.output += m.output || 0;
    acc.cache_read += m.cache_read || 0;
    return acc;
  }, { input: 0, output: 0, cache_read: 0 });
  if (charts.codexToken) charts.codexToken.destroy();
  charts.codexToken = new Chart(document.getElementById('chart-codex-token').getContext('2d'), {
    type: 'doughnut',
    data: {
      labels: ['Input', 'Cached Input', 'Output'],
      datasets: [{ data: [rangeTotals.input, rangeTotals.cache_read, rangeTotals.output], backgroundColor: [TOKEN_COLORS.input, TOKEN_COLORS.cache_read, TOKEN_COLORS.output], borderWidth: 2, borderColor: '#1a1d27' }]
    },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom', labels: { color: '#8892a4', boxWidth: 12 } } } }
  });

  const projectMap = {};
  for (const s of sessions) {
    if (cutoff && s.last_date < cutoff) continue;
    if (!projectMap[s.project]) projectMap[s.project] = { project: s.project, tokens: 0, cost: 0 };
    const p = projectMap[s.project];
    p.tokens += (s.input || 0) + (s.output || 0) + (s.cache_read || 0);
    p.cost += codexCost(s.model, s.input || 0, s.cache_read || 0, s.output || 0);
  }
  const topProjects = Object.values(projectMap).sort((a, b) => b.tokens - a.tokens).slice(0, 10);
  document.getElementById('codex-top-projects').innerHTML = topProjects.map(p => `<tr>
      <td>${esc(p.project)}</td><td class="num">${fmt(p.tokens)}</td><td class="cost">${fmtCost(p.cost)}</td>
    </tr>`).join('');

  const sessionRows = sortCodexSessions(sessions.filter(s => !cutoff || s.last_date >= cutoff)).slice(0, 50);
  document.getElementById('codex-sessions-body').innerHTML = sessionRows.map(s => `<tr>
      <td class="muted" style="font-family:monospace">${esc(s.session_id)}&hellip;</td>
      <td>${esc(s.project)}</td>
      <td class="muted">${esc(s.last)}</td>
      <td class="muted">${esc(s.duration_min)}m</td>
      <td><span class="model-tag">${esc(s.model)}</span></td>
      <td class="num">${fmt(s.turns)}</td>
      <td class="num">${fmt(s.input)}</td>
      <td class="num">${fmt(s.output)}</td>
      <td class="cost">${fmtCost(codexCost(s.model, s.input, s.cache_read, s.output))}</td>
    </tr>`).join('');

  document.getElementById('codex-model-cost-body').innerHTML = byModelAll.map(m => `<tr>
      <td><span class="model-tag">${esc(m.model)}</span></td>
      <td class="num">${fmt(m.turns)}</td>
      <td class="num">${fmt(m.input)}</td>
      <td class="num">${fmt(m.output)}</td>
      <td class="num">${fmt(m.cache_read)}</td>
      <td class="cost">${fmtCost(codexCost(m.model, m.input, m.cache_read, m.output))}</td>
    </tr>`).join('');
}

function renderCombinedTab() {
  const cutoff = getRangeCutoff(selectedRange);
  const cutoff7 = getRangeCutoffDays(7);
  const claudeDailyAll = rawData.daily_by_model || [];
  const codexDailyAll = rawData.codex_daily || [];
  const combinedDaily = rawData.combined_daily || [];
  const claudeSessions = rawData.sessions_all || [];
  const codexSessions = rawData.codex_sessions || [];

  function claudeCostFromDaily(rows, c) {
    const map = {};
    for (const r of rows) {
      if (c && r.day < c) continue;
      if (!map[r.model]) map[r.model] = { input: 0, output: 0, cache_read: 0, cache_creation: 0 };
      map[r.model].input += r.input || 0;
      map[r.model].output += r.output || 0;
      map[r.model].cache_read += r.cache_read || 0;
      map[r.model].cache_creation += r.cache_creation || 0;
    }
    let total = 0;
    for (const [model, t] of Object.entries(map)) total += calcCost(model, t.input, t.output, t.cache_read, t.cache_creation);
    return total;
  }
  function codexCostFromDaily(rows, c) {
    const byModel = aggregateCodexByModel(rows, c);
    return byModel.reduce((s, m) => s + codexCost(m.model, m.input, m.cache_read, m.output), 0);
  }

  const claudeCostAll = claudeCostFromDaily(claudeDailyAll, null);
  const codexCostAll = codexCostFromDaily(codexDailyAll, null);
  const claudeCost7 = claudeCostFromDaily(claudeDailyAll, cutoff7);
  const codexCost7 = codexCostFromDaily(codexDailyAll, cutoff7);
  const totalTurns = (claudeDailyAll.reduce((s, r) => s + (r.turns || 0), 0) + codexDailyAll.reduce((s, r) => s + (r.turns || 0), 0));

  const stats = [
    { label: 'Cost (7d)', value: fmtCostBig(claudeCost7 + codexCost7), sub: 'Claude + Codex', color: '#4ade80' },
    { label: 'Cost (All)', value: fmtCostBig(claudeCostAll + codexCostAll), sub: 'Claude + Codex', color: '#4ade80' },
    { label: 'Claude Cost', value: fmtCostBig(claudeCostAll), sub: 'all-time' },
    { label: 'Codex Cost', value: fmtCostBig(codexCostAll), sub: 'all-time' },
    { label: 'Sessions', value: (claudeSessions.length + codexSessions.length).toLocaleString(), sub: 'all providers' },
    { label: 'Turns', value: fmt(totalTurns), sub: 'all providers' }
  ];
  document.getElementById('combined-stats-row').innerHTML = stats.map(s => `
    <div class="stat-card">
      <div class="label">${s.label}</div>
      <div class="value" style="${s.color ? 'color:' + s.color : ''}">${esc(s.value)}</div>
      ${s.sub ? `<div class="sub">${esc(s.sub)}</div>` : ''}
    </div>
  `).join('');

  const dayMap = {};
  for (const r of combinedDaily) {
    if (cutoff && r.day < cutoff) continue;
    if (!dayMap[r.day]) dayMap[r.day] = { claude: 0, codex: 0 };
    const provider = (r.provider || 'claude') === 'codex' ? 'codex' : 'claude';
    dayMap[r.day][provider] += (r.input || 0) + (r.output || 0) + (r.cache_read || 0);
  }
  const days = Object.keys(dayMap).sort();
  if (charts.combinedDaily) charts.combinedDaily.destroy();
  charts.combinedDaily = new Chart(document.getElementById('chart-combined-daily').getContext('2d'), {
    type: 'bar',
    data: {
      labels: days,
      datasets: [
        { label: 'Claude', data: days.map(d => dayMap[d].claude), backgroundColor: TOKEN_COLORS.claude, stack: 'provider' },
        { label: 'Codex', data: days.map(d => dayMap[d].codex), backgroundColor: TOKEN_COLORS.codex, stack: 'provider' }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { labels: { color: '#8892a4', boxWidth: 12 } } },
      scales: {
        x: { ticks: { color: '#8892a4', maxTicksLimit: RANGE_TICKS[selectedRange] }, grid: { color: '#2a2d3a' } },
        y: { ticks: { color: '#8892a4', callback: v => fmt(v) }, grid: { color: '#2a2d3a' } }
      }
    }
  });

  const tokenTotals = { input: 0, output: 0, cache_read: 0, cache_creation: 0 };
  for (const r of claudeDailyAll) {
    if (cutoff && r.day < cutoff) continue;
    tokenTotals.input += r.input || 0;
    tokenTotals.output += r.output || 0;
    tokenTotals.cache_read += r.cache_read || 0;
    tokenTotals.cache_creation += r.cache_creation || 0;
  }
  for (const r of codexDailyAll) {
    if (cutoff && r.day < cutoff) continue;
    tokenTotals.input += r.input || 0;
    tokenTotals.output += r.output || 0;
    tokenTotals.cache_read += r.cache_read || 0;
  }
  if (charts.combinedToken) charts.combinedToken.destroy();
  charts.combinedToken = new Chart(document.getElementById('chart-combined-token').getContext('2d'), {
    type: 'doughnut',
    data: {
      labels: ['Input', 'Output', 'Cache Read', 'Cache Creation'],
      datasets: [{ data: [tokenTotals.input, tokenTotals.output, tokenTotals.cache_read, tokenTotals.cache_creation], backgroundColor: [TOKEN_COLORS.input, TOKEN_COLORS.output, TOKEN_COLORS.cache_read, TOKEN_COLORS.cache_creation], borderWidth: 2, borderColor: '#1a1d27' }]
    },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom', labels: { color: '#8892a4', boxWidth: 12 } } } }
  });

  const pmap = {};
  for (const s of claudeSessions) {
    if (cutoff && s.last_date < cutoff) continue;
    if (!pmap[s.project]) pmap[s.project] = { project: s.project, sessions: 0, turns: 0, cost: 0 };
    pmap[s.project].sessions += 1;
    pmap[s.project].turns += s.turns || 0;
    pmap[s.project].cost += calcCost(s.model, s.input, s.output, s.cache_read, s.cache_creation);
  }
  for (const s of codexSessions) {
    if (cutoff && s.last_date < cutoff) continue;
    if (!pmap[s.project]) pmap[s.project] = { project: s.project, sessions: 0, turns: 0, cost: 0 };
    pmap[s.project].sessions += 1;
    pmap[s.project].turns += s.turns || 0;
    pmap[s.project].cost += codexCost(s.model, s.input, s.cache_read, s.output);
  }
  const top = Object.values(pmap).sort((a, b) => b.cost - a.cost).slice(0, 15);
  document.getElementById('combined-projects-body').innerHTML = top.map(p => `<tr>
      <td>${esc(p.project)}</td>
      <td class="num">${fmt(p.sessions)}</td>
      <td class="num">${fmt(p.turns)}</td>
      <td class="cost">${fmtCost(p.cost)}</td>
    </tr>`).join('');
}

function csvField(val) {
  const s = String(val);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}
function csvTimestamp() {
  const d = new Date();
  return d.getFullYear() + '-' + String(d.getMonth()+1).padStart(2,'0') + '-' + String(d.getDate()).padStart(2,'0') + '_' + String(d.getHours()).padStart(2,'0') + String(d.getMinutes()).padStart(2,'0');
}
function downloadCSV(reportType, header, rows) {
  const lines = [header.map(csvField).join(',')];
  for (const row of rows) lines.push(row.map(csvField).join(','));
  const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8;' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = reportType + '_' + csvTimestamp() + '.csv';
  a.click();
  URL.revokeObjectURL(a.href);
}
function exportSessionsCSV() {
  const header = ['Session', 'Project', 'Last Active', 'Duration (min)', 'Model', 'Turns', 'Input', 'Output', 'Cache Read', 'Cache Creation', 'Est. Cost'];
  const rows = lastFilteredSessions.map(s => [s.session_id, s.project, s.last, s.duration_min, s.model, s.turns, s.input, s.output, s.cache_read, s.cache_creation, calcCost(s.model, s.input, s.output, s.cache_read, s.cache_creation).toFixed(4)]);
  downloadCSV('sessions', header, rows);
}
function exportProjectsCSV() {
  const header = ['Project', 'Sessions', 'Turns', 'Input', 'Output', 'Cache Read', 'Cache Creation', 'Est. Cost'];
  const rows = lastByProject.map(p => [p.project, p.sessions, p.turns, p.input, p.output, p.cache_read, p.cache_creation, p.cost.toFixed(4)]);
  downloadCSV('projects', header, rows);
}

async function triggerRescan() {
  const btn = document.getElementById('rescan-btn');
  btn.disabled = true;
  btn.textContent = '\u21bb Scanning...';
  try {
    const resp = await fetch('/api/rescan', { method: 'POST' });
    const d = await resp.json();
    btn.textContent = '\u21bb Rescan (' + d.new + ' new, ' + d.updated + ' updated)';
    await loadData();
  } catch (e) {
    btn.textContent = '\u21bb Rescan (error)';
    console.error(e);
  }
  setTimeout(() => { btn.textContent = '\u21bb Rescan'; btn.disabled = false; }, 3000);
}

async function loadData() {
  try {
    const resp = await fetch('/api/data');
    const d = await resp.json();
    if (d.error) {
      document.body.innerHTML = '<div style="padding:40px;color:#f87171">' + esc(d.error) + '</div>';
      return;
    }
    document.getElementById('meta').textContent = 'Updated: ' + d.generated_at + ' - Auto-refresh in 30s';
    const isFirstLoad = rawData === null;
    rawData = d;
    window._data = d;

    if (isFirstLoad) {
      selectedRange = readURLRange();
      document.querySelectorAll('.range-btn').forEach(btn => btn.classList.toggle('active', btn.dataset.range === selectedRange));
      buildFilterUI(d.all_models || []);
      updateSortIcons();
      updateModelSortIcons();
      updateProjectSortIcons();
      updateCodexSortIcons();
      initTabs();
    }

    renderCodexStrip(d.codex_strip || {});
    applyFilter();
  } catch (e) {
    console.error(e);
  }
}

loadData();
setInterval(loadData, 30000);
</script>
</body>
</html>
"""


class DashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_TEMPLATE.encode("utf-8"))

        elif self.path == "/api/data":
            data = get_dashboard_data()
            body = json.dumps(data).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == "/api/rescan":
            # Full rebuild: delete DB and rescan from scratch
            if DB_PATH.exists():
                DB_PATH.unlink()
            from scanner import scan
            result = scan(verbose=False)
            body = json.dumps(result).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()


def serve(host=None, port=None):
    host = host or os.environ.get("HOST", "localhost")
    port = port or int(os.environ.get("PORT", "9123"))
    server = HTTPServer((host, port), DashboardHandler)
    print(f"Dashboard running at http://{host}:{port}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9123
    serve(port=port)
