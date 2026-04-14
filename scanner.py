"""
scanner.py - Scans Claude Code JSONL transcript files and stores data in SQLite.
"""

import json
import os
import glob
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

PROJECTS_DIR = Path.home() / ".claude" / "projects"
XCODE_PROJECTS_DIR = Path.home() / "Library" / "Developer" / "Xcode" / "CodingAssistant" / "ClaudeAgentConfig" / "projects"
DB_PATH = Path.home() / ".claude" / "usage.db"

CODEX_SESSIONS_DIR = Path.home() / ".codex" / "sessions"

CODEX_PRICING = {
    "gpt-5":         {"in": 1.25, "cached_in": 0.125, "out": 10.0},
    "gpt-5.2-codex": {"in": 1.25, "cached_in": 0.125, "out": 10.0},
    "gpt-5.3-codex": {"in": 1.25, "cached_in": 0.125, "out": 10.0},
}
CODEX_MODEL_ALIASES = {
    "gpt-5-codex": "gpt-5",
}

DEFAULT_PROJECTS_DIRS = [PROJECTS_DIR, XCODE_PROJECTS_DIR]


def get_db(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS sessions (
            session_id      TEXT PRIMARY KEY,
            project_name    TEXT,
            first_timestamp TEXT,
            last_timestamp  TEXT,
            git_branch      TEXT,
            total_input_tokens      INTEGER DEFAULT 0,
            total_output_tokens     INTEGER DEFAULT 0,
            total_cache_read        INTEGER DEFAULT 0,
            total_cache_creation    INTEGER DEFAULT 0,
            model           TEXT,
            turn_count      INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS turns (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id              TEXT,
            timestamp               TEXT,
            model                   TEXT,
            input_tokens            INTEGER DEFAULT 0,
            output_tokens           INTEGER DEFAULT 0,
            cache_read_tokens       INTEGER DEFAULT 0,
            cache_creation_tokens   INTEGER DEFAULT 0,
            tool_name               TEXT,
            cwd                     TEXT,
            message_id              TEXT
        );

        CREATE TABLE IF NOT EXISTS processed_files (
            path    TEXT PRIMARY KEY,
            mtime   REAL,
            lines   INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_turns_session ON turns(session_id);
        CREATE INDEX IF NOT EXISTS idx_turns_timestamp ON turns(timestamp);
        CREATE INDEX IF NOT EXISTS idx_sessions_first ON sessions(first_timestamp);
    """)
    # PR 1 migrations — idempotent ALTER TABLE + new table
    try:
        conn.execute("ALTER TABLE sessions ADD COLUMN provider TEXT DEFAULT 'claude'")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE turns ADD COLUMN provider TEXT DEFAULT 'claude'")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE processed_files ADD COLUMN provider TEXT DEFAULT 'claude'")
    except Exception:
        pass
    try:
        conn.execute("DROP INDEX IF EXISTS idx_turns_dedup")
    except Exception:
        pass

    conn.execute("""
        CREATE TABLE IF NOT EXISTS codex_rate_limits (
            id                  INTEGER PRIMARY KEY CHECK (id = 1),
            scraped_at          INTEGER NOT NULL,
            primary_pct         REAL,
            primary_window      INTEGER,
            primary_resets_at   INTEGER,
            secondary_pct       REAL,
            secondary_window    INTEGER,
            secondary_resets_at INTEGER,
            plan_type           TEXT,
            credits_has         INTEGER,
            credits_balance     REAL,
            source_file         TEXT,
            source_ts           INTEGER
        )
    """)
    # PR 2: Gemini tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gemini_quotas (
            model_id            TEXT NOT NULL,
            token_type          TEXT NOT NULL,
            remaining_fraction  REAL NOT NULL,
            reset_time_iso      TEXT,
            reset_description   TEXT,
            fetched_at          INTEGER NOT NULL,
            PRIMARY KEY (model_id, token_type)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gemini_account (
            id          INTEGER PRIMARY KEY CHECK (id = 1),
            email       TEXT,
            plan        TEXT,
            last_state  TEXT,
            last_error  TEXT,
            fetched_at  INTEGER NOT NULL
        )
    """)
    # Add message_id column if upgrading from older schema
    try:
        conn.execute("SELECT message_id FROM turns LIMIT 1")
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE turns ADD COLUMN message_id TEXT")
    # Conditional unique index: only dedup non-null message IDs
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_turns_message_id
        ON turns(message_id) WHERE message_id IS NOT NULL AND message_id != ''
    """)
    conn.commit()


def project_name_from_cwd(cwd):
    """Derive a friendly project name from cwd path."""
    if not cwd:
        return "unknown"
    # Normalize to forward slashes, take last 2 components
    parts = cwd.replace("\\", "/").rstrip("/").split("/")
    if len(parts) >= 2:
        return "/".join(parts[-2:])
    return parts[-1] if parts else "unknown"


def parse_jsonl_file(filepath):
    """Parse a JSONL file and return (session_metas, turns, line_count).

    Deduplicates streaming events by message.id — Claude Code logs multiple
    JSONL records per API response, all sharing the same message.id. Only the
    last record per message_id is kept (it has the final usage tallies).
    """
    seen_messages = {}  # message_id -> turn dict (dedup streaming records)
    turns_no_id = []    # turns without a message_id (kept as-is)
    session_meta = {}   # session_id -> dict
    line_count = 0

    try:
        with open(filepath, encoding="utf-8", errors="replace") as f:
            for line_count, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                rtype = record.get("type")
                if rtype not in ("assistant", "user"):
                    continue

                session_id = record.get("sessionId")
                if not session_id:
                    continue

                timestamp = record.get("timestamp", "")
                cwd = record.get("cwd", "")
                git_branch = record.get("gitBranch", "")

                # Update session metadata from any record
                if session_id not in session_meta:
                    session_meta[session_id] = {
                        "session_id": session_id,
                        "project_name": project_name_from_cwd(cwd),
                        "first_timestamp": timestamp,
                        "last_timestamp": timestamp,
                        "git_branch": git_branch,
                        "model": None,
                    }
                else:
                    meta = session_meta[session_id]
                    if timestamp and (not meta["first_timestamp"] or timestamp < meta["first_timestamp"]):
                        meta["first_timestamp"] = timestamp
                    if timestamp and (not meta["last_timestamp"] or timestamp > meta["last_timestamp"]):
                        meta["last_timestamp"] = timestamp
                    if git_branch and not meta["git_branch"]:
                        meta["git_branch"] = git_branch

                if rtype == "assistant":
                    msg = record.get("message", {})
                    usage = msg.get("usage", {})
                    model = msg.get("model", "")
                    message_id = msg.get("id", "")

                    input_tokens = usage.get("input_tokens", 0) or 0
                    output_tokens = usage.get("output_tokens", 0) or 0
                    cache_read = usage.get("cache_read_input_tokens", 0) or 0
                    cache_creation = usage.get("cache_creation_input_tokens", 0) or 0

                    # Only record turns that have actual token usage
                    if input_tokens + output_tokens + cache_read + cache_creation == 0:
                        continue

                    # Extract tool name from content if present
                    tool_name = None
                    for item in msg.get("content", []):
                        if isinstance(item, dict) and item.get("type") == "tool_use":
                            tool_name = item.get("name")
                            break

                    if model:
                        session_meta[session_id]["model"] = model

                    turn = {
                        "session_id": session_id,
                        "timestamp": timestamp,
                        "model": model,
                        "input_tokens": input_tokens,
                        "output_tokens": output_tokens,
                        "cache_read_tokens": cache_read,
                        "cache_creation_tokens": cache_creation,
                        "tool_name": tool_name,
                        "cwd": cwd,
                        "message_id": message_id,
                    }

                    # Dedup: last record per message_id wins (final usage tallies)
                    if message_id:
                        seen_messages[message_id] = turn
                    else:
                        turns_no_id.append(turn)

    except Exception as e:
        print(f"  Warning: error reading {filepath}: {e}")

    turns = turns_no_id + list(seen_messages.values())
    return list(session_meta.values()), turns, line_count


def aggregate_sessions(session_metas, turns):
    """Aggregate turn data back into session-level stats."""
    from collections import defaultdict

    session_stats = defaultdict(lambda: {
        "total_input_tokens": 0,
        "total_output_tokens": 0,
        "total_cache_read": 0,
        "total_cache_creation": 0,
        "turn_count": 0,
        "model": None,
    })

    for t in turns:
        s = session_stats[t["session_id"]]
        s["total_input_tokens"] += t["input_tokens"]
        s["total_output_tokens"] += t["output_tokens"]
        s["total_cache_read"] += t["cache_read_tokens"]
        s["total_cache_creation"] += t["cache_creation_tokens"]
        s["turn_count"] += 1
        if t["model"]:
            s["model"] = t["model"]

    # Merge into session_metas
    result = []
    for meta in session_metas:
        sid = meta["session_id"]
        stats = session_stats[sid]
        result.append({**meta, **stats})
    return result


def upsert_sessions(conn, sessions):
    for s in sessions:
        # Check if session exists
        existing = conn.execute(
            "SELECT total_input_tokens, total_output_tokens, total_cache_read, "
            "total_cache_creation, turn_count FROM sessions WHERE session_id = ?",
            (s["session_id"],)
        ).fetchone()

        if existing is None:
            conn.execute("""
                INSERT INTO sessions
                    (session_id, project_name, first_timestamp, last_timestamp,
                     git_branch, total_input_tokens, total_output_tokens,
                     total_cache_read, total_cache_creation, model, turn_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                s["session_id"], s["project_name"], s["first_timestamp"],
                s["last_timestamp"], s["git_branch"],
                s["total_input_tokens"], s["total_output_tokens"],
                s["total_cache_read"], s["total_cache_creation"],
                s["model"], s["turn_count"]
            ))
        else:
            # Update: add new tokens on top of existing (since we only insert new turns)
            conn.execute("""
                UPDATE sessions SET
                    last_timestamp = MAX(last_timestamp, ?),
                    total_input_tokens = total_input_tokens + ?,
                    total_output_tokens = total_output_tokens + ?,
                    total_cache_read = total_cache_read + ?,
                    total_cache_creation = total_cache_creation + ?,
                    turn_count = turn_count + ?,
                    model = COALESCE(?, model)
                WHERE session_id = ?
            """, (
                s["last_timestamp"],
                s["total_input_tokens"], s["total_output_tokens"],
                s["total_cache_read"], s["total_cache_creation"],
                s["turn_count"], s["model"],
                s["session_id"]
            ))


def insert_turns(conn, turns):
    conn.executemany("""
        INSERT OR IGNORE INTO turns
            (session_id, timestamp, model, input_tokens, output_tokens,
             cache_read_tokens, cache_creation_tokens, tool_name, cwd, message_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (t["session_id"], t["timestamp"], t["model"],
         t["input_tokens"], t["output_tokens"],
         t["cache_read_tokens"], t["cache_creation_tokens"],
         t["tool_name"], t["cwd"], t.get("message_id", ""))
        for t in turns
    ])


def scan_codex(db_path=DB_PATH, verbose=True):
    """Scan Codex JSONL session files and store data in SQLite."""
    conn = get_db(db_path)
    init_db(conn)

    # Find all rollout-*.jsonl files under ~/.codex/sessions/
    jsonl_files = glob.glob(str(CODEX_SESSIONS_DIR / "**" / "rollout-*.jsonl"), recursive=True)
    jsonl_files.sort()

    if verbose:
        print(f"Found {len(jsonl_files)} Codex session files")

    new_files = 0
    updated_files = 0
    skipped_files = 0
    total_turns = 0
    total_sessions = set()
    latest_rate_limits = None   # (timestamp_unix, rate_limits_dict, source_file)

    for filepath in jsonl_files:
        try:
            mtime = os.path.getmtime(filepath)
        except OSError:
            continue

        row = conn.execute(
            "SELECT mtime, lines FROM processed_files WHERE path = ?",
            (filepath,)
        ).fetchone()

        if row and abs(row["mtime"] - mtime) < 0.01:
            skipped_files += 1
            continue

        is_new = row is None
        if verbose:
            status = "NEW" if is_new else "UPD"
            print(f"  [{status}] {filepath}")

        # Parse the JSONL file
        sessions = {}   # session_id -> dict
        turns = []
        prev_totals = {}  # session_id -> last total_token_usage
        last_model = None

        try:
            with open(filepath, encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    rtype = record.get("type", "")

                    # Session metadata
                    if rtype == "session_meta":
                        sid = record.get("id")
                        if sid:
                            ts_str = record.get("timestamp", "")
                            cwd = record.get("cwd", "")
                            sessions.setdefault(sid, {
                                "session_id": sid,
                                "project_name": project_name_from_cwd(cwd),
                                "first_timestamp": ts_str,
                                "last_timestamp": ts_str,
                                "git_branch": "",
                                "model": None,
                                "provider": "codex",
                            })
                        continue

                    # Track model from turn_context
                    if rtype == "turn_context":
                        m = (record.get("model") or
                             record.get("collaboration_mode", {}).get("settings", {}).get("model"))
                        if m:
                            last_model = m
                        continue

                    # Token count events
                    if rtype == "event_msg":
                        payload = record.get("payload", {})
                        if payload.get("type") != "token_count":
                            continue

                        info = payload.get("info") or {}
                        ts_str = record.get("timestamp", "")
                        sid = record.get("session_id") or record.get("sessionId")

                        # Extract model
                        model = (info.get("model") or info.get("model_name") or
                                 (info.get("metadata") or {}).get("model") or
                                 payload.get("model") or
                                 (payload.get("metadata") or {}).get("model") or
                                 last_model or "gpt-5")

                        # Resolve alias
                        model = CODEX_MODEL_ALIASES.get(model, model)

                        # Token extraction: prefer last_token_usage, fallback subtract
                        last_usage = info.get("last_token_usage") or {}
                        total_usage = info.get("total_token_usage") or {}

                        if last_usage:
                            inp = last_usage.get("input_tokens", 0) or 0
                            out = last_usage.get("output_tokens", 0) or 0
                            cached_in = last_usage.get("cached_input_tokens", 0) or 0
                        elif total_usage and sid:
                            prev = prev_totals.get(sid, {})
                            inp = (total_usage.get("input_tokens", 0) or 0) - (prev.get("input_tokens", 0) or 0)
                            out = (total_usage.get("output_tokens", 0) or 0) - (prev.get("output_tokens", 0) or 0)
                            cached_in = (total_usage.get("cached_input_tokens", 0) or 0) - (prev.get("cached_input_tokens", 0) or 0)
                            inp = max(0, inp)
                            out = max(0, out)
                            cached_in = max(0, cached_in)
                            prev_totals[sid] = dict(total_usage)
                        else:
                            continue

                        # Clamp cached
                        cached_in = min(cached_in, inp)

                        if inp + out == 0:
                            continue

                        # Parse timestamp
                        try:
                            ts_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                            ts_unix = int(ts_dt.timestamp())
                        except Exception:
                            ts_unix = 0
                            ts_dt = None

                        # Update session
                        cwd = info.get("cwd") or record.get("cwd", "")
                        if sid:
                            if sid not in sessions:
                                sessions[sid] = {
                                    "session_id": sid,
                                    "project_name": project_name_from_cwd(cwd),
                                    "first_timestamp": ts_str,
                                    "last_timestamp": ts_str,
                                    "git_branch": "",
                                    "model": model,
                                    "provider": "codex",
                                }
                            else:
                                s = sessions[sid]
                                if ts_str and (not s["last_timestamp"] or ts_str > s["last_timestamp"]):
                                    s["last_timestamp"] = ts_str
                                if ts_str and (not s["first_timestamp"] or ts_str < s["first_timestamp"]):
                                    s["first_timestamp"] = ts_str
                                s["model"] = model

                        turns.append({
                            "session_id": sid or "unknown",
                            "timestamp": ts_str,
                            "model": model,
                            "input_tokens": inp - cached_in,
                            "output_tokens": out,
                            "cache_read_tokens": cached_in,
                            "cache_creation_tokens": 0,
                            "tool_name": None,
                            "cwd": cwd,
                            "provider": "codex",
                        })

                        # Track latest rate_limits block
                        rl = info.get("rate_limits") or payload.get("rate_limits")
                        if rl and ts_unix:
                            if latest_rate_limits is None or ts_unix > latest_rate_limits[0]:
                                latest_rate_limits = (ts_unix, rl, filepath)

        except Exception as e:
            print(f"  Warning: error reading {filepath}: {e}")

        # Upsert sessions (with provider column)
        for s in sessions.values():
            existing = conn.execute(
                "SELECT 1 FROM sessions WHERE session_id = ?", (s["session_id"],)
            ).fetchone()
            if existing is None:
                conn.execute("""
                    INSERT INTO sessions
                        (session_id, project_name, first_timestamp, last_timestamp,
                         git_branch, total_input_tokens, total_output_tokens,
                         total_cache_read, total_cache_creation, model, turn_count, provider)
                    VALUES (?, ?, ?, ?, ?, 0, 0, 0, 0, ?, 0, ?)
                """, (s["session_id"], s["project_name"], s["first_timestamp"],
                      s["last_timestamp"], s["model"], s.get("provider", "codex")))

        # Insert turns — OR IGNORE prevents dupe rows on re-scan (dedup via unique index)
        for t in turns:
            conn.execute("""
                INSERT OR IGNORE INTO turns
                    (session_id, timestamp, model, input_tokens, output_tokens,
                     cache_read_tokens, cache_creation_tokens, tool_name, cwd, provider)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (t["session_id"], t["timestamp"], t["model"],
                  t["input_tokens"], t["output_tokens"],
                  t["cache_read_tokens"], t["cache_creation_tokens"],
                  t["tool_name"], t["cwd"], t.get("provider", "codex")))

        # Record file
        with open(filepath, encoding="utf-8", errors="replace") as f:
            line_count = sum(1 for _ in f)
        conn.execute("""
            INSERT OR REPLACE INTO processed_files (path, mtime, lines, provider)
            VALUES (?, ?, ?, ?)
        """, (filepath, mtime, line_count, "codex"))
        conn.commit()

        if is_new:
            new_files += 1
        else:
            updated_files += 1

        total_turns += len(turns)
        total_sessions.update(sessions.keys())

    # Upsert latest rate_limits to codex_rate_limits table
    if latest_rate_limits:
        ts_unix, rl, src_file = latest_rate_limits
        primary = rl.get("primary", {})
        secondary = rl.get("secondary", {})
        credits = rl.get("credits", {})

        # primary_resets_at: parse ISO string or pass through Unix int
        def _parse_resets(val):
            if not val:
                return None
            if isinstance(val, int):
                return val
            try:
                return int(datetime.fromisoformat(str(val).replace("Z", "+00:00")).timestamp())
            except Exception:
                return None

        conn.execute("""
            INSERT OR REPLACE INTO codex_rate_limits
                (id, scraped_at, primary_pct, primary_window, primary_resets_at,
                 secondary_pct, secondary_window, secondary_resets_at,
                 plan_type, credits_has, credits_balance, source_file, source_ts)
            VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            int(datetime.now().timestamp()),
            primary.get("used_percent"),
            primary.get("window_minutes"),
            _parse_resets(primary.get("resets_at")),
            secondary.get("used_percent"),
            secondary.get("window_minutes"),
            _parse_resets(secondary.get("resets_at")),
            rl.get("plan_type"),
            1 if credits.get("has_credits") else 0,
            credits.get("balance", 0.0),
            src_file,
            ts_unix,
        ))
        conn.commit()

    if new_files or updated_files:
        conn.execute("""
            UPDATE sessions SET
                total_input_tokens = COALESCE((SELECT SUM(input_tokens) FROM turns WHERE turns.session_id = sessions.session_id), 0),
                total_output_tokens = COALESCE((SELECT SUM(output_tokens) FROM turns WHERE turns.session_id = sessions.session_id), 0),
                total_cache_read = COALESCE((SELECT SUM(cache_read_tokens) FROM turns WHERE turns.session_id = sessions.session_id), 0),
                total_cache_creation = COALESCE((SELECT SUM(cache_creation_tokens) FROM turns WHERE turns.session_id = sessions.session_id), 0),
                turn_count = COALESCE((SELECT COUNT(*) FROM turns WHERE turns.session_id = sessions.session_id), 0)
            WHERE provider='codex'
        """)
        conn.commit()

    if verbose:
        print(f"\nCodex scan complete:")
        print(f"  New files:     {new_files}")
        print(f"  Updated files: {updated_files}")
        print(f"  Skipped files: {skipped_files}")
        print(f"  Turns added:   {total_turns}")
        print(f"  Sessions seen: {len(total_sessions)}")

    conn.close()
    return {"new": new_files, "updated": updated_files, "skipped": skipped_files,
            "turns": total_turns, "sessions": len(total_sessions)}


def scan(projects_dir=None, projects_dirs=None, db_path=DB_PATH, verbose=True):
    conn = get_db(db_path)
    init_db(conn)

    if projects_dirs:
        dirs_to_scan = [Path(d) for d in projects_dirs]
    elif projects_dir:
        dirs_to_scan = [Path(projects_dir)]
    else:
        dirs_to_scan = DEFAULT_PROJECTS_DIRS

    jsonl_files = []
    for d in dirs_to_scan:
        if not d.exists():
            continue
        if verbose:
            print(f"Scanning {d} ...")
        jsonl_files.extend(glob.glob(str(d / "**" / "*.jsonl"), recursive=True))
    jsonl_files.sort()

    new_files = 0
    updated_files = 0
    skipped_files = 0
    total_turns = 0
    total_sessions = set()

    for filepath in jsonl_files:
        try:
            mtime = os.path.getmtime(filepath)
        except OSError:
            continue

        row = conn.execute(
            "SELECT mtime, lines FROM processed_files WHERE path = ?",
            (filepath,)
        ).fetchone()

        if row and abs(row["mtime"] - mtime) < 0.01:
            skipped_files += 1
            continue

        is_new = row is None
        if verbose:
            status = "NEW" if is_new else "UPD"
            print(f"  [{status}] {filepath}")

        if is_new:
            # New file: full parse (single read, returns line count)
            session_metas, turns, line_count = parse_jsonl_file(filepath)

            if turns or session_metas:
                sessions = aggregate_sessions(session_metas, turns)
                upsert_sessions(conn, sessions)
                insert_turns(conn, turns)
                for s in sessions:
                    total_sessions.add(s["session_id"])
                total_turns += len(turns)
                new_files += 1

        else:
            # Updated file: read once, process only new lines
            old_lines = row["lines"] if row else 0
            seen_messages = {}  # message_id -> turn (dedup streaming)
            turns_no_id = []
            new_session_metas = {}
            line_count = 0

            try:
                with open(filepath, encoding="utf-8", errors="replace") as f:
                    for line_count, line in enumerate(f, 1):
                        if line_count <= old_lines:
                            continue
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            record = json.loads(line)
                        except json.JSONDecodeError:
                            continue

                        rtype = record.get("type")
                        if rtype not in ("assistant", "user"):
                            continue

                        session_id = record.get("sessionId")
                        if not session_id:
                            continue

                        timestamp = record.get("timestamp", "")
                        cwd = record.get("cwd", "")

                        # Track session metadata from new lines
                        if session_id not in new_session_metas:
                            new_session_metas[session_id] = {
                                "session_id": session_id,
                                "project_name": project_name_from_cwd(cwd),
                                "first_timestamp": timestamp,
                                "last_timestamp": timestamp,
                                "git_branch": record.get("gitBranch", ""),
                                "model": None,
                            }
                        else:
                            meta = new_session_metas[session_id]
                            if timestamp and (not meta["last_timestamp"] or timestamp > meta["last_timestamp"]):
                                meta["last_timestamp"] = timestamp

                        if rtype == "assistant":
                            msg = record.get("message", {})
                            usage = msg.get("usage", {})
                            model = msg.get("model", "")
                            message_id = msg.get("id", "")

                            input_tokens = usage.get("input_tokens", 0) or 0
                            output_tokens = usage.get("output_tokens", 0) or 0
                            cache_read = usage.get("cache_read_input_tokens", 0) or 0
                            cache_creation = usage.get("cache_creation_input_tokens", 0) or 0

                            if input_tokens + output_tokens + cache_read + cache_creation == 0:
                                continue

                            tool_name = None
                            for item in msg.get("content", []):
                                if isinstance(item, dict) and item.get("type") == "tool_use":
                                    tool_name = item.get("name")
                                    break

                            if model:
                                new_session_metas[session_id]["model"] = model

                            turn = {
                                "session_id": session_id,
                                "timestamp": timestamp,
                                "model": model,
                                "input_tokens": input_tokens,
                                "output_tokens": output_tokens,
                                "cache_read_tokens": cache_read,
                                "cache_creation_tokens": cache_creation,
                                "tool_name": tool_name,
                                "cwd": cwd,
                                "message_id": message_id,
                            }

                            if message_id:
                                seen_messages[message_id] = turn
                            else:
                                turns_no_id.append(turn)
            except Exception as e:
                print(f"  Warning: {e}")

            if line_count <= old_lines:
                # File didn't grow (mtime changed but no new content)
                conn.execute("UPDATE processed_files SET mtime = ? WHERE path = ?",
                             (mtime, filepath))
                conn.commit()
                skipped_files += 1
                continue

            new_turns = turns_no_id + list(seen_messages.values())

            if new_turns or new_session_metas:
                sessions = aggregate_sessions(list(new_session_metas.values()), new_turns)
                upsert_sessions(conn, sessions)
                insert_turns(conn, new_turns)
                for s in sessions:
                    total_sessions.add(s["session_id"])
                total_turns += len(new_turns)
            updated_files += 1

        # Record file as processed (line_count already known from the single read)
        conn.execute("""
            INSERT OR REPLACE INTO processed_files (path, mtime, lines)
            VALUES (?, ?, ?)
        """, (filepath, mtime, line_count))
        conn.commit()

    # Recompute session totals from actual turns in DB.
    # This ensures correctness when INSERT OR IGNORE skips duplicate turns
    # but upsert_sessions had already added their tokens additively.
    if new_files or updated_files:
        conn.execute("""
            UPDATE sessions SET
                total_input_tokens = COALESCE((SELECT SUM(input_tokens) FROM turns WHERE turns.session_id = sessions.session_id), 0),
                total_output_tokens = COALESCE((SELECT SUM(output_tokens) FROM turns WHERE turns.session_id = sessions.session_id), 0),
                total_cache_read = COALESCE((SELECT SUM(cache_read_tokens) FROM turns WHERE turns.session_id = sessions.session_id), 0),
                total_cache_creation = COALESCE((SELECT SUM(cache_creation_tokens) FROM turns WHERE turns.session_id = sessions.session_id), 0),
                turn_count = COALESCE((SELECT COUNT(*) FROM turns WHERE turns.session_id = sessions.session_id), 0)
        """)
        conn.commit()

    if verbose:
        print(f"\nScan complete:")
        print(f"  New files:     {new_files}")
        print(f"  Updated files: {updated_files}")
        print(f"  Skipped files: {skipped_files}")
        print(f"  Turns added:   {total_turns}")
        print(f"  Sessions seen: {len(total_sessions)}")

    conn.close()
    return {"new": new_files, "updated": updated_files, "skipped": skipped_files,
            "turns": total_turns, "sessions": len(total_sessions)}


def poll_gemini(conn):
    from gemini_provider import (
        fetch,
        GeminiUnsupportedAuth,
        GeminiNotLoggedIn,
        GeminiNotInstalled,
        GeminiApiError,
    )
    import time

    now = int(time.time())
    cursor = conn.cursor()
    try:
        snapshot = fetch()
        state = "green" if snapshot.account_plan is not None else "yellow"
        error = None

        for q in snapshot.quotas:
            cursor.execute("""
                INSERT OR REPLACE INTO gemini_quotas
                (model_id, token_type, remaining_fraction, reset_time_iso, reset_description, fetched_at)
                VALUES (?, 'input', ?, ?, ?, ?)
            """, (
                q.model_id,
                1.0 - q.used_pct / 100.0,
                q.reset_time,
                q.reset_description,
                snapshot.fetched_at,
            ))

        cursor.execute("""
            INSERT OR REPLACE INTO gemini_account (id, email, plan, last_state, last_error, fetched_at)
            VALUES (1, ?, ?, ?, ?, ?)
        """, (snapshot.account_email, snapshot.account_plan, state, error, snapshot.fetched_at))
    except GeminiUnsupportedAuth:
        cursor.execute("""
            INSERT OR REPLACE INTO gemini_account (id, email, plan, last_state, last_error, fetched_at)
            VALUES (1, NULL, NULL, 'red', 'Use Google account (OAuth)', ?)
        """, (now,))
    except GeminiNotLoggedIn:
        cursor.execute("""
            INSERT OR REPLACE INTO gemini_account (id, email, plan, last_state, last_error, fetched_at)
            VALUES (1, NULL, NULL, 'red', 'Run gemini auth login', ?)
        """, (now,))
    except (GeminiNotInstalled, GeminiApiError) as exc:
        cursor.execute("""
            INSERT OR REPLACE INTO gemini_account (id, email, plan, last_state, last_error, fetched_at)
            VALUES (1, NULL, NULL, 'red', ?, ?)
        """, (str(exc), now))
    conn.commit()


def scan_gemini(db_path=None):
    if db_path is None:
        db_path = Path.home() / ".claude" / "usage.db"
    conn = get_db(db_path)
    init_db(conn)
    poll_gemini(conn)
    conn.close()


if __name__ == "__main__":
    import sys
    projects_dir = None
    for i, arg in enumerate(sys.argv[1:]):
        if arg == "--projects-dir" and i + 1 < len(sys.argv[1:]):
            projects_dir = Path(sys.argv[i + 2])
            break
    scan(projects_dir=projects_dir)
