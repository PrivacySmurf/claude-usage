"""Gemini OAuth + quota fetcher for the AI usage dashboard."""

import base64
import glob
import json
import os
import re
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


class GeminiNotInstalled(Exception):
    pass


class GeminiNotLoggedIn(Exception):
    pass


class GeminiUnsupportedAuth(Exception):
    pass


class GeminiApiError(Exception):
    pass


@dataclass
class GeminiQuota:
    model_id: str
    percent_left: float
    reset_time: Optional[str]
    reset_description: str


@dataclass
class GeminiSnapshot:
    quotas: List[GeminiQuota]
    account_email: Optional[str]
    account_plan: Optional[str]
    fetched_at: int


GEMINI_DIR = Path.home() / ".gemini"
SETTINGS_PATH = GEMINI_DIR / "settings.json"
OAUTH_CREDS_PATH = GEMINI_DIR / "oauth_creds.json"
CACHE_PATH = Path.home() / ".cc-workspace" / ".gemini-oauth-cache.json"

OAUTH2_JS_SUFFIX = (
    "node_modules/@google/gemini-cli/node_modules/@google/gemini-cli-core/"
    "dist/src/code_assist/oauth2.js"
)

CLIENT_ID_RE = re.compile(r"OAUTH_CLIENT_ID\s*=\s*['\"]([\w\-\.]+)['\"]")
CLIENT_SECRET_RE = re.compile(r"OAUTH_CLIENT_SECRET\s*=\s*['\"]([\w\-]+)['\"]")


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        raise
    except Exception as exc:
        raise GeminiApiError(f"Failed to parse JSON: {path}") from exc
    if not isinstance(data, dict):
        raise GeminiApiError(f"Invalid JSON object in {path}")
    return data


def read_auth_type(settings_path: Path = SETTINGS_PATH) -> Optional[str]:
    data = _read_json(settings_path)
    selected = (
        (data.get("security") or {})
        .get("auth", {})
        .get("selectedType")
    )
    if selected in ("api-key", "vertex-ai"):
        raise GeminiUnsupportedAuth(f"Unsupported Gemini auth type: {selected}")
    return selected


def load_oauth_creds(path: Path = OAUTH_CREDS_PATH) -> Dict[str, Any]:
    if not path.exists():
        raise GeminiNotLoggedIn("Gemini OAuth credentials not found")
    creds = _read_json(path)
    if not any(creds.get(k) for k in ("access_token", "refresh_token", "id_token")):
        raise GeminiNotLoggedIn("Gemini OAuth credentials are empty")
    return creds


def is_expired(creds: Dict[str, Any]) -> bool:
    expiry_ms = creds.get("expiry_date")
    try:
        expiry_seconds = float(expiry_ms) / 1000.0
    except (TypeError, ValueError):
        return True
    return expiry_seconds < (time.time() - 60)


def decode_id_token_claims(id_token: Optional[str]) -> Dict[str, Any]:
    if not id_token:
        return {}
    parts = id_token.split(".")
    if len(parts) < 2:
        return {}
    payload = parts[1]
    payload += "=" * ((4 - len(payload) % 4) % 4)
    try:
        decoded = base64.urlsafe_b64decode(payload.encode("utf-8"))
        claims = json.loads(decoded.decode("utf-8"))
    except Exception:
        return {}
    return claims if isinstance(claims, dict) else {}


def resolve_gemini_binary() -> Optional[Path]:
    gemini_bin = shutil.which("gemini")
    if not gemini_bin:
        return None
    try:
        return Path(gemini_bin).resolve()
    except Exception:
        return Path(gemini_bin)


def find_oauth2_js() -> Optional[Path]:
    candidates: List[Path] = [Path("/opt/homebrew/lib") / OAUTH2_JS_SUFFIX]

    nvm_pattern = str(
        Path.home() / ".nvm" / "versions" / "node" / "*" / "lib" / OAUTH2_JS_SUFFIX
    )
    candidates.extend(Path(p) for p in sorted(glob.glob(nvm_pattern)))
    candidates.append(Path("/usr/local/lib") / OAUTH2_JS_SUFFIX)

    gemini_bin = resolve_gemini_binary()
    if gemini_bin:
        suffix_path = Path(OAUTH2_JS_SUFFIX)
        for parent in [gemini_bin] + list(gemini_bin.parents):
            candidates.append(parent / suffix_path)
        for parent in gemini_bin.parents:
            candidates.append(parent / "node_modules" / OAUTH2_JS_SUFFIX)

    ruby_patterns = [
        str(
            Path.home()
            / ".local/share/gem/ruby"
            / "*"
            / "gems"
            / "gemini-cli-*"
            / "node_modules/@google/gemini-cli-core/dist/src/code_assist/oauth2.js"
        ),
        str(
            Path.home()
            / ".local/share/gem/ruby"
            / "*"
            / "gems"
            / "gemini-cli-*"
            / OAUTH2_JS_SUFFIX
        ),
    ]
    for pat in ruby_patterns:
        candidates.extend(Path(p) for p in sorted(glob.glob(pat)))

    seen = set()
    for cand in candidates:
        key = str(cand)
        if key in seen:
            continue
        seen.add(key)
        if cand.is_file():
            return cand
    return None


def extract_client_creds(oauth2_js_path: Path) -> Tuple[str, str]:
    try:
        text = oauth2_js_path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        raise GeminiApiError(f"Failed to read {oauth2_js_path}") from exc

    id_match = CLIENT_ID_RE.search(text)
    secret_match = CLIENT_SECRET_RE.search(text)
    if not id_match or not secret_match:
        raise GeminiApiError(f"OAuth constants not found in {oauth2_js_path}")
    return id_match.group(1), secret_match.group(1)


def _load_cached_client_creds() -> Optional[Tuple[str, str]]:
    try:
        data = _read_json(CACHE_PATH)
    except Exception:
        return None
    cid = data.get("client_id")
    csec = data.get("client_secret")
    if cid and csec:
        return str(cid), str(csec)
    return None


def _save_cached_client_creds(client_id: str, client_secret: str, source: Optional[Path]) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "source": str(source) if source else None,
        "updated_at": int(time.time()),
    }
    tmp = Path(str(CACHE_PATH) + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    os.rename(tmp, CACHE_PATH)


def _get_oauth_client_credentials() -> Tuple[str, str]:
    source = find_oauth2_js()
    if source:
        try:
            client_id, client_secret = extract_client_creds(source)
            _save_cached_client_creds(client_id, client_secret, source)
            return client_id, client_secret
        except Exception:
            pass

    cached = _load_cached_client_creds()
    if cached:
        return cached

    raise GeminiApiError("Cannot find OAuth client credentials")


def _http_json(
    method: str,
    url: str,
    *,
    access_token: Optional[str] = None,
    json_body: Optional[Dict[str, Any]] = None,
    form_body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    headers = {"Accept": "application/json"}
    data = None

    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"

    if json_body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(json_body).encode("utf-8")
    elif form_body is not None:
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        data = urllib.parse.urlencode(form_body).encode("utf-8")

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise GeminiApiError(f"HTTP {exc.code} for {url}: {detail[:300]}") from exc
    except urllib.error.URLError as exc:
        raise GeminiApiError(f"Network error for {url}: {exc}") from exc

    if not body:
        return {}

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        raise GeminiApiError(f"Invalid JSON response from {url}") from exc

    if not isinstance(parsed, dict):
        raise GeminiApiError(f"Unexpected response shape from {url}")
    return parsed


def refresh_access_token(refresh_token: str, client_id: str, client_secret: str) -> Dict[str, Any]:
    data = _http_json(
        "POST",
        "https://oauth2.googleapis.com/token",
        form_body={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
    )
    if not data.get("access_token"):
        raise GeminiApiError("OAuth token refresh returned no access_token")
    return data


def write_back_creds(existing_creds: Dict[str, Any], refreshed: Dict[str, Any], path: Path = OAUTH_CREDS_PATH) -> None:
    expires_in = refreshed.get("expires_in")
    try:
        expires_in_sec = int(expires_in)
    except (TypeError, ValueError):
        raise GeminiApiError("OAuth token refresh returned invalid expires_in")

    updated = dict(existing_creds)
    updated["access_token"] = refreshed.get("access_token")
    updated["expiry_date"] = int((time.time() + expires_in_sec) * 1000)
    if refreshed.get("id_token"):
        updated["id_token"] = refreshed.get("id_token")

    tmp = Path(str(path) + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(updated, f, indent=2)
    os.rename(tmp, path)


def load_code_assist(access_token: str) -> Dict[str, Any]:
    return _http_json(
        "POST",
        "https://cloudcode-pa.googleapis.com/v1internal:loadCodeAssist",
        access_token=access_token,
        json_body={"metadata": {"ideType": "GEMINI_CLI", "pluginType": "GEMINI"}},
    )


def discover_project_via_crm(access_token: str) -> Optional[str]:
    base = "https://cloudresourcemanager.googleapis.com/v1/projects"
    page_token = None

    for _ in range(5):
        url = base
        if page_token:
            url = f"{base}?pageToken={urllib.parse.quote(page_token)}"
        data = _http_json("GET", url, access_token=access_token)
        projects = data.get("projects") or []
        for project in projects:
            if not isinstance(project, dict):
                continue
            pid = str(project.get("projectId") or "")
            labels = project.get("labels") or {}
            if pid.startswith("gen-lang-client"):
                return pid
            if isinstance(labels, dict) and labels.get("generative-language") is not None:
                return pid
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return None


def fetch_quota(access_token: str, project_id: Optional[str]) -> Dict[str, Any]:
    payload = {"project": project_id} if project_id else {}
    return _http_json(
        "POST",
        "https://cloudcode-pa.googleapis.com/v1internal:retrieveUserQuota",
        access_token=access_token,
        json_body=payload,
    )


def format_reset(reset_iso: Optional[str], now: datetime) -> Tuple[Optional[datetime], str]:
    if not reset_iso:
        return None, "Reset time unavailable"
    try:
        dt = datetime.fromisoformat(str(reset_iso).replace("Z", "+00:00"))
    except Exception:
        return None, "Reset time unavailable"

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    delta = int((dt - now).total_seconds())
    if delta <= 0:
        return dt, "Resets soon"

    hours = delta // 3600
    mins = (delta % 3600) // 60
    if hours > 0:
        return dt, f"Resets in {hours}h {mins}m"
    return dt, f"Resets in {mins}m"


def parse_quota_response(data: Dict[str, Any]) -> List[GeminiQuota]:
    buckets = data.get("buckets") or []
    now = datetime.now(timezone.utc)

    family_rows: Dict[str, Dict[str, Any]] = {}
    for bucket in buckets:
        if not isinstance(bucket, dict):
            continue
        model_id = str(bucket.get("modelId") or "")
        low = model_id.lower()

        if "flash-lite" in low or "flash_lite" in low:
            family = "flash_lite"
            label = "Flash Lite"
        elif "flash" in low and "lite" not in low:
            family = "flash"
            label = "Flash"
        elif "pro" in low:
            family = "pro"
            label = "Pro"
        else:
            continue

        try:
            frac = float(bucket.get("remainingFraction"))
        except (TypeError, ValueError):
            continue
        frac = max(0.0, min(1.0, frac))

        best = family_rows.get(family)
        if best is None or frac < best["fraction"]:
            reset_iso = bucket.get("resetTime")
            _, reset_desc = format_reset(reset_iso, now)
            family_rows[family] = {
                "label": label,
                "fraction": frac,
                "reset_time": reset_iso,
                "reset_desc": reset_desc,
            }

    ordered = []
    for key in ("pro", "flash", "flash_lite"):
        row = family_rows.get(key)
        if not row:
            continue
        ordered.append(
            GeminiQuota(
                model_id=row["label"],
                percent_left=(1.0 - row["fraction"]) * 100.0,
                reset_time=row["reset_time"],
                reset_description=row["reset_desc"],
            )
        )
    return ordered


def map_tier(tier_id: Optional[str], hd: Optional[str]) -> Optional[str]:
    if tier_id == "standard-tier":
        return "Paid"
    if tier_id == "free-tier":
        return "Workspace" if hd else "Free"
    if tier_id == "legacy-tier":
        return "Legacy"
    return None


def fetch() -> GeminiSnapshot:
    if not GEMINI_DIR.exists() or not SETTINGS_PATH.is_file() or not os.access(SETTINGS_PATH, os.R_OK):
        raise GeminiNotInstalled("Gemini CLI config not found (~/.gemini/settings.json)")

    read_auth_type(SETTINGS_PATH)
    creds = load_oauth_creds(OAUTH_CREDS_PATH)

    if is_expired(creds):
        refresh_token = creds.get("refresh_token")
        if not refresh_token:
            raise GeminiNotLoggedIn("Gemini OAuth token expired and no refresh token is available")
        client_id, client_secret = _get_oauth_client_credentials()
        refreshed = refresh_access_token(str(refresh_token), client_id, client_secret)
        write_back_creds(creds, refreshed, OAUTH_CREDS_PATH)
        creds = load_oauth_creds(OAUTH_CREDS_PATH)

    access_token = creds.get("access_token")
    if not access_token:
        raise GeminiNotLoggedIn("Gemini OAuth access token missing")

    code_assist = load_code_assist(str(access_token))
    tier_id = ((code_assist.get("currentTier") or {}).get("id"))
    project_id = code_assist.get("cloudaicompanionProject")

    if not project_id:
        project_id = discover_project_via_crm(str(access_token))

    quota_data = fetch_quota(str(access_token), project_id)
    quotas = parse_quota_response(quota_data)

    claims = decode_id_token_claims(creds.get("id_token"))
    email = claims.get("email")
    hd = claims.get("hd")
    plan = map_tier(tier_id, hd)

    return GeminiSnapshot(
        quotas=quotas,
        account_email=str(email) if email else None,
        account_plan=plan,
        fetched_at=int(time.time()),
    )
