import os
import re
import time
import logging
import threading
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, quote, unquote
from functools import wraps

import requests
from flask import Flask, jsonify, request, session, redirect, url_for, \
                  render_template_string, Response, stream_with_context

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "livekick_secret_2026")

# ─── CORS ─────────────────────────────────────────────────────────────────────
@app.after_request
def cors(r):
    r.headers["Access-Control-Allow-Origin"]  = "*"
    r.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
    r.headers["Access-Control-Allow-Methods"] = "GET,POST,DELETE,OPTIONS"
    return r

# ─── Config ───────────────────────────────────────────────────────────────────
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "livekick2026")
FD_KEY         = os.environ.get("FOOTBALL_DATA_API_KEY", "")

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/soccer"
FD_BASE   = "https://api.football-data.org/v4"

COMPETITIONS = {
    "PL":  {"fd_id": 2021, "espn": "eng.1",                 "name": "Premier League",    "enabled": True},
    "PD":  {"fd_id": 2014, "espn": "esp.1",                 "name": "La Liga",           "enabled": True},
    "BL1": {"fd_id": 2002, "espn": "ger.1",                 "name": "Bundesliga",        "enabled": True},
    "SA":  {"fd_id": 2019, "espn": "ita.1",                 "name": "Serie A",           "enabled": True},
    "FL1": {"fd_id": 2015, "espn": "fra.1",                 "name": "Ligue 1",           "enabled": True},
    "CL":  {"fd_id": 2001, "espn": "uefa.champions",        "name": "Champions League",  "enabled": True},
    "EL":  {"fd_id": 2146, "espn": "uefa.europa",           "name": "Europa League",     "enabled": True},
    "FAC": {"fd_id": 2055, "espn": "eng.fa",                "name": "FA Cup",            "enabled": True},
    "ELC": {"fd_id": 2016, "espn": "eng.2",                 "name": "Carabao Cup",       "enabled": True},
    "WC":  {"fd_id": 2000, "espn": "fifa.world",            "name": "FIFA World Cup",    "enabled": True},
}

# In-memory streams — fastest possible, no disk I/O
STATE = {
    "streams":         {},
    "hidden_matches":  set(),
    "maintenance":     False,
    "maintenance_msg": "LiveKick is under maintenance. Check back soon.",
}

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

STREAM_HEADERS = {
    **HEADERS,
    "Referer": "https://colddfootball.neckhards.org/",
    "Origin":  "https://colddfootball.neckhards.org",
    "Accept":  "*/*",
}

# ─── TTLs ─────────────────────────────────────────────────────────────────────
TTL_COMPETITIONS = 43200   # 12 hours
TTL_FAR          = 21600   # 6 hours  — match > 1hr from kickoff
TTL_NEAR         = 60      # 1 minute — match within 1hr of kickoff
TTL_LIVE         = 25      # 25 secs  — live match
TTL_FINISHED     = None    # evict immediately

# ─── Thread-safe lifecycle-aware cache ───────────────────────────────────────
class LiveKickCache:
    def __init__(self):
        self._store    = {}
        self._lock     = threading.Lock()
        self._keylocks = {}

    def _klock(self, key):
        with self._lock:
            if key not in self._keylocks:
                self._keylocks[key] = threading.Lock()
            return self._keylocks[key]

    def get(self, key):
        with self._lock:
            entry = self._store.get(key)
        if not entry:
            return None
        data, exp = entry
        if time.time() > exp:
            self._del(key)
            return None
        return data

    def set(self, key, data, ttl):
        if ttl is None or ttl <= 0:
            self._del(key)
            return
        with self._lock:
            self._store[key] = (data, time.time() + ttl)

    def _del(self, key):
        with self._lock:
            self._store.pop(key, None)
            self._keylocks.pop(key, None)

    def bust(self, prefix=""):
        with self._lock:
            keys = [k for k in self._store if k.startswith(prefix)]
        for k in keys:
            self._del(k)

    def get_or_fetch(self, key, fetch_fn, ttl_fn=None, default_ttl=TTL_FAR):
        hit = self.get(key)
        if hit is not None:
            return hit
        with self._klock(key):
            hit = self.get(key)
            if hit is not None:
                return hit
            try:
                data = fetch_fn()
                ttl  = ttl_fn(data) if ttl_fn else default_ttl
                self.set(key, data, ttl)
                return data
            except Exception as e:
                log.error(f"fetch failed [{key}]: {e}")
                return None

    def cleanup(self):
        now = time.time()
        with self._lock:
            expired = [k for k, (_, exp) in self._store.items() if now > exp]
        for k in expired:
            self._del(k)

    def info(self):
        now = time.time()
        with self._lock:
            alive = {k: round(exp - now) for k, (_, exp) in self._store.items() if exp > now}
        return {"entries": len(alive), "ttls": alive}

cache = LiveKickCache()

def _cleanup():
    while True:
        time.sleep(300)
        cache.cleanup()

threading.Thread(target=_cleanup, daemon=True).start()

# ─── Helpers ──────────────────────────────────────────────────────────────────
def today_str():
    return datetime.utcnow().strftime("%Y-%m-%d")

def parse_utc(s):
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def fmt_kickoff(utc_str):
    try:
        dt = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
        return dt.strftime("%H:%M")
    except Exception:
        return "--:--"

def compute_ttl(matches):
    if not matches:
        return TTL_FAR
    now = datetime.now(timezone.utc)
    has_live = has_near = False
    all_finished = True
    for m in matches:
        st = m.get("status", "SCHEDULED")
        if st == "LIVE":
            has_live = True; all_finished = False
        elif st == "FINISHED":
            pass
        else:
            all_finished = False
            ko = parse_utc(m.get("kickoff_utc", ""))
            if ko and 0 < (ko - now).total_seconds() / 60 <= 60:
                has_near = True
    if has_live:     return TTL_LIVE
    if all_finished: return TTL_FINISHED
    if has_near:     return TTL_NEAR
    return TTL_FAR

def attach_streams(match_id):
    s = STATE["streams"].get(match_id)
    if not s:
        return []
    return [s["stream_url"]] + s.get("fallbacks", [])

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("admin"):
            return redirect(url_for("admin_login"))
        return f(*args, **kwargs)
    return decorated

def fd_headers():
    return {**HEADERS, "X-Auth-Token": FD_KEY}

# ─── ESPN ─────────────────────────────────────────────────────────────────────
def _espn_fetch(espn_slug, date_str=None):
    params = {}
    if date_str:
        params["dates"] = date_str.replace("-", "")
    res = requests.get(f"{ESPN_BASE}/{espn_slug}/scoreboard",
                       headers=HEADERS, params=params, timeout=8)
    res.raise_for_status()
    matches = []
    for event in res.json().get("events", []):
        comp        = event.get("competitions", [{}])[0]
        competitors = comp.get("competitors", [])
        if len(competitors) < 2:
            continue
        home  = next((c for c in competitors if c.get("homeAway") == "home"), competitors[0])
        away  = next((c for c in competitors if c.get("homeAway") == "away"), competitors[1])
        st    = comp.get("status", {})
        stype = st.get("type", {})
        eid   = event.get("id", "")
        if eid in STATE["hidden_matches"]:
            continue
        state_val = stype.get("state", "pre")
        status    = {"in": "LIVE", "post": "FINISHED"}.get(state_val, "SCHEDULED")
        matches.append({
            "match_id":    eid,
            "home_team":   home.get("team", {}).get("displayName", ""),
            "away_team":   away.get("team", {}).get("displayName", ""),
            "home_crest":  home.get("team", {}).get("logo", ""),
            "away_crest":  away.get("team", {}).get("logo", ""),
            "home_score":  home.get("score", "0"),
            "away_score":  away.get("score", "0"),
            "status":      status,
            "minute":      st.get("displayClock") if state_val == "in" else None,
            "kickoff":     fmt_kickoff(event.get("date", "")),
            "kickoff_utc": event.get("date", ""),
            "venue":       comp.get("venue", {}).get("fullName", ""),
            "streams":     attach_streams(eid),
            "source":      "espn",
        })
    return matches

# ─── Football-data.org ────────────────────────────────────────────────────────
def _fd_fetch(fd_id, date_str=None):
    today = date_str or today_str()
    res   = requests.get(f"{FD_BASE}/competitions/{fd_id}/matches",
                         headers=fd_headers(),
                         params={"dateFrom": today, "dateTo": today},
                         timeout=10)
    res.raise_for_status()
    status_map = {
        "SCHEDULED": "SCHEDULED", "TIMED":    "SCHEDULED",
        "IN_PLAY":   "LIVE",      "PAUSED":   "LIVE",
        "FINISHED":  "FINISHED",  "AWARDED":  "FINISHED",
        "SUSPENDED": "LIVE",      "POSTPONED":"FINISHED",
    }
    matches = []
    for m in res.json().get("matches", []):
        mid  = str(m.get("id", ""))
        home = m.get("homeTeam", {})
        away = m.get("awayTeam", {})
        full = m.get("score", {}).get("fullTime", {})
        utc  = m.get("utcDate", "")
        if mid in STATE["hidden_matches"]:
            continue
        matches.append({
            "match_id":    mid,
            "home_team":   home.get("name", ""),
            "away_team":   away.get("name", ""),
            "home_crest":  home.get("crest", ""),
            "away_crest":  away.get("crest", ""),
            "home_score":  str(full.get("home") or 0),
            "away_score":  str(full.get("away") or 0),
            "status":      status_map.get(m.get("status", "SCHEDULED"), "SCHEDULED"),
            "minute":      None,
            "kickoff":     fmt_kickoff(utc),
            "kickoff_utc": utc,
            "venue":       m.get("venue", ""),
            "streams":     attach_streams(mid),
            "source":      "football-data",
        })
    return matches

# ─── Unified fetch ────────────────────────────────────────────────────────────
def fetch_matches(comp_code, date_str=None):
    date_str  = date_str or today_str()
    cache_key = f"matches:{comp_code}:{date_str}"
    comp      = COMPETITIONS.get(comp_code.upper())
    if not comp:
        return []

    def fetch():
        try:
            m = _espn_fetch(comp["espn"], date_str)
            log.info(f"ESPN {comp_code} {date_str}: {len(m)} matches")
            return m
        except Exception as e:
            log.warning(f"ESPN failed {comp_code}: {e} → FD fallback")
        try:
            m = _fd_fetch(comp["fd_id"], date_str)
            log.info(f"FD {comp_code} {date_str}: {len(m)} matches")
            return m
        except Exception as e:
            log.error(f"FD also failed {comp_code}: {e}")
            return []

    def ttl_fn(matches):
        ttl = compute_ttl(matches)
        log.info(f"TTL {comp_code} {date_str}: {ttl}s")
        return ttl

    return cache.get_or_fetch(cache_key, fetch, ttl_fn) or []

# ─── Competitions ─────────────────────────────────────────────────────────────
def fetch_competitions():
    def fetch():
        try:
            res    = requests.get(f"{FD_BASE}/competitions", headers=fd_headers(), timeout=10)
            res.raise_for_status()
            fd_map = {c["id"]: c for c in res.json().get("competitions", [])}
        except Exception as e:
            log.warning(f"FD competitions failed: {e}")
            fd_map = {}
        result = []
        for code, comp in COMPETITIONS.items():
            if not comp["enabled"]:
                continue
            fd_c = fd_map.get(comp["fd_id"], {})
            result.append({
                "code":      code,
                "name":      comp["name"],
                "emblem":    fd_c.get("emblem", ""),
                "season":    fd_c.get("currentSeason", {}).get("startYear"),
                "fd_id":     comp["fd_id"],
                "espn_slug": comp["espn"],
            })
        return result
    return cache.get_or_fetch("competitions:all", fetch, default_ttl=TTL_COMPETITIONS) or []

# ─── Stream proxy ─────────────────────────────────────────────────────────────
@app.route("/proxy/playlist")
def proxy_playlist():
    """
    Rewrites m3u8 playlist so all chunk/segment URLs go through /proxy/chunk.
    Browser never touches the stream server — Flask sends the correct Referer.
    """
    url = unquote(request.args.get("url", "").strip())
    if not url:
        return "url required", 400
    try:
        resp = requests.get(url, headers=STREAM_HEADERS, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        return f"Failed to fetch playlist: {e}", 502

    base  = url.rsplit("/", 1)[0] + "/"
    lines = []
    for line in resp.text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            # Rewrite URI= inside tags (e.g. #EXT-X-KEY)
            if "URI=\"" in s:
                def rewrite_uri(m):
                    abs_u = urljoin(base, m.group(1))
                    return f'URI="{request.host_url}proxy/chunk?url={quote(abs_u, safe="")}"'
                s = re.sub(r'URI="([^"]+)"', rewrite_uri, s)
            lines.append(s)
        else:
            abs_url = urljoin(base, s)
            if ".m3u8" in s:
                lines.append(f"{request.host_url}proxy/playlist?url={quote(abs_url, safe='')}")
            else:
                lines.append(f"{request.host_url}proxy/chunk?url={quote(abs_url, safe='')}")

    return Response("\n".join(lines), headers={
        "Content-Type":                "application/vnd.apple.mpegurl",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control":               "no-cache",
    })

@app.route("/proxy/chunk")
def proxy_chunk():
    """Relays a single TS segment or key file with the correct Referer."""
    url = unquote(request.args.get("url", "").strip())
    if not url:
        return "url required", 400
    try:
        resp = requests.get(url, headers=STREAM_HEADERS, timeout=15, stream=True)
        resp.raise_for_status()
    except Exception as e:
        return f"Failed to fetch chunk: {e}", 502

    ct = resp.headers.get("Content-Type", "video/MP2T")

    def generate():
        for chunk in resp.iter_content(65536):
            if chunk:
                yield chunk

    return Response(stream_with_context(generate()), headers={
        "Content-Type":                ct,
        "Access-Control-Allow-Origin": "*",
        "Cache-Control":               "no-cache",
    })

# ─── Public API ───────────────────────────────────────────────────────────────
@app.route("/ping")
def ping():
    return "pong", 200

@app.route("/health")
def health():
    return jsonify({"status": "ok", "cache": cache.info()})

@app.route("/api/health")
def api_health():
    return jsonify({
        "status":  "maintenance" if STATE["maintenance"] else "ok",
        "message": STATE["maintenance_msg"] if STATE["maintenance"] else "LiveKick is live",
    })

@app.route("/api/competitions")
def api_competitions():
    return jsonify({"success": True, "data": fetch_competitions()})

@app.route("/api/matches/live")
def api_matches_live():
    if STATE["maintenance"]:
        return jsonify({"success": False, "message": STATE["maintenance_msg"]}), 503

    cache_key = f"live:{today_str()}"

    def fetch():
        enabled  = [(c, v) for c, v in COMPETITIONS.items() if v["enabled"]]
        live     = []
        uncached = [(c, v) for c, v in enabled if cache.get(f"matches:{c}:{today_str()}") is None]
        cached_  = [(c, v) for c, v in enabled if cache.get(f"matches:{c}:{today_str()}") is not None]

        for c, v in cached_:
            for m in (cache.get(f"matches:{c}:{today_str()}") or []):
                if m["status"] == "LIVE":
                    live.append({**m, "competition_code": c, "competition_name": v["name"]})

        if uncached:
            with ThreadPoolExecutor(max_workers=min(len(uncached), 6)) as ex:
                futs = {ex.submit(fetch_matches, c): (c, v) for c, v in uncached}
                for fut in as_completed(futs):
                    c, v = futs[fut]
                    try:
                        for m in (fut.result() or []):
                            if m["status"] == "LIVE":
                                live.append({**m, "competition_code": c, "competition_name": v["name"]})
                    except Exception as e:
                        log.error(f"Live fetch {c}: {e}")
        return live

    data = cache.get_or_fetch(cache_key, fetch, default_ttl=TTL_LIVE) or []
    return jsonify({"success": True, "data": {"count": len(data), "matches": data}})

@app.route("/api/matches/<competition_code>")
def api_matches(competition_code):
    if STATE["maintenance"]:
        return jsonify({"success": False, "message": STATE["maintenance_msg"]}), 503

    comp = COMPETITIONS.get(competition_code.upper())
    if not comp:
        return jsonify({"success": False, "error": "Unknown competition"}), 404

    date_str = request.args.get("date", today_str())
    matches  = fetch_matches(competition_code.upper(), date_str)

    return jsonify({
        "success": True,
        "data": {
            "competition": {"code": competition_code.upper(), "name": comp["name"]},
            "date":    date_str,
            "source":  matches[0].get("source", "none") if matches else "none",
            "matches": matches,
        }
    })

@app.route("/api/m3u8/<match_id>")
def api_m3u8_get(match_id):
    if STATE["maintenance"]:
        return jsonify({"success": False, "message": STATE["maintenance_msg"]}), 503
    stream = STATE["streams"].get(match_id)
    if not stream:
        return jsonify({"success": False, "error": "No stream available"}), 404
    return jsonify({
        "success": True,
        "data": {
            "match_id": match_id,
            "streams":  [stream["stream_url"]] + stream.get("fallbacks", []),
            "count":    1 + len(stream.get("fallbacks", [])),
        }
    })

@app.route("/resolve")
@app.route("/stream")
def resolve():
    url = request.args.get("url")
    if not url:
        return jsonify({"error": "url required"}), 400
    # Try to scrape m3u8 from a match page
    try:
        res  = requests.get(url, headers={**HEADERS, "Referer": url}, timeout=10)
        text = res.text
        m    = re.search(r'var\s+videos\s*=\s*\[([^\]]+)\]', text, re.DOTALL)
        servers = []
        if m:
            servers = re.findall(r'["\']([^"\']+\.m3u8[^"\']*)["\']', m.group(1))
        if not servers:
            servers = re.findall(r'https?://[^\s"\'<>]+\.m3u8[^\s"\'<>]*', text)
        if servers:
            seen, unique = set(), []
            for u in servers:
                if u not in seen:
                    seen.add(u); unique.append(u)
            base = re.match(r'(https?://[^/]+)', url)
            return jsonify({
                "stream_url": unique[0], "fallbacks": unique[1:5],
                "referer": (base.group(1) + "/") if base else url,
                "type": "m3u8", "server_count": len(unique)
            })
    except Exception as e:
        return jsonify({"error": str(e)}), 404
    return jsonify({"error": "No stream found"}), 404

# ─── Admin auth ───────────────────────────────────────────────────────────────
LOGIN_HTML = """<!DOCTYPE html><html><head><meta charset="UTF-8"><title>LiveKick Admin</title>
<style>*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,sans-serif;background:#F5F7FA;display:flex;align-items:center;justify-content:center;min-height:100vh}
.box{background:#fff;border-radius:16px;padding:44px;width:360px;box-shadow:0 4px 24px rgba(0,0,0,.09)}
h1{font-size:22px;font-weight:800;margin-bottom:6px}p{color:#9BA3B2;font-size:13px;margin-bottom:28px}
label{display:block;font-size:12px;font-weight:600;color:#6B7280;margin-bottom:6px}
input{width:100%;border:1.5px solid #EDEEF2;border-radius:10px;padding:12px 14px;font-size:14px;outline:none;margin-bottom:18px;background:#FAFAFA}
input:focus{border-color:#1246CC}
button{width:100%;background:#1246CC;color:#fff;border:none;border-radius:10px;padding:13px;font-size:14px;font-weight:700;cursor:pointer}
.err{color:#DC2626;font-size:13px;background:#FEF2F2;padding:10px 14px;border-radius:8px;margin-bottom:16px}
</style></head><body><div class="box">
<h1>⚡ LiveKick</h1><p>Admin Panel</p>
{% if error %}<div class="err">{{ error }}</div>{% endif %}
<form method="POST"><label>Password</label>
<input type="password" name="password" placeholder="Enter password" autofocus>
<button type="submit">Sign In →</button></form>
</div></body></html>"""

@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    error = None
    if request.method == "POST":
        if request.form.get("password") == ADMIN_PASSWORD:
            session["admin"] = True
            return redirect(url_for("admin_panel"))
        error = "Incorrect password."
    return render_template_string(LOGIN_HTML, error=error)

@app.route("/admin/logout")
def admin_logout():
    session.clear()
    return redirect(url_for("admin_login"))

@app.route("/")
def index():
    return redirect(url_for("admin_panel"))

# ─── Admin panel ──────────────────────────────────────────────────────────────
ADMIN_HTML = """<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>LiveKick Admin</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#F5F7FA;color:#0D1117;font-size:14px}
.sidebar{position:fixed;left:0;top:0;width:224px;height:100vh;background:#fff;border-right:1px solid #EDEEF2;display:flex;flex-direction:column;z-index:10}
.sb-logo{padding:22px 20px 18px;border-bottom:1px solid #EDEEF2}
.sb-logo h1{font-size:20px;font-weight:800;color:#0D1117;letter-spacing:-0.5px}
.sb-logo p{font-size:11px;color:#A0A8B4;margin-top:2px}
.nav{flex:1;overflow-y:auto;padding:8px}
.nav-btn{display:flex;align-items:center;gap:10px;width:100%;padding:10px 12px;background:none;border:none;cursor:pointer;border-radius:10px;font-size:13px;font-weight:500;color:#6B7280;text-align:left;transition:all .15s;margin-bottom:2px}
.nav-btn:hover,.nav-btn.active{background:#EEF3FF;color:#1246CC;font-weight:600}
.sb-foot{padding:16px;border-top:1px solid #EDEEF2}
.main{margin-left:224px;padding:28px}
.page{display:none}.page.active{display:block}
.topbar{display:flex;justify-content:space-between;align-items:center;margin-bottom:22px}
.topbar h2{font-size:20px;font-weight:700}
.card{background:#fff;border:1px solid #EDEEF2;border-radius:14px;padding:20px;margin-bottom:18px;box-shadow:0 1px 4px rgba(0,0,0,.04)}
.card-title{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#A0A8B4;margin-bottom:14px}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:18px}
.stat{background:#fff;border:1px solid #EDEEF2;border-radius:12px;padding:16px;text-align:center;box-shadow:0 1px 4px rgba(0,0,0,.04)}
.stat .n{font-size:28px;font-weight:800;color:#1246CC}
.stat .l{font-size:10px;color:#A0A8B4;text-transform:uppercase;letter-spacing:.5px;margin-top:4px}
.stat.red .n{color:#DC2626}.stat.grn .n{color:#16A34A}.stat.org .n{color:#D97706}
label.f{display:block;font-size:12px;font-weight:600;color:#6B7280;margin-bottom:5px;margin-top:14px}
label.f:first-child{margin-top:0}
input[type=text],input[type=password],textarea{width:100%;border:1.5px solid #EDEEF2;border-radius:10px;color:#0D1117;font-size:13px;padding:10px 12px;outline:none;transition:border-color .2s;background:#FAFAFA}
input:focus,textarea:focus{border-color:#1246CC;background:#fff}
textarea{height:76px;resize:vertical;font-family:monospace;font-size:12px}
.fg{display:grid;grid-template-columns:1fr 1fr;gap:18px}
.btn{display:inline-flex;align-items:center;gap:6px;padding:9px 18px;border-radius:10px;font-size:13px;font-weight:600;cursor:pointer;border:none;transition:all .15s}
.btn-p{background:#1246CC;color:#fff}.btn-p:hover{background:#0e38a8}
.btn-d{background:#FEF2F2;color:#DC2626;border:1px solid #FECACA}.btn-d:hover{background:#DC2626;color:#fff}
.btn-g{background:#F5F7FA;color:#6B7280;border:1px solid #EDEEF2}.btn-g:hover{background:#EDEEF2}
.btn-s{background:#F0FDF4;color:#16A34A;border:1px solid #BBF7D0}.btn-s:hover{background:#16A34A;color:#fff}
.btn-sm{padding:5px 12px;font-size:12px}
.alert{padding:10px 14px;border-radius:10px;font-size:13px;margin-bottom:14px;display:none}
.alert-s{background:#F0FDF4;border:1px solid #BBF7D0;color:#16A34A}
.alert-e{background:#FEF2F2;border:1px solid #FECACA;color:#DC2626}
.alert-i{background:#EEF3FF;border:1px solid #D0DCFF;color:#1246CC}
.mc{background:#F8F9FB;border:1px solid #EDEEF2;border-radius:10px;padding:12px 14px;margin-bottom:8px;display:flex;align-items:center;gap:12px}
.mc-teams{flex:1;min-width:0}
.mc-name{font-size:13px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.mc-meta{font-size:11px;color:#A0A8B4;margin-top:3px;display:flex;align-items:center;gap:6px;flex-wrap:wrap}
.bl{background:#FEF2F2;color:#DC2626;border-radius:999px;padding:2px 9px;font-size:10px;font-weight:700}
.bft{background:#F5F7FA;color:#6B7280;border-radius:999px;padding:2px 9px;font-size:10px}
.bsc{background:#EFF6FF;color:#2563EB;border-radius:999px;padding:2px 9px;font-size:11px;font-weight:600}
.bst{background:#F0FDF4;color:#16A34A;border-radius:999px;padding:2px 8px;font-size:10px}
.tab-bar{display:flex;gap:2px;border-bottom:2px solid #EDEEF2;margin-bottom:16px;overflow-x:auto}
.tab{padding:9px 14px;cursor:pointer;color:#A0A8B4;font-size:12px;font-weight:600;border:none;border-bottom:2px solid transparent;margin-bottom:-2px;background:none;white-space:nowrap;transition:all .15s}
.tab:hover{color:#1246CC}.tab.active{color:#1246CC;border-bottom-color:#1246CC}
.tc{display:none}.tc.active{display:block}
.tr{display:flex;align-items:center;justify-content:space-between;padding:12px 0;border-bottom:1px solid #F5F7FA}
.tr:last-child{border:none}
.sw{position:relative;display:inline-block;width:46px;height:26px}
.sw input{opacity:0;width:0;height:0}
.sl{position:absolute;cursor:pointer;inset:0;background:#E5E7EB;border-radius:26px;transition:.3s}
.sl:before{position:absolute;content:"";height:20px;width:20px;left:3px;bottom:3px;background:#fff;border-radius:50%;transition:.3s;box-shadow:0 1px 3px rgba(0,0,0,.2)}
input:checked+.sl{background:#1246CC}
input:checked+.sl:before{transform:translateX(20px)}
.ep{display:flex;align-items:center;gap:10px;padding:9px 0;border-bottom:1px solid #F5F7FA}
.ep:last-child{border:none}
.epm{background:#EEF3FF;color:#1246CC;border-radius:4px;padding:2px 8px;font-size:11px;font-weight:700;font-family:monospace;flex-shrink:0}
.epp{font-family:monospace;font-size:12px;color:#0D1117;flex:1}
.epd{font-size:11px;color:#A0A8B4}
.si{background:#F8F9FB;border:1px solid #EDEEF2;border-radius:10px;padding:13px;margin-bottom:8px;display:flex;align-items:flex-start;gap:10px}
.si-i{flex:1;min-width:0}
.si-t{font-size:13px;font-weight:600;margin-bottom:3px}
.si-u{font-size:11px;color:#A0A8B4;font-family:monospace;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.si-m{font-size:11px;color:#D0DCFF;margin-top:3px}
.sr{display:flex;gap:8px}.sr input{flex:1}
.empty{text-align:center;padding:36px;color:#A0A8B4;font-size:13px}
.cb{background:#F8F9FB;border:1px solid #EDEEF2;border-radius:10px;padding:12px;font-family:monospace;font-size:12px;color:#1246CC;margin-top:10px;white-space:pre-wrap;word-break:break-all;max-height:300px;overflow-y:auto}
</style></head><body>
<div class="sidebar">
  <div class="sb-logo"><h1>⚡ LiveKick</h1><p>Stream Admin</p></div>
  <div class="nav">
    <button class="nav-btn active" onclick="showPage('dashboard',this)">📊 Dashboard</button>
    <button class="nav-btn" onclick="showPage('matches',this)">⚽ Matches</button>
    <button class="nav-btn" onclick="showPage('streams',this)">📡 Streams</button>
    <button class="nav-btn" onclick="showPage('leagues',this)">🏆 Leagues</button>
    <button class="nav-btn" onclick="showPage('settings',this)">⚙️ Settings</button>
  </div>
  <div class="sb-foot">
    <a href="/admin/logout"><button class="btn btn-g" style="width:100%;justify-content:center">🚪 Logout</button></a>
  </div>
</div>
<div class="main">

<!-- DASHBOARD -->
<div class="page active" id="page-dashboard">
  <div class="topbar"><h2>Dashboard</h2></div>
  <div class="stats">
    <div class="stat"><div class="n">{{ streams_count }}</div><div class="l">Streams</div></div>
    <div class="stat grn"><div class="n">{{ leagues_on }}</div><div class="l">Leagues</div></div>
    <div class="stat org"><div class="n">{{ hidden_count }}</div><div class="l">Hidden</div></div>
    <div class="stat {% if state.maintenance %}red{% endif %}">
      <div class="n">{{ 'ON' if state.maintenance else 'OFF' }}</div><div class="l">Maintenance</div>
    </div>
  </div>
  <div class="card">
    <div class="card-title">API Endpoints</div>
    <div class="ep"><span class="epm">GET</span><span class="epp">/api/competitions</span><span class="epd">League list — 12h cache</span></div>
    <div class="ep"><span class="epm">GET</span><span class="epp">/api/matches/{code}?date=</span><span class="epd">Matches — lifecycle-aware cache</span></div>
    <div class="ep"><span class="epm">GET</span><span class="epp">/api/matches/live</span><span class="epd">Live matches — 25s cache</span></div>
    <div class="ep"><span class="epm">GET</span><span class="epp">/api/m3u8/{match_id}</span><span class="epd">Stream URLs — no cache</span></div>
    <div class="ep"><span class="epm">GET</span><span class="epp">/proxy/playlist?url=</span><span class="epd">HLS proxy — for web browser</span></div>
    <div class="ep"><span class="epm">GET</span><span class="epp">/proxy/chunk?url=</span><span class="epd">Segment relay — for web browser</span></div>
    <div class="ep"><span class="epm">GET</span><span class="epp">/ping</span><span class="epd">Keep-alive</span></div>
  </div>
  <div class="card">
    <div class="card-title">Cache Status</div>
    <div style="display:flex;gap:10px">
      <button class="btn btn-g btn-sm" onclick="loadCache()">🔄 Refresh</button>
      <button class="btn btn-d btn-sm" onclick="clearCache()">🗑 Clear All</button>
    </div>
    <div class="cb" id="cacheBox">Click Refresh</div>
  </div>
</div>

<!-- MATCHES -->
<div class="page" id="page-matches">
  <div class="topbar"><h2>Matches</h2><button class="btn btn-g btn-sm" onclick="reloadTab()">🔄 Refresh</button></div>
  <div class="tab-bar" id="tabBar">
    {% for code, comp in competitions.items() %}{% if comp.enabled %}
    <button class="tab {% if loop.first %}active{% endif %}" onclick="switchTab(this,'{{ code }}')" data-code="{{ code }}">{{ comp.name }}</button>
    {% endif %}{% endfor %}
  </div>
  {% for code, comp in competitions.items() %}{% if comp.enabled %}
  <div class="tc {% if loop.first %}active{% endif %}" id="tab-{{ code }}">
    <div id="m-{{ code }}"><div class="empty">Click Refresh to load</div></div>
  </div>
  {% endif %}{% endfor %}
</div>

<!-- STREAMS -->
<div class="page" id="page-streams">
  <div class="topbar"><h2>Streams</h2></div>
  <div class="card">
    <div class="card-title">Add / Update Stream</div>
    <div id="stAlert" class="alert"></div>
    <div class="fg">
      <div>
        <label class="f">Match Title</label><input type="text" id="sTitle" placeholder="Spain vs Belgium">
        <label class="f">Match ID <span style="font-weight:400;color:#A0A8B4">(ESPN ID from Matches tab)</span></label>
        <input type="text" id="sId" placeholder="760457">
        <label class="f">Referer</label><input type="text" id="sRef" value="http://www.fawanews.sc/">
      </div>
      <div>
        <label class="f">Primary m3u8 URL</label><input type="text" id="sUrl" placeholder="http://193.47.62.47/hls/GOOO.m3u8">
        <label class="f">Fallback URLs <span style="font-weight:400;color:#A0A8B4">(one per line)</span></label>
        <textarea id="sFb" placeholder="http://193.47.62.59/hls/GOOO.m3u8"></textarea>
        <label class="f">Auto-scrape from page</label>
        <div class="sr"><input type="text" id="sScrape" placeholder="http://www.fawanews.sc/match.html"><button class="btn btn-g" onclick="scrape()">🔍</button></div>
      </div>
    </div>
    <div style="margin-top:16px;display:flex;gap:10px">
      <button class="btn btn-p" onclick="addStream()">➕ Add</button>
      <button class="btn btn-g" onclick="clearForm()">✕ Clear</button>
    </div>
  </div>
  <div class="card">
    <div class="card-title">Active Streams ({{ streams_count }})</div>
    {% if streams %}
      {% for id, s in streams.items() %}
      <div class="si">
        <div class="si-i">
          <div class="si-t">{{ s.title }}</div>
          <div class="si-u">{{ s.stream_url }}</div>
          <div class="si-m">ID: {{ id }} · Fallbacks: {{ s.fallbacks|length }}</div>
        </div>
        <button class="btn btn-d btn-sm" onclick="delStream('{{ id }}')">✕</button>
      </div>
      {% endfor %}
    {% else %}<div class="empty">No streams yet.</div>{% endif %}
  </div>
</div>

<!-- LEAGUES -->
<div class="page" id="page-leagues">
  <div class="topbar"><h2>Leagues</h2></div>
  <div class="card">
    <div class="card-title">App Visibility</div>
    <div id="lgAlert" class="alert"></div>
    {% for code, comp in competitions.items() %}
    <div class="tr">
      <div><div style="font-size:13px;font-weight:600">{{ comp.name }}</div><div style="font-size:11px;color:#A0A8B4">{{ code }}</div></div>
      <label class="sw"><input type="checkbox" {% if comp.enabled %}checked{% endif %} onchange="toggleLeague('{{ code }}',this.checked)"><span class="sl"></span></label>
    </div>
    {% endfor %}
  </div>
</div>

<!-- SETTINGS -->
<div class="page" id="page-settings">
  <div class="topbar"><h2>Settings</h2></div>
  <div class="card">
    <div class="card-title">Maintenance Mode</div>
    <div id="mAlert" class="alert"></div>
    <label class="f">Message</label>
    <input type="text" id="mMsg" value="{{ state.maintenance_msg }}">
    <div style="margin-top:14px;display:flex;gap:10px">
      <button class="btn {% if state.maintenance %}btn-s{% else %}btn-d{% endif %}" onclick="toggleMaint()">
        {% if state.maintenance %}✅ Disable{% else %}🔴 Enable{% endif %}
      </button>
      <button class="btn btn-g" onclick="saveMsg()">💾 Save Message</button>
    </div>
  </div>
  <div class="card">
    <div class="card-title">Change Password</div>
    <div id="pAlert" class="alert"></div>
    <label class="f">New Password</label>
    <input type="password" id="newPw" placeholder="Min 6 characters">
    <div style="margin-top:14px"><button class="btn btn-g" onclick="changePw()">🔒 Update</button></div>
  </div>
</div>

</div>
<script>
function showPage(id,btn){document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));document.querySelectorAll('.nav-btn').forEach(b=>b.classList.remove('active'));document.getElementById('page-'+id).classList.add('active');if(btn)btn.classList.add('active');if(id==='matches')reloadTab();}
function switchTab(el,code){document.querySelectorAll('#tabBar .tab').forEach(t=>t.classList.remove('active'));el.classList.add('active');document.querySelectorAll('.tc').forEach(t=>t.classList.remove('active'));document.getElementById('tab-'+code).classList.add('active');loadMatches(code);}
function reloadTab(){const a=document.querySelector('#tabBar .tab.active');if(a)loadMatches(a.dataset.code);}
async function loadMatches(code){
  const el=document.getElementById('m-'+code);if(!el)return;
  el.innerHTML='<div class="empty">Loading…</div>';
  try{
    const r=await fetch('/admin/api/matches/'+code);const d=await r.json();
    if(!d.matches||!d.matches.length){el.innerHTML='<div class="empty">No matches today.</div>';return;}
    el.innerHTML=d.matches.map(m=>{
      const badge=m.status==='LIVE'?`<span class="bl">🔴 LIVE ${m.minute||''}</span>`:m.status==='FINISHED'?'<span class="bft">FT</span>':`<span class="bsc">🕐 ${m.kickoff}</span>`;
      const st=m.streams&&m.streams.length?'<span class="bst">📡</span>':'';
      const score=m.status!=='SCHEDULED'?`<strong>${m.home_score}–${m.away_score}</strong>`:'vs';
      return`<div class="mc"><div class="mc-teams"><div class="mc-name">${m.home_team} vs ${m.away_team}</div><div class="mc-meta">${badge}${st}<span>${score}</span>${m.venue?'· '+m.venue:''}</div></div><div style="display:flex;gap:6px">${!m.streams?.length?`<button class="btn btn-s btn-sm" onclick="prefill('${m.match_id}','${m.home_team} vs ${m.away_team}')">📡 Add</button>`:''}<button class="btn btn-g btn-sm" onclick="toggleHide('${m.match_id}',this)">${m.is_hidden?'👁 Show':'🙈 Hide'}</button></div></div>`;
    }).join('');
  }catch(e){el.innerHTML='<div class="empty" style="color:#DC2626">Failed.</div>';}
}
async function toggleHide(id,btn){const r=await fetch('/admin/api/hide/'+id,{method:'POST'});const d=await r.json();btn.textContent=d.hidden?'👁 Show':'🙈 Hide';}
function prefill(id,title){document.getElementById('sId').value=id;document.getElementById('sTitle').value=title;showPage('streams',document.querySelectorAll('.nav-btn')[2]);}
async function scrape(){
  const url=document.getElementById('sScrape').value.trim();if(!url)return;
  al('stAlert','info','🔍 Scraping…');
  const r=await fetch('/resolve?url='+encodeURIComponent(url));const d=await r.json();
  if(d.stream_url){document.getElementById('sUrl').value=d.stream_url;document.getElementById('sFb').value=(d.fallbacks||[]).join('\n');document.getElementById('sRef').value=d.referer||'';al('stAlert','success','✅ Found '+d.server_count+' server(s).');}
  else al('stAlert','error','❌ '+(d.error||'Not found.'));
}
async function addStream(){
  const id=document.getElementById('sId').value.trim(),title=document.getElementById('sTitle').value.trim(),url=document.getElementById('sUrl').value.trim(),raw=document.getElementById('sFb').value.trim(),ref=document.getElementById('sRef').value.trim();
  if(!id||!title||!url){al('stAlert','error','ID, Title and URL required.');return;}
  const fallbacks=raw?raw.split('\n').map(u=>u.trim()).filter(u=>u):[];
  al('stAlert','info','Saving…');
  const r=await fetch('/admin/api/streams',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id,title,stream_url:url,fallbacks,referer:ref})});
  const d=await r.json();
  if(r.ok){al('stAlert','success','✅ Added.');clearForm();setTimeout(()=>location.reload(),1200);}
  else al('stAlert','error','❌ '+(d.error||'Failed.'));
}
async function delStream(id){if(!confirm('Remove?'))return;await fetch('/admin/api/streams/'+id,{method:'DELETE'});location.reload();}
function clearForm(){['sTitle','sId','sUrl','sFb','sScrape'].forEach(i=>{const e=document.getElementById(i);if(e)e.value='';});document.getElementById('sRef').value='http://www.fawanews.sc/';}
async function toggleLeague(code,enabled){const r=await fetch('/admin/api/leagues/'+code,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({enabled})});const d=await r.json();al('lgAlert',d.ok?'success':'error',d.ok?'✅ Updated.':'❌ Failed.');}
async function toggleMaint(){const r=await fetch('/admin/api/maintenance',{method:'POST'});const d=await r.json();al('mAlert','success','✅ '+(d.maintenance?'Enabled.':'Disabled.'));setTimeout(()=>location.reload(),900);}
async function saveMsg(){const msg=document.getElementById('mMsg').value.trim();if(!msg)return;const r=await fetch('/admin/api/maintenance/message',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({message:msg})});const d=await r.json();al('mAlert',d.ok?'success':'error',d.ok?'✅ Saved.':'❌ Failed.');}
async function changePw(){const p=document.getElementById('newPw').value.trim();if(!p||p.length<6){al('pAlert','error','Min 6 characters.');return;}const r=await fetch('/admin/api/password',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:p})});const d=await r.json();if(d.ok){al('pAlert','success','✅ Updated. Logging out…');setTimeout(()=>location.href='/admin/logout',1500);}else al('pAlert','error','❌ '+(d.error||'Failed.'));}
async function loadCache(){const r=await fetch('/health');const d=await r.json();document.getElementById('cacheBox').textContent=JSON.stringify(d.cache,null,2);}
async function clearCache(){if(!confirm('Clear all cache?'))return;const r=await fetch('/admin/api/cache/clear',{method:'POST'});const d=await r.json();if(d.ok){al('stAlert','success','✅ Cleared.');loadCache();}}
function al(id,type,msg){const el=document.getElementById(id);if(!el)return;el.className='alert alert-'+({success:'s',error:'e',info:'i'}[type]||'i');el.textContent=msg;el.style.display='block';if(type!=='info')setTimeout(()=>{el.style.display='none';},5000);}
</script></body></html>"""

@app.route("/admin")
@login_required
def admin_panel():
    return render_template_string(ADMIN_HTML,
        state=STATE, competitions=COMPETITIONS,
        streams=STATE["streams"],
        streams_count=len(STATE["streams"]),
        leagues_on=sum(1 for c in COMPETITIONS.values() if c["enabled"]),
        hidden_count=len(STATE["hidden_matches"]),
    )

# ─── Admin API ────────────────────────────────────────────────────────────────
@app.route("/admin/api/matches/<code>")
@login_required
def admin_matches(code):
    comp = COMPETITIONS.get(code.upper())
    if not comp:
        return jsonify({"error": "Unknown"}), 404
    matches = fetch_matches(code.upper())
    for m in matches:
        m["is_hidden"] = m["match_id"] in STATE["hidden_matches"]
    return jsonify({"matches": matches})

@app.route("/admin/api/hide/<match_id>", methods=["POST"])
@login_required
def admin_hide(match_id):
    if match_id in STATE["hidden_matches"]:
        STATE["hidden_matches"].discard(match_id)
        return jsonify({"hidden": False})
    STATE["hidden_matches"].add(match_id)
    return jsonify({"hidden": True})

@app.route("/admin/api/streams", methods=["POST"])
@login_required
def admin_add_stream():
    d   = request.get_json()
    mid = d.get("id","").strip()
    ttl = d.get("title","").strip()
    url = d.get("stream_url","").strip()
    if not mid or not ttl or not url:
        return jsonify({"error": "id, title, stream_url required"}), 400
    fbs = d.get("fallbacks", [])
    STATE["streams"][mid] = {"id":mid,"title":ttl,"stream_url":url,
                              "fallbacks":fbs,"referer":d.get("referer",""),
                              "streams":[url]+fbs}
    cache.bust("matches:"); cache.bust("live:")
    return jsonify({"message": "Added", "id": mid}), 201

@app.route("/admin/api/streams/<match_id>", methods=["DELETE"])
@login_required
def admin_del_stream(match_id):
    STATE["streams"].pop(match_id, None)
    cache.bust("matches:"); cache.bust("live:")
    return jsonify({"message": "Deleted"})

@app.route("/admin/api/leagues/<code>", methods=["POST"])
@login_required
def admin_toggle_league(code):
    d = request.get_json()
    c = code.upper()
    if c in COMPETITIONS:
        COMPETITIONS[c]["enabled"] = bool(d.get("enabled", True))
        cache.bust("competitions:")
        return jsonify({"ok": True})
    return jsonify({"error": "Unknown"}), 404

@app.route("/admin/api/maintenance", methods=["POST"])
@login_required
def admin_maintenance():
    STATE["maintenance"] = not STATE["maintenance"]
    return jsonify({"maintenance": STATE["maintenance"]})

@app.route("/admin/api/maintenance/message", methods=["POST"])
@login_required
def admin_maint_msg():
    STATE["maintenance_msg"] = request.get_json().get("message","")
    return jsonify({"ok": True})

@app.route("/admin/api/password", methods=["POST"])
@login_required
def admin_password():
    global ADMIN_PASSWORD
    p = request.get_json().get("password","").strip()
    if len(p) < 6:
        return jsonify({"error": "Too short"}), 400
    ADMIN_PASSWORD = p
    return jsonify({"ok": True})

@app.route("/admin/api/cache/clear", methods=["POST"])
@login_required
def admin_clear_cache():
    cache.bust("")
    return jsonify({"ok": True})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
