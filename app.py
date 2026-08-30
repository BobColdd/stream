from flask import Flask, render_template, request, jsonify, send_file
from flask_cors import CORS
import requests
from datetime import datetime, timedelta
import os
import logging
from dotenv import load_dotenv
import time
import json
import threading
import sqlite3
from functools import wraps
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'livekick_secret_key_2026')
CORS(app)

# ============================================
# LEAGUE MANAGEMENT
# ============================================

ALL_LEAGUES = {
    'PL': {'fd_id': 2021, 'espn': 'eng.1', 'name': 'Premier League', 'default': True},
    'CL': {'fd_id': 2001, 'espn': 'uefa.champions', 'name': 'Champions League', 'default': True},
    'BL1': {'fd_id': 2002, 'espn': 'ger.1', 'name': 'Bundesliga', 'default': True},
    'SA': {'fd_id': 2019, 'espn': 'ita.1', 'name': 'Serie A', 'default': True},
    'PD': {'fd_id': 2014, 'espn': 'esp.1', 'name': 'La Liga', 'default': True},
    'FL1': {'fd_id': 2015, 'espn': 'fra.1', 'name': 'Ligue 1', 'default': True},
    'FA': {'fd_id': 2016, 'espn': 'eng.3', 'name': 'FA Cup', 'default': False},
    'ELC': {'fd_id': 2016, 'espn': 'eng.2', 'name': 'Championship', 'default': False},
    'DED': {'fd_id': 2003, 'espn': 'ned.1', 'name': 'Eredivisie', 'default': False},
    'CLI': {'fd_id': 2152, 'espn': 'conmebol.libertadores', 'name': 'Copa Libertadores', 'default': False},
    'WC': {'fd_id': 2000, 'espn': 'fifa.world', 'name': 'World Cup', 'default': False},
}

ACTIVE_LEAGUES_FILE = os.path.join(os.environ.get('DATA_DIR', '/opt/render/data'), 'active_leagues.json')

def load_active_leagues():
    if os.path.exists(ACTIVE_LEAGUES_FILE):
        try:
            with open(ACTIVE_LEAGUES_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {code: info['default'] for code, info in ALL_LEAGUES.items()}

def save_active_leagues(active_leagues):
    os.makedirs(os.path.dirname(ACTIVE_LEAGUES_FILE), exist_ok=True)
    with open(ACTIVE_LEAGUES_FILE, 'w') as f:
        json.dump(active_leagues, f, indent=2)

active_leagues = load_active_leagues()
logger.info(f"Active leagues: {[k for k, v in active_leagues.items() if v]}")

def get_active_competitions():
    return {code: info for code, info in ALL_LEAGUES.items() if active_leagues.get(code, False)}

COMPETITIONS = get_active_competitions()

# ============================================
# CONFIGURATION
# ============================================

BACKEND_URL = os.getenv('BACKEND_URL', 'https://api.football-data.org/v4')
FOOTBALL_DATA_API_KEY = os.environ.get('FOOTBALL_DATA_API_KEY', '214ac19439794667865a917ad93d187c')
PORT = int(os.getenv('PORT', 5000))
DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'

# Data directories
DATA_DIR = os.environ.get('DATA_DIR', '/opt/render/data')
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, 'streams.db')
CLIP_STORAGE_DIR = os.path.join(DATA_DIR, 'clips')
os.makedirs(CLIP_STORAGE_DIR, exist_ok=True)

# ============================================
# DATABASE
# ============================================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS streams
                 (match_id TEXT, url TEXT, PRIMARY KEY (match_id, url))''')
    conn.commit()
    conn.close()

init_db()

def load_m3u8_links():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT match_id, url FROM streams')
    rows = c.fetchall()
    conn.close()
    links = {}
    for match_id, url in rows:
        if match_id not in links:
            links[match_id] = []
        links[match_id].append(url)
    return links

def save_m3u8_links(links):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('DELETE FROM streams')
    for match_id, urls in links.items():
        for url in urls:
            c.execute('INSERT INTO streams (match_id, url) VALUES (?, ?)', (match_id, url))
    conn.commit()
    conn.close()

# ============================================
# CACHE
# ============================================

class FastCache:
    def __init__(self):
        self._cache = {}
    
    def get(self, key):
        if key in self._cache:
            data, timestamp, ttl = self._cache[key]
            if time.time() - timestamp < ttl:
                return data
            del self._cache[key]
        return None
    
    def set(self, key, data, ttl):
        self._cache[key] = (data, time.time(), ttl)
    
    def clear(self):
        self._cache.clear()

cache = FastCache()

# ============================================
# REQUESTS SESSION
# ============================================

def get_session():
    session = requests.Session()
    retry_strategy = Retry(total=2, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.espn.com/',
        'X-Auth-Token': FOOTBALL_DATA_API_KEY
    })
    return session

session = get_session()

# ============================================
# CORS HEADERS
# ============================================

@app.after_request
def add_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-Auth-Token'
    response.headers['Content-Type'] = 'application/json'
    return response

# ============================================
# DATA FETCHING - DAILY FIXTURES (football-data.org)
# ============================================

def fetch_fd_fixtures(fd_id, date_str):
    """Fetch fixtures from football-data.org for a specific date."""
    try:
        response = session.get(
            f'{BACKEND_URL}/competitions/{fd_id}/matches',
            params={'dateFrom': date_str, 'dateTo': date_str},
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        
        fixtures = []
        for match in data.get('matches', []):
            home = match.get('homeTeam', {})
            away = match.get('awayTeam', {})
            
            utc_date = match.get('utcDate')
            kickoff = ''
            if utc_date:
                try:
                    dt = datetime.fromisoformat(utc_date.replace('Z', '+00:00'))
                    kickoff = dt.strftime('%H:%M UTC')
                except:
                    kickoff = utc_date
            
            match_id = match.get('id', '')
            
            fixtures.append({
                'match_id': str(match_id),
                'home_team': home.get('name', 'Home'),
                'away_team': away.get('name', 'Away'),
                'home_crest': home.get('crest'),
                'away_crest': away.get('crest'),
                'kickoff': kickoff,
                'status': 'SCHEDULED',
                'home_score': '-',
                'away_score': '-',
                'minute': None,
                'is_live': False
            })
        
        return fixtures
    except Exception as e:
        logger.error(f"Error fetching FD fixtures: {e}")
        return None

def fetch_daily_fixtures():
    """Fetch today's fixtures for all active leagues."""
    logger.info("Starting daily fixture fetch...")
    today = datetime.now().strftime('%Y-%m-%d')
    
    for code, comp_info in COMPETITIONS.items():
        if not active_leagues.get(code, False):
            continue
        
        fixtures = fetch_fd_fixtures(comp_info['fd_id'], today)
        if fixtures is not None:
            cache_key = f"fixtures_{code}_{today}"
            cache.set(cache_key, fixtures, 86400)  # 24 hours
            logger.info(f"Cached {len(fixtures)} fixtures for {comp_info['name']}")
    
    logger.info("Daily fixture fetch complete")

# ============================================
# DATA FETCHING - LIVE SCORES (ESPN)
# ============================================

def fetch_espn_live_matches(slug):
    """Fetch ONLY currently LIVE matches from ESPN."""
    try:
        response = session.get(
            f'https://site.api.espn.com/apis/site/v2/sports/soccer/{slug}/scoreboard',
            timeout=5
        )
        response.raise_for_status()
        data = response.json()
        
        live_matches = []
        
        for event in data.get('events', []):
            status = event.get('status', {})
            state = status.get('type', {}).get('state', '')
            
            # ONLY process currently LIVE matches
            if state != 'in':
                continue
            
            competitions = event.get('competitions', [])
            if not competitions:
                continue
            
            comp = competitions[0]
            competitors = comp.get('competitors', [])
            if len(competitors) < 2:
                continue
            
            home = competitors[0]
            away = competitors[1]
            
            live_matches.append({
                'match_id': str(event.get('id', '')),
                'home_team': home.get('team', {}).get('displayName', 'Home'),
                'away_team': away.get('team', {}).get('displayName', 'Away'),
                'home_crest': home.get('team', {}).get('logo'),
                'away_crest': away.get('team', {}).get('logo'),
                'home_score': home.get('score', '0'),
                'away_score': away.get('score', '0'),
                'minute': status.get('displayClock', '0'),
                'status': 'LIVE',
                'is_live': True,
                'kickoff': None
            })
        
        return live_matches
    except Exception as e:
        logger.error(f"Error fetching ESPN live matches: {e}")
        return []

# ============================================
# SMART DATA FETCHING
# ============================================

def get_matches_for_today(code):
    """
    Get matches for today - combines fixtures + live updates.
    Only LIVE matches have scores/minutes.
    """
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Get fixtures from cache (daily)
    fixtures_key = f"fixtures_{code}_{today}"
    fixtures = cache.get(fixtures_key)
    
    # If fixtures not in cache, fetch them
    if fixtures is None:
        comp_info = COMPETITIONS.get(code)
        if comp_info:
            fixtures = fetch_fd_fixtures(comp_info['fd_id'], today)
            if fixtures is not None:
                cache.set(fixtures_key, fixtures, 86400)
    
    # Get live matches from ESPN
    comp_info = COMPETITIONS.get(code)
    live_matches = []
    if comp_info:
        live_matches = fetch_espn_live_matches(comp_info['espn'])
    
    # Update fixtures with live data
    if fixtures and live_matches:
        # Create lookup for live matches
        live_lookup = {m['match_id']: m for m in live_matches}
        
        for fixture in fixtures:
            match_id = fixture['match_id']
            if match_id in live_lookup:
                # Update with live data
                live = live_lookup[match_id]
                fixture['status'] = 'LIVE'
                fixture['home_score'] = live['home_score']
                fixture['away_score'] = live['away_score']
                fixture['minute'] = live['minute']
                fixture['is_live'] = True
            else:
                # Not live, keep as scheduled
                fixture['status'] = 'SCHEDULED'
                fixture['is_live'] = False
    
    return fixtures or []

def get_all_today_matches():
    """Get all matches for today across all active leagues."""
    all_matches = []
    
    for code in COMPETITIONS.keys():
        if not active_leagues.get(code, False):
            continue
        
        matches = get_matches_for_today(code)
        
        # Add competition info to each match
        comp_info = COMPETITIONS.get(code)
        for match in matches:
            match['competition_code'] = code
            match['competition_name'] = comp_info['name']
        
        all_matches.extend(matches)
    
    return all_matches

# ============================================
# BACKGROUND UPDATES
# ============================================

def background_live_updater():
    """Background thread to keep live matches updated."""
    while True:
        try:
            # Just cache the live matches for quick access
            live_matches = []
            for code in COMPETITIONS.keys():
                if not active_leagues.get(code, False):
                    continue
                comp_info = COMPETITIONS.get(code)
                if comp_info:
                    live = fetch_espn_live_matches(comp_info['espn'])
                    for match in live:
                        match['competition_code'] = code
                        match['competition_name'] = comp_info['name']
                    live_matches.extend(live)
            
            # Cache live matches with short TTL
            cache.set('live_matches_cached', live_matches, 15)  # 15 seconds
            logger.info(f"Updated {len(live_matches)} live matches")
            
            time.sleep(15)
        except Exception as e:
            logger.error(f"Background updater error: {e}")
            time.sleep(15)

def start_background_updater():
    thread = threading.Thread(target=background_live_updater, daemon=True)
    thread.start()
    logger.info("Background live updater started")

# ============================================
# SCHEDULED DAILY FIXTURE FETCH
# ============================================

def schedule_daily_fetch():
    """Schedule daily fixture fetch at 1 AM EAT."""
    def scheduler():
        while True:
            now = datetime.now()
            # 1 AM EAT = 10 PM UTC (22:00)
            target_hour = 22
            target_minute = 0
            
            next_run = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
            if now > next_run:
                next_run += timedelta(days=1)
            
            wait_seconds = (next_run - now).total_seconds()
            logger.info(f"Next fixture fetch at {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
            
            time.sleep(wait_seconds)
            fetch_daily_fixtures()
    
    thread = threading.Thread(target=scheduler, daemon=True)
    thread.start()
    logger.info("Daily fixture scheduler started")

# ============================================
# API ENDPOINTS
# ============================================

@app.route('/api/matches', methods=['GET'])
def api_matches():
    """Get all matches for today (scheduled + live)."""
    today = datetime.now().strftime('%Y-%m-%d')
    force_refresh = request.args.get('refresh') == 'true'
    
    cache_key = f"all_matches_{today}"
    
    if not force_refresh:
        cached = cache.get(cache_key)
        if cached:
            return jsonify({
                'success': True,
                'data': {
                    'date': today,
                    'matches': cached
                }
            })
    
    matches = get_all_today_matches()
    
    # Cache for 1 minute
    cache.set(cache_key, matches, 60)
    
    return jsonify({
        'success': True,
        'data': {
            'date': today,
            'matches': matches
        }
    })

@app.route('/api/matches/live', methods=['GET'])
def api_live_matches():
    """Get only currently live matches."""
    # Get from cache (updated every 15 seconds)
    live = cache.get('live_matches_cached')
    
    if live is None:
        # Fetch fresh if cache is empty
        live = []
        for code in COMPETITIONS.keys():
            if not active_leagues.get(code, False):
                continue
            comp_info = COMPETITIONS.get(code)
            if comp_info:
                matches = fetch_espn_live_matches(comp_info['espn'])
                for match in matches:
                    match['competition_code'] = code
                    match['competition_name'] = comp_info['name']
                live.extend(matches)
    
    return jsonify({
        'success': True,
        'data': {
            'count': len(live),
            'matches': live
        }
    })

@app.route('/api/matches/<code>', methods=['GET'])
def api_matches_by_league(code):
    """Get matches for a specific league today."""
    if code not in COMPETITIONS:
        return jsonify({'success': False, 'error': 'League not found or inactive'}), 404
    
    if not active_leagues.get(code, False):
        return jsonify({'success': False, 'error': 'League is inactive'}), 404
    
    matches = get_matches_for_today(code)
    comp_info = COMPETITIONS.get(code)
    
    return jsonify({
        'success': True,
        'data': {
            'competition': {
                'code': code,
                'name': comp_info['name'],
                'fd_id': comp_info['fd_id'],
                'espn_slug': comp_info['espn']
            },
            'matches': matches
        }
    })

@app.route('/api/competitions', methods=['GET'])
def api_competitions():
    """Get all active competitions."""
    competitions = []
    for code, info in COMPETITIONS.items():
        if active_leagues.get(code, False):
            competitions.append({
                'code': code,
                'name': info['name'],
                'fd_id': info['fd_id'],
                'espn_slug': info['espn']
            })
    return jsonify({'success': True, 'data': competitions})

@app.route('/api/leagues', methods=['GET'])
def api_leagues():
    """Get all leagues with their active status."""
    leagues = []
    for code, info in ALL_LEAGUES.items():
        leagues.append({
            'code': code,
            'name': info['name'],
            'active': active_leagues.get(code, False),
            'default': info['default']
        })
    return jsonify({'success': True, 'data': leagues})

@app.route('/api/leagues', methods=['POST'])
def api_update_leagues():
    """Update active leagues."""
    data = request.get_json()
    if not data or 'leagues' not in data:
        return jsonify({'success': False, 'error': 'Missing leagues data'}), 400
    
    new_active = data['leagues']
    
    # Validate
    for code in new_active.keys():
        if code not in ALL_LEAGUES:
            return jsonify({'success': False, 'error': f'Invalid league: {code}'}), 400
    
    # Save
    global active_leagues, COMPETITIONS
    active_leagues = new_active
    save_active_leagues(active_leagues)
    COMPETITIONS = get_active_competitions()
    
    # Clear cache to force refresh
    cache.clear()
    
    logger.info(f"Updated active leagues: {[k for k, v in active_leagues.items() if v]}")
    
    return jsonify({
        'success': True,
        'message': 'Leagues updated successfully',
        'data': {
            'active': [k for k, v in active_leagues.items() if v],
            'inactive': [k for k, v in active_leagues.items() if not v]
        }
    })

@app.route('/api/m3u8/<match_id>', methods=['GET', 'POST', 'DELETE'])
def api_m3u8(match_id):
    """Manage m3u8 links for a match."""
    m3u8_links = load_m3u8_links()
    
    if request.method == 'GET':
        streams = m3u8_links.get(match_id, [])
        return jsonify({
            'success': True,
            'data': {
                'match_id': match_id,
                'streams': streams,
                'count': len(streams)
            }
        })
    
    elif request.method == 'POST':
        data = request.get_json()
        if not data or 'url' not in data:
            return jsonify({'success': False, 'error': 'Missing url'}), 400
        
        url = data['url'].strip()
        if not url or not url.startswith(('http://', 'https://')):
            return jsonify({'success': False, 'error': 'Invalid URL'}), 400
        
        streams = m3u8_links.get(match_id, [])
        if url in streams:
            return jsonify({'success': False, 'error': 'Stream already exists'}), 400
        
        streams.append(url)
        m3u8_links[match_id] = streams
        save_m3u8_links(m3u8_links)
        
        return jsonify({
            'success': True,
            'message': 'Stream added',
            'data': {'match_id': match_id, 'streams': streams, 'count': len(streams)}
        })
    
    elif request.method == 'DELETE':
        data = request.get_json()
        if not data or 'url' not in data:
            return jsonify({'success': False, 'error': 'Missing url'}), 400
        
        url = data['url'].strip()
        streams = m3u8_links.get(match_id, [])
        
        if url not in streams:
            return jsonify({'success': False, 'error': 'Stream not found'}), 404
        
        streams.remove(url)
        if streams:
            m3u8_links[match_id] = streams
        else:
            del m3u8_links[match_id]
        
        save_m3u8_links(m3u8_links)
        
        return jsonify({'success': True, 'message': 'Stream removed'})

@app.route('/health')
def health():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'port': PORT,
        'active_leagues': [k for k, v in active_leagues.items() if v],
        'cache_size': len(cache._cache)
    })

# ============================================
# WEB ROUTES (Admin Panel)
# ============================================

@app.route('/')
def index():
    """Main dashboard."""
    return render_template('index.html')

@app.route('/admin/leagues')
def admin_leagues():
    """League management page."""
    return render_template('leagues.html')

@app.route('/admin/matches')
def admin_matches():
    """Match management page."""
    return render_template('matches.html')

# ============================================
# ERROR HANDLERS
# ============================================

@app.errorhandler(404)
def not_found(error):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(error):
    return render_template('500.html'), 500

# ============================================
# START APP
# ============================================

if __name__ == '__main__':
    # Start background services
    start_background_updater()
    schedule_daily_fetch()
    
    # Initial fetch
    fetch_daily_fixtures()
    
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"Starting server on port {port}")
    app.run(host='0.0.0.0', port=port, debug=DEBUG)
