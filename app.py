from flask import Flask, render_template_string, request, redirect, url_for, jsonify, session
from flask_cors import CORS
import requests
from datetime import datetime, timedelta
import json
import os
import sqlite3
from functools import lru_cache
import time
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'livekick_secret_key_2026')
CORS(app)  # Enable CORS for Android app

# Use persistent disk if available
DATA_DIR = os.environ.get('DATA_DIR', '/opt/render/data')
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, 'streams.db')

# Initialize SQLite database for persistent storage
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS streams
                 (match_id TEXT, url TEXT, 
                  PRIMARY KEY (match_id, url))''')
    conn.commit()
    conn.close()

init_db()

# Load m3u8 links from SQLite
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

# Save m3u8 links to SQLite
def save_m3u8_links(links):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('DELETE FROM streams')
    for match_id, urls in links.items():
        for url in urls:
            c.execute('INSERT INTO streams (match_id, url) VALUES (?, ?)', 
                     (match_id, url))
    conn.commit()
    conn.close()

# Competition mapping
COMPETITIONS = {
    'WC': {'fd_id': 2000, 'espn': 'fifa.world', 'name': 'FIFA World Cup'},
    'CL': {'fd_id': 2001, 'espn': 'uefa.champions', 'name': 'Champions League'},
    'PL': {'fd_id': 2021, 'espn': 'eng.1', 'name': 'Premier League'},
    'BL1': {'fd_id': 2002, 'espn': 'ger.1', 'name': 'Bundesliga'},
    'SA': {'fd_id': 2019, 'espn': 'ita.1', 'name': 'Serie A'},
    'PD': {'fd_id': 2014, 'espn': 'esp.1', 'name': 'La Liga'},
    'FL1': {'fd_id': 2015, 'espn': 'fra.1', 'name': 'Ligue 1'},
    'DED': {'fd_id': 2003, 'espn': 'ned.1', 'name': 'Eredivisie'},
    'ELC': {'fd_id': 2016, 'espn': 'eng.2', 'name': 'Championship'},
    'CLI': {'fd_id': 2152, 'espn': 'conmebol.libertadores', 'name': 'Copa Libertadores'}
}

FOOTBALL_DATA_API_KEY = os.environ.get('FOOTBALL_DATA_API_KEY', '214ac19439794667865a917ad93d187c')

# Cache for API responses
cache = {}
CACHE_DURATION = 300  # 5 minutes

def get_cached(key):
    """Get cached data if not expired."""
    if key in cache:
        data, timestamp = cache[key]
        if time.time() - timestamp < CACHE_DURATION:
            return data
    return None

def set_cache(key, data):
    """Set cached data with timestamp."""
    cache[key] = (data, time.time())

def fetch_competitions():
    """Fetch competitions from football-data.org and return as list."""
    url = 'https://api.football-data.org/v4/competitions'
    headers = {'X-Auth-Token': FOOTBALL_DATA_API_KEY}
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        competitions = []
        for comp in data.get('competitions', []):
            code = comp.get('code')
            if code in COMPETITIONS:
                competitions.append({
                    'code': code,
                    'name': COMPETITIONS[code]['name'],
                    'emblem': comp.get('emblem'),
                    'season': comp.get('currentSeason', {}).get('year'),
                    'fd_id': COMPETITIONS[code]['fd_id'],
                    'espn_slug': COMPETITIONS[code]['espn']
                })
        
        logger.info(f"Fetched {len(competitions)} competitions")
        return competitions
    except Exception as e:
        logger.error(f"Error fetching competitions: {e}")
        # Return fallback data as list
        return [{
            'code': code,
            'name': info['name'],
            'emblem': None,
            'season': None,
            'fd_id': info['fd_id'],
            'espn_slug': info['espn']
        } for code, info in COMPETITIONS.items()]

def fetch_espn_matches(slug, date=None):
    """Fetch matches from ESPN API for a specific date."""
    url = f'https://site.api.espn.com/apis/site/v2/sports/soccer/{slug}/scoreboard'
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        matches = []
        events = data.get('events', [])
        
        # Use provided date or today
        if date:
            try:
                target_date = datetime.strptime(date, '%Y-%m-%d').date()
            except:
                target_date = datetime.now().date()
        else:
            target_date = datetime.now().date()
        
        for event in events:
            # Get the event date
            date_str = event.get('date')
            if not date_str:
                continue
                
            try:
                event_date = datetime.fromisoformat(date_str.replace('Z', '+00:00')).date()
                if event_date != target_date:
                    continue
            except:
                pass
            
            status = event.get('status', {})
            type_val = status.get('type', {})
            state = type_val.get('state', '')
            
            competitions = event.get('competitions', [])
            if not competitions:
                continue
                
            comp = competitions[0]
            competitors = comp.get('competitors', [])
            if len(competitors) < 2:
                continue
                
            home = competitors[0]
            away = competitors[1]
            
            if state == 'in':
                match_status = 'LIVE'
            elif state == 'post':
                match_status = 'FINISHED'
            else:
                match_status = 'SCHEDULED'
            
            home_score = home.get('score')
            away_score = away.get('score')
            minute = status.get('displayClock')
            
            kickoff = ''
            if date_str:
                try:
                    dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    kickoff = dt.strftime('%H:%M UTC')
                except:
                    kickoff = date_str
            
            match_id = event.get('id', '')
            
            matches.append({
                'match_id': str(match_id) if match_id else f"espn_{event.get('id', '')}",
                'home_team': home.get('team', {}).get('displayName', 'Home'),
                'away_team': away.get('team', {}).get('displayName', 'Away'),
                'home_crest': home.get('team', {}).get('logo'),
                'away_crest': away.get('team', {}).get('logo'),
                'home_score': home_score if home_score is not None else '-',
                'away_score': away_score if away_score is not None else '-',
                'status': match_status,
                'minute': minute,
                'kickoff': kickoff
            })
        
        return matches
    except Exception as e:
        logger.error(f"Error fetching ESPN matches: {e}")
        return None

def fetch_fd_matches(fd_id, date=None):
    """Fetch matches from football-data.org API for a specific date."""
    if date:
        date_str = date
    else:
        date_str = datetime.now().strftime('%Y-%m-%d')
    
    url = f'https://api.football-data.org/v4/competitions/{fd_id}/matches'
    params = {'dateFrom': date_str, 'dateTo': date_str}
    headers = {'X-Auth-Token': FOOTBALL_DATA_API_KEY}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        matches = []
        for match in data.get('matches', []):
            home = match.get('homeTeam', {})
            away = match.get('awayTeam', {})
            
            status = match.get('status', '')
            if status in ['IN_PLAY', 'PAUSED']:
                match_status = 'LIVE'
            elif status == 'FINISHED':
                match_status = 'FINISHED'
            else:
                match_status = 'SCHEDULED'
            
            score = match.get('score', {})
            home_score = score.get('fullTime', {}).get('home')
            away_score = score.get('fullTime', {}).get('away')
            
            if home_score is None:
                home_score = '-'
            if away_score is None:
                away_score = '-'
            
            kickoff = ''
            utc_date = match.get('utcDate')
            if utc_date:
                try:
                    dt = datetime.fromisoformat(utc_date.replace('Z', '+00:00'))
                    kickoff = dt.strftime('%H:%M UTC')
                except:
                    kickoff = utc_date
            
            match_id = match.get('id', '')
            
            matches.append({
                'match_id': str(match_id) if match_id else f"fd_{match.get('id', '')}",
                'home_team': home.get('name', 'Home'),
                'away_team': away.get('name', 'Away'),
                'home_crest': home.get('crest'),
                'away_crest': away.get('crest'),
                'home_score': home_score,
                'away_score': away_score,
                'status': match_status,
                'minute': None,
                'kickoff': kickoff
            })
        
        return matches
    except Exception as e:
        logger.error(f"Error fetching FD matches: {e}")
        return None

def get_matches_for_competition(code, date=None):
    """Get matches for a competition from ESPN (primary) or FD (fallback)."""
    if code not in COMPETITIONS:
        return None, None
    
    comp_info = COMPETITIONS[code]
    espn_slug = comp_info['espn']
    fd_id = comp_info['fd_id']
    
    # Try ESPN first
    matches_data = fetch_espn_matches(espn_slug, date)
    data_source = 'ESPN'
    
    # If ESPN returns 0 matches or fails, fallback to FD
    if matches_data is None or len(matches_data) == 0:
        matches_data = fetch_fd_matches(fd_id, date)
        data_source = 'football-data.org'
    
    # Load m3u8 links and attach to matches
    m3u8_links = load_m3u8_links()
    if matches_data:
        for match in matches_data:
            match_id = match.get('match_id', '')
            match['streams'] = m3u8_links.get(match_id, [])
    
    return matches_data, data_source

# HTML Templates (keeping the same as before but with minor updates)
INDEX_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>LiveKick Admin</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; background: #f5f7fa; color: #333; padding: 20px; }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 { color: #1a73e8; margin-bottom: 30px; font-weight: 600; font-size: 2rem; }
        .today-date { color: #666; margin-bottom: 24px; font-size: 1.1rem; }
        .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(250px, 1fr)); gap: 20px; }
        .card { background: white; border-radius: 12px; padding: 24px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); cursor: pointer; transition: transform 0.2s, box-shadow 0.2s; text-decoration: none; color: #333; display: flex; flex-direction: column; align-items: center; text-align: center; }
        .card:hover { transform: translateY(-4px); box-shadow: 0 4px 16px rgba(0,0,0,0.12); }
        .card img { width: 64px; height: 64px; object-fit: contain; margin-bottom: 12px; }
        .card h3 { font-size: 1.1rem; font-weight: 600; margin-bottom: 4px; }
        .card .season { color: #777; font-size: 0.9rem; }
        .api-info { margin-top: 40px; padding: 20px; background: white; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); }
        .api-info h3 { color: #1a73e8; margin-bottom: 12px; }
        .api-info .endpoint { margin: 8px 0; padding: 8px 12px; background: #f5f7fa; border-radius: 6px; font-family: monospace; }
        @media (max-width: 768px) { .grid { grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); } }
    </style>
</head>
<body>
    <div class="container">
        <h1>🏆 LiveKick Admin</h1>
        <div class="today-date">📅 {{ today.strftime('%A, %B %d, %Y') }}</div>
        <div class="grid">
            {% for comp in competitions %}
            <a href="{{ url_for('matches_web', code=comp.code) }}" class="card">
                {% if comp.emblem %}
                <img src="{{ comp.emblem }}" alt="{{ comp.name }}">
                {% else %}
                <div style="width:64px;height:64px;background:#e8eaed;border-radius:50%;margin-bottom:12px;"></div>
                {% endif %}
                <h3>{{ comp.name }}</h3>
                <div class="season">{{ comp.season or 'Current Season' }}</div>
            </a>
            {% endfor %}
        </div>
        <div class="api-info">
            <h3>📱 API Endpoints for LiveKick App</h3>
            <div class="endpoint">GET /api/competitions - List all competitions (returns array)</div>
            <div class="endpoint">GET /api/matches/&lt;code&gt; - Get today's matches for a competition</div>
            <div class="endpoint">GET /api/matches/&lt;code&gt;?date=YYYY-MM-DD - Get matches for specific date</div>
            <div class="endpoint">GET /api/matches/live - Get all live matches</div>
            <div class="endpoint">GET /api/m3u8/&lt;match_id&gt; - Get all m3u8 links for a match</div>
        </div>
    </div>
</body>
</html>
'''

MATCHES_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>LiveKick Admin - {{ competition.name }}</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; background: #f5f7fa; color: #333; padding: 20px; }
        .container { max-width: 900px; margin: 0 auto; }
        .header { display: flex; align-items: center; gap: 16px; margin-bottom: 8px; flex-wrap: wrap; }
        .header h1 { color: #1a73e8; font-weight: 600; font-size: 1.8rem; }
        .sub-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 24px; flex-wrap: wrap; gap: 12px; }
        .back-btn { display: inline-block; padding: 8px 16px; background: #1a73e8; color: white; border: none; border-radius: 8px; cursor: pointer; text-decoration: none; font-size: 0.9rem; font-weight: 500; }
        .back-btn:hover { background: #1557b0; }
        .today-date { color: #666; font-size: 0.95rem; }
        .match-list { display: flex; flex-direction: column; gap: 12px; }
        .match-item { background: white; border-radius: 12px; padding: 16px 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); display: flex; flex-direction: column; gap: 12px; }
        .match-row { display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 12px; }
        .match-teams { display: flex; align-items: center; gap: 12px; flex: 1; min-width: 200px; }
        .match-teams img { width: 32px; height: 32px; object-fit: contain; }
        .team-name { font-weight: 500; }
        .match-info { display: flex; align-items: center; gap: 16px; flex-wrap: wrap; }
        .badge { padding: 4px 12px; border-radius: 20px; font-size: 0.75rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; }
        .badge-live { background: #ff1744; color: white; animation: pulse 1.5s infinite; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.6; } }
        .badge-ft { background: #e8eaed; color: #555; }
        .score { font-weight: 700; font-size: 1.1rem; min-width: 60px; text-align: center; }
        .time { color: #666; font-size: 0.9rem; min-width: 80px; }
        .minute { color: #1a73e8; font-weight: 600; font-size: 0.85rem; }
        .source-badge { font-size: 0.7rem; color: #999; margin-top: 8px; text-align: center; }
        .no-matches { text-align: center; padding: 60px 20px; background: white; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); }
        .no-matches .emoji { font-size: 4rem; margin-bottom: 16px; }
        .no-matches p { color: #777; font-size: 1.2rem; }
        .no-matches .date { color: #999; font-size: 1rem; margin-top: 8px; }
        .m3u8-section { margin-top: 8px; padding-top: 12px; border-top: 1px solid #e8eaed; }
        .m3u8-header { display: flex; align-items: center; gap: 12px; margin-bottom: 8px; flex-wrap: wrap; }
        .m3u8-header label { font-size: 0.85rem; color: #666; font-weight: 600; }
        .m3u8-input-row { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
        .m3u8-input-row input { flex: 1; min-width: 200px; padding: 8px 12px; border: 1px solid #ddd; border-radius: 6px; font-size: 0.9rem; }
        .m3u8-input-row input:focus { outline: none; border-color: #1a73e8; box-shadow: 0 0 0 2px rgba(26,115,232,0.1); }
        .m3u8-list { display: flex; flex-direction: column; gap: 6px; margin-top: 8px; }
        .m3u8-item { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 6px 12px; background: #f5f7fa; border-radius: 6px; font-family: monospace; font-size: 0.85rem; word-break: break-all; }
        .m3u8-item .url { flex: 1; min-width: 150px; }
        .m3u8-item .remove-btn { background: none; border: none; color: #ff1744; cursor: pointer; font-size: 1.2rem; padding: 0 4px; }
        .m3u8-item .remove-btn:hover { color: #d50000; }
        .btn { padding: 8px 16px; border: none; border-radius: 6px; font-weight: 500; cursor: pointer; font-size: 0.85rem; transition: background 0.2s; white-space: nowrap; }
        .btn-primary { background: #1a73e8; color: white; }
        .btn-primary:hover { background: #1557b0; }
        .btn-danger { background: #ff1744; color: white; }
        .btn-danger:hover { background: #d50000; }
        .btn-sm { padding: 4px 12px; font-size: 0.75rem; }
        .stream-count { font-size: 0.8rem; color: #1a73e8; font-weight: 600; }
        .no-streams { color: #999; font-size: 0.85rem; font-style: italic; }
        @media (max-width: 768px) {
            .match-row { flex-direction: column; align-items: stretch; }
            .match-teams { justify-content: center; }
            .match-info { justify-content: center; }
            .m3u8-input-row { flex-direction: column; }
            .m3u8-input-row input { width: 100%; }
            .m3u8-item { flex-wrap: wrap; }
        }
    </style>
    <script>
        function addStream(matchId) {
            const input = document.getElementById('input-' + matchId);
            const url = input.value.trim();
            if (!url) {
                alert('Please enter a valid stream URL');
                return;
            }
            if (!url.startsWith('http://') && !url.startsWith('https://')) {
                alert('URL must start with http:// or https://');
                return;
            }
            fetch('/api/m3u8/' + matchId, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ url: url })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    location.reload();
                } else {
                    alert('Error: ' + data.error);
                }
            })
            .catch(error => {
                alert('Error adding stream');
                console.error('Error:', error);
            });
        }
        function removeStream(matchId, url) {
            if (!confirm('Remove this stream link?')) return;
            fetch('/api/m3u8/' + matchId, {
                method: 'DELETE',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ url: url })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    location.reload();
                } else {
                    alert('Error: ' + data.error);
                }
            })
            .catch(error => {
                alert('Error removing stream');
                console.error('Error:', error);
            });
        }
        function clearAllStreams(matchId) {
            if (!confirm('Remove ALL stream links for this match?')) return;
            fetch('/api/m3u8/' + matchId + '/clear', { method: 'DELETE' })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    location.reload();
                } else {
                    alert('Error: ' + data.error);
                }
            })
            .catch(error => {
                alert('Error clearing streams');
                console.error('Error:', error);
            });
        }
    </script>
</head>
<body>
    <div class="container">
        <div class="header">
            <a href="{{ url_for('index') }}" class="back-btn">← Back</a>
            <h1>🏆 {{ competition.name }}</h1>
        </div>
        <div class="sub-header">
            <div class="today-date">📅 {{ today.strftime('%A, %B %d, %Y') }}</div>
        </div>
        
        {% if matches %}
        <div class="match-list">
            {% for match in matches %}
            <div class="match-item">
                <div class="match-row">
                    <div class="match-teams">
                        {% if match.home_crest %}
                        <img src="{{ match.home_crest }}" alt="{{ match.home_team }}">
                        {% endif %}
                        <span class="team-name">{{ match.home_team }}</span>
                        <span style="color:#999;">vs</span>
                        <span class="team-name">{{ match.away_team }}</span>
                        {% if match.away_crest %}
                        <img src="{{ match.away_crest }}" alt="{{ match.away_team }}">
                        {% endif %}
                    </div>
                    <div class="match-info">
                        {% if match.status == 'LIVE' %}
                            <span class="badge badge-live">LIVE</span>
                            <span class="score">{{ match.home_score }} - {{ match.away_score }}</span>
                            {% if match.minute %}
                            <span class="minute">{{ match.minute }}'</span>
                            {% endif %}
                        {% elif match.status == 'FINISHED' %}
                            <span class="badge badge-ft">FT</span>
                            <span class="score">{{ match.home_score }} - {{ match.away_score }}</span>
                        {% else %}
                            <span class="time">{{ match.kickoff }}</span>
                        {% endif %}
                    </div>
                </div>
                
                <div class="m3u8-section">
                    <div class="m3u8-header">
                        <label>📺 Streams:</label>
                        {% if match.streams %}
                            <span class="stream-count">{{ match.streams|length }} streams available</span>
                            <button class="btn btn-danger btn-sm" onclick="clearAllStreams('{{ match.match_id }}')">Clear All</button>
                        {% else %}
                            <span class="no-streams">No streams added</span>
                        {% endif %}
                    </div>
                    
                    {% if match.streams %}
                    <div class="m3u8-list">
                        {% for stream in match.streams %}
                        <div class="m3u8-item">
                            <span class="url">{{ stream }}</span>
                            <button class="remove-btn" onclick="removeStream('{{ match.match_id }}', '{{ stream }}')" title="Remove this stream">✕</button>
                        </div>
                        {% endfor %}
                    </div>
                    {% endif %}
                    
                    <div class="m3u8-input-row">
                        <input id="input-{{ match.match_id }}" type="text" placeholder="Paste stream URL here..." />
                        <button class="btn btn-primary" onclick="addStream('{{ match.match_id }}')">Add Stream</button>
                    </div>
                </div>
            </div>
            {% endfor %}
        </div>
        <div class="source-badge">Data source: {{ data_source }}</div>
        {% else %}
        <div class="no-matches">
            <div class="emoji">📅</div>
            <p>No matches scheduled for today</p>
            <div class="date">{{ today.strftime('%A, %B %d, %Y') }}</div>
        </div>
        {% endif %}
    </div>
</body>
</html>
'''

# Web Routes (Admin Panel)
@app.route('/')
def index():
    competitions = fetch_competitions()
    today = datetime.now()
    return render_template_string(INDEX_TEMPLATE, competitions=competitions, today=today)

@app.route('/matches/<code>')
def matches_web(code):
    if code not in COMPETITIONS:
        return "Competition not found", 404
    
    comp_info = COMPETITIONS[code]
    comp_name = comp_info['name']
    
    matches_data, data_source = get_matches_for_competition(code)
    
    competitions = fetch_competitions()
    comp_display = next((c for c in competitions if c['code'] == code), 
                       {'name': comp_name, 'emblem': None, 'season': None})
    
    today = datetime.now()
    
    return render_template_string(
        MATCHES_TEMPLATE, 
        matches=matches_data or [], 
        competition=comp_display,
        data_source=data_source,
        today=today
    )

# API Routes for LiveKick Android App
@app.route('/api/competitions', methods=['GET'])
def api_competitions():
    """API endpoint to get all competitions as an array."""
    competitions = fetch_competitions()
    
    # Log the response for debugging
    logger.info(f"Returning {len(competitions)} competitions")
    
    # Ensure we're returning a proper JSON response with an array
    response = jsonify({
        'success': True,
        'data': competitions
    })
    
    # Log the actual response content for debugging
    logger.info(f"Response: {response.get_data(as_text=True)}")
    
    return response

@app.route('/api/matches/<code>', methods=['GET'])
def api_matches(code):
    """API endpoint to get matches for a competition."""
    if code not in COMPETITIONS:
        return jsonify({
            'success': False,
            'error': 'Competition not found'
        }), 404
    
    date = request.args.get('date')
    matches_data, data_source = get_matches_for_competition(code, date)
    
    if matches_data is None:
        return jsonify({
            'success': False,
            'error': 'Failed to fetch matches'
        }), 500
    
    comp_info = COMPETITIONS[code]
    
    response_data = {
        'success': True,
        'data': {
            'competition': {
                'code': code,
                'name': comp_info['name'],
                'fd_id': comp_info['fd_id'],
                'espn_slug': comp_info['espn']
            },
            'date': date or datetime.now().strftime('%Y-%m-%d'),
            'source': data_source,
            'matches': matches_data
        }
    }
    
    logger.info(f"Returning {len(matches_data)} matches for {code}")
    return jsonify(response_data)

@app.route('/api/matches/live', methods=['GET'])
def api_live_matches():
    """API endpoint to get all currently live matches across all competitions."""
    live_matches = []
    
    for code, comp_info in COMPETITIONS.items():
        matches_data, _ = get_matches_for_competition(code)
        
        if matches_data:
            for match in matches_data:
                if match.get('status') == 'LIVE':
                    match['competition_code'] = code
                    match['competition_name'] = comp_info['name']
                    live_matches.append(match)
    
    logger.info(f"Returning {len(live_matches)} live matches")
    
    return jsonify({
        'success': True,
        'data': {
            'count': len(live_matches),
            'matches': live_matches
        }
    })

@app.route('/api/m3u8/<match_id>', methods=['GET', 'POST', 'DELETE'])
def api_m3u8(match_id):
    """API endpoint to manage m3u8 links for a match."""
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
            return jsonify({
                'success': False,
                'error': 'Missing url parameter'
            }), 400
        
        url = data['url'].strip()
        if not url:
            return jsonify({
                'success': False,
                'error': 'URL cannot be empty'
            }), 400
        
        if not url.startswith(('http://', 'https://')):
            return jsonify({
                'success': False,
                'error': 'Invalid URL format. Must start with http:// or https://'
            }), 400
        
        streams = m3u8_links.get(match_id, [])
        
        if url in streams:
            return jsonify({
                'success': False,
                'error': 'This stream URL already exists for this match'
            }), 400
        
        streams.append(url)
        m3u8_links[match_id] = streams
        save_m3u8_links(m3u8_links)
        
        return jsonify({
            'success': True,
            'message': 'Stream added successfully',
            'data': {
                'match_id': match_id,
                'streams': streams,
                'count': len(streams)
            }
        })
    
    elif request.method == 'DELETE':
        data = request.get_json()
        if not data or 'url' not in data:
            return jsonify({
                'success': False,
                'error': 'Missing url parameter'
            }), 400
        
        url = data['url'].strip()
        streams = m3u8_links.get(match_id, [])
        
        if url not in streams:
            return jsonify({
                'success': False,
                'error': 'Stream URL not found for this match'
            }), 404
        
        streams.remove(url)
        
        if streams:
            m3u8_links[match_id] = streams
        else:
            del m3u8_links[match_id]
        
        save_m3u8_links(m3u8_links)
        
        return jsonify({
            'success': True,
            'message': 'Stream removed successfully',
            'data': {
                'match_id': match_id,
                'streams': streams,
                'count': len(streams)
            }
        })

@app.route('/api/m3u8/<match_id>/clear', methods=['DELETE'])
def api_m3u8_clear(match_id):
    """Clear all m3u8 links for a match."""
    m3u8_links = load_m3u8_links()
    
    if match_id in m3u8_links:
        del m3u8_links[match_id]
        save_m3u8_links(m3u8_links)
        return jsonify({
            'success': True,
            'message': 'All streams cleared successfully'
        })
    else:
        return jsonify({
            'success': False,
            'error': 'No streams found for this match'
        }), 404

@app.route('/health')
def health():
    """Health check endpoint for Render."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat()
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
