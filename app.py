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
import hashlib
import subprocess
import ffmpeg

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'livekick_secret_key_2026')
CORS(app)

# Configuration
BACKEND_URL = os.getenv('BACKEND_URL', 'https://api.football-data.org/v4')
FOOTBALL_DATA_API_KEY = os.environ.get('FOOTBALL_DATA_API_KEY', '214ac19439794667865a917ad93d187c')
PORT = int(os.getenv('PORT', 5000))
DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'
REMOVAL_DELAY_MINUTES = 3
GOAL_DETECTION_INTERVAL = 2  # Check every 2 seconds
CLIP_DURATION = 30  # 30-second clips

# Use persistent disk if available
DATA_DIR = os.environ.get('DATA_DIR', '/opt/render/data')
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, 'streams.db')
CLIP_STORAGE_DIR = os.path.join(DATA_DIR, 'clips')

# Create clip storage directory
os.makedirs(CLIP_STORAGE_DIR, exist_ok=True)

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

# Fast in-memory cache
class FastCache:
    def __init__(self):
        self._cache = {}
    
    def get(self, key):
        if key in self._cache:
            data, timestamp, ttl = self._cache[key]
            if time.time() - timestamp < ttl:
                return data
            else:
                del self._cache[key]
        return None
    
    def set(self, key, data, ttl):
        self._cache[key] = (data, time.time(), ttl)
    
    def clear(self):
        self._cache.clear()
    
    def size(self):
        return len(self._cache)

cache = FastCache()

# Store for match states (for goal detection)
match_states = {}  # match_id -> {'last_score': (home, away), 'last_check': timestamp}

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

# Configure requests session with connection pooling
def get_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=20,
        pool_maxsize=20
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.headers.update({
        'User-Agent': 'LiveKick/1.0',
        'Accept': 'application/json',
        'X-Auth-Token': FOOTBALL_DATA_API_KEY
    })
    return session

session = get_session()

# Add CORS headers
@app.after_request
def add_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-Auth-Token'
    response.headers['Content-Type'] = 'application/json'
    return response

# --- GOAL DETECTION & CLIP CAPTURE ---

def detect_score_change(old_score, new_score):
    """Detect if a goal was scored."""
    if old_score is None:
        return False, None
    
    old_home, old_away = old_score
    new_home, new_away = new_score
    
    if new_home > old_home:
        return True, 'home'
    elif new_away > old_away:
        return True, 'away'
    return False, None

def capture_clip(stream_url, match_id, goal_team, current_score, timestamp):
    """
    Capture a 30-second clip from the stream using FFmpeg.
    """
    try:
        # Generate unique filename
        clip_filename = f"{match_id}_{timestamp.strftime('%Y%m%d_%H%M%S')}_{goal_team}.mp4"
        clip_path = os.path.join(CLIP_STORAGE_DIR, clip_filename)
        
        logger.info(f"Capturing clip: {clip_filename}")
        logger.info(f"Stream URL: {stream_url}")
        logger.info(f"Output path: {clip_path}")
        
        # Use ffmpeg to capture 30-second clip
        # This assumes the stream is an HLS stream (.m3u8)
        cmd = [
            'ffmpeg',
            '-i', stream_url,
            '-t', str(CLIP_DURATION),
            '-c', 'copy',
            '-f', 'mp4',
            '-y',  # Overwrite output
            clip_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"FFmpeg error: {result.stderr}")
            return None
        
        # Check if file was created
        if not os.path.exists(clip_path):
            logger.error(f"Clip file not created: {clip_path}")
            return None
        
        file_size = os.path.getsize(clip_path)
        logger.info(f"Clip captured successfully: {clip_filename} ({file_size} bytes)")
        
        # Store clip metadata
        clip_metadata = {
            'match_id': match_id,
            'goal_team': goal_team,
            'score': current_score,
            'timestamp': timestamp.isoformat(),
            'filename': clip_filename,
            'url': f"/api/clips/{clip_filename}",
            'size': file_size,
            'duration': CLIP_DURATION
        }
        
        # Save to cache
        cache_key = f"clips_{match_id}"
        clips = cache.get(cache_key) or []
        clips.append(clip_metadata)
        cache.set(cache_key, clips, 86400)  # Keep for 24 hours
        
        return clip_metadata
    except Exception as e:
        logger.error(f"Error capturing clip for match {match_id}: {e}")
        return None

def check_and_capture_goal(match_data, stream_url):
    """
    Check if a goal was scored and capture the clip.
    """
    match_id = match_data.get('match_id')
    current_home = int(match_data.get('home_score', 0))
    current_away = int(match_data.get('away_score', 0))
    current_score = (current_home, current_away)
    status = match_data.get('status')
    
    # Only check live matches
    if status != 'LIVE':
        return None
    
    # Get previous state
    state_key = match_id
    if state_key not in match_states:
        match_states[state_key] = {
            'last_score': current_score,
            'last_check': datetime.now(),
            'goals_recorded': []
        }
        return None
    
    state = match_states[state_key]
    old_score = state['last_score']
    
    # Detect goal
    goal_scored, goal_team = detect_score_change(old_score, current_score)
    
    if goal_scored and stream_url:
        logger.info(f"⚽ GOAL DETECTED! Match {match_id} - {goal_team} scored! Score: {current_score}")
        
        # Capture the clip
        clip = capture_clip(
            stream_url,
            match_id,
            goal_team,
            f"{current_home}-{current_away}",
            datetime.now()
        )
        
        # Update state
        match_states[state_key]['last_score'] = current_score
        match_states[state_key]['last_check'] = datetime.now()
        if clip:
            match_states[state_key]['goals_recorded'].append(clip)
        
        return clip
    
    # Update state if no goal
    match_states[state_key]['last_score'] = current_score
    match_states[state_key]['last_check'] = datetime.now()
    
    return None

def fetch_live_matches_data():
    """Fetch live matches data from ESPN."""
    live_matches = []
    
    for code, comp_info in COMPETITIONS.items():
        try:
            response = session.get(
                f'https://site.api.espn.com/apis/site/v2/sports/soccer/{comp_info["espn"]}/scoreboard',
                timeout=3
            )
            response.raise_for_status()
            data = response.json()
            
            for event in data.get('events', []):
                status = event.get('status', {})
                state = status.get('type', {}).get('state', '')
                
                if state == 'in':  # LIVE matches only
                    competitions = event.get('competitions', [])
                    if not competitions:
                        continue
                    
                    comp = competitions[0]
                    competitors = comp.get('competitors', [])
                    if len(competitors) < 2:
                        continue
                    
                    home = competitors[0]
                    away = competitors[1]
                    
                    match_id = event.get('id', '')
                    date_str = event.get('date', '')
                    kickoff = ''
                    if date_str:
                        try:
                            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                            kickoff = dt.strftime('%H:%M UTC')
                        except:
                            kickoff = date_str
                    
                    live_match = {
                        'match_id': str(match_id),
                        'home_team': home.get('team', {}).get('displayName', 'Home'),
                        'away_team': away.get('team', {}).get('displayName', 'Away'),
                        'home_crest': home.get('team', {}).get('logo'),
                        'away_crest': away.get('team', {}).get('logo'),
                        'home_score': home.get('score', '0'),
                        'away_score': away.get('score', '0'),
                        'status': 'LIVE',
                        'minute': status.get('displayClock'),
                        'kickoff': kickoff,
                        'competition_code': code,
                        'competition_name': comp_info['name']
                    }
                    live_matches.append(live_match)
        except Exception as e:
            logger.error(f"Error fetching live matches for {code}: {e}")
            continue
    
    return {'matches': live_matches}

# --- Background Goal Detection ---

def goal_detection_loop():
    """Background thread that checks for goals in live matches."""
    while True:
        try:
            # Get all live matches
            live_data = fetch_live_matches_data()
            
            if live_data and live_data.get('matches'):
                for match in live_data.get('matches', []):
                    if match.get('status') == 'LIVE':
                        match_id = match.get('match_id')
                        # Get stream URL for this match
                        m3u8_links = load_m3u8_links()
                        streams = m3u8_links.get(match_id, [])
                        
                        if streams:
                            # Use the first stream URL
                            stream_url = streams[0]
                            check_and_capture_goal(match, stream_url)
            
            time.sleep(GOAL_DETECTION_INTERVAL)
        except Exception as e:
            logger.error(f"Error in goal detection loop: {e}")
            time.sleep(GOAL_DETECTION_INTERVAL)

# --- Background Live Updates ---

def fetch_and_cache_live_matches():
    """Background job to fetch and cache live matches with auto-removal."""
    try:
        logger.info("Fetching live matches in background...")
        live_matches = []
        m3u8_links = load_m3u8_links()
        current_time = datetime.now()
        
        for code, comp_info in COMPETITIONS.items():
            try:
                response = session.get(
                    f'https://site.api.espn.com/apis/site/v2/sports/soccer/{comp_info["espn"]}/scoreboard',
                    timeout=3
                )
                response.raise_for_status()
                data = response.json()
                
                for event in data.get('events', []):
                    status = event.get('status', {})
                    state = status.get('type', {}).get('state', '')
                    
                    # Only process live or recently finished matches
                    if state in ['in', 'post']:
                        competitions = event.get('competitions', [])
                        if not competitions:
                            continue
                        
                        comp = competitions[0]
                        competitors = comp.get('competitors', [])
                        if len(competitors) < 2:
                            continue
                        
                        home = competitors[0]
                        away = competitors[1]
                        
                        date_str = event.get('date', '')
                        kickoff = ''
                        if date_str:
                            try:
                                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                                kickoff = dt.strftime('%H:%M UTC')
                            except:
                                kickoff = date_str
                        
                        match_id = event.get('id', '')
                        
                        # Determine if match is finished
                        if state == 'post':
                            match_status = 'FINISHED'
                            minute = 'FT'
                        else:
                            match_status = 'LIVE'
                            minute = status.get('displayClock')
                        
                        live_match = {
                            'match_id': str(match_id),
                            'home_team': home.get('team', {}).get('displayName', 'Home'),
                            'away_team': away.get('team', {}).get('displayName', 'Away'),
                            'home_crest': home.get('team', {}).get('logo'),
                            'away_crest': away.get('team', {}).get('logo'),
                            'home_score': home.get('score', '0'),
                            'away_score': away.get('score', '0'),
                            'status': match_status,
                            'minute': minute,
                            'kickoff': kickoff,
                            'competition_code': code,
                            'competition_name': comp_info['name']
                        }
                        
                        # Track finished time
                        if match_status == 'FINISHED':
                            live_match['finished_at'] = current_time.isoformat()
                        
                        # Add streams
                        match_id_str = str(match_id)
                        live_match['streams'] = m3u8_links.get(match_id_str, [])
                        
                        # Add clips if available
                        clip_key = f"clips_{match_id_str}"
                        live_match['clips'] = cache.get(clip_key) or []
                        
                        live_matches.append(live_match)
            except Exception as e:
                logger.error(f"Error fetching live matches for {code}: {e}")
                continue
        
        # Filter out matches that finished more than 3 minutes ago
        filtered_matches = []
        for match in live_matches:
            if match.get('status') == 'FINISHED':
                finished_at = match.get('finished_at')
                if finished_at:
                    try:
                        if isinstance(finished_at, str):
                            finished_time = datetime.fromisoformat(finished_at.replace('Z', '+00:00'))
                        else:
                            finished_time = finished_at
                        time_diff = (current_time - finished_time).total_seconds() / 60
                        if time_diff <= REMOVAL_DELAY_MINUTES:
                            # Still within 3 minutes - keep it
                            match['removing_in'] = round(REMOVAL_DELAY_MINUTES - time_diff, 1)
                            filtered_matches.append(match)
                    except:
                        filtered_matches.append(match)
                else:
                    filtered_matches.append(match)
            else:
                # Not finished - keep it
                filtered_matches.append(match)
        
        # Cache live matches with 5 second TTL
        cache.set('live_matches_cached', {
            'success': True,
            'data': {
                'count': len(filtered_matches),
                'matches': filtered_matches
            }
        }, 5)
        
        logger.info(f"Cached {len(filtered_matches)} live matches")
    except Exception as e:
        logger.error(f"Error in background live fetch: {e}")

def start_background_updater():
    """Start background thread to update live matches every 5 seconds."""
    def run_updater():
        while True:
            try:
                fetch_and_cache_live_matches()
                time.sleep(5)
            except Exception as e:
                logger.error(f"Background updater error: {e}")
                time.sleep(5)
    
    thread = threading.Thread(target=run_updater, daemon=True)
    thread.start()
    logger.info("Background live match updater started (updates every 5 seconds)")

# --- Daily Fixture Fetch ---

def fetch_daily_fixtures():
    """Fetch all fixtures for today and cache them."""
    logger.info("Starting daily fixture fetch...")
    current_time = datetime.now()
    
    for code, comp_info in COMPETITIONS.items():
        try:
            # Fetch matches for today
            matches = fetch_espn_matches(comp_info['espn'])
            
            if not matches or len(matches) == 0:
                matches = fetch_fd_matches(comp_info['fd_id'])
            
            # Add streams
            m3u8_links = load_m3u8_links()
            if matches:
                for match in matches:
                    match_id = match.get('match_id', '')
                    match['streams'] = m3u8_links.get(match_id, [])
                    
                    # Add clips if available
                    clip_key = f"clips_{match_id}"
                    match['clips'] = cache.get(clip_key) or []
                    
                    # Track finished time
                    if match.get('status') == 'FINISHED':
                        match['finished_at'] = current_time.isoformat()
            
            # Cache with 24-hour TTL
            cache_key = f"matches_{code}_{datetime.now().strftime('%Y-%m-%d')}"
            response_data = {
                'success': True,
                'data': {
                    'competition': {
                        'code': code,
                        'name': comp_info['name'],
                        'fd_id': comp_info['fd_id'],
                        'espn_slug': comp_info['espn']
                    },
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'source': 'football-data.org (cached daily)',
                    'matches': matches or []
                }
            }
            cache.set(cache_key, response_data, 86400)  # 24 hours
            logger.info(f"Cached fixtures for {comp_info['name']}")
        except Exception as e:
            logger.error(f"Error fetching fixtures for {code}: {e}")
    
    logger.info("Daily fixture fetch complete")

def start_scheduler():
    """Start scheduler for daily fixture fetch at 1 AM EAT."""
    def schedule_loop():
        while True:
            now = datetime.now()
            # Target time: 1 AM EAT = 10 PM UTC (22:00)
            target_hour = 22
            target_minute = 0
            
            # Calculate when to run next
            next_run = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
            if now > next_run:
                next_run += timedelta(days=1)
            
            wait_seconds = (next_run - now).total_seconds()
            logger.info(f"Next daily fixture fetch at {next_run.strftime('%Y-%m-%d %H:%M:%S')} (in {wait_seconds/3600:.1f} hours)")
            
            time.sleep(wait_seconds)
            fetch_daily_fixtures()
    
    thread = threading.Thread(target=schedule_loop, daemon=True)
    thread.start()
    logger.info("Daily fixture scheduler started (1 AM EAT)")

# --- Cleanup Old Clips ---

def cleanup_old_clips():
    """Delete clips older than 24 hours."""
    while True:
        try:
            now = time.time()
            deleted_count = 0
            for filename in os.listdir(CLIP_STORAGE_DIR):
                filepath = os.path.join(CLIP_STORAGE_DIR, filename)
                if os.path.getmtime(filepath) < now - 86400:  # 24 hours
                    os.remove(filepath)
                    deleted_count += 1
                    logger.info(f"Removed old clip: {filename}")
            
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} old clips")
            
            time.sleep(3600)  # Check every hour
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
            time.sleep(3600)

def start_cleanup():
    thread = threading.Thread(target=cleanup_old_clips, daemon=True)
    thread.start()
    logger.info("Clip cleanup started (24-hour retention)")

# --- Data Fetching Functions ---

def fetch_competitions():
    """Fetch competitions from football-data.org."""
    cache_key = 'competitions_list'
    cached = cache.get(cache_key)
    if cached:
        return cached
    
    try:
        response = session.get(
            f'{BACKEND_URL}/competitions',
            timeout=15
        )
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
        
        cache.set(cache_key, competitions, 86400)  # 24 hours
        logger.info(f"Fetched {len(competitions)} competitions")
        return competitions
    except Exception as e:
        logger.error(f"Error fetching competitions: {e}")
        return [{
            'code': code,
            'name': info['name'],
            'emblem': None,
            'season': None,
            'fd_id': info['fd_id'],
            'espn_slug': info['espn']
        } for code, info in COMPETITIONS.items()]

def fetch_espn_matches(slug, date=None):
    """Fetch matches from ESPN API."""
    try:
        response = session.get(
            f'https://site.api.espn.com/apis/site/v2/sports/soccer/{slug}/scoreboard',
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        
        matches = []
        current_time = datetime.now()
        
        if date:
            try:
                target_date = datetime.strptime(date, '%Y-%m-%d').date()
            except:
                target_date = datetime.now().date()
        else:
            target_date = datetime.now().date()
        
        for event in data.get('events', []):
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
            type_info = status.get('type', {})
            state = type_info.get('state', '')
            
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
            
            match = {
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
            }
            
            if match_status == 'FINISHED':
                match['finished_at'] = current_time.isoformat()
            
            matches.append(match)
        
        return matches
    except Exception as e:
        logger.error(f"Error fetching ESPN matches: {e}")
        return None

def fetch_fd_matches(fd_id, date=None):
    """Fetch matches from football-data.org API."""
    if date:
        date_str = date
    else:
        date_str = datetime.now().strftime('%Y-%m-%d')
    
    try:
        response = session.get(
            f'{BACKEND_URL}/competitions/{fd_id}/matches',
            params={'dateFrom': date_str, 'dateTo': date_str},
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        
        matches = []
        current_time = datetime.now()
        
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
            
            match_obj = {
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
            }
            
            if match_status == 'FINISHED':
                match_obj['finished_at'] = current_time.isoformat()
            
            matches.append(match_obj)
        
        return matches
    except Exception as e:
        logger.error(f"Error fetching FD matches: {e}")
        return None

def process_match_with_removal(matches, m3u8_links=None):
    """Process matches and remove finished ones after 3 minutes."""
    if m3u8_links is None:
        m3u8_links = load_m3u8_links()
    
    current_time = datetime.now()
    processed_matches = []
    removed_matches = []
    
    for match in matches:
        match_id = match.get('match_id', '')
        status = match.get('status', '')
        
        # Add streams if available
        if match_id:
            match['streams'] = m3u8_links.get(match_id, [])
        
        # Add clips if available
        clip_key = f"clips_{match_id}"
        match['clips'] = cache.get(clip_key) or []
        
        # Check if match is finished
        if status == 'FINISHED':
            finished_at = match.get('finished_at')
            
            if finished_at:
                try:
                    if isinstance(finished_at, str):
                        finished_time = datetime.fromisoformat(finished_at.replace('Z', '+00:00'))
                    else:
                        finished_time = finished_at
                    
                    time_diff = (current_time - finished_time).total_seconds() / 60
                    
                    if time_diff > REMOVAL_DELAY_MINUTES:
                        removed_matches.append(match)
                        logger.info(f"Removed finished match {match_id}")
                        continue
                    else:
                        match['removing_in'] = round(REMOVAL_DELAY_MINUTES - time_diff, 1)
                        processed_matches.append(match)
                except Exception as e:
                    logger.error(f"Error processing finished time for match {match_id}: {e}")
                    processed_matches.append(match)
            else:
                processed_matches.append(match)
        else:
            processed_matches.append(match)
    
    return processed_matches, removed_matches

# --- API ENDPOINTS ---

@app.route('/api/clips/<filename>', methods=['GET'])
def get_clip(filename):
    """Serve a clip file."""
    clip_path = os.path.join(CLIP_STORAGE_DIR, filename)
    if os.path.exists(clip_path):
        return send_file(clip_path, as_attachment=False, mimetype='video/mp4')
    return jsonify({'error': 'Clip not found'}), 404

@app.route('/api/clips/<match_id>', methods=['GET'])
def get_match_clips(match_id):
    """Get all clips for a match."""
    clip_key = f"clips_{match_id}"
    clips = cache.get(clip_key) or []
    return jsonify({
        'success': True,
        'data': {
            'match_id': match_id,
            'clips': clips,
            'count': len(clips)
        }
    })

@app.route('/api/competitions', methods=['GET'])
def api_competitions():
    """Get all competitions as an array."""
    competitions = fetch_competitions()
    logger.info(f"Returning {len(competitions)} competitions")
    return jsonify({'success': True, 'data': competitions})

@app.route('/api/matches/<code>', methods=['GET'])
def api_matches(code):
    """Get matches for a competition with auto-removal and clips."""
    if code not in COMPETITIONS:
        return jsonify({'success': False, 'error': 'Competition not found'}), 404
    
    date_str = request.args.get('date') or datetime.now().strftime('%Y-%m-%d')
    force_refresh = request.args.get('refresh') == 'true'
    cache_key = f"matches_{code}_{date_str}"
    
    # Check cache first
    if not force_refresh:
        cached_data = cache.get(cache_key)
        if cached_data:
            # Process with auto-removal and add clips
            m3u8_links = load_m3u8_links()
            processed_matches, removed = process_match_with_removal(
                cached_data['data']['matches'].copy(),
                m3u8_links
            )
            
            response_data = {
                'success': True,
                'data': {
                    'competition': cached_data['data']['competition'],
                    'date': cached_data['data']['date'],
                    'source': cached_data['data']['source'],
                    'matches': processed_matches,
                    'removed_count': len(removed)
                }
            }
            logger.info(f"Cache hit for {cache_key} - removed {len(removed)} finished matches")
            return jsonify(response_data)
    
    # Fetch fresh data
    comp_info = COMPETITIONS[code]
    matches = fetch_espn_matches(comp_info['espn'], date_str)
    source = 'ESPN (live)'
    
    if not matches or len(matches) == 0:
        matches = fetch_fd_matches(comp_info['fd_id'], date_str)
        source = 'football-data.org (cached)'
    
    # Process with auto-removal
    m3u8_links = load_m3u8_links()
    processed_matches, removed = process_match_with_removal(matches, m3u8_links)
    
    response_data = {
        'success': True,
        'data': {
            'competition': {
                'code': code,
                'name': comp_info['name'],
                'fd_id': comp_info['fd_id'],
                'espn_slug': comp_info['espn']
            },
            'date': date_str,
            'source': source,
            'matches': processed_matches,
            'removed_count': len(removed)
        }
    }
    
    # Cache for 5 minutes
    cache.set(cache_key, response_data, 300)
    logger.info(f"Cached {cache_key} for 5 minutes - removed {len(removed)} finished matches")
    
    return jsonify(response_data)

@app.route('/api/matches/live', methods=['GET'])
def api_live_matches():
    """Get all live matches with auto-removal and clips."""
    cached_data = cache.get('live_matches_cached')
    
    if cached_data:
        # Add fresh clips
        m3u8_links = load_m3u8_links()
        current_time = datetime.now()
        
        filtered_matches = []
        for match in cached_data['data']['matches']:
            match_id = match.get('match_id', '')
            match['streams'] = m3u8_links.get(match_id, [])
            match['clips'] = cache.get(f"clips_{match_id}") or []
            
            # Check if finished and should be removed
            if match.get('status') == 'FINISHED':
                finished_at = match.get('finished_at')
                if finished_at:
                    try:
                        if isinstance(finished_at, str):
                            finished_time = datetime.fromisoformat(finished_at.replace('Z', '+00:00'))
                        else:
                            finished_time = finished_at
                        time_diff = (current_time - finished_time).total_seconds() / 60
                        
                        if time_diff > REMOVAL_DELAY_MINUTES:
                            continue
                        else:
                            match['removing_in'] = round(REMOVAL_DELAY_MINUTES - time_diff, 1)
                    except:
                        pass
            
            filtered_matches.append(match)
        
        return jsonify({
            'success': True,
            'data': {
                'count': len(filtered_matches),
                'matches': filtered_matches
            }
        })
    
    # Fallback
    return fetch_live_matches_fresh()

def fetch_live_matches_fresh():
    """Fetch live matches directly."""
    live_matches = []
    current_time = datetime.now()
    m3u8_links = load_m3u8_links()
    
    for code, comp_info in COMPETITIONS.items():
        try:
            response = session.get(
                f'https://site.api.espn.com/apis/site/v2/sports/soccer/{comp_info["espn"]}/scoreboard',
                timeout=3
            )
            response.raise_for_status()
            data = response.json()
            
            for event in data.get('events', []):
                status = event.get('status', {})
                state = status.get('type', {}).get('state', '')
                
                if state in ['in', 'post']:
                    competitions = event.get('competitions', [])
                    if not competitions:
                        continue
                    
                    comp = competitions[0]
                    competitors = comp.get('competitors', [])
                    if len(competitors) < 2:
                        continue
                    
                    home = competitors[0]
                    away = competitors[1]
                    
                    match_id = event.get('id', '')
                    
                    match_status = 'LIVE' if state == 'in' else 'FINISHED'
                    minute = status.get('displayClock') if state == 'in' else 'FT'
                    
                    live_match = {
                        'match_id': str(match_id),
                        'home_team': home.get('team', {}).get('displayName', 'Home'),
                        'away_team': away.get('team', {}).get('displayName', 'Away'),
                        'home_crest': home.get('team', {}).get('logo'),
                        'away_crest': away.get('team', {}).get('logo'),
                        'home_score': home.get('score', '0'),
                        'away_score': away.get('score', '0'),
                        'status': match_status,
                        'minute': minute,
                        'kickoff': status.get('type', {}).get('description', ''),
                        'competition_code': code,
                        'competition_name': comp_info['name'],
                        'streams': m3u8_links.get(str(match_id), []),
                        'clips': cache.get(f"clips_{match_id}") or []
                    }
                    
                    if match_status == 'FINISHED':
                        live_match['finished_at'] = current_time.isoformat()
                    
                    live_matches.append(live_match)
        except Exception as e:
            logger.error(f"Error fetching live matches for {code}: {e}")
            continue
    
    # Filter finished matches
    filtered = [m for m in live_matches if not (
        m.get('status') == 'FINISHED' and 
        m.get('finished_at') and 
        (datetime.now() - datetime.fromisoformat(m['finished_at'].replace('Z', '+00:00'))).total_seconds() / 60 > REMOVAL_DELAY_MINUTES
    )]
    
    return jsonify({
        'success': True,
        'data': {
            'count': len(filtered),
            'matches': filtered
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
        'timestamp': datetime.now().isoformat(),
        'port': PORT,
        'cache_size': cache.size(),
        'clips_stored': len(os.listdir(CLIP_STORAGE_DIR)) if os.path.exists(CLIP_STORAGE_DIR) else 0,
        'goal_detection_active': True,
        'removal_delay_minutes': REMOVAL_DELAY_MINUTES
    })

# --- Web Routes (Admin Panel) ---

@app.route('/')
def index():
    competitions = fetch_competitions()
    today = datetime.now()
    return render_template('index.html', competitions=competitions, today=today)

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
    
    return render_template(
        'matches.html', 
        matches=matches_data or [], 
        competition=comp_display,
        data_source=data_source,
        today=today,
        removal_delay=REMOVAL_DELAY_MINUTES
    )

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
            match['clips'] = cache.get(f"clips_{match_id}") or []
    
    return matches_data, data_source

@app.route('/live')
def live_matches_web():
    """Show all live matches."""
    live_matches = []
    current_time = datetime.now()
    
    for code, comp_info in COMPETITIONS.items():
        matches = fetch_espn_matches(comp_info['espn'])
        if matches:
            for match in matches:
                if match.get('status') in ['LIVE', 'FINISHED']:
                    # Check if finished and should be removed
                    if match.get('status') == 'FINISHED':
                        finished_at = match.get('finished_at')
                        if finished_at:
                            try:
                                if isinstance(finished_at, str):
                                    finished_time = datetime.fromisoformat(finished_at.replace('Z', '+00:00'))
                                else:
                                    finished_time = finished_at
                                time_diff = (current_time - finished_time).total_seconds() / 60
                                if time_diff > REMOVAL_DELAY_MINUTES:
                                    continue
                            except:
                                pass
                    
                    match['competition_name'] = comp_info['name']
                    # Add streams and clips
                    m3u8_links = load_m3u8_links()
                    match['streams'] = m3u8_links.get(match.get('match_id', ''), [])
                    match['clips'] = cache.get(f"clips_{match.get('match_id', '')}") or []
                    live_matches.append(match)
    
    return render_template('live.html', live_matches=live_matches)

@app.route('/stream/<match_id>')
def stream_player(match_id):
    """Stream player page."""
    m3u8_links = load_m3u8_links()
    streams = m3u8_links.get(match_id, [])
    
    return render_template(
        'stream.html',
        match_id=match_id,
        streams=streams,
        home_team='Home',
        away_team='Away'
    )

@app.route('/clips/<match_id>')
def clips_page(match_id):
    """Clips page for a match."""
    clips = cache.get(f"clips_{match_id}") or []
    return render_template('clips.html', match_id=match_id, clips=clips)

@app.errorhandler(404)
def not_found(error):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(error):
    return render_template('500.html'), 500

# --- Start Background Services ---

def start_background_services():
    """Start all background services."""
    # Start live match updater
    start_background_updater()
    
    # Start goal detection
    thread = threading.Thread(target=goal_detection_loop, daemon=True)
    thread.start()
    logger.info("Goal detection started - checking every 2 seconds")
    
    # Start daily fixture scheduler
    start_scheduler()
    
    # Start clip cleanup
    start_cleanup()

if __name__ == '__main__':
    # Start background services before running app
    start_background_services()
    
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"Starting server on port {port}")
    app.run(host='0.0.0.0', port=port, debug=DEBUG)
