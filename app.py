from flask import Flask, render_template, request, jsonify, session, redirect, url_for, send_from_directory, make_response
from werkzeug.middleware.proxy_fix import ProxyFix
import os
import json
import zipfile
import secrets
import datetime
import shutil
import threading
import time
import traceback
from glob import glob
from pathlib import Path
from dotenv import load_dotenv
from density_finder_rs import (
    find_highest_density_period,
    find_participant_density_period,
    detect_conversations,
    compute_top_words,
    compute_top_emojis,
    count_specific_string,
    aggregate_daily_counts,
    split_sent_received_daily_counts,
    build_group_chat_trends_series,
    build_uploader_trends_series,
)  # type: ignore
from game_blueprint import game_bp

load_dotenv()

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
app.secret_key = os.getenv('FLASK_SECRET_KEY', secrets.token_hex(16))
app.config['UPLOAD_FOLDER'] = 'user_data'
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024 * 1024  # 2GB max
app.config['CHUNK_FOLDER'] = 'temp_chunks'
app.config['COMPUTE_PASSCODE'] = os.getenv('COMPUTE_PASSCODE', 'your_secret_passcode_here')  # Change this!

# Ensure directories exist
Path(app.config['UPLOAD_FOLDER']).mkdir(exist_ok=True)
Path(app.config['CHUNK_FOLDER']).mkdir(exist_ok=True)

# Game routes are isolated in a dedicated blueprint.
app.register_blueprint(game_bp)

def cleanup_old_data():
    """Remove user data older than 3 days"""
    if not os.path.exists(app.config['UPLOAD_FOLDER']):
        return
    
    three_days_ago = datetime.datetime.now() - datetime.timedelta(days=3)
    for user_folder in os.listdir(app.config['UPLOAD_FOLDER']):
        user_path = os.path.join(app.config['UPLOAD_FOLDER'], user_folder)
        if os.path.isdir(user_path):
            keepfolder_path = os.path.join(user_path, 'keep.txt')
            if os.path.exists(keepfolder_path):
                continue
            created_time = datetime.datetime.fromtimestamp(os.path.getctime(user_path))
            if created_time < three_days_ago:
                shutil.rmtree(user_path)
    
    # Clean old chunks (older than 1 hour)
    if os.path.exists(app.config['CHUNK_FOLDER']):
        one_hour_ago = datetime.datetime.now() - datetime.timedelta(hours=1)
        for folder in os.listdir(app.config['CHUNK_FOLDER']):
            folder_path = os.path.join(app.config['CHUNK_FOLDER'], folder)
            if os.path.isdir(folder_path):
                created_time = datetime.datetime.fromtimestamp(os.path.getctime(folder_path))
                if created_time < one_hour_ago:
                    shutil.rmtree(folder_path)

def cleanup_daemon():
    """Background thread to cleanup old data every hour"""
    while True:
        time.sleep(3600)  # Run every hour
        try:
            cleanup_old_data()
        except Exception as e:
            print(f"Cleanup error: {e}")

# Start cleanup daemon
cleanup_thread = threading.Thread(target=cleanup_daemon, daemon=True)
cleanup_thread.start()


class GroupTrendsJob:
    """Tracks one in-flight group trends computation per user."""
    def __init__(self, thread):
        self.thread = thread
        self.started_at = time.time()
        self.error = None


group_trends_jobs = {}
group_trends_jobs_lock = threading.Lock()
group_trends_series_cache = {}
group_trends_series_cache_lock = threading.Lock()

uploader_trends_jobs = {}
uploader_trends_jobs_lock = threading.Lock()
uploader_trends_series_cache = {}
uploader_trends_series_cache_lock = threading.Lock()


def _build_group_chat_precomputed_trends(group_chats):
    """Build aggregate totals and moving-average trends for day and week buckets."""
    daily, weekly = build_group_chat_trends_series(group_chats or [])
    daily_keys, daily_totals, daily_trend, daily_window = daily
    weekly_keys, weekly_totals, weekly_trend, weekly_window = weekly

    return {
        'daily': {
            'keys': daily_keys,
            'totals': daily_totals,
            'trend': daily_trend,
            'window': daily_window
        },
        'weekly': {
            'keys': weekly_keys,
            'totals': weekly_totals,
            'trend': weekly_trend,
            'window': weekly_window
        }
    }


def _build_uploader_precomputed_trends(sent_daily_counts, received_daily_counts):
    """Build daily/weekly totals and moving-average trends for sent/received counts."""
    sent_series, received_series = build_uploader_trends_series(
        sent_daily_counts or {},
        received_daily_counts or {}
    )

    def _series_to_payload(series):
        daily_keys, daily_totals, daily_trend, daily_window, weekly_keys, weekly_totals, weekly_trend, weekly_window = series
        return {
            'keys': daily_keys,
            'daily': {
                'totals': daily_totals,
                'trend': daily_trend,
                'window': daily_window
            },
            'weekly': {
                'keys': weekly_keys,
                'totals': weekly_totals,
                'trend': weekly_trend,
                'window': weekly_window
            }
        }

    return {
        'sent': _series_to_payload(sent_series),
        'received': _series_to_payload(received_series)
    }


def compute_group_chat_trends(user_code):
    """Compute and cache aggregated group chat trends for a user."""
    cache_path = os.path.join(app.config['UPLOAD_FOLDER'], user_code, 'cached_group_chat_trends.json')

    # If another request already produced cache, avoid recomputing.
    if os.path.exists(cache_path):
        return

    conversations = get_conversations(user_code)

    # Filter to only group chats (more than 1 participant)
    group_chats = [c for c in conversations if len(c.get('participants', [])) > 1]

    result = []
    for conv in group_chats:
        messages = load_conversation_data(user_code, conv['id'])
        daily_counts = aggregate_daily_counts(messages)

        result.append({
            'id': conv['id'],
            'title': conv['title'],
            'daily_counts': daily_counts  # {YYYY-MM-DD: count}
        })

    with open(cache_path, 'w') as f:
        json.dump(result, f)


def _group_trends_worker(user_code):
    """Worker thread wrapper that computes trends and tracks failure state."""
    try:
        compute_group_chat_trends(user_code)
        print(f"[CACHE WRITE] Saved group chat trends cache for user {user_code}")
    except Exception as e:
        print(f"[GROUP_TRENDS_ERROR] Failed for user {user_code}: {e}")
        traceback.print_exc()
        with group_trends_jobs_lock:
            job = group_trends_jobs.get(user_code)
            if job:
                job.error = str(e)
        return

    # Job is done successfully; clear it so dictionary doesn't grow forever.
    with group_trends_jobs_lock:
        group_trends_jobs.pop(user_code, None)


def compute_uploader_message_trends(user_code):
    """Compute and cache uploader sent/received message counts over time."""
    cache_path = os.path.join(app.config['UPLOAD_FOLDER'], user_code, 'cached_uploader_message_trends.json')

    if os.path.exists(cache_path):
        return

    uploader_username = load_uploader_name(user_code)
    if not uploader_username:
        with open(cache_path, 'w') as f:
            json.dump({
                'uploader_username': None,
                'sent_daily_counts': {},
                'received_daily_counts': {}
            }, f)
        return

    conversations = get_conversations(user_code)
    sent_daily_counts = {}
    received_daily_counts = {}

    for conv in conversations:
        messages = load_conversation_data(user_code, conv['id'])
        sent_chunk, received_chunk = split_sent_received_daily_counts(messages, uploader_username)
        for day_key, count in sent_chunk.items():
            sent_daily_counts[day_key] = sent_daily_counts.get(day_key, 0) + int(count)
        for day_key, count in received_chunk.items():
            received_daily_counts[day_key] = received_daily_counts.get(day_key, 0) + int(count)

    with open(cache_path, 'w') as f:
        json.dump({
            'uploader_username': uploader_username,
            'sent_daily_counts': sent_daily_counts,
            'received_daily_counts': received_daily_counts
        }, f)


def _uploader_trends_worker(user_code):
    """Worker thread wrapper that computes uploader trends and tracks failure state."""
    try:
        compute_uploader_message_trends(user_code)
        print(f"[CACHE WRITE] Saved uploader message trends cache for user {user_code}")
    except Exception as e:
        print(f"[UPLOADER_TRENDS_ERROR] Failed for user {user_code}: {e}")
        traceback.print_exc()
        with uploader_trends_jobs_lock:
            job = uploader_trends_jobs.get(user_code)
            if job:
                job.error = str(e)
        return

    with uploader_trends_jobs_lock:
        uploader_trends_jobs.pop(user_code, None)


# ── Conversation Detection ────────────────────────────────────────────────────

convo_detection_jobs = {}
convo_detection_jobs_lock = threading.Lock()


class ConvoDetectionJob:
    """Tracks one in-flight conversation detection computation per user."""
    def __init__(self, thread):
        self.thread = thread
        self.started_at = time.time()
        self.error = None


def compute_all_convo_stats(user_code):
    """
    For each conversation thread, run detect_conversations (Rust), cache per-session
    metadata to cached_convo_metadata.json next to cached_analysis.json, and merge
    thread-level aggregates into cached_analysis.json under the 'convo_stats' key.
    A user-level summary is also written to cached_convo_stats.json.
    """
    conversations = get_conversations(user_code)

    global_total_convos = 0
    global_total_response_time = 0.0
    global_response_time_conversation_count = 0
    global_leans: dict = {}
    global_leans_conversation_count = 0
    global_total_msg_count = 0.0
    global_msg_count_conversation_count = 0
    global_total_duration_ms = 0.0
    global_duration_conversation_count = 0
    global_convos_per_day: dict = {}
    global_avg_time_between_convos = 0.0
    global_time_between_convos_conversation_count = 0
    per_chat_convos_per_day: dict = {}

    for conv in conversations:
        conv_id = conv['id']
        thread_folder = os.path.join(app.config['UPLOAD_FOLDER'], user_code, 'inbox', conv_id)
        metadata_path = os.path.join(thread_folder, 'cached_convo_metadata.json')
        analysis_path = os.path.join(thread_folder, 'cached_analysis.json')

        # Load or compute per-session metadata for this thread
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as f:
                thread_result = json.load(f)
        else:
            messages = load_conversation_data(user_code, conv_id)
            if not messages:
                continue

            thread_result = detect_conversations(messages)

            # Cache per-session metadata (without per-message content)
            try:
                with open(metadata_path, 'w') as f:
                    json.dump(thread_result, f)
            except Exception as e:
                print(f"[CONVO_DETECT] Failed to write metadata for {conv_id}: {e}")

        # Merge thread-level aggregates into cached_analysis.json
        thread_agg = thread_result.get('thread_aggregation', {})
        if thread_agg:
            try:
                if os.path.exists(analysis_path):
                    with open(analysis_path, 'r') as f:
                        analysis = json.load(f)
                else:
                    analysis = {}
                analysis['convo_stats'] = thread_agg
                with open(analysis_path, 'w') as f:
                    json.dump(analysis, f)
            except Exception as e:
                print(f"[CONVO_DETECT] Failed to update cached_analysis for {conv_id}: {e}")

        # Accumulate global aggregates
        total_c = thread_agg.get('total_conversations', 0)
        if total_c == 0:
            continue

        global_total_convos += total_c

        art = thread_agg.get('avg_in_convo_response_time', 0.0)
        if art > 0:
            global_total_response_time += art * total_c
            global_response_time_conversation_count += total_c

        atbc = thread_agg.get('avg_time_between_convos', 0.0)
        if atbc > 0:
            global_avg_time_between_convos += atbc * total_c
            global_time_between_convos_conversation_count += total_c

        participation = thread_agg.get('avg_participation_leans', {})
        if participation:
            for sender, pct in participation.items():
                global_leans[sender] = global_leans.get(sender, 0.0) + pct * total_c
            global_leans_conversation_count += total_c

        avg_msg_count = thread_agg.get('avg_msg_count_per_convo', 0.0)
        if avg_msg_count > 0:
            global_total_msg_count += avg_msg_count * total_c
            global_msg_count_conversation_count += total_c

        avg_duration_ms = thread_agg.get('avg_duration_ms_per_convo', 0.0)
        if avg_duration_ms > 0:
            global_total_duration_ms += avg_duration_ms * total_c
            global_duration_conversation_count += total_c

        chat_cpd = thread_agg.get('convos_per_day', {})
        for date_str, cnt in chat_cpd.items():
            global_convos_per_day[date_str] = global_convos_per_day.get(date_str, 0) + cnt

        # Store per-chat conversation counts for the trends chart
        if chat_cpd:
            per_chat_convos_per_day[conv_id] = {
                'title': conv.get('title', conv_id),
                'convos_per_day': chat_cpd,
            }

    # Build global summary
    avg_participation = {}
    if global_leans_conversation_count > 0:
        for sender, total_pct in global_leans.items():
            avg_participation[sender] = total_pct / global_leans_conversation_count

    summary = {
        'total_conversations': global_total_convos,
        'avg_in_convo_response_time': (
            global_total_response_time / global_response_time_conversation_count
            if global_response_time_conversation_count > 0 else 0.0
        ),
        'avg_time_between_convos': (
            global_avg_time_between_convos / global_time_between_convos_conversation_count
            if global_time_between_convos_conversation_count > 0 else 0.0
        ),
        'avg_participation_leans': avg_participation,
        'avg_msg_count_per_convo': (
            global_total_msg_count / global_msg_count_conversation_count
            if global_msg_count_conversation_count > 0 else 0.0
        ),
        'avg_duration_ms_per_convo': (
            global_total_duration_ms / global_duration_conversation_count
            if global_duration_conversation_count > 0 else 0.0
        ),
        'convos_per_day': global_convos_per_day,
        'per_chat_convos_per_day': per_chat_convos_per_day,
    }

    user_stats_path = os.path.join(app.config['UPLOAD_FOLDER'], user_code, 'cached_convo_stats.json')
    with open(user_stats_path, 'w') as f:
        json.dump(summary, f)


def _convo_detection_worker(user_code):
    """Background worker that runs conversation detection for all threads."""
    try:
        compute_all_convo_stats(user_code)
        print(f"[CACHE WRITE] Saved convo stats for user {user_code}")
    except Exception as e:
        print(f"[CONVO_DETECT_ERROR] Failed for user {user_code}: {e}")
        traceback.print_exc()
        with convo_detection_jobs_lock:
            job = convo_detection_jobs.get(user_code)
            if job:
                job.error = str(e)
        return

    with convo_detection_jobs_lock:
        convo_detection_jobs.pop(user_code, None)


#     """Find the period with highest message density"""
#     if not data:
#         return (0, 0)
    
#     MS_PER_DAY = 86_400_000
#     window_ms = window_days * MS_PER_DAY
#     timestamps = sorted([d['timestamp_ms'] for d in data])
#     N = len(timestamps)
    
#     best_start_index = 0
#     max_count = 0
#     start = 0
#     end = 0
    
#     while start < N:
#         window_end_ms = timestamps[start] + window_ms
#         while end < N and timestamps[end] < window_end_ms:
#             end += 1
#         current_count = end - start
#         if current_count > max_count:
#             max_count = current_count
#             best_start_index = start
#         start += 1
    
#     start_ms = timestamps[best_start_index]
#     end_ms = start_ms + window_ms
#     return (start_ms, end_ms)

def nearest_day(ms: int): # find the nearest day timestamp from the ms timestamp
    MS_PER_DAY = 86_400_000
    return (ms // MS_PER_DAY) * MS_PER_DAY

def get_conversations(user_code):
    """Get list of all conversations for a user"""
    user_path = os.path.join(app.config['UPLOAD_FOLDER'], user_code, 'inbox')
    
    if not os.path.exists(user_path):
        return []
    
    conversations = []
    for folder in os.listdir(user_path):
        folder_path = os.path.join(user_path, folder)
        if os.path.isdir(folder_path) and not os.path.islink(folder_path):
            # Try to get conversation name from first message file
            message_files = glob(os.path.join(folder_path, 'message_*.json'))
            if message_files:
                with open(message_files[0], encoding="raw_unicode_escape") as f:
                    data = json.loads(f.read().encode('raw_unicode_escape').decode())
                    title = data.get('title', folder)
                    participants = data.get('participants', [])
                    conversations.append({
                        'id': folder,
                        'title': title,
                        'participants': participants
                    })
        elif os.path.islink(folder_path):
            # Handle symlinks
            try:
                message_files = glob(os.path.join(folder_path, 'message_*.json'))
                if message_files:
                    with open(message_files[0], encoding="raw_unicode_escape") as f:
                        data = json.loads(f.read().encode('raw_unicode_escape').decode())
                        title = data.get('title', folder)
                        participants = data.get('participants', [])
                        conversations.append({
                            'id': folder,
                            'title': title,
                            'participants': participants
                        })
            except:
                pass
    conversations = sorted(conversations, key=lambda x: x['title'].lower())
    return conversations

def load_conversation_data(user_code, conversation_id):
    """Load all messages for a conversation"""
    user_path = os.path.join(app.config['UPLOAD_FOLDER'], user_code, 'inbox')
    conv_path = os.path.join(user_path, conversation_id)
    
    messages = []
    message_files = glob(os.path.join(conv_path, 'message_*.json'))
    
    for file_path in message_files:
        with open(file_path, encoding="raw_unicode_escape") as f:
            messages.extend(json.loads(f.read().encode('raw_unicode_escape').decode())['messages'])
    
    messages = sorted(messages, key=lambda x: x['timestamp_ms'])
    return messages


def find_uploader_name_from_marker(user_code):
    """Find uploader username via the exact marker message in extracted inbox data."""
    inbox_path = os.path.join(app.config['UPLOAD_FOLDER'], user_code, 'inbox')
    if not os.path.exists(inbox_path):
        return None

    marker_text = 'You sent an attachment.'

    for root, _, _ in os.walk(inbox_path):
        message_files = glob(os.path.join(root, 'message_*.json'))
        for file_path in message_files:
            try:
                with open(file_path, encoding="raw_unicode_escape") as f:
                    data = json.loads(f.read().encode('raw_unicode_escape').decode())

                for message in data.get('messages', []):
                    if message.get('content') == marker_text:
                        sender_name = message.get('sender_name')
                        if sender_name:
                            return sender_name
            except Exception:
                # Ignore malformed files and keep scanning.
                continue

    return None


def load_uploader_name(user_code):
    """Load uploader username from top-level me.json for a user code."""
    if not user_code:
        return None

    me_path = os.path.join(app.config['UPLOAD_FOLDER'], user_code, 'me.json')
    if not os.path.exists(me_path):
        return None

    try:
        with open(me_path, 'r') as f:
            payload = json.load(f)
        username = payload.get('username')
        return username if isinstance(username, str) and username.strip() else None
    except Exception:
        return None


@app.context_processor
def inject_template_user_context():
    """Expose uploader username globally so templates can render subtitle context."""
    active_code = session.get('user_code') or session.get('pending_user_code')
    return {
        'current_username': load_uploader_name(active_code)
    }

@app.route('/sw.js')
def service_worker():
    response = make_response(send_from_directory('static', 'sw.js'))
    response.headers['Content-Type'] = 'application/javascript'
    response.headers['Service-Worker-Allowed'] = '/'
    response.headers['Cache-Control'] = 'no-cache'
    return response


@app.route('/')
def index():
    cleanup_old_data()
    return render_template('index.html')

@app.route('/help')
def help_page():
    return render_template('help.html')


@app.route('/upload/init', methods=['POST'])
def upload_init():
    """Initialize chunked upload"""
    access_code = request.json.get('access_code', '').strip()
    filename = request.json.get('filename', '')
    total_chunks = request.json.get('total_chunks', 0)
    file_size = request.json.get('file_size', 0)
    
    if not access_code or not filename:
        return jsonify({'error': 'Access code and filename required'}), 400
    
    # Check if code already exists
    user_path = os.path.join(app.config['UPLOAD_FOLDER'], access_code)
    if os.path.exists(user_path):
        return jsonify({'error': 'Access code already in use. Please choose another.'}), 400
    
    # Use the user's chosen access code
    upload_id = access_code
    
    # Create chunk directory
    chunk_dir = os.path.join(app.config['CHUNK_FOLDER'], upload_id)
    os.makedirs(chunk_dir, exist_ok=True)
    
    # Store metadata
    metadata = {
        'access_code': access_code,
        'filename': filename,
        'total_chunks': total_chunks,
        'file_size': file_size,
        'received_chunks': []
    }
    
    with open(os.path.join(chunk_dir, 'metadata.json'), 'w') as f:
        json.dump(metadata, f)
    
    return jsonify({'upload_id': upload_id})

@app.route('/upload/chunk', methods=['POST'])
def upload_chunk():
    """Upload a single chunk"""
    upload_id = request.args.get('upload_id')
    chunk_number = request.args.get('chunk_number', type=int)
    #chunk_file = request.files.get('chunk')
    
    if not upload_id:
        return jsonify({'error': 'Invalid request'}), 400
    
    chunk_dir = os.path.join(app.config['CHUNK_FOLDER'], upload_id)
    if not os.path.exists(chunk_dir):
        return jsonify({'error': 'Upload session not found'}), 404
    
    # Save chunk - ensure binary mode
    chunk_path = os.path.join(chunk_dir, f'chunk_{chunk_number}')
    with open(chunk_path, 'wb') as f:
        chunk_data = request.get_data()
        f.write(chunk_data)
    
    chunk_size = os.path.getsize(chunk_path)
    
    # Update metadata
    metadata_path = os.path.join(chunk_dir, 'metadata.json')
    with open(metadata_path, 'r') as f:
        metadata = json.load(f)
    
    if chunk_number not in metadata['received_chunks']:
        metadata['received_chunks'].append(chunk_number)
    
    if 'chunk_sizes' not in metadata:
        metadata['chunk_sizes'] = {}
    metadata['chunk_sizes'][str(chunk_number)] = chunk_size
    
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f)
    
    return jsonify({'success': True, 'received': len(metadata['received_chunks']), 'chunk_size': chunk_size})

@app.route('/upload/complete', methods=['POST'])
def upload_complete():
    """Complete the upload and process the file"""
    upload_id = request.json.get('upload_id')
    
    chunk_dir = os.path.join(app.config['CHUNK_FOLDER'], upload_id)
    if not os.path.exists(chunk_dir):
        return jsonify({'error': 'Upload session not found'}), 404
    
    # Load metadata
    with open(os.path.join(chunk_dir, 'metadata.json'), 'r') as f:
        metadata = json.load(f)
    
    # Verify all chunks received
    expected_chunks = set(range(metadata['total_chunks']))
    received_chunks = set(metadata['received_chunks'])
    
    if expected_chunks != received_chunks:
        missing = expected_chunks - received_chunks
        return jsonify({'error': f'Missing chunks: {sorted(list(missing))[:10]}'}), 400
    
    # Use upload_id as the user code
    user_code = upload_id
    user_path = os.path.join(app.config['UPLOAD_FOLDER'], user_code)
    os.makedirs(user_path, exist_ok=True)
    
    # Combine chunks in correct order
    zip_path = os.path.join(user_path, 'upload.zip')
    try:
        total_written = 0
        with open(zip_path, 'wb') as outfile:
            for i in range(metadata['total_chunks']):
                chunk_path = os.path.join(chunk_dir, f'chunk_{i}')
                if not os.path.exists(chunk_path):
                    raise Exception(f'Chunk file missing: {i}')
                
                with open(chunk_path, 'rb') as infile:
                    chunk_data = infile.read()
                    bytes_written = outfile.write(chunk_data)
                    total_written += bytes_written
        
        actual_size = os.path.getsize(zip_path)
        expected_size = metadata.get('file_size', 0)
        
        if expected_size > 0 and actual_size != expected_size:
            chunk_sizes = metadata.get('chunk_sizes', {})
            total_chunks_size = sum(chunk_sizes.values())
            error_msg = f'File size mismatch: expected {expected_size}, got {actual_size} (written: {total_written}, chunks total: {total_chunks_size})'
            raise Exception(error_msg)
            
    except Exception as e:
        if os.path.exists(zip_path):
            os.remove(zip_path)
        shutil.rmtree(user_path, ignore_errors=True)
        shutil.rmtree(chunk_dir, ignore_errors=True)
        return jsonify({'error': f'Failed to combine chunks: {str(e)}'}), 400
    
    # Extract only the inbox folder
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Find and extract only inbox folder
            inbox_found = False
            for name in zip_ref.namelist():
                if 'messages/inbox/' in name:
                    inbox_found = True
                    # Extract to user_path, stripping the path before inbox
                    target_path = name.split('messages/inbox/')[-1]
                    if target_path:  # Skip the inbox folder itself
                        extract_path = os.path.join(user_path, 'inbox', target_path)
                        os.makedirs(os.path.dirname(extract_path), exist_ok=True)
                        if not name.endswith('/'):
                            with zip_ref.open(name) as source, open(extract_path, 'wb') as target:
                                target.write(source.read())
            
            if not inbox_found:
                raise Exception('Instagram inbox folder not found in zip')
        
        os.remove(zip_path)
    except Exception as e:
        shutil.rmtree(user_path)
        shutil.rmtree(chunk_dir)
        return jsonify({'error': f'Failed to extract zip: {str(e)}'}), 400
    
    # Clean up chunks
    shutil.rmtree(chunk_dir)

    # Resolve and persist uploader identity immediately after successful extraction.
    uploader_name = find_uploader_name_from_marker(user_code)
    if not uploader_name:
        session['pending_user_code'] = user_code
        return jsonify({
            'code': user_code,
            'needs_username': True,
            'message': 'Could not determine uploader automatically. Please enter your username.'
        })

    with open(os.path.join(user_path, 'me.json'), 'w') as f:
        json.dump({'username': uploader_name}, f)
    
    # Store in session
    session['user_code'] = user_code
    session.pop('pending_user_code', None)
    
    return jsonify({'code': user_code})


@app.route('/upload/set_me', methods=['POST'])
def upload_set_me():
    """Persist uploader username when auto-detection was not possible."""
    payload = request.get_json(silent=True) or {}
    code = (payload.get('code') or session.get('pending_user_code') or '').strip()
    username = (payload.get('username') or '').strip()

    if not code:
        return jsonify({'error': 'Missing upload code'}), 400
    if not username:
        return jsonify({'error': 'Username is required'}), 400

    user_path = os.path.join(app.config['UPLOAD_FOLDER'], code)
    if not os.path.exists(user_path):
        return jsonify({'error': 'Upload data not found for this code'}), 404

    with open(os.path.join(user_path, 'me.json'), 'w') as f:
        json.dump({'username': username}, f)

    session['user_code'] = code
    session.pop('pending_user_code', None)
    return jsonify({'success': True, 'code': code})

@app.route('/login', methods=['POST'])
def login():
    code = request.form.get('code', '').strip()
    
    if not code:
        return jsonify({'error': 'Code required'}), 400
    
    user_path = os.path.join(app.config['UPLOAD_FOLDER'], code)
    if not os.path.exists(user_path):
        return jsonify({'error': 'Invalid code or data expired'}), 404
    
    session['user_code'] = code
    session.pop('pending_user_code', None)
    return jsonify({'success': True})

@app.route('/dashboard')
def dashboard():
    if 'user_code' not in session:
        return redirect(url_for('index'))
    return render_template('dashboard.html')

@app.route('/api/conversations')
def api_conversations():
    if 'user_code' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    conversations = get_conversations(session['user_code'])
    return jsonify(conversations)

@app.route('/api/conversation/<conversation_id>')
def api_conversation(conversation_id):
    if 'user_code' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    analysis_path = os.path.join(app.config['UPLOAD_FOLDER'], session['user_code'], 'inbox', conversation_id, 'cached_analysis.json')

    # check if cached data exists
    if os.path.exists(analysis_path):
        with open(analysis_path, 'r') as f:
            cached_data = json.load(f)
            if 'messages' in cached_data:
                # If convo_stats is missing from cache, compute and patch it now
                if 'convo_stats' not in cached_data:
                    try:
                        messages = load_conversation_data(session['user_code'], conversation_id)
                        thread_result = detect_conversations(messages)
                        cached_data['convo_stats'] = thread_result.get('thread_aggregation', {})
                        # Also write the per-session metadata cache
                        metadata_path = os.path.join(
                            app.config['UPLOAD_FOLDER'], session['user_code'],
                            'inbox', conversation_id, 'cached_convo_metadata.json'
                        )
                        if not os.path.exists(metadata_path):
                            with open(metadata_path, 'w') as mf:
                                json.dump(thread_result, mf)
                        with open(analysis_path, 'w') as cf:
                            json.dump(cached_data, cf)
                    except Exception as e:
                        print(f"[CONVO_DETECT] Failed to patch convo_stats for {conversation_id}: {e}")
                return jsonify(cached_data)

    # If cache exists but doesn't have raw messages (from compact format), rebuild below.
    
    messages = load_conversation_data(session['user_code'], conversation_id)
    
    # Get conversation info
    conversations = get_conversations(session['user_code'])
    conv_info = next((c for c in conversations if c['id'] == conversation_id), None)
    
    # Calculate statistics
    total_messages = len(messages)
    messages_by_sender = {}
    attachments_by_sender = {}
    attachments = 0
    
    for msg in messages:
        sender = msg.get('sender_name', 'Unknown')
        messages_by_sender[sender] = messages_by_sender.get(sender, 0) + 1
        if 'content' not in msg:
            attachments += 1
            attachments_by_sender[sender] = attachments_by_sender.get(sender, 0) + 1
    
    oldest = messages[0] if messages else None
    latest = messages[-1] if messages else None
    
    # Calculate overall average per day
    overall_avg_per_day = 0
    if oldest and latest:
        time_diff_days = ((latest['timestamp_ms'] - oldest['timestamp_ms']) / 86400000)
        print( time_diff_days)
        if time_diff_days > 1:
            overall_avg_per_day = total_messages / time_diff_days
        else:
            overall_avg_per_day = total_messages
    
    # Calculate max density period
    maxed_density = {'start_ms': 0, 'end_ms': 0, 'count': 0, 'days': 1}
    for days in range(1, 31):
        start_ms, end_ms = find_highest_density_period(messages, days)
        count = sum(1 for m in messages if start_ms <= m['timestamp_ms'] < end_ms)
        if count / days > maxed_density['count'] / maxed_density['days']:
            maxed_density = {
                'start_ms': start_ms,
                'end_ms': end_ms,
                'count': count,
                'days': days
            }
    
    # Calculate gap and response times
    max_gap = {'time': 0, 'msg1': None, 'msg2': None}
    min_gap = {'time': float('inf'), 'msg1': None, 'msg2': None}
    total_gaps = 0
    gap_count = 0
    
    for i in range(1, len(messages)):
        gap = messages[i]['timestamp_ms'] - messages[i-1]['timestamp_ms']
        total_gaps += gap
        gap_count += 1
        
        if gap > max_gap['time']:
            max_gap = {'time': gap, 'msg1': messages[i-1], 'msg2': messages[i]}
        if gap < min_gap['time']:
            min_gap = {'time': gap, 'msg1': messages[i-1], 'msg2': messages[i]}
    
    avg_gap = total_gaps / gap_count if gap_count > 0 else 0
    
    # Calculate response times
    max_response = {'time': 0, 'msg1': None, 'msg2': None}
    min_response = {'time': float('inf'), 'msg1': None, 'msg2': None}
    total_responses = 0
    response_count = 0
    prev_response = messages[0]
    
    for i in range(1, len(messages)):
        if messages[i]['sender_name'] != prev_response['sender_name']:
            response_time = messages[i]['timestamp_ms'] - prev_response['timestamp_ms']
            total_responses += response_time
            response_count += 1
            
            if response_time > max_response['time']:
                max_response = {'time': response_time, 'msg1': prev_response, 'msg2': messages[i]}
            if response_time < min_response['time']:
                min_response = {'time': response_time, 'msg1': prev_response, 'msg2': messages[i]}
            
            prev_response = messages[i]
    
    avg_response = total_responses / response_count if response_count > 0 else 0
    
    if min_gap['time'] == float('inf'):
        min_gap = {'time': 'Infinity', 'msg1': None, 'msg2': None}
    if min_response['time'] == float('inf'):
        min_response = {'time': 'Infinity', 'msg1': None, 'msg2': None}
    d = {
        'conversation': conv_info,
        'total_messages': total_messages,
        'messages_by_sender': messages_by_sender,
        'attachments_by_sender': attachments_by_sender,
        'attachments': attachments,
        'oldest': oldest,
        'latest': latest,
        'messages': messages,
        'max_density': maxed_density,
        'overall_avg_per_day': overall_avg_per_day,
        'gaps': {
            'avg': avg_gap,
            'max': max_gap,
            'min': min_gap
        },
        'responses': {
            'avg': avg_response,
            'max': max_response,
            'min': min_response
        },
        'average_message_length': {
            "overall": sum(len(m.get('content', '')) for m in messages) / total_messages if total_messages > 0 else 0,
            "by_sender": {sender: sum(len(m.get('content', '')) for m in messages if m.get('sender_name') == sender) / count for sender, count in messages_by_sender.items()}
        }
    }

    # Compute conversation detection stats and include in analysis
    try:
        thread_result = detect_conversations(messages)
        d['convo_stats'] = thread_result.get('thread_aggregation', {})
        # Cache the per-session metadata separately
        metadata_path = os.path.join(
            app.config['UPLOAD_FOLDER'], session['user_code'],
            'inbox', conversation_id, 'cached_convo_metadata.json'
        )
        if not os.path.exists(metadata_path):
            with open(metadata_path, 'w') as mf:
                json.dump(thread_result, mf)
    except Exception as e:
        print(f"[CONVO_DETECT] Failed to compute convo_stats for {conversation_id}: {e}")

    with open(os.path.join(app.config['UPLOAD_FOLDER'], session['user_code'], 'inbox', conversation_id, 'cached_analysis.json'), 'w') as f:
        json.dump(d, f)
    return jsonify(d)

@app.route('/api/participant_period', methods=['POST'])
def participant_period():
    if 'user_code' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    conversation_id = request.json.get('conversation_id')
    participant = request.json.get('participant')
    days = request.json.get('days', 1)
    find_max = request.json.get('find_max', True)
    
    if not conversation_id or not participant:
        return jsonify({'error': 'Missing required parameters'}), 400
    
    try:
        days = int(days)
    except (TypeError, ValueError):
        return jsonify({'error': 'Days must be a valid integer'}), 400
    
    if days < 1:
        return jsonify({'error': 'Days must be a positive integer'}), 400
    
    messages = load_conversation_data(session['user_code'], conversation_id)
    
    # Use Rust implementation for efficient calculation
    start_ms, end_ms = find_participant_density_period(messages, days, participant, find_max)
    
    # Count messages in the period
    total_count = sum(1 for m in messages if start_ms <= m['timestamp_ms'] < end_ms)
    participant_count = sum(1 for m in messages if start_ms <= m['timestamp_ms'] < end_ms and m.get('sender_name') == participant)
    
    return jsonify({
        'start_ms': start_ms,
        'end_ms': end_ms,
        'total_count': total_count,
        'participant_count': participant_count,
        'days': days,
        'participant': participant
    })

@app.route('/api/custom_density', methods=['POST'])
def custom_density():
    if 'user_code' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    conversation_id = request.json.get('conversation_id')
    days = request.json.get('days', 1)
    
    if not conversation_id:
        return jsonify({'error': 'Missing conversation_id'}), 400
    
    try:
        days = int(days)
    except (TypeError, ValueError):
        return jsonify({'error': 'Days must be a valid integer'}), 400
    
    if days < 1:
        return jsonify({'error': 'Days must be a positive integer'}), 400
    
    messages = load_conversation_data(session['user_code'], conversation_id)
    
    # Use Rust implementation for efficient calculation
    start_ms, end_ms = find_highest_density_period(messages, days)
    count = sum(1 for m in messages if start_ms <= m['timestamp_ms'] < end_ms)
    
    return jsonify({
        'start_ms': start_ms,
        'end_ms': end_ms,
        'count': count,
        'days': days
    })

@app.route('/api/compute_word', methods=['POST'])
def compute_word():
    if 'user_code' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    conversation_id = request.json.get('conversation_id')
    passcode = request.json.get('passcode')
    
    messages = load_conversation_data(session['user_code'], conversation_id)

    if passcode != app.config['COMPUTE_PASSCODE'] and len(messages) > 15000:
        return jsonify({'error': 'Invalid passcode'}), 403
    
    most_common = compute_top_words(messages, 10)
    return jsonify({'words': most_common})

@app.route('/api/compute_emoji', methods=['POST'])
def compute_emoji():
    if 'user_code' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    conversation_id = request.json.get('conversation_id')
    messages = load_conversation_data(session['user_code'], conversation_id)

    most_common = compute_top_emojis(messages, 10)
    return jsonify({'emojis': most_common})

@app.route('/api/count_specific_string', methods=['POST'])
def count_specific_word():
    if 'user_code' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    conversation_id = request.json.get('conversation_id')
    target_string = request.json.get('string', '').lower()
    
    if not target_string:
        return jsonify({'error': 'Target string required'}), 400
    
    messages = load_conversation_data(session['user_code'], conversation_id)

    count = count_specific_string(messages, target_string)
    
    return jsonify({'string': target_string, 'count': count})

@app.route('/api/share_chat', methods=['POST'])
def share_chat():
    if 'user_code' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    conversation_id = request.json.get('conversation_id')
    target_code = request.json.get('target_code')
    
    if not conversation_id or not target_code:
        return jsonify({'error': 'Missing parameters'}), 400
    
    source_path = os.path.join(app.config['UPLOAD_FOLDER'], session['user_code'], 'inbox', conversation_id)
    target_user_root = os.path.join(app.config['UPLOAD_FOLDER'], target_code)
    target_user_path = os.path.join(app.config['UPLOAD_FOLDER'], target_code, 'inbox')
    target_path = os.path.join(target_user_path, conversation_id)
    
    if not os.path.exists(source_path):
        return jsonify({'error': 'Conversation not found'}), 404

    # Intentionally keep shared target without uploader identity metadata.
    target_me_path = os.path.join(target_user_root, 'me.json')
    if os.path.exists(target_me_path):
        os.remove(target_me_path)
    
    # Create target user path if it doesn't exist
    os.makedirs(target_user_path, exist_ok=True)
    
    # Check if already exists
    if os.path.exists(target_path):
        return jsonify({'message': 'Chat already exists in target code'})
    
    # Create symlink
    try:
        os.symlink(os.path.abspath(source_path), target_path)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': f'Failed to share: {str(e)}'}), 500

@app.route('/api/delete_account', methods=['POST'])
def delete_account():
    if 'user_code' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    user_code = session['user_code']
    user_path = os.path.join(app.config['UPLOAD_FOLDER'], user_code)
    
    if os.path.exists(user_path):
        shutil.rmtree(user_path)
    
    session.clear()
    return jsonify({'success': True})

@app.route('/trends')
def trends_dashboard():
    if 'user_code' not in session:
        return redirect(url_for('index'))
    return render_template('trends-dashboard.html')

@app.route('/api/group_chat_trends')
def api_group_chat_trends():
    if 'user_code' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    user_code = session['user_code']
    cache_path = os.path.join(app.config['UPLOAD_FOLDER'], user_code, 'cached_group_chat_trends.json')
    
    # Check if cached data exists
    if os.path.exists(cache_path):
        print(f"[CACHE HIT] Loading group chat trends from cache for user {user_code}")
        with open(cache_path, 'r') as f:
            cached_data = json.load(f)

            all_dates = []
            month_keys = set()
            for group_chat in cached_data:
                for day_key in (group_chat.get('daily_counts') or {}).keys():
                    all_dates.append(day_key)
                    if len(day_key) >= 7:
                        month_keys.add(day_key[:7])

            sorted_month_keys = sorted(month_keys)
            min_date = min(all_dates) if all_dates else None
            max_date = max(all_dates) if all_dates else None

            full_requested = str(request.args.get('full', '')).lower() in ('1', 'true', 'yes')
            months_requested_raw = request.args.get('months', '1')
            try:
                months_requested = max(1, int(months_requested_raw))
            except ValueError:
                months_requested = 1

            months_loaded = len(sorted_month_keys) if full_requested else min(months_requested, len(sorted_month_keys))
            allowed_months = set(sorted_month_keys[:months_loaded])

            if full_requested or not sorted_month_keys:
                response_data = cached_data
            else:
                response_data = []
                for group_chat in cached_data:
                    daily_counts = group_chat.get('daily_counts') or {}
                    filtered_daily_counts = {
                        date_key: count
                        for date_key, count in daily_counts.items()
                        if len(date_key) >= 7 and date_key[:7] in allowed_months
                    }

                    # Skip conversations that have no data in the requested month window.
                    if not filtered_daily_counts:
                        continue

                    response_data.append({
                        'id': group_chat.get('id'),
                        'title': group_chat.get('title'),
                        'daily_counts': filtered_daily_counts
                    })

            loaded_dates = []
            for group_chat in response_data:
                loaded_dates.extend((group_chat.get('daily_counts') or {}).keys())

            loaded_min_date = min(loaded_dates) if loaded_dates else None
            loaded_max_date = max(loaded_dates) if loaded_dates else None

            series_cache_key = (user_code, 'full' if full_requested else f'months:{months_loaded}')
            cache_mtime = os.path.getmtime(cache_path)

            precomputed_trends = None
            with group_trends_series_cache_lock:
                cached_series = group_trends_series_cache.get(series_cache_key)
                if cached_series and cached_series.get('mtime') == cache_mtime:
                    precomputed_trends = cached_series.get('value')

            if precomputed_trends is None:
                precomputed_trends = _build_group_chat_precomputed_trends(response_data)
                with group_trends_series_cache_lock:
                    group_trends_series_cache[series_cache_key] = {
                        'mtime': cache_mtime,
                        'value': precomputed_trends
                    }

            return jsonify({
                'data': response_data,
                'cached': True,
                'cache_file': cache_path,
                'partial': not full_requested,
                'months_loaded': months_loaded,
                'total_months': len(sorted_month_keys),
                'month_keys': sorted_month_keys,
                'precomputed_trends': precomputed_trends,
                'range': {
                    'min_date': min_date,
                    'max_date': max_date
                },
                'loaded_range': {
                    'min_date': loaded_min_date,
                    'max_date': loaded_max_date
                }
            })

    # No cache yet: start one background computation per user and let clients poll.
    with group_trends_jobs_lock:
        existing_job = group_trends_jobs.get(user_code)

        if existing_job and existing_job.thread.is_alive():
            elapsed = round(time.time() - existing_job.started_at, 2)
            return jsonify({
                'status': 'processing',
                'message': 'Group chat trends are still being computed',
                'elapsed_seconds': elapsed
            }), 202

        if existing_job and existing_job.error:
            # Previous worker failed; clear it so the next call can restart computation.
            last_error = existing_job.error
            group_trends_jobs.pop(user_code, None)
            return jsonify({
                'status': 'failed',
                'error': f'Background computation failed: {last_error}'
            }), 500

        print(f"[CACHE MISS] Starting background group chat trends compute for user {user_code}")
        worker = threading.Thread(target=_group_trends_worker, args=(user_code,), daemon=True)
        group_trends_jobs[user_code] = GroupTrendsJob(thread=worker)
        worker.start()

    return jsonify({
        'status': 'processing',
        'message': 'Started background computation for group chat trends'
    }), 202


@app.route('/api/uploader_message_trends')
def api_uploader_message_trends():
    if 'user_code' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    user_code = session['user_code']
    cache_path = os.path.join(app.config['UPLOAD_FOLDER'], user_code, 'cached_uploader_message_trends.json')

    if os.path.exists(cache_path):
        print(f"[CACHE HIT] Loading uploader message trends from cache for user {user_code}")
        with open(cache_path, 'r') as f:
            cached_payload = json.load(f)

        sent_daily_counts = cached_payload.get('sent_daily_counts') or {}
        received_daily_counts = cached_payload.get('received_daily_counts') or {}
        all_dates = list(set(sent_daily_counts.keys()) | set(received_daily_counts.keys()))
        month_keys = sorted({date_key[:7] for date_key in all_dates if isinstance(date_key, str) and len(date_key) >= 7})
        min_date = min(all_dates) if all_dates else None
        max_date = max(all_dates) if all_dates else None

        full_requested = str(request.args.get('full', '')).lower() in ('1', 'true', 'yes')
        months_requested_raw = request.args.get('months', '1')
        try:
            months_requested = max(1, int(months_requested_raw))
        except ValueError:
            months_requested = 1

        months_loaded = len(month_keys) if full_requested else min(months_requested, len(month_keys))
        allowed_months = set(month_keys[:months_loaded])

        if full_requested or not month_keys:
            filtered_sent_counts = sent_daily_counts
            filtered_received_counts = received_daily_counts
        else:
            filtered_sent_counts = {
                date_key: count
                for date_key, count in sent_daily_counts.items()
                if isinstance(date_key, str) and len(date_key) >= 7 and date_key[:7] in allowed_months
            }
            filtered_received_counts = {
                date_key: count
                for date_key, count in received_daily_counts.items()
                if isinstance(date_key, str) and len(date_key) >= 7 and date_key[:7] in allowed_months
            }

        loaded_dates = list(set(filtered_sent_counts.keys()) | set(filtered_received_counts.keys()))
        loaded_min_date = min(loaded_dates) if loaded_dates else None
        loaded_max_date = max(loaded_dates) if loaded_dates else None

        series_cache_key = (user_code, 'full' if full_requested else f'months:{months_loaded}')
        cache_mtime = os.path.getmtime(cache_path)

        precomputed_trends = None
        with uploader_trends_series_cache_lock:
            cached_series = uploader_trends_series_cache.get(series_cache_key)
            if cached_series and cached_series.get('mtime') == cache_mtime:
                precomputed_trends = cached_series.get('value')

        if precomputed_trends is None:
            precomputed_trends = _build_uploader_precomputed_trends(
                filtered_sent_counts,
                filtered_received_counts
            )
            with uploader_trends_series_cache_lock:
                uploader_trends_series_cache[series_cache_key] = {
                    'mtime': cache_mtime,
                    'value': precomputed_trends
                }

        return jsonify({
            'data': {
                'uploader_username': cached_payload.get('uploader_username'),
                'sent_daily_counts': filtered_sent_counts,
                'received_daily_counts': filtered_received_counts
            },
            'cached': True,
            'cache_file': cache_path,
            'partial': not full_requested,
            'months_loaded': months_loaded,
            'total_months': len(month_keys),
            'month_keys': month_keys,
            'precomputed_trends': precomputed_trends,
            'range': {
                'min_date': min_date,
                'max_date': max_date
            },
            'loaded_range': {
                'min_date': loaded_min_date,
                'max_date': loaded_max_date
            }
        })

    with uploader_trends_jobs_lock:
        existing_job = uploader_trends_jobs.get(user_code)

        if existing_job and existing_job.thread.is_alive():
            elapsed = round(time.time() - existing_job.started_at, 2)
            return jsonify({
                'status': 'processing',
                'message': 'Uploader message trends are still being computed',
                'elapsed_seconds': elapsed
            }), 202

        if existing_job and existing_job.error:
            last_error = existing_job.error
            uploader_trends_jobs.pop(user_code, None)
            return jsonify({
                'status': 'failed',
                'error': f'Background computation failed: {last_error}'
            }), 500

        print(f"[CACHE MISS] Starting background uploader message trends compute for user {user_code}")
        worker = threading.Thread(target=_uploader_trends_worker, args=(user_code,), daemon=True)
        uploader_trends_jobs[user_code] = GroupTrendsJob(thread=worker)
        worker.start()

    return jsonify({
        'status': 'processing',
        'message': 'Started background computation for uploader message trends'
    }), 202


@app.route('/api/convo_stats')
def api_convo_stats():
    """
    Return aggregated conversation detection statistics for the current user.
    Only aggregate data is returned (no per-session details).
    If not yet computed, a background job is started and 202 is returned.
    """
    if 'user_code' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    user_code = session['user_code']
    stats_path = os.path.join(app.config['UPLOAD_FOLDER'], user_code, 'cached_convo_stats.json')

    if os.path.exists(stats_path):
        print(f"[CACHE HIT] Loading convo stats from cache for user {user_code}")
        with open(stats_path, 'r') as f:
            summary = json.load(f)
        # Invalidate old caches that pre-date per-chat data
        if 'per_chat_convos_per_day' not in summary:
            print(f"[CACHE STALE] Invalidating old convo stats cache for user {user_code}")
            os.remove(stats_path)
        else:
            return jsonify({'cached': True, 'data': summary})

    # No cache yet – start background computation if not already running.
    with convo_detection_jobs_lock:
        existing_job = convo_detection_jobs.get(user_code)

        if existing_job and existing_job.thread.is_alive():
            elapsed = round(time.time() - existing_job.started_at, 2)
            return jsonify({
                'status': 'processing',
                'message': 'Conversation stats are still being computed',
                'elapsed_seconds': elapsed
            }), 202

        if existing_job and existing_job.error:
            last_error = existing_job.error
            convo_detection_jobs.pop(user_code, None)
            return jsonify({
                'status': 'failed',
                'error': f'Background computation failed: {last_error}'
            }), 500

        print(f"[CACHE MISS] Starting background convo detection for user {user_code}")
        worker = threading.Thread(target=_convo_detection_worker, args=(user_code,), daemon=True)
        convo_detection_jobs[user_code] = ConvoDetectionJob(thread=worker)
        worker.start()

    return jsonify({
        'status': 'processing',
        'message': 'Started background computation for conversation stats'
    }), 202


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=7000, debug=True, threaded=True)