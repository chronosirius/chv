from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from werkzeug.middleware.proxy_fix import ProxyFix
import os
import json
import zipfile
import secrets
import datetime
import shutil
import threading
import time
from glob import glob
from pathlib import Path
from werkzeug.utils import secure_filename
from collections import Counter
from dotenv import load_dotenv
import emoji

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

def cleanup_old_data():
    """Remove user data older than 3 days"""
    if not os.path.exists(app.config['UPLOAD_FOLDER']):
        return
    
    three_days_ago = datetime.datetime.now() - datetime.timedelta(days=3)
    for user_folder in os.listdir(app.config['UPLOAD_FOLDER']):
        user_path = os.path.join(app.config['UPLOAD_FOLDER'], user_folder)
        if os.path.isdir(user_path):
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

def find_highest_density_period(data, window_days=5):
    """Find the period with highest message density"""
    if not data:
        return (0, 0)
    
    MS_PER_DAY = 86_400_000
    window_ms = window_days * MS_PER_DAY
    timestamps = sorted([d['timestamp_ms'] for d in data])
    N = len(timestamps)
    
    best_start_index = 0
    max_count = 0
    start = 0
    end = 0
    
    while start < N:
        window_end_ms = timestamps[start] + window_ms
        while end < N and timestamps[end] < window_end_ms:
            end += 1
        current_count = end - start
        if current_count > max_count:
            max_count = current_count
            best_start_index = start
        start += 1
    
    start_ms = timestamps[best_start_index]
    end_ms = start_ms + window_ms
    return (start_ms, end_ms)

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

@app.route('/')
def index():
    cleanup_old_data()
    return render_template('index.html')

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
    chunk_number = int(request.args.get('chunk_number'))
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
    
    # Store in session
    session['user_code'] = user_code
    
    return jsonify({'code': user_code})

@app.route('/login', methods=['POST'])
def login():
    code = request.form.get('code', '').strip()
    
    if not code:
        return jsonify({'error': 'Code required'}), 400
    
    user_path = os.path.join(app.config['UPLOAD_FOLDER'], code)
    if not os.path.exists(user_path):
        return jsonify({'error': 'Invalid code or data expired'}), 404
    
    session['user_code'] = code
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
        start_ms, end_ms = find_highest_density_period(messages, window_days=days)
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
    prev_response = messages[0] if messages else None
    
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

    return jsonify({
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
        }
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
    
    # Get all words
    word_counts = Counter()
    stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might', 'can', 'i', 'you', 'he', 'she', 'it', 'we', 'they', 'them', 'their', 'this', 'that', 'these', 'those', 'my', 'your', 'his', 'her', 'its', 'our', 'attachment'}
    
    for msg in messages:
        if 'content' in msg:
            words = msg['content'].lower().split()
            for word in words:
                # Remove punctuation
                word = ''.join(c for c in word if c.isalnum())
                if word and word not in stop_words and len(word) > 1:
                    word_counts[word] += 1
    
    if word_counts:
        most_common = word_counts.most_common(10)
        return jsonify({'words': most_common})
    
    return jsonify({'words': []})

@app.route('/api/compute_emoji', methods=['POST'])
def compute_emoji():
    if 'user_code' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    conversation_id = request.json.get('conversation_id')
    messages = load_conversation_data(session['user_code'], conversation_id)

    # Get all emojis
    emoji_counts = Counter()
    for msg in messages:
        if 'content' in msg:
            for char in msg['content']:
                if char in emoji.EMOJI_DATA:
                    emoji_counts[char] += 1

    if emoji_counts:
        most_common = emoji_counts.most_common(10)
        return jsonify({'emojis': most_common})
    
    return jsonify({'emojis': []})

@app.route('/api/count_specific_string', methods=['POST'])
def count_specific_word():
    if 'user_code' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    conversation_id = request.json.get('conversation_id')
    target_string = request.json.get('string', '').lower()
    
    if not target_string:
        return jsonify({'error': 'Target string required'}), 400
    
    messages = load_conversation_data(session['user_code'], conversation_id)

    count = 0
    for msg in messages:
        if 'content' in msg:
            count += msg['content'].lower().count(target_string)
    
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
    target_user_path = os.path.join(app.config['UPLOAD_FOLDER'], target_code, 'inbox')
    target_path = os.path.join(target_user_path, conversation_id)
    
    if not os.path.exists(source_path):
        return jsonify({'error': 'Conversation not found'}), 404
    
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

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=7000, debug=True, threaded=True)