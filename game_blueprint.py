from flask import Blueprint, jsonify, render_template, request, session, redirect, url_for, current_app
import datetime
import json
import os
import secrets
import time
import random
from glob import glob
from collections import Counter
from density_finder_rs import find_highest_density_period  # type: ignore


game_bp = Blueprint('game', __name__)


ROUND_STORE = {}
ROUND_TTL_SECONDS = 60 * 60


def _cleanup_round_store():
    cutoff = time.time() - ROUND_TTL_SECONDS
    expired_keys = [key for key, value in ROUND_STORE.items() if value.get('created_at', 0) < cutoff]
    for key in expired_keys:
        ROUND_STORE.pop(key, None)


def _user_inbox_path(user_code):
    return os.path.join(current_app.config['UPLOAD_FOLDER'], user_code, 'inbox')


def _get_conversations(user_code):
    user_path = _user_inbox_path(user_code)
    if not os.path.exists(user_path):
        return []

    conversations = []
    for folder in os.listdir(user_path):
        folder_path = os.path.join(user_path, folder)
        if not os.path.isdir(folder_path) and not os.path.islink(folder_path):
            continue

        try:
            message_files = glob(os.path.join(folder_path, 'message_*.json'))
            if not message_files:
                continue

            with open(message_files[0], encoding='raw_unicode_escape') as f:
                data = json.loads(f.read().encode('raw_unicode_escape').decode())

            title = data.get('title', folder)
            participants = data.get('participants', [])
            conversations.append({
                'id': folder,
                'title': title,
                'participants': participants,
            })
        except Exception:
            continue

    return sorted(conversations, key=lambda x: x['title'].lower())


def _load_messages(user_code, conversation_id):
    conv_path = os.path.join(_user_inbox_path(user_code), conversation_id)
    messages = []

    for file_path in glob(os.path.join(conv_path, 'message_*.json')):
        with open(file_path, encoding='raw_unicode_escape') as f:
            parsed = json.loads(f.read().encode('raw_unicode_escape').decode())
            messages.extend(parsed.get('messages', []))

    return sorted(messages, key=lambda x: x.get('timestamp_ms', 0))


def _pick_segment(messages, difficulty):
    count = len(messages)
    if count == 0:
        return []

    if difficulty == 'hard':
        desired_min, desired_max = 25, 50
        region_start, region_end = 0.0, 0.45
    elif difficulty == 'medium':
        desired_min, desired_max = 50, 75
        region_start, region_end = 0.25, 0.75
    else:
        desired_min, desired_max = 75, 150
        region_start, region_end = 0.5, 1.0

    seg_len_min = min(desired_min, count)
    seg_len_max = min(desired_max, count)
    seg_len = random.randint(seg_len_min, max(seg_len_min, seg_len_max))

    start_floor = int(count * region_start)
    start_ceiling = int(count * region_end) - seg_len

    if start_ceiling < start_floor:
        start_floor = 0
        start_ceiling = max(0, count - seg_len)

    start_index = random.randint(start_floor, max(start_floor, start_ceiling))
    end_index = min(count, start_index + seg_len)

    return messages[start_index:end_index]


def _extract_participant_names(conversation):
    names = []
    for participant in conversation.get('participants', []):
        if isinstance(participant, dict):
            name = participant.get('name')
            if name:
                names.append(name)
        elif isinstance(participant, str):
            names.append(participant)
    return names


def _detect_user_sender_name(messages, conversation):
    sender_counts = Counter(m.get('sender_name', 'Unknown') for m in messages)
    participant_names = set(_extract_participant_names(conversation))

    not_in_participants = [sender for sender, _ in sender_counts.most_common() if sender not in participant_names]
    if not_in_participants:
        return not_in_participants[0]

    # Fallback for 1:1 where title is usually the other person.
    title = (conversation.get('title') or '').strip()
    if title and len(sender_counts) == 2 and title in sender_counts:
        for sender, _ in sender_counts.most_common():
            if sender != title:
                return sender

    return None


def _load_uploader_username(user_code):
    """Load uploader username from top-level me.json."""
    if not user_code:
        return None

    me_path = os.path.join(current_app.config['UPLOAD_FOLDER'], user_code, 'me.json')
    if not os.path.exists(me_path):
        return None

    try:
        with open(me_path, 'r') as f:
            payload = json.load(f)
        username = payload.get('username')
        if isinstance(username, str) and username.strip():
            return username.strip()
    except Exception:
        return None

    return None


def _label_messages(messages, user_sender_name=None):
    sender_counts = Counter(m.get('sender_name', 'Unknown') for m in messages)
    ordered_senders = [sender for sender, _ in sender_counts.most_common() if sender != user_sender_name]
    sender_map = {sender: f'Person {idx + 1}' for idx, sender in enumerate(ordered_senders)}

    labeled = []
    for message in messages:
        content = message.get('content')
        sender_name = message.get('sender_name', 'Unknown')
        sender_label = 'You' if user_sender_name and sender_name == user_sender_name else sender_map.get(sender_name, 'Person ?')
        labeled.append({
            'timestamp_ms': message.get('timestamp_ms', 0),
            'sender_name': sender_name,
            'sender_label': sender_label,
            'content': content if content else '(attachment)',
        })

    return labeled, sender_map


def _serialize_messages(messages, anonymize, user_sender_name=None):
    if anonymize:
        labeled_messages, _ = _label_messages(messages, user_sender_name=user_sender_name)
        for labeled_msg in labeled_messages:
            sender_label = labeled_msg.get('sender_label', 'Person ?')
            labeled_msg['sender_display'] = sender_label
            # Include actual username for messages from the user
            if sender_label == 'You' and user_sender_name:
                labeled_msg['sender_actual_name'] = user_sender_name
            labeled_msg.pop('sender_label', None)
        return labeled_messages

    visible = []
    for message in messages:
        content = message.get('content')
        visible.append({
            'timestamp_ms': message.get('timestamp_ms', 0),
            'sender_display': message.get('sender_name', 'Unknown'),
            'content': content if content else '(attachment)',
        })
    return visible


def _build_stats(messages, sender_map):
    total_messages = len(messages)
    if total_messages == 0:
        return {
            'total_messages': 0,
            'messages_by_sender': {},
            'attachments_by_sender': {},
            'attachments': 0,
            'oldest_timestamp_ms': None,
            'latest_timestamp_ms': None,
            'overall_avg_per_day': 0,
            'max_density': {'start_ms': 0, 'end_ms': 0, 'count': 0, 'days': 1},
            'gaps': {'avg': 0, 'max': None, 'min': None},
            'responses': {'avg': 0, 'max': None, 'min': None},
            'average_message_length': {'overall': 0, 'by_sender': {}},
        }

    messages_by_sender = Counter()
    attachments_by_sender = Counter()
    attachment_total = 0

    for msg in messages:
        sender = msg.get('sender_name', 'Unknown')
        sender_label = sender_map.get(sender, 'Person ?')
        messages_by_sender[sender_label] += 1
        if 'content' not in msg:
            attachment_total += 1
            attachments_by_sender[sender_label] += 1

    oldest = messages[0]
    latest = messages[-1]

    day_span = (latest.get('timestamp_ms', 0) - oldest.get('timestamp_ms', 0)) / 86_400_000
    if day_span > 1:
        overall_avg = total_messages / day_span
    else:
        overall_avg = float(total_messages)

    max_density = {'start_ms': 0, 'end_ms': 0, 'count': 0, 'days': 1}
    for days in range(1, 31):
        start_ms, end_ms = find_highest_density_period(messages, days)
        count = sum(1 for m in messages if start_ms <= m.get('timestamp_ms', 0) < end_ms)
        if (count / days) > (max_density['count'] / max_density['days']):
            max_density = {
                'start_ms': start_ms,
                'end_ms': end_ms,
                'count': count,
                'days': days,
            }

    def snapshot_message(msg):
        if not msg:
            return None
        content = msg.get('content')
        return {
            'timestamp_ms': msg.get('timestamp_ms'),
            'sender_name': sender_map.get(msg.get('sender_name', 'Unknown'), msg.get('sender_name', 'Unknown')),
            'content': content if content else '(attachment)',
        }

    max_gap = {'time': 0, 'from': None, 'to': None, 'msg1': None, 'msg2': None}
    min_gap = {'time': float('inf'), 'from': None, 'to': None, 'msg1': None, 'msg2': None}
    total_gaps = 0
    gap_count = 0

    for idx in range(1, total_messages):
        prev_msg = messages[idx - 1]
        cur_msg = messages[idx]
        gap = cur_msg.get('timestamp_ms', 0) - prev_msg.get('timestamp_ms', 0)
        total_gaps += gap
        gap_count += 1

        if gap > max_gap['time']:
            max_gap = {
                'time': gap,
                'from': prev_msg.get('timestamp_ms'),
                'to': cur_msg.get('timestamp_ms'),
                'msg1': snapshot_message(prev_msg),
                'msg2': snapshot_message(cur_msg),
            }
        if gap < min_gap['time']:
            min_gap = {
                'time': gap,
                'from': prev_msg.get('timestamp_ms'),
                'to': cur_msg.get('timestamp_ms'),
                'msg1': snapshot_message(prev_msg),
                'msg2': snapshot_message(cur_msg),
            }

    avg_gap = (total_gaps / gap_count) if gap_count else 0
    if min_gap['time'] == float('inf'):
        min_gap = None

    max_response = {'time': 0, 'from': None, 'to': None, 'msg1': None, 'msg2': None}
    min_response = {'time': float('inf'), 'from': None, 'to': None, 'msg1': None, 'msg2': None}
    total_responses = 0
    response_count = 0
    prev_response = messages[0]

    for idx in range(1, total_messages):
        current = messages[idx]
        if current.get('sender_name') != prev_response.get('sender_name'):
            response_time = current.get('timestamp_ms', 0) - prev_response.get('timestamp_ms', 0)
            total_responses += response_time
            response_count += 1

            if response_time > max_response['time']:
                max_response = {
                    'time': response_time,
                    'from': prev_response.get('timestamp_ms'),
                    'to': current.get('timestamp_ms'),
                    'msg1': snapshot_message(prev_response),
                    'msg2': snapshot_message(current),
                }
            if response_time < min_response['time']:
                min_response = {
                    'time': response_time,
                    'from': prev_response.get('timestamp_ms'),
                    'to': current.get('timestamp_ms'),
                    'msg1': snapshot_message(prev_response),
                    'msg2': snapshot_message(current),
                }

            prev_response = current

    avg_response = (total_responses / response_count) if response_count else 0
    if min_response['time'] == float('inf'):
        min_response = None

    avg_length_by_sender = {}
    for sender, label in sender_map.items():
        sender_messages = [m for m in messages if m.get('sender_name') == sender]
        if sender_messages:
            avg_length_by_sender[label] = sum(len(m.get('content', '')) for m in sender_messages) / len(sender_messages)

    return {
        'total_messages': total_messages,
        'messages_by_sender': dict(messages_by_sender),
        'attachments_by_sender': dict(attachments_by_sender),
        'attachments': attachment_total,
        'oldest_timestamp_ms': oldest.get('timestamp_ms'),
        'latest_timestamp_ms': latest.get('timestamp_ms'),
        'overall_avg_per_day': overall_avg,
        'max_density': max_density,
        'gaps': {'avg': avg_gap, 'max': max_gap, 'min': min_gap},
        'responses': {'avg': avg_response, 'max': max_response, 'min': min_response},
        'average_message_length': {
            'overall': sum(len(m.get('content', '')) for m in messages) / total_messages,
            'by_sender': avg_length_by_sender,
        },
    }


def _sample_period_messages(messages, period_days):
    if not messages:
        return []

    latest_ts = messages[-1].get('timestamp_ms', 0)
    period_start = latest_ts - (period_days * 86_400_000)
    sampled = [m for m in messages if m.get('timestamp_ms', 0) >= period_start]

    if not sampled:
        return messages

    return sampled


def _trim_stats_for_difficulty(stats, difficulty):
    if difficulty != 'hard':
        return stats

    # Hard stats mode intentionally exposes only three high-level stat groups.
    return {
        'total_messages': stats.get('total_messages', 0),
        'messages_by_sender': stats.get('messages_by_sender', {}),
        'overall_avg_per_day': stats.get('overall_avg_per_day', 0),
    }


@game_bp.route('/game')
def game_page():
    if 'user_code' not in session:
        return redirect(url_for('index'))
    return render_template('game.html')


@game_bp.route('/api/game/options')
def game_options():
    if 'user_code' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    conversations = _get_conversations(session['user_code'])
    options = [
        {
            'id': c['id'],
            'title': c['title'],
            'participant_count': len(c.get('participants', [])),
        }
        for c in conversations
    ]
    return jsonify({'options': options})


@game_bp.route('/api/game/round', methods=['POST'])
def game_round():
    if 'user_code' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    payload = request.get_json(silent=True) or {}
    mode = (payload.get('mode') or 'message').strip().lower()
    difficulty = (payload.get('difficulty') or 'medium').strip().lower()

    if mode not in ('message', 'stats'):
        return jsonify({'error': 'Invalid mode'}), 400

    if difficulty not in ('easy', 'medium', 'hard'):
        return jsonify({'error': 'Invalid difficulty'}), 400

    _cleanup_round_store()
    user_code = session['user_code']

    conversations = _get_conversations(user_code)
    valid_conversations = []
    for conv in conversations:
        msg_count_hint = 0
        try:
            first_file = glob(os.path.join(_user_inbox_path(user_code), conv['id'], 'message_*.json'))
            if first_file:
                with open(first_file[0], encoding='raw_unicode_escape') as f:
                    parsed = json.loads(f.read().encode('raw_unicode_escape').decode())
                    msg_count_hint = len(parsed.get('messages', []))
        except Exception:
            pass

        if msg_count_hint > 0:
            valid_conversations.append(conv)

    if not valid_conversations:
        return jsonify({'error': 'No conversations with messages found'}), 404

    chosen = random.choice(valid_conversations)
    messages = _load_messages(user_code, chosen['id'])
    if not messages:
        return jsonify({'error': 'Selected conversation has no messages'}), 404

    sender_counts = Counter(m.get('sender_name', 'Unknown') for m in messages)
    sender_map = {sender: f'Person {idx + 1}' for idx, (sender, _) in enumerate(sender_counts.most_common())}
    uploader_username = _load_uploader_username(user_code)

    response_payload = {
        'mode': mode,
        'difficulty': difficulty,
        'hint': {
            'is_group_chat': len(chosen.get('participants', [])) > 2,
            'participant_count': len(chosen.get('participants', [])),
            'total_history_messages': len(messages),
        },
    }

    if mode == 'message':
        segment = _pick_segment(messages, difficulty)
        # Only trust persisted uploader identity for right/blue message placement.
        user_sender_name = uploader_username
        response_payload['messages'] = _serialize_messages(segment, anonymize=True, user_sender_name=user_sender_name)
    else:
        period_for_stats = random.choice([7, 14, 30, 60, 90])
        scoped_messages = _sample_period_messages(messages, period_for_stats)
        built_stats = _build_stats(scoped_messages, sender_map)
        response_payload['locked_period_days'] = period_for_stats
        response_payload['stats'] = _trim_stats_for_difficulty(built_stats, difficulty)

    round_id = secrets.token_urlsafe(16)
    ROUND_STORE[round_id] = {
        'created_at': time.time(),
        'user_code': user_code,
        'conversation_id': chosen['id'],
        'title': chosen['title'],
        'participant_count': len(chosen.get('participants', [])),
        'mode': mode,
    }

    response_payload['round_id'] = round_id
    return jsonify(response_payload)


@game_bp.route('/api/game/guess', methods=['POST'])
def game_guess():
    if 'user_code' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    payload = request.get_json(silent=True) or {}
    round_id = (payload.get('round_id') or '').strip()
    guessed_conversation_id = (payload.get('conversation_id') or '').strip()

    if not round_id or not guessed_conversation_id:
        return jsonify({'error': 'round_id and conversation_id are required'}), 400

    _cleanup_round_store()
    round_data = ROUND_STORE.get(round_id)

    if not round_data:
        return jsonify({'error': 'Round not found or expired'}), 404

    if round_data.get('user_code') != session.get('user_code'):
        return jsonify({'error': 'Round does not belong to this user'}), 403

    is_correct = guessed_conversation_id == round_data.get('conversation_id')

    return jsonify({
        'correct': is_correct,
        'correct_conversation_id': round_data.get('conversation_id'),
        'correct_title': round_data.get('title'),
        'participant_count': round_data.get('participant_count'),
        'mode': round_data.get('mode'),
    })
