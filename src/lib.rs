use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::{HashMap, HashSet};
use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc};

const MS_PER_DAY: u64 = 86_400_000;


#[pyfunction]
fn find_highest_density_period(data: &Bound<'_, PyList>, period: u8) -> (u64, u64) {
    /*
    Finds the period of `window_days` with the highest density of points.

    Args:
        data: A list of dictionaries, each containing a 'timestamp_ms' key.
        window_days: The size of the sliding window in days.

    Returns:
        A tuple (start_ms, end_ms) representing the start and end 
        milliseconds of the highest density period.
    */

    let n = data.len(); // 
    if n <= 1 {
        return (0, 0);
    }

    let window_ms = (period as u64) * MS_PER_DAY;

    let mut timestamps: Vec<u64> = Vec::with_capacity(n);
    for item in data.iter() {
        let dict = item.cast::<pyo3::types::PyDict>().unwrap();
        let ts: u64 = dict.get_item("timestamp_ms").unwrap().unwrap().cast::<pyo3::types::PyInt>().unwrap().extract().unwrap();
        timestamps.push(ts);
    }

    let mut best_start_index: usize = 0;
    let mut best_end_index: usize = 0;
    let mut max_count: u64 = 0;
    let mut start_index: usize = 0;
    let mut end_index: usize = 0;

    while start_index < n {
        while end_index < n && timestamps[end_index] - timestamps[start_index] <= window_ms {
            end_index += 1;
        }
        let count = (end_index - start_index) as u64;
        if count > max_count {
            max_count = count;
            best_start_index = start_index;
            best_end_index = end_index;
        }
        start_index += 1;
    }

    let start_ms = timestamps[best_start_index];
    // Ensure end_ms doesn't exceed the last timestamp
    // this is actually guaranteed if we extract the end_ms from the timestamps array
    let end_ms = timestamps[best_end_index - 1];

    return (start_ms, end_ms);
}

struct Message {
    timestamp_ms: u64,
    sender_name: String,
}


// max time w/o optimizations: 65-72 seconds for a 1-day period on 30k messages
// with optimizations: 3 seconds
#[pyfunction]
fn find_participant_max_count_period(data: &Bound<'_, PyList>, period: u8, participant: String) -> (u64, u64) {
    /*
    Finds the period of `period` days with the highest count of messages from a specific participant.

    Args:
        data: A list of dictionaries, each containing 'timestamp_ms' and 'sender_name' keys.
        period: The size of the sliding window in days.
        participant: The name of the participant to filter by.

    Returns:
        A tuple (start_ms, end_ms) representing the start and end 
        milliseconds of the period.
    */
    let n = data.len();
    if n <= 1 {
        return (0, 0);
    }

    let window_ms = (period as u64) * MS_PER_DAY;
    let messages: Vec<Message> = data.iter().map(|item| {
        let dict = item.cast::<pyo3::types::PyDict>().unwrap();
        let ts: u64 = dict.get_item("timestamp_ms").unwrap().unwrap().cast::<pyo3::types::PyInt>().unwrap().extract().unwrap();
        let sender: String = dict.get_item("sender_name").unwrap().unwrap().extract().unwrap();
        Message {
            timestamp_ms: ts,
            sender_name: sender,
        }
    }).collect();
    let end_ms = messages.last().unwrap().timestamp_ms;
    let mut max_count = 0;
    let mut best_start_index = 0;
    let mut best_end_index = 0;
    for (ind, msg) in messages.iter().enumerate() {
        if msg.timestamp_ms > end_ms - window_ms {
            println!("Skipping message at index {}: {:#?}", ind, (msg.timestamp_ms, msg.sender_name.clone()));
            break;
        }
        // optimized version: ...ms
        let (byperiod, tot): (usize, usize) = {
            let mut count = 0;
            let mut tot = 0;
            for msg2 in messages[ind..].iter() {
                if msg2.timestamp_ms >= msg.timestamp_ms + window_ms {
                    break;
                }
                tot += 1;
                if msg2.sender_name == participant {
                    count += 1;
                }
            }
            (count, tot)
        };
        

        if byperiod > max_count {
            max_count = byperiod;
            best_start_index = ind;
            // Find the end index for the best period
            best_end_index = std::cmp::min(ind + tot - 1, n - 1); // end index // clamp it for safety but theoretically should not be required
            // unfortunately we can't break early here like we can in the min case
        }
    }

    return (messages[best_start_index].timestamp_ms, messages[best_end_index].timestamp_ms);
}

#[pyfunction]
fn find_participant_min_count_period(data: &Bound<'_, PyList>, period: u8, participant: String) -> (u64, u64) {
    /*
    Finds the period of `period` days with the lowest density of messages from a specific participant
    Args:
        data: A list of dictionaries, each containing 'timestamp_ms' and 'sender_name' keys.
        period: The size of the sliding window in days.
        participant: The name of the participant to filter by.
    Returns:
        A tuple (start_ms, end_ms) representing the start and end 
        milliseconds of the period.
    */

    let n = data.len();
    if n <= 1 {
        return (0, 0);
    }
    let window_ms = (period as u64) * MS_PER_DAY;
    let messages: Vec<Message> = data.iter().map(|item| {
        let dict = item.cast::<pyo3::types::PyDict>().unwrap();
        let ts: u64 = dict.get_item("timestamp_ms").unwrap().unwrap().cast::<pyo3::types::PyInt>().unwrap().extract().unwrap();
        let sender: String = dict.get_item("sender_name").unwrap().unwrap().extract().unwrap();
        Message {
            timestamp_ms: ts,
            sender_name: sender,
        }
    }).collect();
    let end_ms = messages.last().unwrap().timestamp_ms;
    //return (0, end_ms); // debugging and REMEMBER TO REMOVE
    let mut min_count = usize::MAX;
    let mut best_start_index = 0;
    let mut best_end_index = 0;
    for (ind, msg) in messages.iter().enumerate() {
        if msg.timestamp_ms > end_ms - window_ms {
            println!("Skipping message at index {}: {:#?}", ind, (msg.timestamp_ms, msg.sender_name.clone()));
            break;
        }
        // optimized version: ...ms
        let (byperiod, tot): (usize, usize) = {
            let mut count = 0;
            let mut tot = 0;
            for msg2 in messages[ind..].iter() {
                if msg2.timestamp_ms >= msg.timestamp_ms + window_ms {
                    break;
                }
                tot += 1;
                if msg2.sender_name == participant {
                    count += 1;
                }
            }
            (count, tot)
        };
        
        // // original code: ...ms
        // let byperiod = messages.iter()
        //     .filter(|m| m.sender_name == participant && m.timestamp_ms >= msg.timestamp_ms && m.timestamp_ms < msg.timestamp_ms + window_ms)
        //     .count();

        if byperiod < min_count {
            min_count = byperiod;
            best_start_index = ind;
            // Find the end index for the best period
            // this best index will not work because byperiod is only counting messages from participant, not all messages
            // so we need to recalculate it
            // use tot value for that
            best_end_index = std::cmp::min(ind + tot - 1, n - 1); // end index // clamp it for safety but theoretically should not be required
            if byperiod == 0 {
                break; // can't do better than zero
            }
        }
    }

    return (messages[best_start_index].timestamp_ms, messages[best_end_index].timestamp_ms);
}

// #[pyfunction]
// fn find_participant_density_period(data: &Bound<'_, PyList>, period: u8, participant: String, find_max: bool) -> (u64, u64) {
//     /*
//     Finds the period of `period` days with the highest or lowest density of messages from a specific participant.

//     Args:
//         data: A list of dictionaries, each containing 'timestamp_ms' and 'sender_name' keys.
//         period: The size of the sliding window in days.
//         participant: The name of the participant to filter by.
//         find_max: If true, find the period with maximum messages; if false, find minimum.

//     Returns:
//         A tuple (start_ms, end_ms) representing the start and end 
//         milliseconds of the period.
//     */

//     let n = data.len();
//     if n <= 1 {
//         return (0, 0);
//     }

//     let window_ms = (period as u64) * MS_PER_DAY;

//     // Extract timestamps for the specific participant
//     let mut participant_timestamps: Vec<u64> = Vec::new();
//     let mut all_timestamps: Vec<u64> = Vec::new();
    
//     for item in data.iter() {
//         let dict = item.cast::<pyo3::types::PyDict>().unwrap();
//         let ts: u64 = dict.get_item("timestamp_ms").unwrap().unwrap().cast::<pyo3::types::PyInt>().unwrap().extract().unwrap();
//         all_timestamps.push(ts);
        
//         let sender: String = dict.get_item("sender_name").unwrap().unwrap().extract().unwrap();
//         if sender == participant {
//             participant_timestamps.push(ts);
//         }
//     }

//     if all_timestamps.is_empty() {
//         return (0, 0);
//     }

//     all_timestamps.sort_unstable();
//     participant_timestamps.sort_unstable();

//     let last_timestamp = all_timestamps[all_timestamps.len() - 1];
//     let mut best_start_ms: u64 = all_timestamps[0];
//     let mut best_end_ms: u64 = std::cmp::min(all_timestamps[0] + window_ms, last_timestamp);
//     let mut best_count: usize = if find_max { 0 } else { usize::MAX };

//     // Slide window across all timestamps
//     for start_ts in &all_timestamps {
//         let calculated_end_ts = start_ts + window_ms;
//         // Ensure end_ts doesn't exceed the last timestamp
//         let end_ts = std::cmp::min(calculated_end_ts, last_timestamp);
        
//         // Count participant messages in this window
//         let count = participant_timestamps.iter()
//             .filter(|&&ts| ts >= *start_ts && ts < end_ts)
//             .count();

//         if (find_max && count > best_count) || (!find_max && count < best_count) {
//             best_count = count;
//             best_start_ms = *start_ts;
//             best_end_ms = end_ts;
//         }
//     }

//     (best_start_ms, best_end_ms)
// }

#[pyfunction]
fn find_participant_density_period(data: &Bound<'_, PyList>, period: u8, participant: String, find_max: bool) -> (u64, u64) {
    if find_max {
        find_participant_max_count_period(data, period, participant)
    } else {
        find_participant_min_count_period(data, period, participant)
    }
}

#[pyfunction]
fn compute_top_words(data: &Bound<'_, PyList>, top_n: usize) -> Vec<(String, usize)> {
    let stop_words: HashSet<&str> = [
        "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with", "by", "from", "as", "is", "was", "are", "were", "be", "been", "being", "have", "has", "had", "do", "does", "did", "will", "would", "could", "should", "may", "might", "can", "i", "you", "he", "she", "it", "we", "they", "them", "their", "this", "that", "these", "those", "my", "your", "his", "her", "its", "our", "attachment",
    ]
    .into_iter()
    .collect();

    let mut word_counts: HashMap<String, usize> = HashMap::new();

    for item in data.iter() {
        let dict = match item.cast::<pyo3::types::PyDict>() {
            Ok(d) => d,
            Err(_) => continue,
        };

        let content_obj = match dict.get_item("content") {
            Ok(Some(v)) => v,
            _ => continue,
        };

        let content: String = match content_obj.extract() {
            Ok(c) => c,
            Err(_) => continue,
        };

        for raw_word in content.to_lowercase().split_whitespace() {
            let cleaned: String = raw_word.chars().filter(|c| c.is_alphanumeric()).collect();
            if cleaned.chars().count() > 1 && !stop_words.contains(cleaned.as_str()) {
                *word_counts.entry(cleaned).or_insert(0) += 1;
            }
        }
    }

    let mut pairs: Vec<(String, usize)> = word_counts.into_iter().collect();
    pairs.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    pairs.truncate(top_n);
    pairs
}

#[pyfunction]
fn compute_top_emojis(data: &Bound<'_, PyList>, top_n: usize) -> Vec<(String, usize)> {
    let mut emoji_counts: HashMap<String, usize> = HashMap::new();

    for item in data.iter() {
        let dict = match item.cast::<pyo3::types::PyDict>() {
            Ok(d) => d,
            Err(_) => continue,
        };

        let content_obj = match dict.get_item("content") {
            Ok(Some(v)) => v,
            _ => continue,
        };

        let content: String = match content_obj.extract() {
            Ok(c) => c,
            Err(_) => continue,
        };

        let mut buf = [0u8; 4];
        for ch in content.chars() {
            let ch_str = ch.encode_utf8(&mut buf);
            if emojis::get(ch_str).is_some() {
                *emoji_counts.entry(ch.to_string()).or_insert(0) += 1;
            }
        }
    }

    let mut pairs: Vec<(String, usize)> = emoji_counts.into_iter().collect();
    pairs.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    pairs.truncate(top_n);
    pairs
}

#[pyfunction]
fn count_specific_string(data: &Bound<'_, PyList>, target_string: String) -> usize {
    if target_string.is_empty() {
        return 0;
    }

    let needle = target_string.to_lowercase();
    let mut total = 0usize;

    for item in data.iter() {
        let dict = match item.cast::<pyo3::types::PyDict>() {
            Ok(d) => d,
            Err(_) => continue,
        };

        let content_obj = match dict.get_item("content") {
            Ok(Some(v)) => v,
            _ => continue,
        };

        let content: String = match content_obj.extract() {
            Ok(c) => c,
            Err(_) => continue,
        };

        let lower = content.to_lowercase();
        let mut i = 0usize;
        while let Some(rel) = lower[i..].find(&needle) {
            total += 1;
            i += rel + needle.len();
            if i >= lower.len() {
                break;
            }
        }
    }

    total
}

type TrendSeries = (Vec<String>, Vec<u64>, Vec<f64>, usize);
type UploaderSeries = (Vec<String>, Vec<u64>, Vec<f64>, usize, Vec<String>, Vec<u64>, Vec<f64>, usize);

fn date_key_from_timestamp_ms(timestamp_ms: u64) -> Option<String> {
    let ts = i64::try_from(timestamp_ms).ok()?;
    let dt: DateTime<Utc> = DateTime::from_timestamp_millis(ts)?;
    Some(dt.format("%Y-%m-%d").to_string())
}

fn parse_valid_date_key(key: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(key, "%Y-%m-%d").ok()
}

fn moving_average(values: &[u64], window_size: usize) -> Vec<f64> {
    if window_size == 0 {
        return vec![0.0; values.len()];
    }

    let mut out = Vec::with_capacity(values.len());
    let mut running_sum = 0u64;

    for (i, value) in values.iter().enumerate() {
        running_sum += *value;
        if i >= window_size {
            running_sum -= values[i - window_size];
        }

        let sample_len = std::cmp::min(window_size, i + 1) as f64;
        let avg = (running_sum as f64) / sample_len;
        out.push((avg * 100.0).round() / 100.0);
    }

    out
}

fn build_trend_series_from_daily_totals(daily_totals: &HashMap<String, u64>) -> (TrendSeries, TrendSeries) {
    let mut sorted_daily_keys: Vec<String> = daily_totals.keys().cloned().collect();
    sorted_daily_keys.sort();
    let daily_values: Vec<u64> = sorted_daily_keys
        .iter()
        .map(|k| *daily_totals.get(k).unwrap_or(&0))
        .collect();
    let daily_trend = moving_average(&daily_values, 7);

    let mut weekly_totals: HashMap<String, u64> = HashMap::new();
    for (date_key, total) in daily_totals {
        if let Some(date) = parse_valid_date_key(date_key) {
            let weekday_offset = i64::from(date.weekday().num_days_from_monday());
            let week_start = (date - Duration::days(weekday_offset))
                .format("%Y-%m-%d")
                .to_string();
            *weekly_totals.entry(week_start).or_insert(0) += *total;
        }
    }

    let mut sorted_weekly_keys: Vec<String> = weekly_totals.keys().cloned().collect();
    sorted_weekly_keys.sort();
    let weekly_values: Vec<u64> = sorted_weekly_keys
        .iter()
        .map(|k| *weekly_totals.get(k).unwrap_or(&0))
        .collect();
    let weekly_trend = moving_average(&weekly_values, 4);

    (
        (sorted_daily_keys, daily_values, daily_trend, 7),
        (sorted_weekly_keys, weekly_values, weekly_trend, 4),
    )
}

fn normalize_daily_counts_from_pydict(daily_counts: &Bound<'_, PyDict>) -> HashMap<String, u64> {
    let mut normalized: HashMap<String, u64> = HashMap::new();

    for (key_obj, value_obj) in daily_counts.iter() {
        let date_key: String = match key_obj.extract() {
            Ok(k) => k,
            Err(_) => continue,
        };

        if date_key.len() != 10 || parse_valid_date_key(&date_key).is_none() {
            continue;
        }

        let count_i64: i64 = match value_obj.extract() {
            Ok(v) => v,
            Err(_) => continue,
        };

        if count_i64 < 0 {
            continue;
        }

        let count_u64 = count_i64 as u64;
        *normalized.entry(date_key).or_insert(0) += count_u64;
    }

    normalized
}

#[pyfunction]
fn aggregate_daily_counts(data: &Bound<'_, PyList>) -> HashMap<String, u64> {
    let mut daily_counts: HashMap<String, u64> = HashMap::new();

    for item in data.iter() {
        let dict = match item.cast::<PyDict>() {
            Ok(d) => d,
            Err(_) => continue,
        };

        let ts_obj = match dict.get_item("timestamp_ms") {
            Ok(Some(v)) => v,
            _ => continue,
        };

        let ts: u64 = match ts_obj.extract() {
            Ok(v) => v,
            Err(_) => continue,
        };

        if let Some(day_key) = date_key_from_timestamp_ms(ts) {
            *daily_counts.entry(day_key).or_insert(0) += 1;
        }
    }

    daily_counts
}

#[pyfunction]
fn split_sent_received_daily_counts(
    data: &Bound<'_, PyList>,
    uploader_username: String,
) -> (HashMap<String, u64>, HashMap<String, u64>) {
    let mut sent_daily_counts: HashMap<String, u64> = HashMap::new();
    let mut received_daily_counts: HashMap<String, u64> = HashMap::new();

    for item in data.iter() {
        let dict = match item.cast::<PyDict>() {
            Ok(d) => d,
            Err(_) => continue,
        };

        let ts_obj = match dict.get_item("timestamp_ms") {
            Ok(Some(v)) => v,
            _ => continue,
        };
        let ts: u64 = match ts_obj.extract() {
            Ok(v) => v,
            Err(_) => continue,
        };

        let sender_obj = match dict.get_item("sender_name") {
            Ok(Some(v)) => v,
            _ => continue,
        };
        let sender_name: String = match sender_obj.extract() {
            Ok(v) => v,
            Err(_) => continue,
        };

        if let Some(day_key) = date_key_from_timestamp_ms(ts) {
            if sender_name == uploader_username {
                *sent_daily_counts.entry(day_key).or_insert(0) += 1;
            } else {
                *received_daily_counts.entry(day_key).or_insert(0) += 1;
            }
        }
    }

    (sent_daily_counts, received_daily_counts)
}

#[pyfunction]
fn build_group_chat_trends_series(data: &Bound<'_, PyList>) -> (TrendSeries, TrendSeries) {
    let mut daily_totals: HashMap<String, u64> = HashMap::new();

    for item in data.iter() {
        let group_chat = match item.cast::<PyDict>() {
            Ok(v) => v,
            Err(_) => continue,
        };

        let daily_counts_obj = match group_chat.get_item("daily_counts") {
            Ok(Some(v)) => v,
            _ => continue,
        };
        let daily_counts = match daily_counts_obj.cast::<PyDict>() {
            Ok(v) => v,
            Err(_) => continue,
        };

        let normalized = normalize_daily_counts_from_pydict(&daily_counts);
        for (date_key, count) in normalized {
            *daily_totals.entry(date_key).or_insert(0) += count;
        }
    }

    build_trend_series_from_daily_totals(&daily_totals)
}

#[pyfunction]
fn build_uploader_trends_series(
    sent_daily_counts: &Bound<'_, PyDict>,
    received_daily_counts: &Bound<'_, PyDict>,
) -> (UploaderSeries, UploaderSeries) {
    let sent_normalized = normalize_daily_counts_from_pydict(sent_daily_counts);
    let received_normalized = normalize_daily_counts_from_pydict(received_daily_counts);

    let (sent_daily, sent_weekly) = build_trend_series_from_daily_totals(&sent_normalized);
    let (received_daily, received_weekly) = build_trend_series_from_daily_totals(&received_normalized);

    (
        (
            sent_daily.0,
            sent_daily.1,
            sent_daily.2,
            sent_daily.3,
            sent_weekly.0,
            sent_weekly.1,
            sent_weekly.2,
            sent_weekly.3,
        ),
        (
            received_daily.0,
            received_daily.1,
            received_daily.2,
            received_daily.3,
            received_weekly.0,
            received_weekly.1,
            received_weekly.2,
            received_weekly.3,
        ),
    )
}

#[pymodule]
fn density_finder_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(find_highest_density_period, m)?)?;
    m.add_function(wrap_pyfunction!(find_participant_density_period, m)?)?;
    m.add_function(wrap_pyfunction!(compute_top_words, m)?)?;
    m.add_function(wrap_pyfunction!(compute_top_emojis, m)?)?;
    m.add_function(wrap_pyfunction!(count_specific_string, m)?)?;
    m.add_function(wrap_pyfunction!(aggregate_daily_counts, m)?)?;
    m.add_function(wrap_pyfunction!(split_sent_received_daily_counts, m)?)?;
    m.add_function(wrap_pyfunction!(build_group_chat_trends_series, m)?)?;
    m.add_function(wrap_pyfunction!(build_uploader_trends_series, m)?)?;
    Ok(())
}