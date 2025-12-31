use pyo3::prelude::*;
use pyo3::types::PyList;

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

#[pymodule]
fn density_finder_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(find_highest_density_period, m)?)?;
    m.add_function(wrap_pyfunction!(find_participant_density_period, m)?)?;
    Ok(())
}