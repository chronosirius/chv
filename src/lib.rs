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
    let end_ms = timestamps[best_end_index - 1] + 1;

    return (start_ms, end_ms);
}

#[pymodule]
fn density_finder_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(find_highest_density_period, m)?)?;
    Ok(())
}