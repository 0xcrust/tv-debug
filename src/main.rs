#![allow(dead_code)]

use std::collections::HashMap;

use chrono::{DateTime, Datelike, Days, NaiveDate, TimeDelta, Timelike, Utc};
use rand::Rng;
use serde::{Deserialize, Serialize};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv()?;
    env_logger::init();

    let upper_bound = Utc::now();
    let lower_bound = upper_bound.checked_sub_days(Days::new(14)).unwrap(); // From two weeks ago
    let resolution = 60;

    log::info!(
        "Running API tests for data availability between {} and {}. Resolution = {}",
        lower_bound,
        upper_bound,
        resolution
    );
    test_time_period_for_api(lower_bound, upper_bound, resolution, Mode::Simple).await?;
    Ok(())
}

enum Mode {
    Simple,
    Randomized { limit: usize },
}

async fn test_time_period_for_api(
    lower_time_bound: DateTime<Utc>,
    upper_time_bound: DateTime<Utc>,
    resolution_minutes: u32,
    mode: Mode,
) -> anyhow::Result<()> {
    match mode {
        Mode::Simple => {
            test_api_for_period(
                resolution_minutes,
                lower_time_bound.timestamp(),
                upper_time_bound.timestamp(),
            )
            .await?;
        }
        Mode::Randomized { limit } => {
            let periods = generate_random_time_periods(lower_time_bound, upper_time_bound, limit);
            for (from, to) in periods {
                test_api_for_period(resolution_minutes, from, to).await?;
            }
        }
    }

    Ok(())
}

/// ${BASE_URL}history?symbol=${symbolInfo.name}&resolution=${apiResolution}&from=${from}&to=${to}
fn make_url(api_resolution: u32, from_ts: i64, to_ts: i64) -> String {
    let base = std::env::var("BASE_URL").expect("BASE_URL env variable is missing");
    format!(
        "{}history?symbol=SOL/USDC&resolution={}&from={}&to={}",
        base, api_resolution, from_ts, to_ts
    )
}

async fn test_api_for_period(resolution_minutes: u32, from: i64, to: i64) -> anyhow::Result<()> {
    let url = make_url(resolution_minutes, from, to);
    let from_utc = DateTime::from_timestamp(from, 0).unwrap();
    let to_utc = DateTime::from_timestamp(to, 0).unwrap();

    log::info!("Getting API results from {} to {}", from_utc, to_utc);
    log::debug!("Start timestamp = {}. End timestamp = {}", from, to);
    log::debug!("Request url: {}", url);

    let result: StructuredApiResult = reqwest::get(url).await?.json::<ApiResult>().await?.into();
    if result.0.is_empty() {
        log::info!("No results gotten for time period");
        return Ok(());
    }

    let mut next_normalized_time =
        next_normalized_time_for_resolution(from_utc, resolution_minutes);
    while next_normalized_time < to_utc {
        let next_ts = next_normalized_time.timestamp();
        match result.0.get(&next_ts) {
            None => log::info!(
                "{}: \x1b[31mX\x1b[0m No candle data found from API",
                next_normalized_time
            ),
            Some(_) => log::info!(
                "{}: \x1b[32m✓\x1b[0m Found candle data from API",
                next_normalized_time
            ),
        }
        next_normalized_time =
            next_normalized_time_for_resolution(next_normalized_time, resolution_minutes);
    }

    Ok(())
}

fn next_normalized_time_for_resolution(
    time: DateTime<Utc>,
    resolution_minutes: u32,
) -> DateTime<Utc> {
    let mut final_time = NaiveDate::from_ymd_opt(time.year(), time.month(), time.day())
        .unwrap()
        .and_hms_opt(time.hour(), 0, 0)
        .unwrap()
        .and_local_timezone(Utc)
        .unwrap();

    while final_time.timestamp() <= time.timestamp() {
        final_time = final_time
            .checked_add_signed(TimeDelta::minutes(resolution_minutes as i64))
            .unwrap();
    }

    final_time
}

fn generate_random_time_periods(
    lower_time_bound: DateTime<Utc>,
    upper_time_bound: DateTime<Utc>,
    limit: usize,
) -> Vec<(i64, i64)> {
    let mut rng = rand::thread_rng();
    let mut vec = Vec::with_capacity(limit);

    for _ in 0..limit {
        let start = rng.gen_range(lower_time_bound.timestamp()..=upper_time_bound.timestamp());
        let end = rng.gen_range(start..=upper_time_bound.timestamp());

        vec.push((start, end))
    }

    vec
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ApiResult {
    s: String,
    time: Vec<i64>,
    close: Vec<f64>,
    open: Vec<f64>,
    high: Vec<f64>,
    low: Vec<f64>,
    volume: Vec<u64>,
}

type Time = i64;
#[derive(Debug, Clone)]
struct StructuredApiResult(HashMap<Time, CandleData>);

#[derive(Debug, Clone)]
struct CandleData {
    close: f64,
    open: f64,
    high: f64,
    low: f64,
    volume: u64,
}

impl From<ApiResult> for StructuredApiResult {
    fn from(value: ApiResult) -> Self {
        let mut hmap = HashMap::with_capacity(value.time.len());
        for (i, time) in value.time.iter().enumerate() {
            hmap.insert(
                *time,
                CandleData {
                    close: value.close[i],
                    open: value.open[i],
                    high: value.high[i],
                    low: value.low[i],
                    volume: value.volume[i],
                },
            );
        }
        StructuredApiResult(hmap)
    }
}

/*
Known periods for which there is missing data(From viewing chart)
- 12 May 21:00 13 May 02:00(No candles inbetween)
- 13 May 05:00 13 May 08:00(No candles inbetween)
- 13 May 10:00 13 May 12:00(No candles inbetween)
*/
async fn static_inspect() -> anyhow::Result<()> {
    let may_12_2100 = NaiveDate::from_ymd_opt(2024, 05, 12)
        .unwrap()
        .and_hms_opt(21, 0, 0)
        .unwrap()
        .and_local_timezone(Utc)
        .unwrap();
    let may_12_2100_ts = may_12_2100.timestamp();
    assert_eq!(may_12_2100_ts, 1715547600);

    let may_13_0200 = NaiveDate::from_ymd_opt(2024, 05, 13)
        .unwrap()
        .and_hms_opt(02, 0, 0)
        .unwrap()
        .and_local_timezone(Utc)
        .unwrap();
    let may_13_0200_ts = may_13_0200.timestamp();
    assert_eq!(may_13_0200_ts, 1715565600);

    let may_13_0500 = NaiveDate::from_ymd_opt(2024, 05, 13)
        .unwrap()
        .and_hms_opt(05, 0, 0)
        .unwrap()
        .and_local_timezone(Utc)
        .unwrap();
    let may_12_0500_ts = may_13_0500.timestamp();
    assert_eq!(may_12_0500_ts, 1715576400);

    let may_13_0800 = NaiveDate::from_ymd_opt(2024, 05, 13)
        .unwrap()
        .and_hms_opt(08, 0, 0)
        .unwrap()
        .and_local_timezone(Utc)
        .unwrap();
    let may_13_0800_ts = may_13_0800.timestamp();
    assert_eq!(may_13_0800_ts, 1715587200);

    let may_13_1000 = NaiveDate::from_ymd_opt(2024, 05, 13)
        .unwrap()
        .and_hms_opt(10, 0, 0)
        .unwrap()
        .and_local_timezone(Utc)
        .unwrap();
    let may_13_1000_ts = may_13_1000.timestamp();
    assert_eq!(may_13_1000_ts, 1715594400);

    let may_13_1200 = NaiveDate::from_ymd_opt(2024, 05, 13)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        .and_local_timezone(Utc)
        .unwrap();
    let may_13_1200_ts = may_13_1200.timestamp();
    assert_eq!(may_13_1200_ts, 1715601600);

    let url = make_url(60, may_12_2100_ts, may_13_0200_ts);
    let result = reqwest::get(url).await?.json::<ApiResult>().await?;
    println!("result: {:#?}", result);
    println!(
        "got bars for date: {}",
        DateTime::from_timestamp(result.time[0], 0).unwrap()
    );

    let url = make_url(60, may_12_0500_ts, may_13_0800_ts);
    let result = reqwest::get(url).await?.json::<ApiResult>().await?;
    println!("result: {:#?}", result);
    println!(
        "got bars for date: {}",
        DateTime::from_timestamp(result.time[0], 0).unwrap()
    );

    let url = make_url(60, may_13_1000_ts, may_13_1200_ts);
    let result = reqwest::get(url).await?.json::<ApiResult>().await?;
    println!("result: {:#?}", result);
    println!(
        "got bars for date: {}",
        DateTime::from_timestamp(result.time[0], 0).unwrap()
    );

    Ok(())
}
