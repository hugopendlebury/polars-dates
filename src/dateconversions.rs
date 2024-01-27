use std::collections::HashMap;
use std::io::Error;

use polars::prelude::*;
use lazy_static::lazy_static;
use chrono::{LocalResult, NaiveDateTime, TimeZone, DateTime};

use chrono_tz::Tz;
use pyo3_polars::export::polars_core::error::PolarsError;

use pyo3_polars::export::polars_core::utils::arrow::temporal_conversions::{
    timestamp_ms_to_datetime, timestamp_ns_to_datetime, timestamp_us_to_datetime,
};

use tzf_rs::DefaultFinder;

lazy_static! {
    static ref FINDER: DefaultFinder = DefaultFinder::new();
}


use std::mem;

//Rust doesn't impletement eq for f64 so split the f64 into parts
fn integer_decode(val: f64) -> (u64, i16, i8) {
    let bits: u64 = unsafe { mem::transmute(val) };
    let sign: i8 = if bits >> 63 == 0 { 1 } else { -1 };
    let mut exponent: i16 = ((bits >> 52) & 0x7ff) as i16;
    let mantissa = if exponent == 0 {
        (bits & 0xfffffffffffff) << 1
    } else {
        (bits & 0xfffffffffffff) | 0x10000000000000
    };

    exponent -= 1023 + 52;
    (mantissa, exponent, sign)
}

#[derive(Hash, Eq, PartialEq, Clone, Copy)]
struct Distance((u64, i16, i8));

impl Distance {
    fn new(val: f64) -> Distance {
        Distance(integer_decode(val))
    }
}

#[derive(Eq, Hash, PartialEq, Clone, Copy)]
struct Coordinates{
    lat: Distance,
    lon: Distance
}

#[derive(Eq, Hash, PartialEq, Clone, Copy)]
struct CoodinateTime{
    lcation: Coordinates,
    dt: i64
}

pub(crate) fn impl_lookup_timezone(
    lat: &Series,
    lons: &Series
)  -> PolarsResult<Series> {
    let mut cache = HashMap::<Coordinates, &str>::new();
    let lats_iter = lat.f64()?.into_iter();
    let lons_iter = lons.f64()?.into_iter();
    cache.get(&Coordinates{lat: Distance::new(1.1), lon:Distance::new(1.2)});
    let results = lats_iter.zip(lons_iter).map(|coords| {
        
        let lat = coords.0.map_or(0.0, |f| f);
        let lng = coords.1.map_or(0.0, |f| f);
        let lkp_key = Coordinates{lat: Distance::new(lat), lon:Distance::new(lng)};
        let cache_key = cache.get(&lkp_key);

        match cache_key {
            Some(key) => key,
            None => {
                let timezone_names = FINDER.get_tz_names(lng, lat);
                let time_zone = timezone_names.last().map_or("UNKNOWN", |f| f);
                cache.insert(lkp_key, time_zone);
                time_zone
            }
        }


    });

    Ok(Series::from_iter(results))
}

fn parse_time_zone(time_zone: &str) -> Result<Tz, PolarsError> {
    let x: Result<Tz, String> = time_zone.parse::<Tz>();
    match x {
        Ok(r) => { Ok(r)},
        Err(_) => Err(PolarsError::Io(Error::new(std::io::ErrorKind::Unsupported, 
            format!("Unable to convert timezone {}", time_zone))))
    }
}

enum Ambiguous {
    Earliest,
    Latest,
    Raise
}

pub(crate) fn impl_to_local_in_new_timezone(
    dates: &Series,
    lat: &Series,
    lons: &Series,
    from_tz: &str,
    ambiguous: &str,
)  -> PolarsResult<Series> {
    let dtype = dates.dtype();

    let from_time_zone = "UTC";
    let from_tz = parse_time_zone(from_time_zone)?;


    let mut coordinates_cache = HashMap::<Coordinates, &str>::new();
    let mut dates_cache = HashMap::<CoodinateTime, NaiveDateTime>::new();
    
    let dates_iter = dates.datetime()?.into_iter();
    let lats_iter = lat.f64()?.into_iter();
    let lons_iter = lons.f64()?.into_iter();

    let results = lats_iter.zip(lons_iter).
            zip(dates_iter).map(|coords| {
        
                let lat = coords.0.0.map_or(0.0, |f| f);
                let lng = coords.0.1.map_or(0.0, |f| f);
                let coordinates = Coordinates{lat: Distance::new(lat), lon:Distance::new(lng)};
                let cache_key = coordinates_cache.get(&coordinates);

                let time_zone = match cache_key {
                    Some(key) => key,
                    None => {
                        let timezone_names = FINDER.get_tz_names(lng, lat);
                        let time_zone = timezone_names.last().map_or("UNKNOWN", |f| f);
                        coordinates_cache.insert(coordinates, time_zone);
                        time_zone
                    }
                };
                
                //let tz  = parse_time_zone(time_zone);
                let timestamp = coords.1;
            

                let timestamp_to_datetime: fn(i64) -> NaiveDateTime = match dtype {
                    DataType::Datetime(TimeUnit::Microseconds, _) => timestamp_us_to_datetime,
                    DataType::Datetime(TimeUnit::Milliseconds, _) => timestamp_ms_to_datetime,
                    DataType::Datetime(TimeUnit::Nanoseconds, _) => timestamp_ns_to_datetime,
                    _ => panic!("Unsupported dtype {}", dtype)
                };

                match timestamp {
                    Some(dt) => {
                        let location_time = CoodinateTime{lcation: coordinates, dt};
                        let cached_date =  dates_cache.get(&location_time);
                        match cached_date {
                            Some(dt) => Ok::<Option<NaiveDateTime>, PolarsError>(Some(*dt)),
                            None => {
                                let ndt = timestamp_to_datetime(dt);
                                let to_tz = parse_time_zone(time_zone)?;
                                let result = naive_local_to_naive_local_in_new_time_zone(&from_tz, &to_tz, ndt, &Ambiguous::Raise)?;
                                dates_cache.insert(location_time, result);
                                Ok::<Option<NaiveDateTime>, PolarsError>(Some(result))
                            }
                        }
 


                    },
                    _ => Ok(None),
                }

            });
    
    let data = results.map(|r| {
        match r {
           Ok(d) => { d },
           Err(_) => { None }
        }
    });

    let s = Series::new("ts", data.collect::<Vec<_>>());
    Ok(s)


}

fn naive_local_to_naive_local_in_new_time_zone(
    from_tz: &Tz,
    to_tz: &Tz,
    ndt: NaiveDateTime,
    ambiguous: &Ambiguous,
) -> PolarsResult<NaiveDateTime> {

    match from_tz.from_local_datetime(&ndt) {
        LocalResult::Single(dt) => Ok(dt.with_timezone(to_tz).naive_local()),
        LocalResult::Ambiguous(dt_earliest, dt_latest) => match ambiguous {
            Ambiguous::Earliest => Ok(dt_earliest.with_timezone(to_tz).naive_local()),
            Ambiguous::Latest => Ok(dt_latest.with_timezone(to_tz).naive_local()),
            Ambiguous::Raise => {
                polars_bail!(ComputeError: "datetime '{}' is ambiguous in time zone '{}'. Please use `ambiguous` to tell how it should be localized.", ndt, to_tz)
            }
        },
        LocalResult::None => polars_bail!(ComputeError:
            "datetime '{}' is non-existent in time zone '{}'. Non-existent datetimes are not yet supported",
            ndt, to_tz
        ),
    }
}