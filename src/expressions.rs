use crate::dateconversions::*;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct FromDatetimeKwargs {
    from_tz: String
}

#[polars_expr(output_type=Utf8)]
fn lookup_timezone(inputs: &[Series]) -> PolarsResult<Series> {
    
    let lats = &inputs[0];
    let lons = &inputs[1];
    impl_lookup_timezone(lats, lons)
}

#[polars_expr(output_type_func=from_local_datetime)]
fn to_local_in_new_timezone(inputs: &[Series]) -> PolarsResult<Series> {

    let dates = &inputs[0];
    let lats = &inputs[1];
    let lons = &inputs[2];
    impl_to_local_in_new_timezone(dates, lats, lons, "", "raise")
}

pub fn from_local_datetime(input_fields: &[Field]) -> PolarsResult<Field> {
    let field = input_fields[0].clone();
    let dtype = match field.dtype {
        DataType::Datetime(_, _) => field.dtype,
        _ => polars_bail!(InvalidOperation:
            "dtype '{}' not supported", field.dtype
        ),
    };

    Ok(Field::new(&field.name, dtype))
}