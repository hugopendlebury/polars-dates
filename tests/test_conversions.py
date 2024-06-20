import pytest

import polars as pl
from datetime import datetime
import polars_geodates as pl_dates


def test_london_new_york():
    df = (
        pl.DataFrame(data = {'lat' : [51.5054, 51.5054, 40.7128], 'lon' : [0.0235, 0.0235, -74.0060]}).with_columns(
        timezones = pl_dates.lookup_timezone(pl.col("lat"), pl.col("lon"))
    )
    )
    time_zones = df["timezones"].to_list()
    assert time_zones == ["Europe/London", "Europe/London", "America/New_York"]

def test_timezone_conversions():

    df = (
         pl.DataFrame(data = {'lat' : [51.5054, 51.5054, 40.7128, 40.7128, 40.7128], 
                             'lon' : [0.0235, 0.0235, -74.0060, -74.0060, -74.0060],
                             'dt': [datetime(2024,1,24,9,0,0),
                                     datetime(2024,1,24,9,0,0),
                                     datetime(2024,1,24,9,0,0),
                                     datetime(2024,1,24,3,0,0),
                                     datetime(2024,1,24,3,0,0)],
                             'values' : [5,5,5,10,10]}
                     )
         .with_columns(
              timezone_conv = pl.col("dt").dateconversions.to_local_in_new_timezone(pl.col("lat"), pl.col("lon"))
         )
    )
    converted_dates = df['timezone_conv'].to_list()  
    expected_dates = [datetime(2024,1,24,9,0,0),
                        datetime(2024,1,24,9,0,0),
                        datetime(2024,1,24,4,0,0),
                        datetime(2024,1,23,22,0,0),
                        datetime(2024,1,23,22,0,0)]
    assert converted_dates == expected_dates