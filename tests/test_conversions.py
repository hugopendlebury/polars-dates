import pytest

import polars as pl
import polars_dates as pl_dates


def test_london_new_york():
    df = (
        pl.DataFrame(data = {'lat' : [51.5054, 51.5054, 40.7128], 'lon' : [0.0235, 0.0235, -74.0060]}).with_columns(
        timezones = pl_dates.lookup_timezone(pl.col("lat"), pl.col("lon"))
    )
    )
    time_zones = df["timezones"].to_list()
    assert time_zones == ["Europe/London", "Europe/London", "America/New_York"]