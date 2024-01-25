# Polars-Dates

Date routines for [Polars](https://www.pola.rs/).

- ✅ blazingly fast, written in Rust!
- ✅ seamless Polars integration!


Installation
------------

First, you need to [install Polars](https://pola-rs.github.io/polars/user-guide/installation/).

Then, you'll need to install `polars-validation`:
```console
pip install polars-dates
```

Usage
-------------
The module creates a custom namespace which is attached to a polars expression

What does this mean. If you are using polars regularly you will be aware of the .str and .arr 
(now renamed .list) namespaces which have special functions related to string and arrays / lists

This module creates a custom namespace 


Basic Example
-------------
Say we start with a dataframe with 5 rows.
This sample has 2 different places (Canary Wharf in London and New York).
Using the Latitude and logitude we can lookup the timezone 

The example below is useful since you will get autocomplete in the code editor

```python
import polars as pl
import polars_dates as pl_dates
from datetime import datetime
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
         pl_dates.lookup_timezone(pl.col("lat"), pl.col("lon"))
    )
) 
```

Note you can also use the namespace. Which might help convay what is being done in a cleaner fashion

e.g. in the example below we will use the same test dataframe but convert the fields to a localised datetime

```python
import polars as pl
import polars_dates as pl_dates
from datetime import datetime
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
```

Note in the example above how the dateconversions namespace is applied to the column dt.

This could also be written as

pl_dates.to_local_in_new_timezone(pl.col("dt"), pl.col("lat"), pl.col("lon"))

The exact form used will depend on personal preference and each has advantages and disadvantages.

