from __future__ import annotations

import re
import sys
from datetime import date
from typing import TYPE_CHECKING, Literal, Sequence

import polars as pl
from polars.utils.udfs import _get_shared_lib_location

from polars_dates.utils import parse_into_expr

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

if TYPE_CHECKING:
    from polars.type_aliases import IntoExpr

lib = _get_shared_lib_location(__file__)
print(f"lib is {lib}")


if TYPE_CHECKING:
    from polars import Expr
    from polars.type_aliases import Ambiguous


def echo(expr: str | pl.Expr) -> pl.Expr:
    """
    Return the Julian date corresponding to given datetimes.

    Examples
    --------
    >>> from datetime import datetime
    >>> import polars_xdt as xdt
    >>> df = pl.DataFrame(
    ...     {
    ...         "date_col": [
    ...             datetime(2013, 1, 1, 0, 30),
    ...             datetime(2024, 1, 7, 13, 18, 51),
    ...         ],
    ...     }
    ... )
    >>> with pl.Config(float_precision=10) as cfg:
    ...     df.with_columns(julian_date=xdt.to_julian_date("date_col"))
    shape: (2, 2)
    ┌─────────────────────┬────────────────────┐
    │ date_col            ┆ julian_date        │
    │ ---                 ┆ ---                │
    │ datetime[μs]        ┆ f64                │
    ╞═════════════════════╪════════════════════╡
    │ 2013-01-01 00:30:00 ┆ 2456293.5208333335 │
    │ 2024-01-07 13:18:51 ┆ 2460317.0547569445 │
    └─────────────────────┴────────────────────┘
    """
    print("Echo function called")
    expr = parse_into_expr(expr)
    return expr.register_plugin(
        lib=lib,
        symbol="echo",
        is_elementwise=True,
        args=[],
    )

def lookup_timezone(expr: str | pl.Expr, other: IntoExpr) -> pl.Expr:
    """
    Return the Julian date corresponding to given datetimes.

    Examples
    --------
    >>> from datetime import datetime
    >>> import polars_xdt as xdt
    >>> df = pl.DataFrame(
    ...     {
    ...         "date_col": [
    ...             datetime(2013, 1, 1, 0, 30),
    ...             datetime(2024, 1, 7, 13, 18, 51),
    ...         ],
    ...     }
    ... )
    >>> with pl.Config(float_precision=10) as cfg:
    ...     df.with_columns(julian_date=xdt.to_julian_date("date_col"))
    shape: (2, 2)
    ┌─────────────────────┬────────────────────┐
    │ date_col            ┆ julian_date        │
    │ ---                 ┆ ---                │
    │ datetime[μs]        ┆ f64                │
    ╞═════════════════════╪════════════════════╡
    │ 2013-01-01 00:30:00 ┆ 2456293.5208333335 │
    │ 2024-01-07 13:18:51 ┆ 2460317.0547569445 │
    └─────────────────────┴────────────────────┘
    """
    print(f"lookup_timezone function called {expr} {other}")
    expr = parse_into_expr(expr)
    print(f"Yo I got {expr}")
    return expr.register_plugin(
        lib=lib,
        symbol="lookup_timezone",
        args=[other],
        is_elementwise=True,
    )


def to_local_in_new_timezone(expr: str | pl.Expr, lat: IntoExpr, lon: IntoExpr) -> pl.Expr:
    """
    Return the Julian date corresponding to given datetimes.

    Examples
    --------
    >>> from datetime import datetime
    >>> import polars_xdt as xdt
    >>> df = pl.DataFrame(
    ...     {
    ...         "date_col": [
    ...             datetime(2013, 1, 1, 0, 30),
    ...             datetime(2024, 1, 7, 13, 18, 51),
    ...         ],
    ...     }
    ... )
    >>> with pl.Config(float_precision=10) as cfg:
    ...     df.with_columns(julian_date=xdt.to_julian_date("date_col"))
    shape: (2, 2)
    ┌─────────────────────┬────────────────────┐
    │ date_col            ┆ julian_date        │
    │ ---                 ┆ ---                │
    │ datetime[μs]        ┆ f64                │
    ╞═════════════════════╪════════════════════╡
    │ 2013-01-01 00:30:00 ┆ 2456293.5208333335 │
    │ 2024-01-07 13:18:51 ┆ 2460317.0547569445 │
    └─────────────────────┴────────────────────┘
    """
    print(f"lookup_timezone function called {expr} {lat} {lon}")
    expr = parse_into_expr(expr)
    print(f"Yo I got {expr}")
    return expr.register_plugin(
        lib=lib,
        symbol="to_local_in_new_timezone",
        args=[lat, lon],
        is_elementwise=True,
    )