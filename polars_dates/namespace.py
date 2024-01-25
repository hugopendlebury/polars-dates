from __future__ import annotations

from typing import Any, Callable

import polars as pl

from polars_dates import functions


@pl.api.register_expr_namespace("dateconversions")
class ExprXDTNamespace:
    """eXtra stuff for DateTimes."""

    def __init__(self, expr: pl.Expr) -> None:
        print("INIT EXPRESSIONS")
        self._expr = expr

    def __getattr__(self, function_name: str) -> Callable[[Any], pl.Expr]:
        def func(*args: Any, **kwargs: Any) -> pl.Expr:
            return getattr(functions, function_name)(
                self._expr, *args, **kwargs
            )

        return func
