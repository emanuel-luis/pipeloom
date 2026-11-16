"""Bronze: untouched copies of the TPC-H tables, with the original column names.

A model never reads itself, so each parameter binds the source table of the same name.
"""

import pandas as pd

from pipeloom import model


@model
def orders(orders: pd.DataFrame) -> pd.DataFrame:
    return orders


@model
def lineitem(lineitem: pd.DataFrame) -> pd.DataFrame:
    return lineitem


@model
def customer(customer: pd.DataFrame) -> pd.DataFrame:
    return customer


@model
def nation(nation: pd.DataFrame) -> pd.DataFrame:
    return nation


@model
def region(region: pd.DataFrame) -> pd.DataFrame:
    return region
