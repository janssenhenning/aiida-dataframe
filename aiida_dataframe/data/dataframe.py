"""
This module defines a AiiDA Data plugin for pandas DataFrames
"""
from __future__ import annotations

import json
from typing import Any

import pandas as pd

from aiida.orm import JsonableData


class DataFrameJsonableWrapper:
    """
    Wrapper to add the needed Protocol in order for the
    DataFrame to interact correctly with the Jsonable Data class

    :param df: pandas Dataframe
    :param orient: Defines the format of the json that is stored in the database
                   Valid are: 'split', 'records', 'index', 'columns', 'values', 'table'
    """

    def __init__(self, df: pd.DataFrame, orient: str = "table") -> None:
        self.df = df
        self.orient = orient

    def as_dict(self) -> dict[str, Any]:
        """
        Serialize the Dataframe as a json serializable dictionary
        """
        multiindex = False
        if isinstance(self.df.columns, pd.MultiIndex):
            self.df.columns = self.df.columns.to_flat_index()
            multiindex = True

        json_string = self.df.to_json(orient=self.orient)
        d = json.loads(json_string)
        d["_orient"] = self.orient
        d["_multindex"] = multiindex
        if "columns" not in d:
            d["columns"] = list(self.df.columns)
        if "index" not in d:
            d["index"] = list(self.df.index)
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> DataFrameJsonableWrapper:
        """
        Construct a pandas Dataframe from a dictionary
        """
        orient = d.pop("_orient", "table")
        multiindex = d.pop("_multiindex", False)
        d.pop("columns", None)
        d.pop("index", None)

        json_string = json.dumps(d)
        df = pd.read_json(json_string, orient=orient)
        if multiindex:
            df.columns = pd.MultiIndex.from_tuples(df.columns)
        return cls(df, orient=orient)


class PandasFrameData(JsonableData):
    """
    Data plugin for pandas DataFrame objects. Dataframes are serializaed to json
    using the :py:meth:`~pandas.DataFrame.to_json()` method and are deserialized
    using :py:func:`~pandas.read_json()`

    :param df: pandas Dataframe
    :param orient: Defines the format of the json that is stored in the database (default: 'table')
                   Valid are: 'split', 'records', 'index', 'columns', 'values', 'table'
    """

    def __init__(self, df: pd.DataFrame, orient: str = "table", **kwargs: Any) -> None:

        if df is None:
            raise TypeError("the `df` argument cannot be `None`.")

        if not isinstance(df, pd.DataFrame):
            raise TypeError("the `df` argument is not a pandas DataFrame.")

        obj = DataFrameJsonableWrapper(df, orient=orient)
        super().__init__(obj, **kwargs)

        self.orient = orient

    @property
    def df(self) -> pd.DataFrame:
        """
        Return the pandas DataFrame instance associated with this node
        """
        return self.obj.df
