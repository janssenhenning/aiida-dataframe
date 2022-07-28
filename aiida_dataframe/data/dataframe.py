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
        json_string = self.df.to_json(orient=self.orient)
        return json.loads(json_string)

    @classmethod
    def from_dict(
        cls, d: dict[str, Any], orient: str = "table"
    ) -> DataFrameJsonableWrapper:
        json_string = json.dumps(d)
        return cls(pd.read_json(json_string, orient=orient), orient=orient)


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
