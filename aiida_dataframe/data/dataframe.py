"""
This module defines a AiiDA Data plugin for pandas DataFrames to be
stored in the file repository as HDF5 files
"""
from __future__ import annotations

import hashlib
from pathlib import Path
import shutil
import tempfile
from typing import Any

import pandas as pd
from pandas.util import hash_pandas_object

from aiida.common import exceptions
from aiida.orm import SinglefileData


class PandasFrameData(SinglefileData):
    """
    Data plugin for pandas DataFrame objects. Dataframes are serialized to Hdf5
    using the :py:meth:`~pandas.DataFrame.to_hdf()` method  and stored in the
    file repository and are deserialized using :py:func:`~pandas.read_hdf()`

    The whole DataFrame can be retrieved by using the :py:meth:`df` property
    The names of columns and indices are stored in attributes to be queryable through
    the database

    :param df: pandas Dataframe
    """

    DEFAULT_FILENAME = "dataframe.h5"

    def __init__(
        self, df: pd.DataFrame, filename: str | None = None, **kwargs: Any
    ) -> None:
        if df is None:
            raise TypeError("the `df` argument cannot be `None`.")

        if not isinstance(df, pd.DataFrame):
            raise TypeError("the `df` argument is not a pandas DataFrame.")

        super().__init__(None, **kwargs)
        self._update_dataframe(df, filename=filename)
        self._df = df

    def _update_dataframe(self, df: pd.DataFrame, filename: str | None = None) -> None:
        """
        Update the stored HDF5 file. Raises if the node is already stored
        """
        if self.is_stored:
            raise exceptions.ModificationNotAllowed(
                "cannot update the DataFrame on a stored node"
            )
        if filename is None:
            try:
                filename = self.filename
            except AttributeError:
                filename = self.DEFAULT_FILENAME

        with tempfile.TemporaryDirectory() as td:
            df.to_hdf(Path(td) / self.DEFAULT_FILENAME, "w", format="table")

            with open(Path(td) / self.DEFAULT_FILENAME, "rb") as file:
                self.set_file(file, filename=filename)

        self.set_attribute("_pandas_data_hash", self._hash_dataframe(df))
        self.set_attribute("index", list(df.index))
        self.set_attribute("columns", list(df.columns.to_flat_index()))
        self._df = df

    @staticmethod
    def _hash_dataframe(df):
        """
        Return a hash corresponding to the Data inside the dataframe (not column names)
        """
        return hashlib.sha256(hash_pandas_object(df, index=True).values).hexdigest()

    def _get_dataframe_from_repo(self) -> pd.DataFrame:
        """
        Get dataframe associated with this node from the file repository.
        """
        with tempfile.TemporaryDirectory() as td:
            with open(Path(td) / self.filename, "wb") as temp_handle:
                with self.open(self.filename, mode="rb") as file:
                    # Copy the content of source to target in chunks
                    shutil.copyfileobj(file, temp_handle)  # type: ignore[arg-type]

            # Workaround for empty dataframe
            with pd.HDFStore(
                Path(td) / self.filename, mode="r", errors="strict"
            ) as store:
                if len(store.groups()) == 0:
                    return pd.DataFrame([], columns=self.get_attribute("columns"))
                return pd.read_hdf(store)

    def _get_dataframe(self) -> pd.DataFrame:
        """
        Get dataframe associated with this node.
        """
        try:
            self._df
        except AttributeError:
            self._df = self._get_dataframe_from_repo()

        if self.is_stored:
            return self._df.copy(deep=True)
        return self._df

    @property
    def df(self) -> pd.DataFrame:
        """
        Return the pandas DataFrame instance associated with this node
        """
        return self._get_dataframe()

    @df.setter
    def df(self, df: pd.DataFrame) -> None:
        """
        Update the associated dataframe
        """
        self._update_dataframe(df)

    def store(self, *args, **kwargs) -> PandasFrameData:
        """
        Store the node. Before the node is stored
        sync the HDF5 storage with the _df attribute on the node
        This catches changes to the node made by using setitem
        on the dataframe e.g. `df["A"] = new_value`
        This is only done if the hashes of the DATA does not match up
        """
        current_hash = self._hash_dataframe(self._df)
        if current_hash != self.get_attribute("_pandas_data_hash"):
            self._update_dataframe(self._df)

        return super().store(*args, **kwargs)
