"""
This module defines a AiiDA Data plugin for pandas DataFrames to be
stored in the file repository as HDF5 files
"""
from __future__ import annotations

from pathlib import Path
import shutil
import tempfile
from typing import Any

import pandas as pd

from aiida.common import exceptions
from aiida.orm import SinglefileData


class PandasFrameHDF5Data(SinglefileData):
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

        super().__init__(None, filename=filename, **kwargs)
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

        with tempfile.TemporaryDirectory() as td:
            df.to_hdf(Path(td) / self.DEFAULT_FILENAME, "w", format="table")

            with open(Path(td) / self.DEFAULT_FILENAME, "rb") as file:
                self.set_file(file, filename=filename)

        self.set_attribute("index", list(self.df.index))
        self.set_attribute("columns", list(self.df.columns.to_flat_index()))

    def _get_dataframe(self) -> pd.DataFrame:
        """
        Get dataframe associated with this node.
        """
        try:
            self._df
        except AttributeError:

            with tempfile.TemporaryDirectory() as td:
                with open(Path(td) / self.filename, "wb") as temp_handle:
                    with self.open(self.filename, mode="rb") as file:
                        # Copy the content of source to target in chunks
                        shutil.copyfileobj(file, temp_handle)  # type: ignore[arg-type]

                self._df = pd.read_hdf(Path(td) / self.filename)

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
