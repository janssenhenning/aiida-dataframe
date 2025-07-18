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

from packaging.version import Version
import pandas as pd
from pandas.util import hash_pandas_object  # pylint: disable=no-name-in-module

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

        if Version(pd.__version__) >= Version("2.0.0") and Version(
            pd.__version__
        ) < Version("3.0.0"):
            # Bug in pandas HDF5 IO concerning timestamps the data type is not correctly roundtripped
            # The only viable way for a user to use this plugin with pandas 2.X and timestamps is to manually make sure
            # that a conversion is doen before storing the dataframe
            # We raise an error in this case as we do not want to mess with timestamps if there is a fix on the horizon
            # This is fixed in pandas 3.0 (once it's released)
            # See https://github.com/pandas-dev/pandas/issues/59004

            dtypes = df.dtypes.astype("string").values
            if any(
                dtype.startswith("datetime64") and dtype != "datetime64[ns]"
                for dtype in dtypes
            ):
                raise ValueError(
                    "Timestamp entries in a dataframe are not correctly handled in HDF5 IO for pandas 2.X.\n"
                    "Either convert to datetime64[ns] manually before storing or remove the entry.\n"
                    "This issue will be fixed in pandas 3.0\n"
                    "For more information see https://github.com/pandas-dev/pandas/issues/59004"
                )

        # The filename property on the class might not be set at this point (this happens only after a set_file call)
        # Therefore a fallback is needed when no filename is specified on this method directly
        if filename is None:
            try:
                filename = self.filename
            except AttributeError:
                filename = self.DEFAULT_FILENAME

        # We write the HDF file out to a temporary directory first
        # to reopen it as a byte IO stream as the AiiDA file repository expects
        with tempfile.TemporaryDirectory() as td:
            df.to_hdf(Path(td) / filename, "w", format="table")
            with open(Path(td) / filename, "rb") as file:
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

            file_path = Path(td) / self.filename

            # The HDF file is copied into a temporary file, since the file handle of the AiiDA file repository
            # cannot handle reading starting from the end (as the file repository is compressed),
            # which the HDF5 IO methods of pandas will do
            # This only happens once per instance as the dataframe is on the df property after loading
            with open(file_path, "wb") as temp_handle:
                with self.open(self.filename, mode="rb") as file:
                    # Copy the content of source to target in chunks
                    shutil.copyfileobj(file, temp_handle)  # type: ignore[arg-type]

            with pd.HDFStore(file_path, mode="r", errors="strict") as store:
                # Workaround for empty dataframe to avoid error in pd.read_hdf
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

        If the node is already stored in the database, each access of this
        property will result in a deep copy of the dataframe being returned
        to avoid the df property coming out of sync with the underlying HDF file
        via e.g. in place modifications methods within pandas
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
        This catches changes to the node made by using __setitem__
        on the dataframe e.g. `df["A"] = new_value`
        This is only done if the hashes of the DATA inside the dataframe does not match up
        """
        if not self.is_stored:
            # Check if the dataframe directly attached to the node
            # has been mutated in place before storing
            # If so the underlying file is updated
            current_hash = self._hash_dataframe(self._df)
            if current_hash != self.get_attribute("_pandas_data_hash"):
                self._update_dataframe(self._df)

        return super().store(*args, **kwargs)
