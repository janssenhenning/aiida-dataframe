""" Tests for Data plugin."""

import numpy as np
import pandas as pd

from aiida.plugins import DataFactory


def test_roundtrip():
    """
    Simple test of storing a Dataframe in the databse and ensuring
    the contents stay the same after retrieving it from the DataBase
    """

    # Example from pandas Docs
    df = pd.DataFrame(
        {
            "A": 1.0,
            "B": pd.Timestamp("20130102"),
            "C": pd.Series(1, index=list(range(4)), dtype="float32"),
            "D": np.array([3] * 4, dtype="int32"),
            "E": pd.Categorical(["test", "train", "test", "train"]),
            "F": "foo",
        }
    )

    PandasFrameData = DataFactory("dataframe.frame")
    df_node = PandasFrameData(df)
    df_node.store()

    df_stored = df_node.df
    assert df.equals(df_stored)
