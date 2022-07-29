""" Tests for Data plugin."""

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from aiida.orm import QueryBuilder, load_node
from aiida.plugins import DataFactory


def test_roundtrip():
    """
    Simple test of storing a Dataframe in the database and ensuring
    the contents stay the same after retrieving it from the DataBase
    """

    # Example from pandas Docs
    df = pd.DataFrame(
        {
            "A": 1.0,
            "B": pd.Timestamp("20130102"),
            "C": pd.Series(1, index=list(range(4)), dtype="float64"),
            "D": np.array([3] * 4, dtype="int64"),
            "E": pd.Categorical(["test", "train", "test", "train"]),
            "F": "foo",
        }
    )

    PandasFrameData = DataFactory("dataframe.frame")
    node = PandasFrameData(df)
    node.store()
    assert node.is_stored

    loaded = load_node(node.pk)
    assert loaded is not node
    assert_frame_equal(loaded.df, df)


def test_multiindex_columns_roundtrip():
    """
    Test that MultiIndex columns are correctly reconstructed
    """
    # Example from pandas docs
    df = pd.DataFrame(
        {
            "row": [0, 1, 2],
            "One_X": [1.1, 1.1, 1.1],
            "One_Y": [1.2, 1.2, 1.2],
            "Two_X": [1.11, 1.11, 1.11],
            "Two_Y": [1.22, 1.22, 1.22],
        }
    )
    df = df.set_index("row")
    df.columns = pd.MultiIndex.from_tuples([tuple(c.split("_")) for c in df.columns])

    PandasFrameData = DataFactory("dataframe.frame")
    node = PandasFrameData(df)
    node.store()
    assert node.is_stored

    loaded = load_node(node.pk)
    assert loaded is not node
    assert_frame_equal(loaded.df, df)


def test_multiindex_index_roundtrip():
    """
    Test that MultiIndex indices are correctly reconstructed
    """
    coords = [("AA", "one"), ("AA", "six"), ("BB", "one"), ("BB", "two"), ("BB", "six")]
    index = pd.MultiIndex.from_tuples(coords)
    df = pd.DataFrame([11, 22, 33, 44, 55], index, ["MyData"])

    PandasFrameData = DataFactory("dataframe.frame")
    node = PandasFrameData(df)
    node.store()
    assert node.is_stored

    loaded = load_node(node.pk)
    assert loaded is not node
    assert loaded.df.equals(df)


def test_query_columns():
    """
    Test querying for DataFrames containing specific column names
    """

    PandasFrameData = DataFactory("dataframe.frame")

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

    df_node_1 = PandasFrameData(df)
    df_node_1.store()

    df = pd.DataFrame(
        {
            "A_RENAMED": 1.0,
            "B": pd.Timestamp("20130102"),
            "C": pd.Series(1, index=list(range(4)), dtype="float32"),
            "D": np.array([3] * 4, dtype="int32"),
            "E": pd.Categorical(["test", "train", "test", "train"]),
            "F": "foo",
        }
    )

    df_node_2 = PandasFrameData(df)
    df_node_2.store()

    query = QueryBuilder().append(
        PandasFrameData, filters={"attributes.columns": {"contains": ["A"]}}
    )

    assert len(query.all()) == 1
    assert query.one()[0].uuid == df_node_1.uuid


def test_query_index():
    """
    Test querying for DataFrames on the index attribute
    """
    df = pd.DataFrame(
        {
            "row": [0, 1, 2],
            "One_X": [1.1, 1.1, 1.1],
            "One_Y": [1.2, 1.2, 1.2],
            "Two_X": [1.11, 1.11, 1.11],
            "Two_Y": [1.22, 1.22, 1.22],
        }
    )
    df = df.set_index("row")
    PandasFrameData = DataFactory("dataframe.frame")
    df_node_1 = PandasFrameData(df)
    df_node_1.store()

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

    df_node_2 = PandasFrameData(df)
    df_node_2.store()

    query = QueryBuilder().append(
        PandasFrameData, filters={"attributes.index": {"shorter": 4}}
    )

    assert len(query.all()) == 1
    assert query.one()[0].uuid == df_node_1.uuid


def test_non_serializable():
    """
    Make sure that non-serializable things are caught
    early before storing the Dataframe
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
            "G": 1 + 2j,
        }
    )

    PandasFrameData = DataFactory("dataframe.frame")

    # Complex numbers can not be serialized/deserialized in a consistent manner
    with pytest.raises(TypeError):
        PandasFrameData(df)


def test_wrong_inputs():
    """
    Wrong inputs given to __init__
    """

    PandasFrameData = DataFactory("dataframe.frame")
    # No data
    with pytest.raises(TypeError):
        PandasFrameData()

    # Wrong type
    with pytest.raises(TypeError):
        PandasFrameData([1, 2, 3])
