""" Tests for Data plugin."""

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from aiida.common import exceptions
from aiida.orm import QueryBuilder, load_node
from aiida.plugins import DataFactory


@pytest.mark.parametrize(
    "entry_point",
    ("dataframe.frame",),
)
def test_roundtrip(entry_point):
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

    PandasFrameData = DataFactory(entry_point)
    node = PandasFrameData(df)
    node.store()
    assert node.is_stored

    loaded = load_node(node.pk)
    assert loaded is not node
    assert_frame_equal(loaded.df, df)


@pytest.mark.parametrize(
    "entry_point",
    ("dataframe.frame",),
)
def test_multiindex_columns_roundtrip(entry_point):
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

    PandasFrameData = DataFactory(entry_point)
    node = PandasFrameData(df)
    node.store()
    assert node.is_stored

    loaded = load_node(node.pk)
    assert loaded is not node
    assert_frame_equal(loaded.df, df)


@pytest.mark.parametrize(
    "entry_point",
    ("dataframe.frame",),
)
def test_multiindex_index_roundtrip(entry_point):
    """
    Test that MultiIndex indices are correctly reconstructed
    """
    coords = [("AA", "one"), ("AA", "six"), ("BB", "one"), ("BB", "two"), ("BB", "six")]
    index = pd.MultiIndex.from_tuples(coords)
    df = pd.DataFrame([11, 22, 33, 44, 55], index, ["MyData"])

    PandasFrameData = DataFactory(entry_point)
    node = PandasFrameData(df)
    node.store()
    assert node.is_stored

    loaded = load_node(node.pk)
    assert loaded is not node
    assert_frame_equal(loaded.df, df)


@pytest.mark.parametrize(
    "entry_point",
    ("dataframe.frame",),
)
def test_query_columns(entry_point):
    """
    Test querying for DataFrames containing specific column names
    """

    PandasFrameData = DataFactory(entry_point)

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


@pytest.mark.parametrize(
    "entry_point",
    ("dataframe.frame",),
)
def test_query_multiindex_columns(entry_point):
    """
    Test querying for DataFrames containing specific column names
    """

    PandasFrameData = DataFactory(entry_point)

    # Example from pandas Docs
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

    df_node_1 = PandasFrameData(df)
    df_node_1.store()

    df = pd.DataFrame(
        {
            "row": [0, 1, 2],
            "OneRename_X": [1.1, 1.1, 1.1],
            "One_Y": [1.2, 1.2, 1.2],
            "Two_X": [1.11, 1.11, 1.11],
            "Two_Y": [1.22, 1.22, 1.22],
        }
    )
    df = df.set_index("row")
    df.columns = pd.MultiIndex.from_tuples([tuple(c.split("_")) for c in df.columns])

    df_node_2 = PandasFrameData(df)
    df_node_2.store()

    query = QueryBuilder().append(
        PandasFrameData, filters={"attributes.columns": {"contains": [["One", "X"]]}}
    )

    assert len(query.all()) == 1
    assert query.one()[0].uuid == df_node_1.uuid


@pytest.mark.parametrize(
    "entry_point",
    ("dataframe.frame",),
)
def test_query_index(entry_point):
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
    PandasFrameData = DataFactory(entry_point)
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


@pytest.mark.parametrize("entry_point", ("dataframe.frame",))
def test_complex_hdf5(entry_point):
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

    PandasFrameData = DataFactory(entry_point)
    node = PandasFrameData(df)
    node.store()
    assert node.is_stored

    loaded = load_node(node.pk)
    assert loaded is not node
    assert_frame_equal(loaded.df, df)


@pytest.mark.parametrize("entry_point", ("dataframe.frame",))
def test_nan_values_hdf5(entry_point):
    """
    Make sure that NaNs are eliminated (transformed to None)
    before being stored in the Database
    """

    df = pd.DataFrame(
        {
            "None": [
                np.NAN,
                float("NaN"),
                np.inf,
                float("inf"),
            ],
        }
    )

    PandasFrameData = DataFactory(entry_point)
    node = PandasFrameData(df)
    node.store()
    assert node.is_stored

    loaded = load_node(node.pk)
    assert loaded is not node
    assert_frame_equal(loaded.df, df)


@pytest.mark.parametrize(
    "entry_point",
    ("dataframe.frame",),
)
def test_wrong_inputs(entry_point):
    """
    Wrong inputs given to __init__
    """

    PandasFrameData = DataFactory(entry_point)
    # No data
    with pytest.raises(TypeError):
        PandasFrameData(None)

    # Wrong type
    with pytest.raises(TypeError):
        PandasFrameData([1, 2, 3])


@pytest.mark.parametrize(
    "entry_point",
    ("dataframe.frame",),
)
def test_modification_after_store(entry_point):
    """
    Raise if the dataframe is modified after storing
    """

    PandasFrameData = DataFactory(entry_point)

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

    node = PandasFrameData(df)
    node.store()

    with pytest.raises(exceptions.ModificationNotAllowed):
        node.df = node.df.rename({"A": "A_rename"})


@pytest.mark.parametrize(
    "entry_point",
    ("dataframe.frame",),
)
def test_modification_before_store(entry_point):
    """
    Test that modifying the dataframe before storing is propagated
    """

    PandasFrameData = DataFactory(entry_point)

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

    node = PandasFrameData(df)
    node.df = node.df.rename({"A": "A_rename"})
    node.store()

    loaded = load_node(node.pk)
    assert loaded is not node
    assert_frame_equal(loaded.df, df.rename({"A": "A_rename"}))


@pytest.mark.parametrize(
    "entry_point",
    ("dataframe.frame",),
)
def test_setitem_modification(entry_point):
    """
    Test that modifying the dataframe before storing is propagated
    """

    PandasFrameData = DataFactory(entry_point)

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
    df_changed = df.copy(deep=True)
    df_changed["F"] = ["foo", "foo", "bar", "bar"]

    node = PandasFrameData(df)
    node.df["F"] = ["foo", "foo", "bar", "bar"]
    node.store()

    loaded = load_node(node.pk)
    assert loaded is not node
    assert_frame_equal(loaded.df, df_changed)


@pytest.mark.parametrize(
    "entry_point",
    ("dataframe.frame",),
)
def test_empty_dataframe(entry_point):
    """
    Test that storing an empty dataframe works
    """

    PandasFrameData = DataFactory(entry_point)

    # Example from pandas Docs
    df = pd.DataFrame([], columns=["A", "B"])

    node = PandasFrameData(df)
    node.store()

    loaded = load_node(node.pk)
    assert loaded is not node
    assert_frame_equal(loaded.df, df)


@pytest.mark.parametrize(
    "entry_point",
    ("dataframe.frame",),
)
def test_modification_before_instance_update(entry_point):
    """
    Test that modifying the dataframe before storing is propagated
    """

    PandasFrameData = DataFactory(entry_point)

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
    df_changed = df.copy(deep=True)
    df_changed = df_changed.set_index("C")

    node = PandasFrameData(df)
    node.df = node.df.set_index("C")
    assert_frame_equal(node.df, df_changed)


@pytest.mark.parametrize(
    "entry_point",
    ("dataframe.frame",),
)
def test_non_default_filename(entry_point):
    """
    Test that modifying the dataframe before storing is propagated
    """

    PandasFrameData = DataFactory(entry_point)

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

    node = PandasFrameData(df, filename="non_default.h5")
    node.store()

    assert node.list_object_names() == ["non_default.h5"]

    loaded = load_node(node.pk)
    assert loaded is not node
    assert loaded.list_object_names() == ["non_default.h5"]
    assert_frame_equal(loaded.df, df)
