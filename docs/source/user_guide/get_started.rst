===============
Getting started
===============

This page should contain a short guide on what the plugin does and
a short example on how to use the plugin.

Installation
++++++++++++

Use the following commands to install the plugin::

    git clone https://github.com/janssenhenning/aiida-dataframe .
    cd aiida-dataframe
    pip install -e .  # also installs aiida, if missing (but not postgres)
    #pip install -e .[pre-commit,testing] # install extras for more features
    verdi quicksetup  # better to set up a new profile
    verdi plugin list aiida.data  # should now show your data plugins

Usage
++++++

The plugin provides a Data plugin :py:class:`~aiida_dataframe.data.dataframe.PandasFrameData`
that is able to serialize and deserialize :py:class:`~pandas.DataFrame` objects for the AiiDA
database (stored in HDF5 format in the File repository)

Example for storing a DataFrame::

   import pandas as pd
   import numpy as np
   from aiida.plugins import DataFactory

   PandasFrameData = DataFactory('dataframe.frame')
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
   df_node = PandasFrameData(df)
   df_node.store()

The underlying DataFrame is accessible using the `df` property of the Data node::

   print(df_node.df.head())

.. warning:: Note on Mutability of DataFrame objects

    Methods on :py:class:`pandas.DataFrame` objects return a new instance of the
    object and do not mutate the original instance. This means that as soon as the
    :py:class:`~aiida_dataframe.data.dataframe.PandasFrameData` is initialized the associated
    DataFrame essentially is fixed. Any operation on the dataframe on the
    :py:class:`~aiida_dataframe.data.dataframe.PandasFrameData` class will completely overwrite
    and recreate the associated HDF5 file in it's repository.

    Some methods of `pandas` have an `in_place` option to mutate the original. This is
    explicitly not supported if the :py:class:`pandas.DataFrame` is already associated
    with a node the changes will be ignored if you load it from the database
