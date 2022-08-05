[![Build Status][ci-badge]][ci-link]
[![Coverage Status][cov-badge]][cov-link]
[![Docs status][docs-badge]][docs-link]
[![PyPI version][pypi-badge]][pypi-link]

# aiida-dataframe

AiiDA data plugin for pandas DataFrame objects

## Features

 * Store `pandas.DataFrame` objects in the Database:
   ```python
   import pandas as pd
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
   ```

 * Retrieving the `pandas.DataFrame` from the Database :
   ```python
   from aiida.orm import QueryBuilder
   df_node = QueryBuilder().append(PandasFrameData).first()[0]
   df = df_node.df #The df property reconstructs the pandas DataFrame
   print(df.head())
   ```

## Installation

```shell
pip install aiida-dataframe
verdi quicksetup  # better to set up a new profile
verdi plugin list aiida.data  # should now show your data plugins
```

## Usage

The plugin also includes verdi commands to inspect its data types:
```shell
verdi data dataframe list
verdi data dataframe export <PK>
verdi data dataframe show <PK>
```

## Development

```shell
git clone https://github.com/janssenhenning/aiida-dataframe .
cd aiida-dataframe
pip install --upgrade pip
pip install -e .[pre-commit,testing]  # install extra dependencies
pre-commit install  # install pre-commit hooks
pytest -v  # discover and run all tests
```

See the [developer guide](http://aiida-dataframe.readthedocs.io/en/latest/developer_guide/index.html) for more information.

## License

MIT
## Contact

henning.janssen@gmx.net


[ci-badge]: https://github.com/janssenhenning/aiida-dataframe/workflows/ci/badge.svg?branch=main
[ci-link]: https://github.com/janssenhenning/aiida-dataframe/actions
[cov-badge]: https://codecov.io/gh/janssenhenning/aiida-dataframe/branch/main/graph/badge.svg
[cov-link]: https://codecov.io/gh/janssenhenning/aiida-dataframe
[docs-badge]: https://readthedocs.org/projects/aiida-dataframe/badge
[docs-link]: http://aiida-dataframe.readthedocs.io/
[pypi-badge]: https://badge.fury.io/py/aiida-dataframe.svg
[pypi-link]: https://badge.fury.io/py/aiida-dataframe
