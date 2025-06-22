"""Tests for command line interface."""

import traceback

from click.testing import CliRunner
import numpy as np
from packaging.version import Version
import pandas as pd
import pytest

from aiida.plugins import DataFactory

from aiida_dataframe.cli import export, list_, show

pandas_2_xfail = pytest.mark.xfail(
    Version(pd.__version__) >= Version("2.0.0")
    and Version(pd.__version__) < Version("3.0.0"),
    reason="Pandas 2 does not handle datetime64[s] with HDF5 correctly. Correct failure behaviour tested in test_roundtrip",
    raises=ValueError,
)


# pylint: disable=attribute-defined-outside-init
class TestDataCli:
    """Test verdi data cli plugin."""

    def setup_method(self):
        """Prepare nodes for cli tests."""
        PandasFrameData = DataFactory("dataframe.frame")
        # Example from the pandas Docs
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
        self.df_node = PandasFrameData(df)
        self.df_node.store()
        self.runner = CliRunner()

    @pandas_2_xfail
    def test_dataframe_list(self):
        """Test 'verdi data dataframe list'
        Tests that it can be reached and that it lists the node we have set up.
        """
        result = self.runner.invoke(list_, catch_exceptions=False)

        # Check for success
        assert result.exception is None, "".join(
            traceback.format_exception(*result.exc_info)
        )
        assert result.exit_code == 0, result.output

        assert str(self.df_node.pk) in result.output

    @pandas_2_xfail
    def test_dataframe_export(self, file_regression):
        """Test 'verdi dataframe export'
        Tests that it can be reached and that it shows the contents of the node
        we have set up.
        """
        result = self.runner.invoke(
            export, [str(self.df_node.pk)], catch_exceptions=False
        )

        # Check for success
        assert result.exception is None, "".join(
            traceback.format_exception(*result.exc_info)
        )
        assert result.exit_code == 0, result.output

        file_regression.check(result.output.strip("\n"), extension=".csv")

    @pandas_2_xfail
    def test_dataframe_export_to_file(self, file_regression):
        """Test 'verdi dataframe export --outfile <FILENAME>'
        Tests that it can be reached and that it exports the contents of the node we have set up
        to the specified file correctly
        """
        TEST_FILE = "result.csv"

        with self.runner.isolated_filesystem():
            result = self.runner.invoke(
                export,
                [str(self.df_node.pk), "--outfile", TEST_FILE],
                catch_exceptions=False,
            )

            # Check for success
            assert result.exception is None, "".join(
                traceback.format_exception(*result.exc_info)
            )
            assert result.exit_code == 0, result.output

            with open(TEST_FILE, encoding="utf-8") as file:
                content = file.read()

            # The content of result.csv should be the same as in the test without --outfile
            file_regression.check(
                content, basename="test_dataframe_export", extension=".csv"
            )

    @pandas_2_xfail
    def test_dataframe_show(self, file_regression):
        """Test 'verdi dataframe show'
        Tests that it can be reached and that it shows the contents of the node
        we have set up.
        """
        result = self.runner.invoke(
            show, [str(self.df_node.pk)], catch_exceptions=False
        )

        # Check for success
        assert result.exception is None, "".join(
            traceback.format_exception(*result.exc_info)
        )
        assert result.exit_code == 0, result.output

        file_regression.check(result.output)
