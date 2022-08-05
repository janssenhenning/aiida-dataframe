""" Tests for command line interface."""
from click.testing import CliRunner
import numpy as np
import pandas as pd

from aiida.plugins import DataFactory

from aiida_dataframe.cli import export, list_, show


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

    def test_dataframe_list(self):
        """Test 'verdi data dataframe list'
        Tests that it can be reached and that it lists the node we have set up.
        """
        result = self.runner.invoke(list_, catch_exceptions=False)
        assert str(self.df_node.pk) in result.output

    def test_dataframe_export(self, file_regression):
        """Test 'verdi dataframe export'
        Tests that it can be reached and that it shows the contents of the node
        we have set up.
        """
        result = self.runner.invoke(
            export, [str(self.df_node.pk)], catch_exceptions=False
        )
        file_regression.check(result.output.strip("\n"), extension=".csv")

    def test_dataframe_show(self, file_regression):
        """Test 'verdi dataframe show'
        Tests that it can be reached and that it shows the contents of the node
        we have set up.
        """
        result = self.runner.invoke(
            show, [str(self.df_node.pk)], catch_exceptions=False
        )
        file_regression.check(result.output)
