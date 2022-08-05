"""
Data types provided by plugin

Register data types via the "aiida.data" entry point in pyproject.toml.
"""
from .dataframe import PandasFrameData

__all__ = ("PandasFrameData",)
