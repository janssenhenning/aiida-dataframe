"""
Data types provided by plugin

Register data types via the "aiida.data" entry point in setup.json.
"""
from .dataframe import PandasFrameData

__all__ = ("PandasFrameData",)
