"""
Command line interface (cli) for aiida_dataframe.

Register new commands either via the "console_scripts" entry point or plug them
directly into the 'verdi' command by using AiiDA-specific entry points like
"aiida.cmdline.data" (both in the setup.json file).
"""
from aiida.cmdline.commands.cmd_data import verdi_data


# See aiida.cmdline.data entry point in setup.json
@verdi_data.group("dataframe")
def data_cli():
    """Command line interface for aiida-dataframe"""
