"""
Command line interface (cli) for aiida_dataframe.

Register new commands either via the "console_scripts" entry point or plug them
directly into the 'verdi' command by using AiiDA-specific entry points like
"aiida.cmdline.data" (both in the pyproject.toml file).
"""
import sys
import tempfile

import click
from tabulate import tabulate

from aiida.cmdline.commands.cmd_data import verdi_data
from aiida.cmdline.params.types import DataParamType
from aiida.cmdline.utils import decorators
from aiida.orm import QueryBuilder
from aiida.plugins import DataFactory


# See aiida.cmdline.data entry point in setup.json
@verdi_data.group("dataframe")
def data_cli():
    """Command line interface for aiida-dataframe"""


@data_cli.command("list")
@decorators.with_dbenv()
def list_():  # pylint: disable=redefined-builtin
    """
    Display all PandasFrameData nodes
    """
    PandasFrameData = DataFactory("dataframe.frame")

    qb = QueryBuilder()
    qb.append(PandasFrameData)
    results = qb.all()

    s = ""
    for result in results:
        obj = result[0]
        s += f"{str(obj)}, pk: {obj.pk}\n"
    sys.stdout.write(s)


@data_cli.command("show")
@click.argument(
    "node",
    metavar="IDENTIFIER",
    type=DataParamType(sub_classes=("aiida.data:dataframe.frame",)),
)
@decorators.with_dbenv()
def show(node):
    """Export a PandasDataFrame node (identified by PK, UUID or label) to plain text."""
    click.echo(tabulate(node.df, headers="keys", tablefmt="psql"))


@data_cli.command("export")
@click.argument(
    "node",
    metavar="IDENTIFIER",
    type=DataParamType(sub_classes=("aiida.data:dataframe.frame",)),
)
@click.option(
    "--outfile",
    "-o",
    type=click.Path(dir_okay=False),
    help="Write output to file (default: print to stdout).",
)
@decorators.with_dbenv()
def export(node, outfile):
    """Export a PandasDataFrame node (identified by PK, UUID or label) to plain text."""

    if outfile:
        node.df.to_csv(outfile)
    else:
        with tempfile.TemporaryFile("w+", newline="") as file:
            node.df.to_csv(file)
            file.seek(0)
            content = file.read()
        click.echo(content)
