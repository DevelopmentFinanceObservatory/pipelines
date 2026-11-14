"""Check the StatVars a build defines against the knowledge graph.

`run_graph_check` reads each export's StatVar table, sends its definitions to the
Graph API's check route, and renders what the route finds as a Markdown report.
"""

from dfo_pipelines.graph_check.client import GraphCheckError, check_rows
from dfo_pipelines.graph_check.report import FINDING_CODES, CheckSummary, render_report
from dfo_pipelines.graph_check.rows import CheckRow, statvar_check_rows
from dfo_pipelines.graph_check.run import run_graph_check

__all__ = [
    "FINDING_CODES",
    "CheckRow",
    "CheckSummary",
    "GraphCheckError",
    "check_rows",
    "render_report",
    "run_graph_check",
    "statvar_check_rows",
]
