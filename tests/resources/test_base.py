import dagster as dg
from pydantic import ValidationError

from tests.helpers import project_definitions


def _rejects_undeclared_keyword(resource_type: type[dg.ConfigurableResource]) -> bool:
    """Whether constructing the resource with an undeclared keyword argument raises"""
    try:
        resource_type(undeclared="value")
    except ValidationError as error:
        return any(detail["type"] == "extra_forbidden" for detail in error.errors())
    return False


def test_project_resources_reject_undeclared_keyword_arguments() -> None:
    """Test every resource class the project defines raises on an undeclared keyword"""
    project_resource_types = {
        key: type(resource)
        for key, resource in (project_definitions().resources or {}).items()
        if type(resource).__module__.startswith("dfo_pipelines")
    }

    assert project_resource_types
    assert [
        key
        for key, resource_type in project_resource_types.items()
        if not _rejects_undeclared_keyword(resource_type)
    ] == []
