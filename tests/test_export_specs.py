import importlib
from pathlib import Path

import dagster as dg

import dfo_pipelines.defs
import dfo_pipelines.defs.export_specs
from dfo_pipelines.data_commons.export import ExportSpec
from dfo_pipelines.definitions import defs
from dfo_pipelines.defs.automation import EAGER_ON_DEPS_OR_CODE_CHANGE_WHEN_CHECKS_PASS
from dfo_pipelines.defs.export_specs import EXPORT_SPECS


def defs_module_names() -> list[str]:
    """The name of every module under `dfo_pipelines.defs` but the registry

    Found by file rather than by `pkgutil.walk_packages`, which does not descend
    into the pipeline directories since they are namespace packages.
    """
    defs_dir = Path(dfo_pipelines.defs.__file__).parent
    names = []
    for path in sorted(defs_dir.rglob("*.py")):
        parts = path.relative_to(defs_dir).with_suffix("").parts
        if parts[-1] == "__init__":
            parts = parts[:-1]
        names.append(".".join((dfo_pipelines.defs.__name__, *parts)))
    names.remove(dfo_pipelines.defs.export_specs.__name__)
    return names


def module_level_export_specs() -> list[ExportSpec]:
    """Every ExportSpec bound to a module-level name under `dfo_pipelines.defs`

    The registry module is left out, so a spec counts only where a pipeline
    binds it.
    """
    specs: list[ExportSpec] = []
    for name in defs_module_names():
        module = importlib.import_module(name)
        specs.extend(
            value for value in vars(module).values() if isinstance(value, ExportSpec)
        )
    return specs


def test_export_specs_lists_every_module_level_export_spec() -> None:
    """Test no pipeline defines an ExportSpec the registry leaves out"""
    specs = module_level_export_specs()
    registered = {id(spec) for spec in EXPORT_SPECS}

    assert specs
    unregistered = sorted(
        spec.import_name for spec in specs if id(spec) not in registered
    )
    assert unregistered == []


def test_export_specs_mint_unique_provenance_dcids() -> None:
    """Test no two exports register the same provenance node"""
    dcids = [spec.minted_provenance_dcid for spec in EXPORT_SPECS]

    assert len(dcids) == len(set(dcids))


def test_export_specs_version_their_exports_by_documentation() -> None:
    """Test each spec's export asset takes its documentation version and automates on it"""
    graph = defs().resolve_asset_graph()
    nodes = {
        spec.import_name: graph.get(dg.AssetKey(f"{spec.import_name}_dc_export"))
        for spec in EXPORT_SPECS
    }
    code_versions = {name: node.code_version for name, node in nodes.items()}
    automation_conditions = {
        name: node.automation_condition for name, node in nodes.items()
    }

    assert code_versions == {
        spec.import_name: spec.documentation_version for spec in EXPORT_SPECS
    }
    assert automation_conditions == {
        spec.import_name: EAGER_ON_DEPS_OR_CODE_CHANGE_WHEN_CHECKS_PASS
        for spec in EXPORT_SPECS
    }
