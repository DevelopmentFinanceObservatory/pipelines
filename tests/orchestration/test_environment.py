import dagster as dg
import pytest

from dfo_pipelines.definitions import defs
from dfo_pipelines.orchestration.environment import Environment, dfo_environment


@pytest.mark.parametrize("environment", list(Environment))
def test_dfo_environment_reads_each_environment(
    monkeypatch: pytest.MonkeyPatch, environment: Environment
) -> None:
    """Test each environment's value selects that environment"""
    monkeypatch.setenv("DFO_ENVIRONMENT", environment.value)

    assert dfo_environment() is environment


def test_dfo_environment_fails_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test an unset DFO_ENVIRONMENT fails, naming the variable"""
    monkeypatch.delenv("DFO_ENVIRONMENT", raising=False)

    with pytest.raises(ValueError, match="DFO_ENVIRONMENT"):
        dfo_environment()


def test_dfo_environment_fails_when_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test an unknown DFO_ENVIRONMENT fails, naming the allowed values"""
    monkeypatch.setenv("DFO_ENVIRONMENT", "production")

    with pytest.raises(ValueError, match=r"DFO_ENVIRONMENT.*local"):
        dfo_environment()


def test_defs_bind_the_local_file_stores_to_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test the local environment keeps landing and export files under data/"""
    monkeypatch.setenv("DFO_ENVIRONMENT", Environment.LOCAL.value)

    resources = defs().resources

    assert resources is not None
    assert resources["landing_file_store"].base_path == "data/landing"
    assert resources["export_file_store"].base_path == "data/exports"


def test_defs_bind_the_dev_landing_store_to_its_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test the dev environment reads the landing store from LANDING_STORE_URI"""
    monkeypatch.setenv("DFO_ENVIRONMENT", Environment.DEV.value)

    resources = defs().resources

    assert resources is not None
    assert resources["landing_file_store"].base_path == dg.EnvVar("LANDING_STORE_URI")
