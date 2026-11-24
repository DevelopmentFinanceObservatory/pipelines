import pytest

from dfo_pipelines.definitions import defs
from dfo_pipelines.orchestration.environment import (
    EXPORT_BASE_PATHS,
    LANDING_BASE_PATHS,
    Environment,
    dfo_environment,
)


@pytest.mark.parametrize("environment", list(Environment))
def test_dfo_environment_reads_each_environment(
    monkeypatch: pytest.MonkeyPatch, environment: Environment
) -> None:
    """Test each environment's value selects that environment"""
    monkeypatch.setenv("DFO_ENVIRONMENT", environment.value)

    assert dfo_environment() is environment


def test_every_environment_has_base_paths() -> None:
    """Test each environment has a landing and an export base path"""
    assert set(LANDING_BASE_PATHS) == set(Environment)
    assert set(EXPORT_BASE_PATHS) == set(Environment)


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


@pytest.mark.parametrize("environment", list(Environment))
def test_defs_bind_the_file_stores_of_the_environment(
    monkeypatch: pytest.MonkeyPatch, environment: Environment
) -> None:
    """Test the definitions bind each file store to the environment's base path"""
    monkeypatch.setenv("DFO_ENVIRONMENT", environment.value)

    resources = defs().resources

    assert resources is not None
    assert resources["landing_file_store"].base_path == LANDING_BASE_PATHS[environment]
    assert resources["export_file_store"].base_path == EXPORT_BASE_PATHS[environment]
