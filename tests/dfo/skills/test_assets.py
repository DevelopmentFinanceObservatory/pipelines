import re
import socket
import subprocess
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from functools import cache
from pathlib import Path
from typing import Any
from unittest.mock import patch

import dagster as dg
import pytest
from upath import UPath

from dfo_pipelines.data_commons.export import ExportSpec
from dfo_pipelines.definitions import defs
from dfo_pipelines.defs.dfo.ontology.assets import ONTOLOGY_MCF
from dfo_pipelines.defs.dfo.skills.assets import (
    IMPORT_NAME,
    MAX_FILE_BYTES,
    MAX_PATH_CHARACTERS,
    MAX_REFERENCES,
    dfo_skills_dc_export,
    dfo_skills_dc_ingestion,
    dfo_skills_raw_directory,
    dfo_skills_source,
)
from dfo_pipelines.defs.export_specs import EXPORT_SPECS
from dfo_pipelines.defs.resources import (
    KnowledgeGraphResource,
    SkillsRepositoryResource,
    UPathFileStoreResource,
)
from tests.helpers import AutomationHarness, meta

FIXTURES = Path(__file__).parent / "fixtures"

REPOSITORY_URL = "https://github.com/example-org/example-skills"
# The repository the export fixtures' links were written for.
SKILLS_REPOSITORY = SkillsRepositoryResource(url=REPOSITORY_URL)


def git(repository: Path, *args: str) -> str:
    """Run git in a repository, with an identity so commits work anywhere"""
    return subprocess.run(
        ["git", "-c", "user.name=test", "-c", "user.email=test@example.com", *args],
        cwd=repository,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def commit(
    repository: Path, files: dict[str, str], *, internal: dict[str, str] | None = None
) -> str:
    """Replace the repository's skills with the given files and commit it

    `files` go under `skills/` and `internal` under `internal/skills/`, which
    the repository lacks when none are given.
    """
    git(repository, "rm", "-rq", "--ignore-unmatch", "skills", "internal")
    roots = {"skills": files, "internal/skills": internal or {}}
    for root, root_files in roots.items():
        for name, text in root_files.items():
            path = repository / root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
    git(repository, "add", "-A")
    git(repository, "commit", "-qm", "update skills")
    return git(repository, "rev-parse", "HEAD")


INTERNAL_SKILLS = {
    "dfo-faq/SKILL.md": "---\nname: dfo-faq\ndescription: test\n---\n",
    "dfo-faq/attachments.yaml": "",
}

SKILLS = {
    "oda-concepts/SKILL.md": "---\nname: oda-concepts\ndescription: test\n---\n",
    "oda-concepts/references/core-oda.md": "What this covers: Core ODA.\n",
    "oda-concepts/attachments.yaml": "",
    "_template/SKILL.md": "---\nname: skill-template\ndescription: test\n---\n",
}


def redirect_repository(monkeypatch: pytest.MonkeyPatch, location: str) -> None:
    """Have git fetch REPOSITORY_URL from another location, through its own config"""
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", f"url.{location}.insteadOf")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", REPOSITORY_URL)


@pytest.fixture
def upstream(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A local repository that git fetches in place of the skills repository"""
    repository = tmp_path / "dfo-skills"
    repository.mkdir()
    git(repository, "init", "-q", "--initial-branch=main")
    commit(repository, SKILLS)
    redirect_repository(monkeypatch, repository.as_uri())
    return repository


def materialize_raw_directory(tmp_path: Path) -> Any:
    """Run the raw directory asset against a file store under tmp_path"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))
    return dfo_skills_raw_directory(
        context=dg.build_asset_context(),
        raw_file_store=file_store,
        skills_repository=SKILLS_REPOSITORY,
    )


def files_under(directory: Path) -> dict[str, str]:
    """Every file below a directory, keyed on its path relative to it"""
    return {
        path.relative_to(directory).as_posix(): path.read_text()
        for path in directory.rglob("*")
        if path.is_file()
    }


def raw_files(
    skills: dict[str, str], internal: dict[str, str] | None = None
) -> dict[str, str]:
    """The files a raw directory holds for a repository's two roots"""
    return {f"skills/{name}": text for name, text in skills.items()} | {
        f"internal/skills/{name}": text for name, text in (internal or {}).items()
    }


def test_raw_directory_copies_the_skills_at_the_head_of_main(
    tmp_path: Path, upstream: Path
) -> None:
    """Test the published skills arrive unchanged, traced to their commit"""
    result = materialize_raw_directory(tmp_path)

    assert files_under(Path(str(result.value))) == raw_files(SKILLS)
    assert meta(result, "version", dg.TextMetadataValue).value == git(
        upstream, "rev-parse", "HEAD"
    )
    assert "commit" not in (result.metadata or {})
    [check] = result.check_results
    assert check.check_name == "skills_not_empty"
    assert check.passed


@dg.asset(name="dfo_ontology_raw_directory")
def _stub_ontology_raw_directory() -> str:
    """A compiled ontology, standing in for the one the ontology pipeline writes"""
    return str(FIXTURES / "ontology")


def test_raw_directory_check_fails_when_only_the_template_exists(
    tmp_path: Path, upstream: Path
) -> None:
    """Test the template is not counted as a skill, and blocks the export"""
    commit(upstream, {"_template/SKILL.md": SKILLS["_template/SKILL.md"]})

    result = dg.materialize(
        [dfo_skills_raw_directory, dfo_skills_dc_export, _stub_ontology_raw_directory],
        resources={
            "raw_file_store": UPathFileStoreResource(base_path=str(tmp_path / "raw")),
            "export_file_store": UPathFileStoreResource(
                base_path=str(tmp_path / "processed")
            ),
            "skills_repository": SKILLS_REPOSITORY,
        },
        raise_on_error=False,
    )

    [check] = result.get_asset_check_evaluations()
    assert not check.passed
    assert not result.asset_materializations_for_node("dfo_skills_dc_export")


def test_raw_directory_drops_a_skill_deleted_upstream_on_a_same_day_rerun(
    tmp_path: Path, upstream: Path
) -> None:
    """Test a rerun replaces the day's copy rather than adding to it"""
    commit(upstream, SKILLS, internal=INTERNAL_SKILLS)
    materialize_raw_directory(tmp_path)
    replaced = {"oda-channels/SKILL.md": "---\nname: oda-channels\n---\n"}
    commit(upstream, replaced)

    result = materialize_raw_directory(tmp_path)

    assert files_under(Path(str(result.value))) == raw_files(replaced)


def test_raw_directory_keeps_the_previous_copy_when_the_clone_fails(
    tmp_path: Path, upstream: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test a failed fetch leaves the day's earlier copy in place"""
    published = Path(str(materialize_raw_directory(tmp_path).value))

    redirect_repository(monkeypatch, (tmp_path / "missing").as_uri())
    with pytest.raises(RuntimeError, match="git clone failed"):
        materialize_raw_directory(tmp_path)

    assert files_under(published) == raw_files(SKILLS)


def test_raw_directory_fails_when_git_stalls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test a remote that never responds fails the run rather than hanging it"""
    # Connections queue in the backlog but are never accepted or answered.
    with socket.create_server(("127.0.0.1", 0)) as server:
        host, port = server.getsockname()
        redirect_repository(monkeypatch, f"http://{host}:{port}/dfo-skills")
        with (
            patch("dfo_pipelines.defs.dfo.skills.assets.GIT_TIMEOUT_SECONDS", 1),
            pytest.raises(RuntimeError, match="git clone timed out after 1 seconds"),
        ):
            materialize_raw_directory(tmp_path)


@pytest.mark.parametrize(
    ("link", "target", "error"),
    [
        (
            "skills/oda-concepts/references/leak.md",
            "SKILL.md",
            "oda-concepts/references/leak.md",
        ),
        ("skills", ".", "skills/ as a symlink"),
        ("internal", ".", "internal/ as a symlink"),
        (
            "internal/skills/dfo-faq/references/leak.md",
            "SKILL.md",
            "under internal/skills/: ['dfo-faq/references/leak.md']",
        ),
    ],
    ids=[
        "file_under_skills",
        "skills_directory",
        "parent_of_the_internal_root",
        "file_under_internal_skills",
    ],
)
def test_raw_directory_rejects_a_symlink_and_keeps_the_previous_copy(
    tmp_path: Path, upstream: Path, link: str, target: str, error: str
) -> None:
    """Test a committed symlink fails the fetch rather than copying its target"""
    published = Path(str(materialize_raw_directory(tmp_path).value))
    secret = tmp_path / "secret"
    secret.mkdir()
    (secret / "SKILL.md").write_text("not for the graph")
    git(upstream, "rm", "-rq", "--ignore-unmatch", link)
    (upstream / link).parent.mkdir(parents=True, exist_ok=True)
    (upstream / link).symlink_to(secret / target)
    git(upstream, "add", "-A")
    git(upstream, "commit", "-qm", "add a symlink")

    with pytest.raises(RuntimeError, match=re.escape(error)):
        materialize_raw_directory(tmp_path)

    assert files_under(published) == raw_files(SKILLS)


class _SkillsAutomation(AutomationHarness):
    """Drives the skills assets against an ephemeral instance, as the daemon does

    The local repository stands in for the skills repository, so committing to
    it is what moves the observed version.
    """

    def __init__(self, tmp_path: Path) -> None:
        super().__init__(
            tmp_path,
            source=dfo_skills_source,
            raw_assets=[dfo_skills_raw_directory],
            resources={"skills_repository": SKILLS_REPOSITORY},
        )

    def _start_mocks(self) -> None:
        """The upstream fixture redirects git, so there is nothing to patch"""


def test_source_is_requested_once_per_daily_cron_tick(
    tmp_path: Path, upstream: Path
) -> None:
    """Test the source's condition requests one observation per UTC day"""
    with _SkillsAutomation(tmp_path) as automation:
        # A baseline tick: every condition is reset on its initial evaluation,
        # so the first tick requests nothing whatever the time is.
        automation.source_tick(at=datetime(2026, 8, 25, 12, 0, tzinfo=UTC))

        assert automation.source_tick(at=datetime(2026, 8, 25, 13, 0, tzinfo=UTC)) == 0
        assert automation.source_tick(at=datetime(2026, 8, 26, 0, 1, tzinfo=UTC)) == 1
        assert automation.source_tick(at=datetime(2026, 8, 26, 0, 2, tzinfo=UTC)) == 0


def test_raw_directory_is_not_rerun_when_main_is_unchanged(
    tmp_path: Path, upstream: Path
) -> None:
    """Test a repeat observation of the same commit requests no rerun"""
    with _SkillsAutomation(tmp_path) as automation:
        automation.observe()
        automation.materialize(dfo_skills_raw_directory)
        # A baseline tick, not evidence: every condition is reset on its initial
        # evaluation, so the first tick requests nothing whatever the source did.
        automation.tick()

        automation.observe()

        assert automation.tick() == {"dfo_skills_raw_directory": 0}


def test_raw_directory_is_rerun_once_when_main_moves(
    tmp_path: Path, upstream: Path
) -> None:
    """Test a new commit on main requests the raw directory, and only once"""
    with _SkillsAutomation(tmp_path) as automation:
        automation.observe()
        automation.materialize(dfo_skills_raw_directory)
        automation.tick()  # baseline, as above

        commit(upstream, SKILLS | {"oda-channels/SKILL.md": "---\nname: x\n---\n"})
        automation.observe()

        assert automation.tick() == {"dfo_skills_raw_directory": 1}

        automation.materialize(dfo_skills_raw_directory)

        assert automation.tick() == {"dfo_skills_raw_directory": 0}


def test_source_observes_the_head_commit_of_the_repository(upstream: Path) -> None:
    """Test the source reports the head commit as its version and the repository as its uri"""
    observe = dfo_skills_source.observe_fn
    assert observe is not None

    result = observe(skills_repository=SKILLS_REPOSITORY)

    head = git(upstream, "rev-parse", "HEAD")
    assert isinstance(result, dg.ObserveResult)
    assert result.data_version == dg.DataVersion(head)
    assert result.metadata == {
        "dagster/uri": dg.TextMetadataValue(REPOSITORY_URL),
        "version": dg.TextMetadataValue(head),
    }


def test_raw_directory_reports_the_version_the_source_observed(
    tmp_path: Path, upstream: Path
) -> None:
    """Test the raw directory's version equals the source's version for the same commit"""
    observe = dfo_skills_source.observe_fn
    assert observe is not None

    observation = observe(skills_repository=SKILLS_REPOSITORY)
    result = materialize_raw_directory(tmp_path)

    assert isinstance(observation, dg.ObserveResult)
    assert observation.metadata is not None
    assert (
        meta(result, "version", dg.TextMetadataValue).value
        == observation.metadata["version"].value
    )


def test_dc_export_writes_the_expected_bundle(tmp_path: Path) -> None:
    """Test a skill, its reference and its attachments export as the expected bundle

    The skill's attachments.yaml lists two ontology nodes and two export
    provenances, exported in file order, and is itself left out of the bundle.
    """
    result = dfo_skills_dc_export(
        context=dg.build_asset_context(),
        export_file_store=UPathFileStoreResource(base_path=str(tmp_path)),
        skills_repository=SKILLS_REPOSITORY,
        dfo_skills_raw_directory=str(FIXTURES),
        dfo_ontology_raw_directory=str(FIXTURES / "ontology"),
    )

    assert isinstance(result, dg.MaterializeResult)
    out_dir = Path(meta(result, "dagster/uri", dg.TextMetadataValue).value)
    assert files_under(out_dir) == files_under(FIXTURES / "dc_export")


@dg.asset(name="dfo_skills_raw_directory")
def _stub_skills_raw_directory() -> str:
    """Raw skills, standing in for a clone of dfo-skills"""
    return str(FIXTURES)


def test_ingestion_ingests_the_bundle_the_export_wrote(
    tmp_path: Path, knowledge_graph: KnowledgeGraphResource
) -> None:
    """Test the ingestion is handed the export's directory under its import name"""
    destination = f"gs://test-bucket/ingestion/input/{IMPORT_NAME}"

    with patch(
        "dfo_pipelines.defs.dfo.skills.assets.ingest_bundle",
        return_value=destination,
    ) as ingest_bundle:
        result = dg.materialize(
            [
                _stub_skills_raw_directory,
                _stub_ontology_raw_directory,
                dfo_skills_dc_export,
                dfo_skills_dc_ingestion,
            ],
            resources={
                "export_file_store": UPathFileStoreResource(
                    base_path=str(tmp_path / "processed")
                ),
                "knowledge_graph": knowledge_graph,
                "skills_repository": SKILLS_REPOSITORY,
            },
        )

    assert result.success
    [export] = result.asset_materializations_for_node("dfo_skills_dc_export")
    [ingestion] = result.asset_materializations_for_node("dfo_skills_dc_ingestion")
    bundle = export.metadata["dagster/uri"].value
    ingest_bundle.assert_called_once()
    args, kwargs = ingest_bundle.call_args
    assert str(args[0]) == bundle
    assert kwargs["import_name"] == IMPORT_NAME
    assert kwargs["settings"] == knowledge_graph.settings
    assert ingestion.metadata["dagster/uri"].value == destination
    assert "import_name" not in export.metadata
    assert ingestion.metadata["import_name"].value == IMPORT_NAME


def write_skill(
    skills_root: Path,
    name: str,
    references: dict[str, str] | None = None,
    *,
    files: dict[str, str] | None = None,
) -> None:
    """Write a minimal raw skill directory

    `references` are the files under references/, and `files` any other files,
    keyed on their path relative to the skill directory. The attachments.yaml
    is empty, attaching the skill nowhere, unless `files` holds one.
    """
    skill_dir = skills_root / name
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(f"---\nname: {name}\ndescription: test\n---\n")
    written = {f"references/{path}": text for path, text in (references or {}).items()}
    for relative, text in ({"attachments.yaml": ""} | written | (files or {})).items():
        path = skill_dir / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)


def write_ontology(ontology_root: Path, dcids: Iterable[str] = ()) -> Path:
    """Write a minimal compiled ontology MCF holding the given node dcids"""
    ontology_root.mkdir(parents=True, exist_ok=True)
    (ontology_root / ONTOLOGY_MCF).write_text(
        "".join(f"Node: {dcid}\n" for dcid in dcids)
    )
    return ontology_root


def export_skills(
    tmp_path: Path,
    ontology_root: Path | None = None,
    *,
    raw_directory: Path | None = None,
    skills_repository: SkillsRepositoryResource = SKILLS_REPOSITORY,
) -> tuple[dg.MaterializeResult, Path]:
    """Run the export asset against a raw skills and ontology directory

    The raw directory defaults to tmp_path, whose `skills/` and
    `internal/skills/` are then the roots, and the ontology to an empty one.
    """
    if ontology_root is None:
        ontology_root = write_ontology(tmp_path / "ontology")
    result = dfo_skills_dc_export(
        context=dg.build_asset_context(),
        export_file_store=UPathFileStoreResource(base_path=str(tmp_path / "processed")),
        skills_repository=skills_repository,
        dfo_skills_raw_directory=str(raw_directory or tmp_path),
        dfo_ontology_raw_directory=str(ontology_root),
    )
    assert isinstance(result, dg.MaterializeResult)
    return result, Path(meta(result, "dagster/uri", dg.TextMetadataValue).value)


def test_raw_directory_and_export_carry_a_skill_from_each_root(
    tmp_path: Path, upstream: Path
) -> None:
    """Test both roots reach the raw directory and each skill links to its own"""
    commit(upstream, SKILLS, internal=INTERNAL_SKILLS)

    raw_result = materialize_raw_directory(tmp_path)

    raw_directory = Path(str(raw_result.value))
    assert files_under(raw_directory) == raw_files(SKILLS, INTERNAL_SKILLS)
    assert meta(raw_result, "skill_count", dg.IntMetadataValue).value == 2

    export_result, out_dir = export_skills(tmp_path, raw_directory=raw_directory)

    assert meta(export_result, "skill_count", dg.IntMetadataValue).value == 2
    nodes = (out_dir / "custom_nodes.mcf").read_text()
    assert f'url: "{REPOSITORY_URL}/tree/main/skills/oda-concepts"\n' in nodes
    assert f'url: "{REPOSITORY_URL}/tree/main/internal/skills/dfo-faq"\n' in nodes


def test_dc_export_rejects_a_name_in_both_roots(tmp_path: Path) -> None:
    """Test two skills that would share a graph id fail before anything is written"""
    write_skill(tmp_path / "skills", "example-skill")
    write_skill(tmp_path / "internal/skills", "example-skill")
    write_skill(tmp_path / "internal/skills", "other-skill")

    with pytest.raises(RuntimeError, match=r"\['example-skill'\] are in more than one"):
        export_skills(tmp_path)

    assert not (tmp_path / "processed").exists()


def test_dc_export_fails_when_the_internal_root_cannot_be_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test a store error on the internal root is not read as having no such root

    The root is patched to behave as a remote store's does when a request for
    it fails: listing it raises, and asking whether it is a directory answers
    False.
    """
    write_skill(tmp_path / "skills", "example-skill")
    write_skill(tmp_path / "internal/skills", "other-skill")
    internal_root = UPath(tmp_path) / "internal/skills"
    path_class = type(internal_root)
    is_dir, iterdir = path_class.is_dir, path_class.iterdir

    def failing_is_dir(self: UPath, **kwargs: Any) -> bool:
        return self != internal_root and is_dir(self, **kwargs)

    def failing_iterdir(self: UPath) -> Iterator[UPath]:
        if self == internal_root:
            raise PermissionError(f"cannot list {self}")
        return iterdir(self)

    monkeypatch.setattr(path_class, "is_dir", failing_is_dir)
    monkeypatch.setattr(path_class, "iterdir", failing_iterdir)

    with pytest.raises(PermissionError, match="cannot list"):
        export_skills(tmp_path)

    assert not (tmp_path / "processed").exists()


@pytest.mark.parametrize(
    "unreadable",
    ["internal/skills/other-skill", "skills/example-skill/references/note.md"],
    ids=["skill-directory", "reference-file"],
)
def test_dc_export_fails_when_a_listed_entry_cannot_be_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, unreadable: str
) -> None:
    """Test a store error on a listed entry does not drop it from the export

    The entry is patched to behave as a remote store's does when a request for
    its metadata fails: reading its metadata raises, and asking whether it is a
    directory or a file answers False.
    """
    write_skill(tmp_path / "skills", "example-skill", {"note.md": "A note.\n"})
    write_skill(tmp_path / "internal/skills", "other-skill")
    entry = UPath(tmp_path) / unreadable
    path_class = type(entry)
    is_dir, is_file, stat = path_class.is_dir, path_class.is_file, path_class.stat

    def failing_is_dir(self: UPath, **kwargs: Any) -> bool:
        return self != entry and is_dir(self, **kwargs)

    def failing_is_file(self: UPath, **kwargs: Any) -> bool:
        return self != entry and is_file(self, **kwargs)

    def failing_stat(self: UPath, **kwargs: Any) -> Any:
        if self == entry:
            raise PermissionError(f"cannot read {self}")
        return stat(self, **kwargs)

    monkeypatch.setattr(path_class, "is_dir", failing_is_dir)
    monkeypatch.setattr(path_class, "is_file", failing_is_file)
    monkeypatch.setattr(path_class, "stat", failing_stat)

    with pytest.raises(PermissionError):
        export_skills(tmp_path)

    assert not (tmp_path / "processed").exists()


def test_dc_export_rejects_a_raw_directory_without_a_skills_root(
    tmp_path: Path,
) -> None:
    """Test a raw directory holding skills at its top level is not read as empty"""
    write_skill(tmp_path, "example-skill")

    with pytest.raises(RuntimeError, match="has no skills/ directory"):
        export_skills(tmp_path)

    assert not (tmp_path / "processed").exists()


def test_raw_directory_and_export_skip_a_draft_with_only_a_readme(
    tmp_path: Path, upstream: Path
) -> None:
    """Test a `_draft/README.md`-only directory is skipped by both, without raising"""
    commit(upstream, SKILLS | {"_draft/README.md": "Notes for a draft skill.\n"})

    raw_result = materialize_raw_directory(tmp_path)
    assert meta(raw_result, "skill_count", dg.IntMetadataValue).value == 1

    export_result, _ = export_skills(
        tmp_path, raw_directory=Path(str(raw_result.value))
    )
    assert meta(export_result, "skill_count", dg.IntMetadataValue).value == 1


def test_raw_directory_and_export_skip_a_draft_with_a_skill_file(
    tmp_path: Path, upstream: Path
) -> None:
    """Test a `_draft/SKILL.md` with valid front matter is still skipped by both"""
    commit(
        upstream,
        SKILLS | {"_draft/SKILL.md": "---\nname: draft-skill\n---\n"},
    )

    raw_result = materialize_raw_directory(tmp_path)
    assert meta(raw_result, "skill_count", dg.IntMetadataValue).value == 1

    export_result, _ = export_skills(
        tmp_path, raw_directory=Path(str(raw_result.value))
    )
    assert meta(export_result, "skill_count", dg.IntMetadataValue).value == 1


def test_dc_export_exports_a_nested_reference(tmp_path: Path) -> None:
    """Test a reference nested under references/ is exported with a path-based DCID"""
    skills_root = tmp_path / "skills"
    write_skill(skills_root, "example-skill", {"sub/a.md": "nested content\n"})

    result, out_dir = export_skills(tmp_path)

    nodes = (out_dir / "custom_nodes.mcf").read_text()
    assert 'Node: dcid:skill/example-skill/sub/a\nname: "references/sub/a.md"' in nodes
    assert "agentSkillReferences: dcid:skill/example-skill/sub/a" in nodes
    assert meta(result, "reference_count", dg.IntMetadataValue).value == 1


def test_dc_export_disambiguates_references_sharing_a_stem(tmp_path: Path) -> None:
    """Test references/a/x.md and references/b/x.md produce two distinct nodes"""
    skills_root = tmp_path / "skills"
    write_skill(
        skills_root, "example-skill", {"a/x.md": "from a\n", "b/x.md": "from b\n"}
    )

    result, out_dir = export_skills(tmp_path)

    nodes = (out_dir / "custom_nodes.mcf").read_text()
    assert "Node: dcid:skill/example-skill/a/x" in nodes
    assert "Node: dcid:skill/example-skill/b/x" in nodes
    assert meta(result, "reference_count", dg.IntMetadataValue).value == 2


def assert_export_fails(
    tmp_path: Path,
    skill_name: str,
    file: str,
    problem: str = "",
    ontology_root: Path | None = None,
) -> None:
    """Assert the export raises an error naming the skill, file and problem

    The export must write nothing.
    """
    with pytest.raises(RuntimeError) as error:
        export_skills(tmp_path, ontology_root)

    assert f"skill {skill_name}: {file}: {problem}" in str(error.value)
    assert not (tmp_path / "processed").exists()


def test_dc_export_links_to_the_configured_repository_and_branch(
    tmp_path: Path,
) -> None:
    """Test the provenance and each skill link to the repository the resource names"""
    skills_root = tmp_path / "skills"
    write_skill(skills_root, "example-skill")
    url = "https://github.com/another-org/skills"

    _, out_dir = export_skills(
        tmp_path,
        skills_repository=SkillsRepositoryResource(url=url, branch="release"),
    )

    assert f'url: "{url}"\n' in (out_dir / "provenance.mcf").read_text()
    assert (
        f'url: "{url}/tree/release/skills/example-skill"\n'
        in (out_dir / "custom_nodes.mcf").read_text()
    )


@pytest.mark.parametrize(
    "url",
    [
        f"{REPOSITORY_URL}/",
        f"{REPOSITORY_URL}.git",
        "git@github.com:example-org/example-skills",
        "",
        f"{REPOSITORY_URL} ",
        "example-org/example-skills",
        "--upload-pack=example",
    ],
    ids=[
        "trailing_slash",
        "dot_git",
        "ssh",
        "empty",
        "trailing_space",
        "owner_and_repository_only",
        "git_option",
    ],
)
def test_assets_refuse_a_url_that_is_not_a_github_repository_page(
    tmp_path: Path, url: str
) -> None:
    """Test a URL that would publish broken skill links stops the export"""
    skills_root = tmp_path / "skills"
    write_skill(skills_root, "example-skill")

    with pytest.raises(dg.DagsterResourceFunctionError) as error:
        export_skills(tmp_path, skills_repository=SkillsRepositoryResource(url=url))

    assert isinstance(error.value.__cause__, ValueError)
    assert repr(url) in str(error.value.__cause__)
    assert not (tmp_path / "processed").exists()


@cache
def _project_skills_repository() -> object:
    """The skills repository resource as the project's definitions wire it

    Cached because loading the project's definitions takes several seconds.
    """
    project_resources = defs().resources
    assert project_resources is not None
    return project_resources["skills_repository"]


def _observe_project_source() -> dg.ExecuteInProcessResult:
    """Observe the skills source with the resource the project's definitions wire"""
    project = dg.Definitions(
        assets=[dfo_skills_source],
        jobs=[dg.define_asset_job("observe", selection=[dfo_skills_source.key])],
        resources={"skills_repository": _project_skills_repository()},
    )
    return project.resolve_job_def("observe").execute_in_process()


def test_project_reads_the_repository_from_the_environment(
    upstream: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test the deployment's variable decides which repository is observed"""
    monkeypatch.setenv("DFO_SKILLS_REPOSITORY_URL", REPOSITORY_URL)

    result = _observe_project_source()

    [observation] = result.asset_observations_for_node("dfo_skills_source")
    assert observation.metadata["version"].value == git(upstream, "rev-parse", "HEAD")


def test_project_fails_a_skills_run_at_launch_without_the_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test an unset variable fails the run before any step, naming the variable"""
    monkeypatch.delenv("DFO_SKILLS_REPOSITORY_URL", raising=False)

    with pytest.raises(dg.DagsterInvalidConfigError, match="DFO_SKILLS_REPOSITORY_URL"):
        _observe_project_source()


@pytest.mark.parametrize(
    "path",
    [
        "references/diagram.png",
        "notes.md",
        "scripts/run.py",
        "references.md",
        "references/attachments.yaml",
    ],
    ids=[
        "non_markdown_reference",
        "root_markdown",
        "script",
        "references_file",
        "nested_attachments",
    ],
)
def test_dc_export_rejects_a_file_outside_the_bundle_layout(
    tmp_path: Path, path: str
) -> None:
    """Test a file other than SKILL.md, attachments.yaml and references/**/*.md fails"""
    skills_root = tmp_path / "skills"
    write_skill(skills_root, "example-skill", files={path: "stray"})

    assert_export_fails(tmp_path, "example-skill", path)


def test_dc_export_rejects_a_front_matter_name_that_differs_from_the_directory(
    tmp_path: Path,
) -> None:
    """Test a SKILL.md naming another skill than its directory fails the run"""
    skills_root = tmp_path / "skills"
    write_skill(skills_root, "example-skill")
    (skills_root / "example-skill" / "SKILL.md").write_text(
        "---\nname: other-skill\ndescription: test\n---\n"
    )

    assert_export_fails(tmp_path, "example-skill", "SKILL.md")


@pytest.mark.parametrize(
    ("skill_name", "skill_file"),
    [
        ("example-skill", "\n---\nname: example-skill\ndescription: test\n---\n"),
        ("example-skill", "----\nname: example-skill\ndescription: test\n----\n"),
        ("example-skill", "---\nname: example-skill\ndescription: [a, b]\n---\n"),
        ("example-skill", "---\nname: example-skill\n---\n"),
        ("example-skill", "---\nname: [example-skill]\ndescription: test\n---\n"),
        ("example-skill", "---\nname: example-skill\ndescription: &d test\n---\n"),
        (
            "example-skill",
            "---\nname: example-skill\ndescription: test\nalso: *d\n---\n",
        ),
        ("Foo_Bar", "---\nname: Foo_Bar\ndescription: test\n---\n"),
        ("a" * 65, f"---\nname: {'a' * 65}\ndescription: test\n---\n"),
    ],
    ids=[
        "leading_blank_line",
        "four_dash_fence",
        "list_description",
        "missing_description",
        "non_string_name",
        "yaml_anchor",
        "yaml_alias",
        "invalid_directory_id",
        "id_over_64_characters",
    ],
)
def test_dc_export_rejects_what_the_reader_would_drop(
    tmp_path: Path, skill_name: str, skill_file: str
) -> None:
    """Test a skill id or front matter the reader rejects fails the run"""
    skills_root = tmp_path / "skills"
    write_skill(skills_root, skill_name)
    (skills_root / skill_name / "SKILL.md").write_text(skill_file)

    assert_export_fails(tmp_path, skill_name, "SKILL.md")


def test_dc_export_rejects_a_reserved_skill_id(tmp_path: Path) -> None:
    """Test a skill whose id the Skills API reader reserves fails the run"""
    skills_root = tmp_path / "skills"
    write_skill(skills_root, "healthz")

    assert_export_fails(tmp_path, "healthz", "SKILL.md", "directory name is a skill id")


def test_dc_export_keeps_the_carriage_returns_of_a_crlf_skill_file(
    tmp_path: Path,
) -> None:
    """Test a CRLF SKILL.md parses and is exported byte for byte"""
    skills_root = tmp_path / "skills"
    write_skill(skills_root, "example-skill")
    (skills_root / "example-skill" / "SKILL.md").write_bytes(
        b"---\r\nname: example-skill\r\ndescription: test\r\n---\r\nBody\r\n"
    )

    _, out_dir = export_skills(tmp_path)

    nodes = (out_dir / "custom_nodes.mcf").read_text()
    assert "description: test\\r\\n---\\r\\nBody\\r\\n" in nodes


def test_dc_export_rejects_a_skill_file_over_the_size_limit(tmp_path: Path) -> None:
    """Test a SKILL.md over 256 KiB fails the run, and one at the limit passes"""
    skills_root = tmp_path / "skills"
    write_skill(skills_root, "example-skill")
    front_matter = "---\nname: example-skill\ndescription: test\n---\n"
    skill_file = skills_root / "example-skill" / "SKILL.md"
    skill_file.write_text(front_matter + "a" * (MAX_FILE_BYTES - len(front_matter)))

    export_skills(tmp_path / "at_limit", raw_directory=tmp_path)

    skill_file.write_text(front_matter + "a" * (MAX_FILE_BYTES - len(front_matter) + 1))

    assert_export_fails(tmp_path, "example-skill", "SKILL.md")


def test_dc_export_rejects_a_reference_over_the_size_limit(tmp_path: Path) -> None:
    """Test a reference over 256 KiB fails the run, naming the reference"""
    skills_root = tmp_path / "skills"
    write_skill(skills_root, "example-skill", {"big.md": "a" * (MAX_FILE_BYTES + 1)})

    assert_export_fails(tmp_path, "example-skill", "references/big.md")


def test_dc_export_measures_the_size_limit_in_encoded_bytes(tmp_path: Path) -> None:
    """Test a reference under the limit in characters but over it in bytes fails"""
    skills_root = tmp_path / "skills"
    write_skill(
        skills_root, "example-skill", {"wide.md": "\u00e9" * (MAX_FILE_BYTES // 2 + 1)}
    )

    assert_export_fails(tmp_path, "example-skill", "references/wide.md")


def test_dc_export_rejects_a_reference_that_is_not_utf8(tmp_path: Path) -> None:
    """Test a reference whose bytes are not UTF-8 fails the run, naming it"""
    skills_root = tmp_path / "skills"
    write_skill(skills_root, "example-skill", {"latin.md": ""})
    not_utf8 = b"caf\xe9\n"  # codespell:ignore caf
    (skills_root / "example-skill/references/latin.md").write_bytes(not_utf8)

    assert_export_fails(tmp_path, "example-skill", "references/latin.md")


def test_dc_export_rejects_more_than_the_reference_limit(tmp_path: Path) -> None:
    """Test 51 references fail the run, and 50 pass"""
    skills_root = tmp_path / "skills"
    write_skill(
        skills_root,
        "example-skill",
        {f"ref-{i}.md": "content\n" for i in range(MAX_REFERENCES)},
    )

    result, _ = export_skills(tmp_path / "at_limit", raw_directory=tmp_path)
    assert meta(result, "reference_count", dg.IntMetadataValue).value == MAX_REFERENCES

    (skills_root / "example-skill/references/extra.md").write_text("content\n")

    assert_export_fails(tmp_path, "example-skill", "references/")


def test_dc_export_rejects_a_path_over_the_length_limit(tmp_path: Path) -> None:
    """Test a bundle-relative path over 512 characters fails the run"""
    skills_root = tmp_path / "skills"
    # Segments stay short enough for the filesystem's name limit.
    segments = ["a" * 50] * 10
    path = "/".join(["references", *segments, "x.md"])
    assert len(path) > MAX_PATH_CHARACTERS
    write_skill(skills_root, "example-skill", files={path: "content\n"})

    assert_export_fails(tmp_path, "example-skill", path)


def test_dc_export_rejects_a_skill_directory_without_a_skill_file(
    tmp_path: Path,
) -> None:
    """Test a non-draft directory with no SKILL.md fails the run, naming it"""
    skills_root = tmp_path / "skills"
    (skills_root / "example-skill").mkdir(parents=True)

    assert_export_fails(tmp_path, "example-skill", "SKILL.md")


@pytest.mark.parametrize(
    "filename",
    ["Capitalized.md", "has space.md"],
    ids=["uppercase", "space"],
)
def test_dc_export_rejects_an_invalid_reference_filename(
    tmp_path: Path, filename: str
) -> None:
    """Test a reference filename outside the DCID charset raises a named error"""
    skills_root = tmp_path / "skills"
    write_skill(skills_root, "example-skill", {filename: "content\n"})

    with pytest.raises(RuntimeError, match=re.escape(filename)):
        export_skills(tmp_path)


@pytest.mark.parametrize(
    "attachments",
    ["", "# Attaches nowhere.\n", "applies_to: []\nsources: []\n"],
    ids=["empty_file", "comments_only", "empty_lists"],
)
def test_dc_export_writes_no_attachment_properties_for_empty_lists(
    tmp_path: Path, attachments: str
) -> None:
    """Test a skill that attaches nowhere exports with neither attachment property"""
    skills_root = tmp_path / "skills"
    write_skill(skills_root, "example-skill", files={"attachments.yaml": attachments})

    _, out_dir = export_skills(tmp_path)

    nodes = (out_dir / "custom_nodes.mcf").read_text()
    assert "skillAppliesTo: " not in nodes
    assert "skillAppliesToProvenance: " not in nodes


def test_dc_export_rejects_a_skill_without_an_attachments_file(
    tmp_path: Path,
) -> None:
    """Test a skill directory with no attachments.yaml fails the run"""
    skills_root = tmp_path / "skills"
    write_skill(skills_root, "example-skill")
    (skills_root / "example-skill" / "attachments.yaml").unlink()

    assert_export_fails(tmp_path, "example-skill", "attachments.yaml", "is missing")


@pytest.mark.parametrize(
    ("attachments", "problem"),
    [
        ("- dcid:ExampleNode\n", "is not a mapping"),
        ("applies_to: [\n", "file does not parse as YAML"),
        (
            "applies_to: []\nsource: []\n",
            "has keys other than applies_to and sources: ['source']",
        ),
        ("applies_to: dcid:ExampleNode\n", "'applies_to' is not a list"),
        ("sources:\n", "'sources' is not a list"),
        ("applies_to:\n  - 930\n", "'applies_to' entry 930 does not match"),
        (
            "applies_to:\n  - ExampleNode\n",
            "'applies_to' entry 'ExampleNode' does not match",
        ),
        (
            "applies_to:\n  - 'dcid:A, dcid:B'\n",
            "'applies_to' entry 'dcid:A, dcid:B' does not match",
        ),
        (
            "sources:\n  - dcid:OECDCreditorReportingSystem\n",
            "'sources' entry 'dcid:OECDCreditorReportingSystem' does not match",
        ),
        (
            "sources:\n  - dcid:provenance/a/b\n",
            "'sources' entry 'dcid:provenance/a/b' does not match",
        ),
        (
            "applies_to:\n  - dcid:ExampleNode\n  - dcid:ExampleNode\n",
            "'applies_to' lists ['dcid:ExampleNode'] more than once",
        ),
        ("applies_to: &a []\n", "file uses a YAML anchor or alias"),
        (
            "applies_to: &a [dcid:ExampleNode]\nsources: *a\n",
            "file uses a YAML anchor or alias",
        ),
        (
            "applies_to: []\napplies_to: [dcid:ExampleNode]\n",
            "file does not parse as YAML: a mapping repeats a key",
        ),
    ],
    ids=[
        "not_a_mapping",
        "invalid_yaml",
        "unknown_key",
        "string_value",
        "null_value",
        "non_string_entry",
        "entry_without_dcid_prefix",
        "entry_with_comma",
        "source_outside_provenance",
        "source_token_with_slash",
        "duplicate_entry",
        "yaml_anchor",
        "yaml_alias",
        "repeated_key",
    ],
)
def test_dc_export_rejects_a_malformed_attachments_file(
    tmp_path: Path, attachments: str, problem: str
) -> None:
    """Test an attachments.yaml outside the sidecar format fails the run"""
    skills_root = tmp_path / "skills"
    write_skill(skills_root, "example-skill", files={"attachments.yaml": attachments})

    assert_export_fails(tmp_path, "example-skill", "attachments.yaml", problem)


def test_dc_export_rejects_an_applies_to_target_missing_from_the_ontology(
    tmp_path: Path,
) -> None:
    """Test an unresolved ontology target fails the whole export, naming it"""
    skills_root = tmp_path / "skills"
    write_skill(
        skills_root,
        "example-skill",
        files={"attachments.yaml": "applies_to: [dcid:ExampleNode, dcid:Missing]\n"},
    )
    write_skill(
        skills_root,
        "other-skill",
        files={"attachments.yaml": "applies_to: [dcid:ExampleNode]\n"},
    )

    assert_export_fails(
        tmp_path,
        "example-skill",
        "attachments.yaml",
        "'applies_to' names nodes missing from the compiled ontology: ['dcid:Missing']",
        ontology_root=write_ontology(tmp_path / "ontology", ["dcid:ExampleNode"]),
    )


def test_dc_export_rejects_a_source_naming_no_exports_provenance(
    tmp_path: Path,
) -> None:
    """Test a provenance no export registers fails the export, naming it"""
    skills_root = tmp_path / "skills"
    write_skill(
        skills_root,
        "example-skill",
        files={"attachments.yaml": "sources: [dcid:provenance/NotAnExport]\n"},
    )

    assert_export_fails(
        tmp_path,
        "example-skill",
        "attachments.yaml",
        "'sources' names no export's provenance: ['dcid:provenance/NotAnExport']",
    )


@pytest.mark.parametrize(
    "spec", EXPORT_SPECS, ids=[spec.provenance_dcid for spec in EXPORT_SPECS]
)
def test_dc_export_attaches_a_skill_to_any_exports_provenance(
    tmp_path: Path, spec: ExportSpec
) -> None:
    """Test every registered export's provenance is a valid `sources` target"""
    skills_root = tmp_path / "skills"
    write_skill(
        skills_root,
        "example-skill",
        files={"attachments.yaml": f"sources: [{spec.minted_provenance_dcid}]\n"},
    )

    _, out_dir = export_skills(tmp_path)

    nodes = (out_dir / "custom_nodes.mcf").read_text()
    assert f"skillAppliesToProvenance: {spec.minted_provenance_dcid}\n" in nodes


def test_dc_export_attaches_a_skill_to_an_ontology_node_whose_dcid_has_a_slash(
    tmp_path: Path,
) -> None:
    """Test an `applies_to` target may carry a `/`, as place dcids do"""
    skills_root = tmp_path / "skills"
    write_skill(
        skills_root,
        "example-skill",
        files={"attachments.yaml": "applies_to: [dcid:country/PacificIslandsUS]\n"},
    )

    _, out_dir = export_skills(
        tmp_path,
        write_ontology(tmp_path / "ontology", ["dcid:country/PacificIslandsUS"]),
    )

    nodes = (out_dir / "custom_nodes.mcf").read_text()
    assert "skillAppliesTo: dcid:country/PacificIslandsUS\n" in nodes
