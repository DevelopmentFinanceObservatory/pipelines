import re
import socket
import subprocess
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

import dagster as dg
import pytest

from dfo_pipelines.data_commons.export import ExportSpec
from dfo_pipelines.defs.dfo.ontology.assets import ONTOLOGY_MCF
from dfo_pipelines.defs.dfo.skills.assets import (
    IMPORT_NAME,
    MAX_FILE_BYTES,
    MAX_PATH_CHARACTERS,
    MAX_REFERENCES,
    SKILLS_BRANCH,
    dfo_skills_dc_export,
    dfo_skills_dc_ingestion,
    dfo_skills_raw_directory,
    dfo_skills_source,
)
from dfo_pipelines.defs.export_specs import EXPORT_SPECS
from dfo_pipelines.defs.resources import (
    KnowledgeGraphResource,
    UPathFileStoreResource,
)
from tests.helpers import AutomationHarness, meta

FIXTURES = Path(__file__).parent / "fixtures"


def git(repository: Path, *args: str) -> str:
    """Run git in a repository, with an identity so commits work anywhere"""
    return subprocess.run(
        ["git", "-c", "user.name=test", "-c", "user.email=test@example.com", *args],
        cwd=repository,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def commit(repository: Path, files: dict[str, str]) -> str:
    """Replace the repository's `skills/` with the given files and commit it"""
    git(repository, "rm", "-rq", "--ignore-unmatch", "skills")
    for name, text in files.items():
        path = repository / "skills" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    git(repository, "add", "-A")
    git(repository, "commit", "-qm", "update skills")
    return git(repository, "rev-parse", "HEAD")


SKILLS = {
    "oda-concepts/SKILL.md": "---\nname: oda-concepts\ndescription: test\n---\n",
    "oda-concepts/references/core-oda.md": "What this covers: Core ODA.\n",
    "oda-concepts/attachments.yaml": "",
    "_template/SKILL.md": "---\nname: skill-template\ndescription: test\n---\n",
}


@pytest.fixture
def upstream(tmp_path: Path) -> Iterator[Path]:
    """A local repository standing in for dfo-skills, cloned in its place"""
    repository = tmp_path / "dfo-skills"
    repository.mkdir()
    git(repository, "init", "-q", f"--initial-branch={SKILLS_BRANCH}")
    commit(repository, SKILLS)
    with patch(
        "dfo_pipelines.defs.dfo.skills.assets.SKILLS_REPOSITORY_URL",
        repository.as_uri(),
    ):
        yield repository


def materialize_raw_directory(tmp_path: Path) -> Any:
    """Run the raw directory asset against a file store under tmp_path"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))
    return dfo_skills_raw_directory(
        context=dg.build_asset_context(), raw_file_store=file_store
    )


def files_under(directory: Path) -> dict[str, str]:
    """Every file below a directory, keyed on its path relative to it"""
    return {
        path.relative_to(directory).as_posix(): path.read_text()
        for path in directory.rglob("*")
        if path.is_file()
    }


def test_raw_directory_copies_the_skills_at_the_head_of_main(
    tmp_path: Path, upstream: Path
) -> None:
    """Test the published skills arrive unchanged, traced to their commit"""
    result = materialize_raw_directory(tmp_path)

    assert files_under(Path(str(result.value))) == SKILLS
    assert meta(result, "commit", dg.TextMetadataValue).value == git(
        upstream, "rev-parse", "HEAD"
    )
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
    materialize_raw_directory(tmp_path)
    replaced = {"oda-channels/SKILL.md": "---\nname: oda-channels\n---\n"}
    commit(upstream, replaced)

    result = materialize_raw_directory(tmp_path)

    assert files_under(Path(str(result.value))) == replaced


def test_raw_directory_keeps_the_previous_copy_when_the_clone_fails(
    tmp_path: Path, upstream: Path
) -> None:
    """Test a failed fetch leaves the day's earlier copy in place"""
    published = Path(str(materialize_raw_directory(tmp_path).value))

    with (
        patch(
            "dfo_pipelines.defs.dfo.skills.assets.SKILLS_REPOSITORY_URL",
            (tmp_path / "missing").as_uri(),
        ),
        pytest.raises(RuntimeError, match="git clone failed"),
    ):
        materialize_raw_directory(tmp_path)

    assert files_under(published) == SKILLS


def test_raw_directory_fails_when_git_stalls(tmp_path: Path) -> None:
    """Test a remote that never responds fails the run rather than hanging it"""
    # Connections queue in the backlog but are never accepted or answered.
    with socket.create_server(("127.0.0.1", 0)) as server:
        host, port = server.getsockname()
        with (
            patch(
                "dfo_pipelines.defs.dfo.skills.assets.SKILLS_REPOSITORY_URL",
                f"http://{host}:{port}/dfo-skills",
            ),
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
    ],
    ids=["file_under_skills", "skills_directory"],
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
    (upstream / link).symlink_to(secret / target)
    git(upstream, "add", "-A")
    git(upstream, "commit", "-qm", "add a symlink")

    with pytest.raises(RuntimeError, match=re.escape(error)):
        materialize_raw_directory(tmp_path)

    assert files_under(published) == SKILLS


class _SkillsAutomation(AutomationHarness):
    """Drives the skills assets against an ephemeral instance, as the daemon does

    The local repository stands in for dfo-skills for as long as the context is
    open, so committing to it is what moves the observed version.
    """

    def __init__(self, tmp_path: Path, upstream: Path) -> None:
        super().__init__(
            tmp_path, source=dfo_skills_source, raw_assets=[dfo_skills_raw_directory]
        )
        self.upstream = upstream

    def _start_mocks(self) -> None:
        self._stack.enter_context(
            patch(
                "dfo_pipelines.defs.dfo.skills.assets.SKILLS_REPOSITORY_URL",
                self.upstream.as_uri(),
            )
        )


def test_source_is_requested_once_per_daily_cron_tick(
    tmp_path: Path, upstream: Path
) -> None:
    """Test the source's condition requests one observation per UTC day"""
    with _SkillsAutomation(tmp_path, upstream) as automation:
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
    with _SkillsAutomation(tmp_path, upstream) as automation:
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
    with _SkillsAutomation(tmp_path, upstream) as automation:
        automation.observe()
        automation.materialize(dfo_skills_raw_directory)
        automation.tick()  # baseline, as above

        commit(upstream, SKILLS | {"oda-channels/SKILL.md": "---\nname: x\n---\n"})
        automation.observe()

        assert automation.tick() == {"dfo_skills_raw_directory": 1}

        automation.materialize(dfo_skills_raw_directory)

        assert automation.tick() == {"dfo_skills_raw_directory": 0}


def test_dc_export_writes_the_expected_bundle(tmp_path: Path) -> None:
    """Test a skill, its reference and its attachments export as the expected bundle

    The skill's attachments.yaml lists two ontology nodes and two export
    provenances, exported in file order, and is itself left out of the bundle.
    """
    result = dfo_skills_dc_export(
        context=dg.build_asset_context(),
        export_file_store=UPathFileStoreResource(base_path=str(tmp_path)),
        dfo_skills_raw_directory=str(FIXTURES / "skills"),
        dfo_ontology_raw_directory=str(FIXTURES / "ontology"),
    )

    assert isinstance(result, dg.MaterializeResult)
    out_dir = Path(meta(result, "dagster/uri", dg.TextMetadataValue).value)
    assert files_under(out_dir) == files_under(FIXTURES / "dc_export")


@dg.asset(name="dfo_skills_raw_directory")
def _stub_skills_raw_directory() -> str:
    """Raw skills, standing in for a clone of dfo-skills"""
    return str(FIXTURES / "skills")


def test_ingestion_ingests_the_bundle_the_export_wrote(tmp_path: Path) -> None:
    """Test the ingestion is handed the export's directory under its import name"""
    knowledge_graph = KnowledgeGraphResource(
        project_id="test-project",
        region="test-region",
        bucket_name="test-bucket",
        prep_job_name="test-prep-job",
        workflow_name="test-workflow",
    )
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
    skills_root: Path,
    ontology_root: Path | None = None,
) -> tuple[dg.MaterializeResult, Path]:
    """Run the export asset against a raw skills and ontology directory

    The ontology defaults to an empty one.
    """
    if ontology_root is None:
        ontology_root = write_ontology(tmp_path / "ontology")
    result = dfo_skills_dc_export(
        context=dg.build_asset_context(),
        export_file_store=UPathFileStoreResource(base_path=str(tmp_path / "processed")),
        dfo_skills_raw_directory=str(skills_root),
        dfo_ontology_raw_directory=str(ontology_root),
    )
    assert isinstance(result, dg.MaterializeResult)
    return result, Path(meta(result, "dagster/uri", dg.TextMetadataValue).value)


def test_raw_directory_and_export_skip_a_draft_with_only_a_readme(
    tmp_path: Path, upstream: Path
) -> None:
    """Test a `_draft/README.md`-only directory is skipped by both, without raising"""
    commit(upstream, SKILLS | {"_draft/README.md": "Notes for a draft skill.\n"})

    raw_result = materialize_raw_directory(tmp_path)
    assert meta(raw_result, "skill_count", dg.IntMetadataValue).value == 1

    export_result, _ = export_skills(tmp_path, Path(str(raw_result.value)))
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

    export_result, _ = export_skills(tmp_path, Path(str(raw_result.value)))
    assert meta(export_result, "skill_count", dg.IntMetadataValue).value == 1


def test_dc_export_exports_a_nested_reference(tmp_path: Path) -> None:
    """Test a reference nested under references/ is exported with a path-based DCID"""
    skills_root = tmp_path / "skills"
    write_skill(skills_root, "example-skill", {"sub/a.md": "nested content\n"})

    result, out_dir = export_skills(tmp_path, skills_root)

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

    result, out_dir = export_skills(tmp_path, skills_root)

    nodes = (out_dir / "custom_nodes.mcf").read_text()
    assert "Node: dcid:skill/example-skill/a/x" in nodes
    assert "Node: dcid:skill/example-skill/b/x" in nodes
    assert meta(result, "reference_count", dg.IntMetadataValue).value == 2


def assert_export_fails(
    tmp_path: Path,
    skills_root: Path,
    skill_name: str,
    file: str,
    problem: str = "",
    ontology_root: Path | None = None,
) -> None:
    """Assert the export raises an error naming the skill, file and problem

    The export must write nothing.
    """
    with pytest.raises(RuntimeError) as error:
        export_skills(tmp_path, skills_root, ontology_root)

    assert f"skill {skill_name}: {file}: {problem}" in str(error.value)
    assert not (tmp_path / "processed").exists()


def test_dc_export_writes_the_skills_directory_url(tmp_path: Path) -> None:
    """Test a skill node's url points at its directory on the skills branch"""
    skills_root = tmp_path / "skills"
    write_skill(skills_root, "example-skill")

    _, out_dir = export_skills(tmp_path, skills_root)

    nodes = (out_dir / "custom_nodes.mcf").read_text()
    assert (
        f'url: "https://github.com/ONEcampaign/dfo-skills/tree/{SKILLS_BRANCH}'
        '/skills/example-skill"'
    ) in nodes


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

    assert_export_fails(tmp_path, skills_root, "example-skill", path)


def test_dc_export_rejects_a_front_matter_name_that_differs_from_the_directory(
    tmp_path: Path,
) -> None:
    """Test a SKILL.md naming another skill than its directory fails the run"""
    skills_root = tmp_path / "skills"
    write_skill(skills_root, "example-skill")
    (skills_root / "example-skill" / "SKILL.md").write_text(
        "---\nname: other-skill\ndescription: test\n---\n"
    )

    assert_export_fails(tmp_path, skills_root, "example-skill", "SKILL.md")


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

    assert_export_fails(tmp_path, skills_root, skill_name, "SKILL.md")


def test_dc_export_rejects_a_reserved_skill_id(tmp_path: Path) -> None:
    """Test a skill whose id the Skills API reader reserves fails the run"""
    skills_root = tmp_path / "skills"
    write_skill(skills_root, "healthz")

    assert_export_fails(
        tmp_path, skills_root, "healthz", "SKILL.md", "directory name is a skill id"
    )


def test_dc_export_keeps_the_carriage_returns_of_a_crlf_skill_file(
    tmp_path: Path,
) -> None:
    """Test a CRLF SKILL.md parses and is exported byte for byte"""
    skills_root = tmp_path / "skills"
    write_skill(skills_root, "example-skill")
    (skills_root / "example-skill" / "SKILL.md").write_bytes(
        b"---\r\nname: example-skill\r\ndescription: test\r\n---\r\nBody\r\n"
    )

    _, out_dir = export_skills(tmp_path, skills_root)

    nodes = (out_dir / "custom_nodes.mcf").read_text()
    assert "description: test\\r\\n---\\r\\nBody\\r\\n" in nodes


def test_dc_export_rejects_a_skill_file_over_the_size_limit(tmp_path: Path) -> None:
    """Test a SKILL.md over 256 KiB fails the run, and one at the limit passes"""
    skills_root = tmp_path / "skills"
    write_skill(skills_root, "example-skill")
    front_matter = "---\nname: example-skill\ndescription: test\n---\n"
    skill_file = skills_root / "example-skill" / "SKILL.md"
    skill_file.write_text(front_matter + "a" * (MAX_FILE_BYTES - len(front_matter)))

    export_skills(tmp_path / "at_limit", skills_root)

    skill_file.write_text(front_matter + "a" * (MAX_FILE_BYTES - len(front_matter) + 1))

    assert_export_fails(tmp_path, skills_root, "example-skill", "SKILL.md")


def test_dc_export_rejects_a_reference_over_the_size_limit(tmp_path: Path) -> None:
    """Test a reference over 256 KiB fails the run, naming the reference"""
    skills_root = tmp_path / "skills"
    write_skill(skills_root, "example-skill", {"big.md": "a" * (MAX_FILE_BYTES + 1)})

    assert_export_fails(tmp_path, skills_root, "example-skill", "references/big.md")


def test_dc_export_measures_the_size_limit_in_encoded_bytes(tmp_path: Path) -> None:
    """Test a reference under the limit in characters but over it in bytes fails"""
    skills_root = tmp_path / "skills"
    write_skill(
        skills_root, "example-skill", {"wide.md": "\u00e9" * (MAX_FILE_BYTES // 2 + 1)}
    )

    assert_export_fails(tmp_path, skills_root, "example-skill", "references/wide.md")


def test_dc_export_rejects_a_reference_that_is_not_utf8(tmp_path: Path) -> None:
    """Test a reference whose bytes are not UTF-8 fails the run, naming it"""
    skills_root = tmp_path / "skills"
    write_skill(skills_root, "example-skill", {"latin.md": ""})
    (skills_root / "example-skill/references/latin.md").write_bytes(b"caf\xe9\n")

    assert_export_fails(tmp_path, skills_root, "example-skill", "references/latin.md")


def test_dc_export_rejects_more_than_the_reference_limit(tmp_path: Path) -> None:
    """Test 51 references fail the run, and 50 pass"""
    skills_root = tmp_path / "skills"
    write_skill(
        skills_root,
        "example-skill",
        {f"ref-{i}.md": "content\n" for i in range(MAX_REFERENCES)},
    )

    result, _ = export_skills(tmp_path / "at_limit", skills_root)
    assert meta(result, "reference_count", dg.IntMetadataValue).value == MAX_REFERENCES

    (skills_root / "example-skill/references/extra.md").write_text("content\n")

    assert_export_fails(tmp_path, skills_root, "example-skill", "references/")


def test_dc_export_rejects_a_path_over_the_length_limit(tmp_path: Path) -> None:
    """Test a bundle-relative path over 512 characters fails the run"""
    skills_root = tmp_path / "skills"
    # Segments stay short enough for the filesystem's name limit.
    segments = ["a" * 50] * 10
    path = "/".join(["references", *segments, "x.md"])
    assert len(path) > MAX_PATH_CHARACTERS
    write_skill(skills_root, "example-skill", files={path: "content\n"})

    assert_export_fails(tmp_path, skills_root, "example-skill", path)


def test_dc_export_rejects_a_skill_directory_without_a_skill_file(
    tmp_path: Path,
) -> None:
    """Test a non-draft directory with no SKILL.md fails the run, naming it"""
    skills_root = tmp_path / "skills"
    (skills_root / "example-skill").mkdir(parents=True)

    assert_export_fails(tmp_path, skills_root, "example-skill", "SKILL.md")


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
        export_skills(tmp_path, skills_root)


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

    _, out_dir = export_skills(tmp_path, skills_root)

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

    assert_export_fails(
        tmp_path, skills_root, "example-skill", "attachments.yaml", "is missing"
    )


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

    assert_export_fails(
        tmp_path, skills_root, "example-skill", "attachments.yaml", problem
    )


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
        skills_root,
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
        skills_root,
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

    _, out_dir = export_skills(tmp_path, skills_root)

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
        skills_root,
        write_ontology(tmp_path / "ontology", ["dcid:country/PacificIslandsUS"]),
    )

    nodes = (out_dir / "custom_nodes.mcf").read_text()
    assert "skillAppliesTo: dcid:country/PacificIslandsUS\n" in nodes
