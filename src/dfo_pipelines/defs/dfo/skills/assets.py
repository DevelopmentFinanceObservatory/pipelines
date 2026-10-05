import os
import re
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

import dagster as dg
import yaml
from dcp_tools import CustomDataManager
from upath import UPath
from yaml.constructor import ConstructorError

from dfo_pipelines.data_commons.ingestion import ingest_bundle
from dfo_pipelines.data_commons.mcf import escape_markdown
from dfo_pipelines.defs.automation import EAGER_WHEN_CHECKS_PASS
from dfo_pipelines.defs.dfo.ontology.assets import (
    ONTOLOGY_MCF,
    dfo_ontology_dc_ingestion,
    mcf_node_dcids,
)
from dfo_pipelines.defs.export_specs import EXPORT_SPECS
from dfo_pipelines.defs.resources import (
    KnowledgeGraphResource,
    UPathFileStoreResource,
)

IMPORT_NAME = "dfo_skills"
SKILLS_REPOSITORY_URL = "https://github.com/ONEcampaign/dfo-skills"
SKILLS_BRANCH = "main"
SKILL_FILE = "SKILL.md"
# Each skill's graph attachments, read here and left out of the published bundle.
ATTACHMENTS_FILE = "attachments.yaml"
# dfo-skills is small, so a git call running this long has stalled.
GIT_TIMEOUT_SECONDS = 300

SKILL_CLASS_DCID = "AgentSkill"
REFERENCE_CLASS_DCID = "AgentSkillReference"
REFERENCES_PROPERTY = "agentSkillReferences"
APPLIES_TO_PROPERTY = "skillAppliesTo"
APPLIES_TO_PROVENANCE_PROPERTY = "skillAppliesToProvenance"

# The Skills API contract's limits, which the reader enforces on the bundle's
# files; `attachments.yaml` is not in the bundle and is read separately.
MAX_FILE_BYTES = 256 * 1024
MAX_REFERENCES = 50
MAX_PATH_CHARACTERS = 512

# dfo-skills' naming rule for a skill's `name`, applied to each segment of a
# reference's path.
_DCID_SEGMENT_PATTERN = re.compile(r"[a-z0-9]+(-[a-z0-9]+)*")
# The Agent Skills name rule's length bound for a skill id, which the reader
# enforces with the same pattern.
MAX_SKILL_ID_LENGTH = 64
# The Skills API reader drops these ids because its literal routes take
# precedence over skill routes (`skills_index.config` in dfo-apis).
RESERVED_SKILL_IDS = frozenset({"healthz"})
# The reader's front matter block: `---` on the text's first line, closed by
# the next line that is `---`, with LF or CRLF line endings.
_FRONT_MATTER_BLOCK = re.compile(
    r"\A---[ \t]*\r?\n(.*?)^---[ \t]*\r?$", re.DOTALL | re.MULTILINE
)
# The shape of each attachments.yaml list's entries, as the dfo-skills validator
# checks them: an ontology reference for `applies_to`, and the provenance of a
# dfo-pipelines export, whose token has no `/`, for `sources`.
_ATTACHMENT_PATTERNS = {
    "applies_to": re.compile(r"dcid:[^\s\"',]+"),
    "sources": re.compile(r"dcid:provenance/[^\s\"',/]+"),
}


def _is_skill_directory(path: UPath) -> bool:
    """A directory is a skill unless its name starts with `_`

    Matches the dfo-skills validator, which skips every `skills/` entry whose
    name starts with `_` (the repository's convention for drafts and scaffolds).
    """
    return path.is_dir() and not path.name.startswith("_")


@dataclass(frozen=True)
class _Reference:
    """A reference file: its bundle-relative path, DCID suffix, and Markdown"""

    bundle_path: str
    suffix: str
    markdown: str


@dataclass(frozen=True)
class _Skill:
    """A validated skill directory: its id, `SKILL.md`, references, and attachments

    `applies_to` holds the ontology nodes the skill explains, and `sources` the
    provenance of each export whose dataset it explains as a whole.
    """

    name: str
    markdown: str
    description: str
    references: list[_Reference]
    applies_to: tuple[str, ...]
    sources: tuple[str, ...]


class _UniqueKeyLoader(yaml.SafeLoader):
    """A safe loader that refuses a mapping repeating a key

    PyYAML reads a repeated key as its last value alone, which would silently
    drop an earlier attachment list.
    """

    def construct_mapping(self, node, deep=False):
        mapping = super().construct_mapping(node, deep=deep)
        if len(mapping) < len(node.value):
            raise ConstructorError(
                None, None, "a mapping repeats a key", node.start_mark
            )
        return mapping


def _bundle_error(skill_name: str, bundle_path: str, problem: str) -> RuntimeError:
    """The error for a skill file that breaks the Skills API contract's limits"""
    return RuntimeError(f"skill {skill_name}: {bundle_path}: {problem}")


def _load_yaml(
    skill_name: str,
    bundle_path: str,
    source: str,
    *,
    loader: type[yaml.BaseLoader] | type[yaml.SafeLoader],
    subject: str,
) -> object:
    """YAML text parsed with `loader`, refusing a YAML anchor or alias

    `subject` names the YAML in an error, after the skill and file.
    """
    try:
        events = yaml.parse(source, Loader=loader)
        # An alias event carries the name it refers to in `anchor`.
        if any(
            isinstance(event, yaml.NodeEvent) and event.anchor is not None
            for event in events
        ):
            raise _bundle_error(
                skill_name, bundle_path, f"{subject} uses a YAML anchor or alias"
            )
        return yaml.load(source, Loader=loader)
    except yaml.YAMLError as error:
        raise _bundle_error(
            skill_name, bundle_path, f"{subject} does not parse as YAML: {error}"
        ) from error


def _read_description(skill_name: str, markdown: str) -> str:
    """The `description` in a `SKILL.md`'s front matter, read as the reader does

    The block must open on the text's first character. It parses with YAML's base
    loader, which reads every scalar as a string. The result must be a mapping with
    string `name` and `description` keys, and `name` must equal the directory
    name. A YAML anchor or alias is refused.
    """
    if (block := _FRONT_MATTER_BLOCK.match(markdown)) is None:
        raise _bundle_error(
            skill_name, SKILL_FILE, "does not open with a '---' delimited front matter"
        )
    parsed = _load_yaml(
        skill_name,
        SKILL_FILE,
        block.group(1),
        loader=yaml.BaseLoader,
        subject="front matter",
    )
    if not isinstance(parsed, dict):
        raise _bundle_error(skill_name, SKILL_FILE, "front matter is not a mapping")
    name, description = parsed.get("name"), parsed.get("description")
    if not isinstance(name, str):
        raise _bundle_error(skill_name, SKILL_FILE, "front matter has no string 'name'")
    if not isinstance(description, str):
        raise _bundle_error(
            skill_name, SKILL_FILE, "front matter has no string 'description'"
        )
    if name != skill_name:
        raise _bundle_error(
            skill_name,
            SKILL_FILE,
            f"front matter name {name[:80]!r} differs from the directory name",
        )
    return description


def _read_bundle_text(skill_path: UPath, file_path: UPath) -> str:
    """A skill file's text, decoded as UTF-8 and checked against the size limit

    Bytes are decoded without newline translation, so the text is the file
    byte for byte.
    """
    bundle_path = file_path.relative_to(skill_path).as_posix()
    data = file_path.read_bytes()
    if len(data) > MAX_FILE_BYTES:
        raise _bundle_error(
            skill_path.name,
            bundle_path,
            f"{len(data)} bytes exceeds the {MAX_FILE_BYTES} byte limit",
        )
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError as error:
        raise _bundle_error(
            skill_path.name, bundle_path, "is not valid UTF-8"
        ) from error


def _attachment_list(skill_name: str, attachments: dict, key: str) -> tuple[str, ...]:
    """One attachments.yaml list, checked against its key's entry pattern

    A missing key is an empty list. The list holds unique strings that each
    match the key's pattern in _ATTACHMENT_PATTERNS in full.
    """
    entries = attachments.get(key, [])
    if not isinstance(entries, list):
        raise _bundle_error(skill_name, ATTACHMENTS_FILE, f"{key!r} is not a list")
    pattern = _ATTACHMENT_PATTERNS[key]
    for entry in entries:
        if not isinstance(entry, str) or not pattern.fullmatch(entry):
            raise _bundle_error(
                skill_name,
                ATTACHMENTS_FILE,
                f"{key!r} entry {entry!r} does not match {pattern.pattern}",
            )
    if repeated := sorted({entry for entry in entries if entries.count(entry) > 1}):
        raise _bundle_error(
            skill_name, ATTACHMENTS_FILE, f"{key!r} lists {repeated} more than once"
        )
    return tuple(entries)


def _read_attachments(skill_path: UPath) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """A skill's `applies_to` and `sources` lists, read from its attachments.yaml

    The file is a mapping whose only keys are `applies_to` and `sources`. An
    empty file declares a skill that attaches nowhere. A YAML anchor, alias or
    repeated key is refused, as in front matter.
    """
    skill_name = skill_path.name
    path = skill_path / ATTACHMENTS_FILE
    if not path.is_file():
        raise _bundle_error(skill_name, ATTACHMENTS_FILE, "is missing")
    parsed = _load_yaml(
        skill_name,
        ATTACHMENTS_FILE,
        _read_bundle_text(skill_path, path),
        loader=_UniqueKeyLoader,
        subject="file",
    )
    if parsed is None:
        parsed = {}
    if not isinstance(parsed, dict):
        raise _bundle_error(skill_name, ATTACHMENTS_FILE, "is not a mapping")
    if unknown := sorted(str(key) for key in parsed if key not in _ATTACHMENT_PATTERNS):
        raise _bundle_error(
            skill_name,
            ATTACHMENTS_FILE,
            f"has keys other than {' and '.join(_ATTACHMENT_PATTERNS)}: {unknown}",
        )
    return (
        _attachment_list(skill_name, parsed, "applies_to"),
        _attachment_list(skill_name, parsed, "sources"),
    )


def _reference_dcid_suffix(skill_path: UPath, reference_path: UPath) -> str:
    """A reference's DCID suffix: its path under `references/`, without `.md`

    The whole path is used so `references/a/x.md` and `references/b/x.md` get
    distinct suffixes.
    """
    relative = reference_path.relative_to(skill_path / "references").with_suffix("")
    suffix = relative.as_posix()
    for segment in suffix.split("/"):
        if not _DCID_SEGMENT_PATTERN.fullmatch(segment):
            raise RuntimeError(
                f"reference {reference_path} has a path segment outside "
                f"[a-z0-9]+(-[a-z0-9]+)*: {segment!r}"
            )
    return suffix


def _read_skill(skill_path: UPath) -> _Skill:
    """Read a skill directory, failing on anything the Skills API contract rejects

    The directory name is a lower-case hyphenated id of at most
    MAX_SKILL_ID_LENGTH characters and not in RESERVED_SKILL_IDS, and the front
    matter `name` equals it. The directory holds `SKILL.md`, `attachments.yaml`,
    and Markdown files under `references/`, at most MAX_REFERENCES of them.
    `attachments.yaml` is read by the export, not by the Skills API reader, and
    stays out of the bundle. Every path is at most MAX_PATH_CHARACTERS long and
    every file at most MAX_FILE_BYTES.
    """
    skill_name = skill_path.name
    if len(skill_name) > MAX_SKILL_ID_LENGTH or not _DCID_SEGMENT_PATTERN.fullmatch(
        skill_name
    ):
        raise _bundle_error(
            skill_name,
            SKILL_FILE,
            f"directory name is not a skill id: lower-case [a-z0-9]+(-[a-z0-9]+)* "
            f"of at most {MAX_SKILL_ID_LENGTH} characters",
        )
    if skill_name in RESERVED_SKILL_IDS:
        raise _bundle_error(
            skill_name,
            SKILL_FILE,
            "directory name is a skill id reserved by the Skills API reader",
        )
    if not (skill_path / SKILL_FILE).is_file():
        raise _bundle_error(skill_name, SKILL_FILE, "is missing")

    reference_paths: list[UPath] = []
    for path in sorted(skill_path.rglob("*")):
        if not path.is_file():
            continue
        bundle_path = path.relative_to(skill_path).as_posix()
        if len(bundle_path) > MAX_PATH_CHARACTERS:
            raise _bundle_error(
                skill_name,
                bundle_path,
                f"path is {len(bundle_path)} characters, over the "
                f"{MAX_PATH_CHARACTERS} character limit",
            )
        if bundle_path.startswith("references/") and bundle_path.endswith(".md"):
            reference_paths.append(path)
        elif bundle_path not in (SKILL_FILE, ATTACHMENTS_FILE):
            raise _bundle_error(
                skill_name,
                bundle_path,
                f"a skill directory holds only {SKILL_FILE}, {ATTACHMENTS_FILE} "
                "and Markdown files under references/",
            )
    if len(reference_paths) > MAX_REFERENCES:
        raise _bundle_error(
            skill_name,
            "references/",
            f"{len(reference_paths)} references exceeds the {MAX_REFERENCES} limit",
        )

    markdown = _read_bundle_text(skill_path, skill_path / SKILL_FILE)
    description = _read_description(skill_name, markdown)
    applies_to, sources = _read_attachments(skill_path)
    references = [
        _Reference(
            bundle_path=path.relative_to(skill_path).as_posix(),
            suffix=_reference_dcid_suffix(skill_path, path),
            markdown=_read_bundle_text(skill_path, path),
        )
        for path in reference_paths
    ]
    return _Skill(
        name=skill_name,
        markdown=markdown,
        description=description,
        references=references,
        applies_to=applies_to,
        sources=sources,
    )


def _run_git(*args: str, cwd: Path | None = None) -> str:
    """Run a git command and return its stdout, failing rather than prompting"""
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
            env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
            timeout=GIT_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as error:
        raise RuntimeError(
            f"git {args[0]} timed out after {GIT_TIMEOUT_SECONDS} seconds"
        ) from error
    if result.returncode != 0:
        raise RuntimeError(
            f"git {args[0]} failed:\n{result.stderr}\n"
            "dfo-skills is private: check git can authenticate to GitHub"
        )
    return result.stdout.strip()


@dg.observable_source_asset(
    group_name="dfo_skills",
    automation_condition=dg.AutomationCondition.on_cron("@daily"),
)
def dfo_skills_source() -> dg.ObserveResult:
    """Observed commit at the head of the dfo-skills main branch"""
    output = _run_git("ls-remote", SKILLS_REPOSITORY_URL, f"refs/heads/{SKILLS_BRANCH}")
    if not output:
        raise RuntimeError(f"dfo-skills has no {SKILLS_BRANCH} branch")
    commit = output.split()[0]
    return dg.ObserveResult(
        metadata={"commit": dg.MetadataValue.text(commit)},
        data_version=dg.DataVersion(commit),
    )


@dg.asset(
    deps=[dfo_skills_source],
    group_name="dfo_skills",
    kinds={"file"},
    automation_condition=EAGER_WHEN_CHECKS_PASS,
    check_specs=[
        dg.AssetCheckSpec(
            name="skills_not_empty", asset="dfo_skills_raw_directory", blocking=True
        )
    ],
)
def dfo_skills_raw_directory(
    context: dg.AssetExecutionContext, raw_file_store: UPathFileStoreResource
):
    """DFO agent skills, as published on the dfo-skills main branch

    dfo-skills is private, so git needs GitHub credentials wherever this runs.
    """
    raw_directory = raw_file_store.build_date_partitioned_path("dfo_skills/skills")

    with tempfile.TemporaryDirectory() as staging_dir:
        clone_path = Path(staging_dir) / "dfo-skills"
        _run_git(
            "clone",
            "--depth=1",
            f"--branch={SKILLS_BRANCH}",
            SKILLS_REPOSITORY_URL,
            str(clone_path),
        )
        commit = _run_git("rev-parse", "HEAD", cwd=clone_path)
        skills_path = clone_path / "skills"
        # Reading a symlink would copy whatever it points at on this machine.
        if skills_path.is_symlink():
            raise RuntimeError(f"dfo-skills {commit} has skills/ as a symlink")
        if not skills_path.is_dir():
            raise RuntimeError(f"dfo-skills {commit} has no skills/ directory")
        symlinks = [path for path in skills_path.rglob("*") if path.is_symlink()]
        if symlinks:
            raise RuntimeError(
                f"dfo-skills {commit} has symlinks under skills/: "
                f"{sorted(str(path.relative_to(skills_path)) for path in symlinks)}"
            )
        files = [path for path in skills_path.rglob("*") if path.is_file()]

        # Cleared after the clone succeeds, so a failed fetch keeps the day's
        # copy and a rerun drops skills deleted upstream.
        if raw_directory.exists():
            raw_directory.rmdir(recursive=True)
        raw_directory.mkdir(parents=True)
        for path in files:
            target = raw_directory / path.relative_to(skills_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(path.read_bytes())

    skill_count = sum(
        1
        for path in raw_directory.iterdir()
        if _is_skill_directory(path) and (path / "SKILL.md").is_file()
    )
    context.log.info(
        f"Wrote {skill_count} skills from dfo-skills {commit} to {raw_directory}"
    )
    return dg.MaterializeResult(
        value=str(raw_directory),
        metadata={
            "dagster/uri": dg.MetadataValue.text(str(raw_directory)),
            "commit": dg.MetadataValue.text(commit),
            "skill_count": dg.MetadataValue.int(skill_count),
        },
        check_results=[
            dg.AssetCheckResult(check_name="skills_not_empty", passed=(skill_count > 0))
        ],
    )


def _check_attachment_targets(
    skills: list[_Skill], *, ontology_dcids: set[str], provenance_dcids: set[str]
) -> None:
    """Fail if an attachment names a node the graph does not hold

    An `applies_to` target must be a node in the compiled ontology, and a
    `sources` target the provenance of an export in EXPORT_SPECS. dcp-tools
    does not check that a target resolves, so a missing one would reach Data
    Commons as an edge to nothing. One failure fails the whole export, since a
    bundle without the skill would delete it from the graph on ingestion.
    """
    for skill in skills:
        if missing := sorted(set(skill.applies_to) - ontology_dcids):
            raise _bundle_error(
                skill.name,
                ATTACHMENTS_FILE,
                f"'applies_to' names nodes missing from the compiled ontology: "
                f"{missing}",
            )
        if missing := sorted(set(skill.sources) - provenance_dcids):
            raise _bundle_error(
                skill.name,
                ATTACHMENTS_FILE,
                f"'sources' names no export's provenance: {missing}",
            )


@dg.asset(group_name="dfo_skills", automation_condition=EAGER_WHEN_CHECKS_PASS)
def dfo_skills_dc_export(
    context: dg.AssetExecutionContext,
    export_file_store: UPathFileStoreResource,
    dfo_skills_raw_directory: str,
    dfo_ontology_raw_directory: str,
) -> dg.MaterializeResult:
    """Data Commons export for the DFO agent skills

    Each skill and each of its references becomes a node holding its markdown,
    and a skill links to the targets its attachments.yaml lists. A skill
    directory that breaks the Skills API contract's limits, or an attachment
    target that does not resolve, fails the run before anything is written.
    """
    provenance_dcid = "dcid:provenance/DFOSkills"
    out_dir = export_file_store.build_date_partitioned_path(IMPORT_NAME)

    skill_paths = sorted(
        path
        for path in UPath(dfo_skills_raw_directory).iterdir()
        if _is_skill_directory(path)
    )
    skills = [_read_skill(skill_path) for skill_path in skill_paths]

    _check_attachment_targets(
        skills,
        ontology_dcids=mcf_node_dcids(UPath(dfo_ontology_raw_directory) / ONTOLOGY_MCF),
        provenance_dcids={spec.minted_provenance_dcid for spec in EXPORT_SPECS},
    )

    manager = CustomDataManager()
    manager.set_import_name(IMPORT_NAME)
    manager.add_provenance(
        dcid=provenance_dcid,
        name="DFO Skills",
        url=SKILLS_REPOSITORY_URL,
        # Defined by the ontology import, which must be ingested first.
        source="dcid:source/ONEData",
    )
    manager.add_node(
        dcid=SKILL_CLASS_DCID,
        name="Agent Skill",
        type_of="Class",
        description="A packaged set of instructions an agent loads for a task.",
        additional_properties={"subClassOf": "dcid:Intangible"},
    )
    manager.add_node(
        dcid=REFERENCE_CLASS_DCID,
        name="Agent Skill Reference",
        type_of="Class",
        description="A supporting document an agent skill loads on demand.",
        additional_properties={"subClassOf": "dcid:Intangible"},
    )
    manager.add_property(
        dcid=REFERENCES_PROPERTY,
        name="agent skill references",
        domain_includes=SKILL_CLASS_DCID,
        range_includes=REFERENCE_CLASS_DCID,
        description="The supporting documents a skill loads on demand.",
    )
    manager.add_property(
        dcid=APPLIES_TO_PROPERTY,
        name="skill applies to",
        domain_includes=SKILL_CLASS_DCID,
        range_includes="Thing",
        description=(
            "An ontology property or value whose data the skill explains how to "
            "interpret. A skill applies to a StatVar when it links to any of the "
            "StatVar's constraint properties or constraint values, or to one of "
            "their ancestors, since a link also covers the linked node's "
            "descendants in the ontology's hierarchy."
        ),
    )
    manager.add_property(
        dcid=APPLIES_TO_PROVENANCE_PROPERTY,
        name="skill applies to provenance",
        domain_includes=SKILL_CLASS_DCID,
        range_includes="Provenance",
        description=(
            "The provenance of a dataset whose series the skill explains how to "
            "use. A skill applies to every series imported under that provenance."
        ),
    )

    reference_count = 0
    for skill in skills:
        skill_dcid = f"dcid:skill/{skill.name}"

        url = f"{SKILLS_REPOSITORY_URL}/tree/{SKILLS_BRANCH}/skills/{skill.name}"
        properties = {
            "url": f'"{url}"',
            "text": escape_markdown(skill.markdown),
        }
        if skill.references:
            properties[REFERENCES_PROPERTY] = ", ".join(
                f"{skill_dcid}/{reference.suffix}" for reference in skill.references
            )
        if skill.applies_to:
            properties[APPLIES_TO_PROPERTY] = ", ".join(skill.applies_to)
        if skill.sources:
            properties[APPLIES_TO_PROVENANCE_PROPERTY] = ", ".join(skill.sources)
        manager.add_node(
            dcid=skill_dcid,
            name=skill.name,
            type_of=SKILL_CLASS_DCID,
            # MCF ends a value at any quote, and unlike `text` a description is
            # read as plain text, so its double quotes become single ones.
            description=skill.description.replace('"', "'"),
            additional_properties=properties,
        )

        for reference in skill.references:
            manager.add_node(
                dcid=f"{skill_dcid}/{reference.suffix}",
                name=reference.bundle_path,
                type_of=REFERENCE_CLASS_DCID,
                additional_properties={"text": escape_markdown(reference.markdown)},
            )
        reference_count += len(skill.references)

    manager.add_mcf_file(file_name="*.mcf", provenance=provenance_dcid)
    manager.export_all(str(out_dir))
    context.log.info(
        f"Exported {len(skills)} skills and {reference_count} references to {out_dir}"
    )
    return dg.MaterializeResult(
        value=str(out_dir),
        metadata={
            "dagster/uri": dg.MetadataValue.text(str(out_dir)),
            "import_name": dg.MetadataValue.text(IMPORT_NAME),
            "skill_count": dg.MetadataValue.int(len(skills)),
            "reference_count": dg.MetadataValue.int(reference_count),
        },
    )


# TODO: Make this eager in a single-slot pool once the ingestion step can wait for
# the workflow to finish: https://github.com/ONEcampaign/dcp-tools/issues/109
@dg.asset(group_name="dfo_skills", kinds={"gcs"}, deps=[dfo_ontology_dc_ingestion])
def dfo_skills_dc_ingestion(
    context: dg.AssetExecutionContext,
    knowledge_graph: KnowledgeGraphResource,
    dfo_skills_dc_export: str,
) -> dg.MaterializeResult:
    """DFO agent skills bundle ingested into the Data Commons knowledge graph

    Materialized by hand only: an ingestion rewrites the shared graph and runs
    for as long as it takes after the workflow is started. It follows the
    ontology's ingestion, which defines the source and the nodes skills link to.
    """
    destination = ingest_bundle(
        UPath(dfo_skills_dc_export),
        import_name=IMPORT_NAME,
        settings=knowledge_graph.settings,
        log=context.log,
    )
    return dg.MaterializeResult(
        metadata={
            "dagster/uri": dg.MetadataValue.text(destination),
            "import_name": dg.MetadataValue.text(IMPORT_NAME),
        },
    )
