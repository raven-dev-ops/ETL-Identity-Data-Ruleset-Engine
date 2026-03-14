from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "create_github_backlog.py"
SPEC = importlib.util.spec_from_file_location("create_github_backlog", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

HISTORICAL_BACKLOG_PATH = Path(__file__).resolve().parents[1] / "planning" / "github-issues-backlog.md"
ACTIVE_BACKLOG_PATH = (
    Path(__file__).resolve().parents[1] / "planning" / "active-github-issues-backlog.md"
)


def test_parse_backlog_current_catalog() -> None:
    parsed = MODULE.parse_backlog(
        HISTORICAL_BACKLOG_PATH.read_text(encoding="utf-8"),
        source_label="planning/github-issues-backlog.md",
    )

    assert parsed.milestones == ("M1", "M2", "M3", "M4", "M5", "M6")
    assert len(parsed.labels) == 16
    assert len(parsed.epics) == 6
    assert len(parsed.issues) == 36
    assert parsed.issues[0].catalog_number == 7
    assert parsed.issues[-1].catalog_number == 42
    assert parsed.issues[0].title == "Bootstrap repository skeleton"
    assert parsed.issues[-1].title == "Clean up README encoding and formatting artifacts"


def test_parse_backlog_active_catalog() -> None:
    parsed = MODULE.parse_backlog(
        ACTIVE_BACKLOG_PATH.read_text(encoding="utf-8"),
        source_label="planning/active-github-issues-backlog.md",
    )

    assert parsed.milestones == ("v0.4.0", "v0.5.0", "v0.6.0")
    assert len(parsed.labels) == 21
    assert len(parsed.epics) == 3
    assert len(parsed.issues) == 20
    assert parsed.issues[0].catalog_number == 61
    assert parsed.issues[-1].catalog_number == 80
    assert {issue.status for issue in parsed.issues} == {"closed", "open"}
    assert parsed.issues[0].status == "closed"
    assert parsed.issues[1].status == "closed"
    assert parsed.issues[2].status == "closed"
    assert parsed.issues[3].status == "closed"
    assert parsed.issues[4].status == "closed"
    assert parsed.issues[5].status == "closed"
    assert parsed.epics[0].description_items == (
        "Epic created from planning/active-github-issues-backlog.md",
    )


def test_select_sync_backlog_skips_closed_catalog_entries_by_default() -> None:
    parsed = MODULE.parse_backlog(
        ACTIVE_BACKLOG_PATH.read_text(encoding="utf-8"),
        source_label="planning/active-github-issues-backlog.md",
    )

    sync_backlog = MODULE.select_sync_backlog(parsed, include_closed=False)

    assert sync_backlog.milestones == ("v0.4.0", "v0.5.0", "v0.6.0")
    assert len(sync_backlog.epics) == 3
    assert len(sync_backlog.issues) == 14
    assert {issue.catalog_number for issue in sync_backlog.issues} == set(range(67, 81))


def test_select_sync_backlog_can_include_closed_catalog_entries() -> None:
    parsed = MODULE.parse_backlog(
        ACTIVE_BACKLOG_PATH.read_text(encoding="utf-8"),
        source_label="planning/active-github-issues-backlog.md",
    )

    sync_backlog = MODULE.select_sync_backlog(parsed, include_closed=True)

    assert sync_backlog == parsed


def test_build_issue_body_uses_expected_sections() -> None:
    issue = MODULE.IssueItem(
        title="Example issue",
        milestone="M1",
        labels=("type:chore", "area:repo"),
        depends_on="#1",
        description_items=("first", "second"),
        acceptance_items=("done",),
    )

    body = MODULE.build_issue_body(issue)

    assert "## Milestone" in body
    assert "- `M1`" in body
    assert "## Depends On" in body
    assert "- #1" in body
    assert "## Description" in body
    assert "- first" in body
    assert "## Acceptance Criteria" in body
    assert "- done" in body


def test_translate_depends_on_maps_catalog_numbers_to_github_numbers() -> None:
    translated = MODULE.translate_depends_on(
        "#12, #13, #14, #15",
        {12: 18, 13: 19, 14: 20, 15: 21},
    )

    assert translated == "#18, #19, #20, #21"
    assert MODULE.translate_depends_on("none", {1: 7}) == "none"


def test_build_issue_body_can_include_epic_reference_and_translated_dependencies() -> None:
    issue = MODULE.IssueItem(
        title="Example issue",
        milestone="M3",
        labels=("type:feature", "area:normalize"),
        depends_on="#12, #13",
        description_items=("first",),
        acceptance_items=("done",),
        catalog_number=16,
    )

    body = MODULE.build_issue_body(
        issue,
        depends_on="#18, #19",
        epic_reference="#3",
    )

    assert "## Epic" in body
    assert "- #3" in body
    assert "## Depends On" in body
    assert "- #18, #19" in body


def test_build_epic_issue_number_map_uses_epic_titles_and_milestones() -> None:
    epics = (
        MODULE.IssueItem(
            title="Normalization and Data Quality Core",
            milestone="M3",
            labels=("type:epic",),
            depends_on="none",
            description_items=(),
            acceptance_items=(),
        ),
    )

    epic_issue_number_map = MODULE.build_epic_issue_number_map(
        epics,
        {
            MODULE.normalize_issue_title("Normalization and Data Quality Core"): MODULE.ExistingIssue(
                number=3,
                title="Normalization and Data Quality Core",
                state="CLOSED",
            )
        },
    )

    assert epic_issue_number_map == {"M3": 3}
    assert MODULE.build_epic_reference_map(epic_issue_number_map) == {"M3": "#3"}


def test_build_epic_body_reflects_child_issue_completion_status() -> None:
    epic = MODULE.IssueItem(
        title="Reporting, Hardening, and Release",
        milestone="M6",
        labels=("type:epic",),
        depends_on="none",
        description_items=("Epic created from planning/github-issues-backlog.md",),
        acceptance_items=("Child issues linked and tracked to completion.",),
    )
    child_issues = (
        MODULE.IssueItem(
            title="Prepare v0.1.0 release checklist, changelog, and tag procedure",
            milestone="M6",
            labels=("type:chore",),
            depends_on="none",
            description_items=(),
            acceptance_items=(),
            catalog_number=38,
        ),
        MODULE.IssueItem(
            title="Expand CI to Linux and Windows matrix with coverage reporting",
            milestone="M6",
            labels=("type:chore",),
            depends_on="none",
            description_items=(),
            acceptance_items=(),
            catalog_number=40,
        ),
    )

    body = MODULE.build_epic_body(
        epic,
        child_issues=child_issues,
        existing_issues={
            MODULE.normalize_issue_title(epic.title): MODULE.ExistingIssue(
                number=6,
                title=epic.title,
                state="CLOSED",
            ),
            MODULE.normalize_issue_title(child_issues[0].title): MODULE.ExistingIssue(
                number=38,
                title=child_issues[0].title,
                state="CLOSED",
            ),
            MODULE.normalize_issue_title(child_issues[1].title): MODULE.ExistingIssue(
                number=40,
                title=child_issues[1].title,
                state="OPEN",
            ),
        },
    )

    assert "- `M6`" in body
    assert "- Child issues complete: `1/2`" in body
    assert "- [x] #38 Prepare v0.1.0 release checklist, changelog, and tag procedure" in body
    assert "- [ ] #40 Expand CI to Linux and Windows matrix with coverage reporting" in body
    assert "- Outstanding: #40 Expand CI to Linux and Windows matrix with coverage reporting" in body


def test_normalize_issue_title_ignores_backticks_and_extra_spacing() -> None:
    assert (
        MODULE.normalize_issue_title(" Prepare `v0.1.0`  release checklist ")
        == MODULE.normalize_issue_title("Prepare v0.1.0 release checklist")
    )


def test_validate_backlog_rejects_normalized_title_collisions() -> None:
    parsed = MODULE.ParsedBacklog(
        milestones=("v0.2.0",),
        labels=("type:docs",),
        epics=(),
        issues=(
            MODULE.IssueItem(
                title="Document `v0.1.1` release steps",
                milestone="v0.2.0",
                labels=("type:docs",),
                depends_on="none",
                description_items=("first",),
                acceptance_items=("done",),
                catalog_number=52,
            ),
            MODULE.IssueItem(
                title="Document v0.1.1 release steps",
                milestone="v0.2.0",
                labels=("type:docs",),
                depends_on="none",
                description_items=("second",),
                acceptance_items=("done",),
                catalog_number=53,
            ),
        ),
    )

    errors = MODULE.validate_backlog(parsed)

    assert errors == (
        "normalized-title collision for 'document v0.1.1 release steps': "
        "Document `v0.1.1` release steps | Document v0.1.1 release steps",
    )


def test_parse_bullet_items_keeps_wrapped_continuation_lines() -> None:
    section = """
- Add a shared loader for `config/*.yml` used by normalization,
  blocking, matching, thresholds, and survivorship flows.
- Validate required keys, allowed values, and cross-file consistency at
  startup.
""".strip()

    items = MODULE.parse_bullet_items(section)

    assert items == (
        "Add a shared loader for `config/*.yml` used by normalization, "
        "blocking, matching, thresholds, and survivorship flows.",
        "Validate required keys, allowed values, and cross-file consistency at startup.",
    )
