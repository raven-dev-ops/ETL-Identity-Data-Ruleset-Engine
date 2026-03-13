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

BACKLOG_PATH = Path(__file__).resolve().parents[1] / "planning" / "github-issues-backlog.md"


def test_parse_backlog_current_catalog() -> None:
    parsed = MODULE.parse_backlog(BACKLOG_PATH.read_text(encoding="utf-8"))

    assert parsed.milestones == ("M1", "M2", "M3", "M4", "M5", "M6")
    assert len(parsed.labels) == 16
    assert len(parsed.epics) == 6
    assert len(parsed.issues) == 36
    assert parsed.issues[0].catalog_number == 7
    assert parsed.issues[-1].catalog_number == 42
    assert parsed.issues[0].title == "Bootstrap repository skeleton"
    assert parsed.issues[-1].title == "Clean up README encoding and formatting artifacts"


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
    assert "- ``M1``" in body
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
        {"Normalization and Data Quality Core": 3},
    )

    assert epic_issue_number_map == {"M3": 3}
    assert MODULE.build_epic_reference_map(epic_issue_number_map) == {"M3": "#3"}


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
