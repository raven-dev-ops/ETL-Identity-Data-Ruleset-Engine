from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BACKLOG_PATH = REPO_ROOT / "planning" / "post-v0.1.0-github-issues-backlog.md"
ISSUE_PATTERN = re.compile(
    r"(?ms)^###\s+(?P<number>\d+)\)\s+(?P<title>.+?)\r?\n\r?\n(?P<body>.*?)(?=^###\s+\d+\)|^##\s+Suggested Epic Issues)"
)
EPIC_PATTERN = re.compile(r"^\d+\.\s+Epic:\s+(.+?)\s+\(`([^`]+)`\)", re.MULTILINE)
DEPENDENCY_REFERENCE_PATTERN = re.compile(r"#(?P<number>\d+)")


@dataclass(frozen=True)
class IssueItem:
    title: str
    milestone: str
    labels: tuple[str, ...]
    depends_on: str
    description_items: tuple[str, ...]
    acceptance_items: tuple[str, ...]
    catalog_number: int | None = None


@dataclass(frozen=True)
class ExistingIssue:
    number: int
    title: str
    state: str


@dataclass(frozen=True)
class ParsedBacklog:
    milestones: tuple[str, ...]
    labels: tuple[str, ...]
    epics: tuple[IssueItem, ...]
    issues: tuple[IssueItem, ...]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create GitHub labels, milestones, epics, and issues from the planning backlog."
    )
    parser.add_argument("--repo", required=True, help="Repository slug in OWNER/REPO format.")
    parser.add_argument(
        "--backlog-path",
        default=str(DEFAULT_BACKLOG_PATH),
        help="Path to the planning backlog markdown file to sync.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the operations that would run without calling GitHub.",
    )
    return parser.parse_args()


def resolve_gh_executable(repo_root: Path = REPO_ROOT) -> str:
    candidates = (
        repo_root / ".venv" / "Scripts" / "gh.exe",
        repo_root / ".venv" / "bin" / "gh",
    )
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)

    path_gh = shutil.which("gh")
    if path_gh:
        return path_gh

    raise SystemExit(
        "GitHub CLI not found. Run the bootstrap script to install the venv-scoped gh binary."
    )


def invoke_gh(gh_exe: str, *args: str) -> str:
    completed = subprocess.run(
        [gh_exe, *args],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "unknown error"
        raise SystemExit(f"gh command failed ({completed.returncode}): {' '.join(args)}\n{detail}")
    return completed.stdout.strip()


def ordered_unique(items: list[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(items))


def normalize_issue_title(title: str) -> str:
    return re.sub(r"\s+", " ", title.replace("`", "")).strip().casefold()


def format_backlog_source_label(backlog_path: Path, repo_root: Path = REPO_ROOT) -> str:
    try:
        relative_path = backlog_path.resolve().relative_to(repo_root.resolve())
        return relative_path.as_posix()
    except ValueError:
        return str(backlog_path).replace("\\", "/")


def get_section_body(content: str, start_heading: str, end_heading: str) -> str:
    pattern = re.compile(
        rf"(?ms)^## {re.escape(start_heading)}\s*(?P<body>.*?)(?=^## {re.escape(end_heading)}|\Z)"
    )
    match = pattern.search(content)
    return match.group("body") if match else ""


def parse_bullet_items(section: str) -> tuple[str, ...]:
    items: list[str] = []
    current_parts: list[str] = []

    for raw_line in section.splitlines():
        line = raw_line.rstrip()
        bullet_match = re.match(r"^\s*-\s+(.+)$", line)
        if bullet_match:
            if current_parts:
                items.append(" ".join(current_parts).strip())
            current_parts = [bullet_match.group(1).strip()]
            continue

        if current_parts and line.startswith(" "):
            current_parts.append(line.strip())

    if current_parts:
        items.append(" ".join(current_parts).strip())

    return tuple(items)


def parse_backlog(backlog_text: str, *, source_label: str) -> ParsedBacklog:
    milestone_body = get_section_body(backlog_text, "Milestones", "Label Set To Create")
    label_body = get_section_body(backlog_text, "Label Set To Create", "Issue Catalog")

    milestones = ordered_unique(re.findall(r"-\s*`([^`]+)`", milestone_body))
    labels = ordered_unique(re.findall(r"-\s*`([^`]+)`", label_body))

    issues: list[IssueItem] = []
    for match in ISSUE_PATTERN.finditer(backlog_text):
        body = match.group("body")
        milestone_match = re.search(r"-\s+Milestone:\s+`([^`]+)`", body)
        labels_match = re.search(r"-\s+Labels:\s+(.+)", body)
        depends_match = re.search(r"-\s+Depends on:\s+(.+)", body)
        description_match = re.search(
            r"(?ms)-\s+Description:\s*(?P<text>.*?)(?=-\s+Acceptance criteria:)",
            body,
        )
        acceptance_match = re.search(r"(?ms)-\s+Acceptance criteria:\s*(?P<text>.*)", body)

        issue_labels = tuple(
            re.findall(r"`([^`]+)`", labels_match.group(1)) if labels_match else []
        )
        description_items = parse_bullet_items(description_match.group("text") if description_match else "")
        acceptance_items = parse_bullet_items(acceptance_match.group("text") if acceptance_match else "")

        issues.append(
            IssueItem(
                title=match.group("title").strip(),
                milestone=milestone_match.group(1) if milestone_match else "",
                labels=issue_labels,
                depends_on=depends_match.group(1).strip() if depends_match else "none",
                description_items=description_items,
                acceptance_items=acceptance_items,
                catalog_number=int(match.group("number")),
            )
        )

    epic_body = get_section_body(backlog_text, "Suggested Epic Issues", "Suggested Issue Creation Order")
    epics = tuple(
        IssueItem(
            title=epic_match.group(1).strip(),
            milestone=epic_match.group(2).strip(),
            labels=("type:epic",),
            depends_on="none",
            description_items=(f"Epic created from {source_label}",),
            acceptance_items=("Child issues linked and tracked to completion.",),
        )
        for epic_match in EPIC_PATTERN.finditer(epic_body)
    )

    return ParsedBacklog(
        milestones=milestones,
        labels=labels,
        epics=epics,
        issues=tuple(issues),
    )


def validate_backlog(parsed: ParsedBacklog) -> tuple[str, ...]:
    errors: list[str] = []
    normalized_title_map: dict[str, list[str]] = {}
    known_milestones = set(parsed.milestones)
    known_catalog_numbers = {issue.catalog_number for issue in parsed.issues if issue.catalog_number is not None}
    seen_catalog_numbers: set[int] = set()

    for item in (*parsed.epics, *parsed.issues):
        normalized_title_map.setdefault(normalize_issue_title(item.title), []).append(item.title)
        if item.milestone and item.milestone not in known_milestones:
            errors.append(f"unknown milestone referenced by {item.title!r}: {item.milestone}")

    for normalized_title, titles in normalized_title_map.items():
        unique_titles = tuple(dict.fromkeys(titles))
        if len(unique_titles) > 1:
            formatted_titles = " | ".join(unique_titles)
            errors.append(
                f"normalized-title collision for {normalized_title!r}: {formatted_titles}"
            )

    for issue in parsed.issues:
        if issue.catalog_number is None:
            continue
        if issue.catalog_number in seen_catalog_numbers:
            errors.append(f"duplicate catalog number: {issue.catalog_number}")
        seen_catalog_numbers.add(issue.catalog_number)

        for match in DEPENDENCY_REFERENCE_PATTERN.finditer(issue.depends_on):
            dependency_number = int(match.group("number"))
            if dependency_number not in known_catalog_numbers:
                errors.append(
                    f"unknown dependency reference in {issue.title!r}: #{dependency_number}"
                )

    return tuple(errors)


def get_label_color(label: str) -> str:
    if label.startswith("type:"):
        return "5319E7"
    if label.startswith("area:"):
        return "0E8A16"
    if label == "priority:p0":
        return "B60205"
    if label == "priority:p1":
        return "FBCA04"
    if label == "priority:p2":
        return "CCCCCC"
    return "1D76DB"


def get_label_description(label: str) -> str:
    if label.startswith("type:"):
        return "Issue type label"
    if label.startswith("area:"):
        return "Subsystem ownership label"
    if label.startswith("priority:"):
        return "Delivery priority label"
    return "Project label"


def translate_depends_on(depends_on: str, catalog_to_github_number: dict[int, int]) -> str:
    def _replace(match: re.Match[str]) -> str:
        catalog_number = int(match.group("number"))
        github_number = catalog_to_github_number.get(catalog_number)
        if github_number is None:
            return match.group(0)
        return f"#{github_number}"

    return DEPENDENCY_REFERENCE_PATTERN.sub(_replace, depends_on)


def build_issue_body(
    issue: IssueItem,
    *,
    depends_on: str | None = None,
    epic_reference: str | None = None,
) -> str:
    lines = [
        "## Milestone",
        "",
        f"- `{issue.milestone}`",
    ]
    if epic_reference:
        lines.extend(["", "## Epic", "", f"- {epic_reference}"])
    lines.extend(["", "## Depends On", "", f"- {depends_on or issue.depends_on}", "", "## Description"])
    lines.extend(f"- {item}" for item in issue.description_items)
    lines.extend(["", "## Acceptance Criteria"])
    lines.extend(f"- {item}" for item in issue.acceptance_items)
    return "\n".join(lines)


def build_epic_body(
    epic: IssueItem,
    *,
    child_issues: tuple[IssueItem, ...],
    existing_issues: dict[str, ExistingIssue],
) -> str:
    ordered_children = tuple(
        sorted(
            child_issues,
            key=lambda item: (item.catalog_number is None, item.catalog_number or 0, item.title),
        )
    )
    closed_count = sum(
        1
        for child_issue in ordered_children
        if existing_issues.get(
            normalize_issue_title(child_issue.title),
            ExistingIssue(0, child_issue.title, "OPEN"),
        ).state.upper()
        == "CLOSED"
    )
    total_count = len(ordered_children)
    open_children = tuple(
        child_issue
        for child_issue in ordered_children
        if existing_issues.get(
            normalize_issue_title(child_issue.title),
            ExistingIssue(0, child_issue.title, "OPEN"),
        ).state.upper()
        != "CLOSED"
    )

    lines = [
        "## Milestone",
        "",
        f"- `{epic.milestone}`",
        "",
        "## Progress",
        "",
        f"- Child issues complete: `{closed_count}/{total_count}`",
    ]
    if open_children:
        remaining_references = ", ".join(
            f"#{existing_issues[normalize_issue_title(child_issue.title)].number}"
            for child_issue in open_children
            if normalize_issue_title(child_issue.title) in existing_issues
        )
        lines.append(f"- Remaining child issues: {remaining_references or 'tracked but not yet created'}")
    else:
        lines.append("- Remaining child issues: none")

    lines.extend(["", "## Child Issues"])
    for child_issue in ordered_children:
        existing_issue = existing_issues.get(normalize_issue_title(child_issue.title))
        issue_number = existing_issue.number if existing_issue else child_issue.catalog_number
        checkbox = "x" if existing_issue and existing_issue.state.upper() == "CLOSED" else " "
        reference = f"#{issue_number}" if issue_number is not None else child_issue.title
        lines.append(f"- [{checkbox}] {reference} {child_issue.title}")

    lines.extend(["", "## Current Status"])
    if open_children:
        lines.append(f"- This epic still has `{len(open_children)}` open child issue(s).")
        for child_issue in open_children:
            existing_issue = existing_issues.get(normalize_issue_title(child_issue.title))
            if existing_issue is not None:
                lines.append(f"- Outstanding: #{existing_issue.number} {child_issue.title}")
            else:
                lines.append(f"- Outstanding: {child_issue.title}")
    else:
        lines.append("- All child issues linked to this epic are closed.")
        lines.append("- The milestone scope is complete in the repository and tracker.")

    lines.extend(["", "## Exit Criteria"])
    lines.extend(f"- {item}" for item in epic.acceptance_items)
    return "\n".join(lines)


def get_existing_issue_map(gh_exe: str, repo: str) -> dict[str, ExistingIssue]:
    existing = json.loads(
        invoke_gh(
            gh_exe,
            "issue",
            "list",
            "--repo",
            repo,
            "--state",
            "all",
            "--limit",
            "500",
            "--json",
            "number,title,state",
        )
    )
    issue_map: dict[str, ExistingIssue] = {}
    for item in existing:
        normalized_title = normalize_issue_title(item["title"])
        candidate = ExistingIssue(
            number=item["number"],
            title=item["title"],
            state=item["state"],
        )
        current = issue_map.get(normalized_title)
        if current is None or candidate.number < current.number:
            issue_map[normalized_title] = candidate
    return issue_map


def build_catalog_number_map(
    issues: tuple[IssueItem, ...],
    existing_issue_map: dict[str, ExistingIssue],
) -> dict[int, int]:
    mapping: dict[int, int] = {}
    for issue in issues:
        if issue.catalog_number is None:
            continue
        existing_issue = existing_issue_map.get(normalize_issue_title(issue.title))
        if existing_issue is not None:
            mapping[issue.catalog_number] = existing_issue.number
    return mapping


def build_epic_issue_number_map(
    epics: tuple[IssueItem, ...],
    existing_issue_map: dict[str, ExistingIssue],
) -> dict[str, int]:
    mapping: dict[str, int] = {}
    for epic in epics:
        existing_issue = existing_issue_map.get(normalize_issue_title(epic.title))
        if existing_issue is None or not epic.milestone:
            continue
        mapping[epic.milestone] = existing_issue.number
    return mapping


def build_epic_reference_map(epic_issue_number_map: dict[str, int]) -> dict[str, str]:
    return {milestone: f"#{issue_number}" for milestone, issue_number in epic_issue_number_map.items()}


def ensure_labels(gh_exe: str, repo: str, labels: tuple[str, ...], dry_run: bool) -> None:
    for label in labels:
        args = [
            "label",
            "create",
            "--repo",
            repo,
            "--force",
            "--color",
            get_label_color(label),
            "--description",
            get_label_description(label),
            label,
        ]
        if dry_run:
            print(f"[DRY-RUN] gh {' '.join(args)}")
            continue
        invoke_gh(gh_exe, *args)
        print(f"label upserted: {label}")


def ensure_milestones(gh_exe: str, repo: str, milestones: tuple[str, ...], dry_run: bool) -> None:
    if dry_run:
        for milestone in milestones:
            print(f"[DRY-RUN] gh api repos/{repo}/milestones --method POST -f title='{milestone}'")
        return

    existing = json.loads(invoke_gh(gh_exe, "api", f"repos/{repo}/milestones?state=all&per_page=100"))
    existing_titles = {item["title"] for item in existing}

    for milestone in milestones:
        if milestone in existing_titles:
            print(f"milestone exists: {milestone}")
            continue
        invoke_gh(gh_exe, "api", f"repos/{repo}/milestones", "--method", "POST", "-f", f"title={milestone}")
        print(f"milestone created: {milestone}")


def ensure_issues(
    gh_exe: str,
    repo: str,
    issues: tuple[IssueItem, ...],
    dry_run: bool,
) -> dict[str, ExistingIssue]:
    existing_issue_map: dict[str, ExistingIssue] = {}
    if not dry_run:
        existing_issue_map = get_existing_issue_map(gh_exe, repo)
    existing_titles = set(existing_issue_map)

    for issue in issues:
        if not dry_run and normalize_issue_title(issue.title) in existing_titles:
            print(f"issue exists: {issue.title}")
            continue

        args = ["issue", "create", "--repo", repo, "--title", issue.title, "--body", build_issue_body(issue)]
        if issue.milestone:
            args.extend(["--milestone", issue.milestone])
        for label in issue.labels:
            args.extend(["--label", label])

        if dry_run:
            summary = ", ".join(issue.labels) if issue.labels else "no labels"
            print(f"[DRY-RUN] gh issue create --repo {repo} --title \"{issue.title}\" ({summary})")
            continue

        invoke_gh(gh_exe, *args)
        print(f"issue created: {issue.title}")

    if dry_run:
        return existing_issue_map
    return get_existing_issue_map(gh_exe, repo)


def sync_epic_bodies(
    gh_exe: str,
    repo: str,
    epics: tuple[IssueItem, ...],
    *,
    issues: tuple[IssueItem, ...],
    existing_issue_map: dict[str, ExistingIssue],
    dry_run: bool,
) -> None:
    for epic in epics:
        existing_epic = existing_issue_map.get(normalize_issue_title(epic.title))
        issue_number = existing_epic.number if existing_epic is not None else None
        body = build_epic_body(
            epic,
            child_issues=tuple(issue for issue in issues if issue.milestone == epic.milestone),
            existing_issues=existing_issue_map,
        )
        if dry_run:
            print(f"[DRY-RUN] gh issue edit --repo {repo} {issue_number or '<pending>'} --body <epic>")
            continue
        if issue_number is None:
            continue
        invoke_gh(gh_exe, "issue", "edit", str(issue_number), "--repo", repo, "--body", body)
        print(f"epic body synced: {epic.title}")


def sync_issue_bodies(
    gh_exe: str,
    repo: str,
    issues: tuple[IssueItem, ...],
    *,
    existing_issue_map: dict[str, ExistingIssue],
    catalog_number_map: dict[int, int],
    epic_reference_map: dict[str, str],
    dry_run: bool,
) -> None:
    for issue in issues:
        existing_issue = existing_issue_map.get(normalize_issue_title(issue.title))
        issue_number = existing_issue.number if existing_issue is not None else None
        body = build_issue_body(
            issue,
            depends_on=translate_depends_on(issue.depends_on, catalog_number_map),
            epic_reference=epic_reference_map.get(issue.milestone),
        )
        if dry_run:
            print(
                f"[DRY-RUN] gh issue edit --repo {repo} "
                f"{issue_number or '<pending>'} --body <translated>"
            )
            continue
        if issue_number is None:
            continue
        invoke_gh(gh_exe, "issue", "edit", str(issue_number), "--repo", repo, "--body", body)
        print(f"issue body synced: {issue.title}")


def get_parent_issue_number(gh_exe: str, repo: str, issue_number: int) -> int | None:
    completed = subprocess.run(
        [gh_exe, "api", f"repos/{repo}/issues/{issue_number}/parent"],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode == 0:
        payload = json.loads(completed.stdout)
        return int(payload["number"])

    detail = completed.stderr.strip() or completed.stdout.strip() or "unknown error"
    if "No parent issue found" in detail:
        return None
    raise SystemExit(
        f"gh command failed ({completed.returncode}): api repos/{repo}/issues/{issue_number}/parent\n{detail}"
    )


def get_issue_database_id(gh_exe: str, repo: str, issue_number: int) -> int:
    payload = json.loads(
        invoke_gh(
            gh_exe,
            "api",
            f"repos/{repo}/issues/{issue_number}",
        )
    )
    return int(payload["id"])


def try_set_sub_issue_link(
    gh_exe: str,
    repo: str,
    *,
    parent_issue_number: int,
    child_issue_id: int,
) -> bool:
    completed = subprocess.run(
        [
            gh_exe,
            "api",
            f"repos/{repo}/issues/{parent_issue_number}/sub_issues",
            "--method",
            "POST",
            "-F",
            f"sub_issue_id={child_issue_id}",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode == 0:
        return True

    detail = completed.stderr.strip() or completed.stdout.strip() or "unknown error"
    if "Not Found" in detail:
        return False
    raise SystemExit(
        f"gh command failed ({completed.returncode}): "
        f"api repos/{repo}/issues/{parent_issue_number}/sub_issues --method POST -F sub_issue_id={child_issue_id}\n"
        f"{detail}"
    )


def remove_sub_issue_link(
    gh_exe: str,
    repo: str,
    *,
    parent_issue_number: int,
    child_issue_id: int,
) -> bool:
    completed = subprocess.run(
        [
            gh_exe,
            "api",
            f"repos/{repo}/issues/{parent_issue_number}/sub_issue",
            "--method",
            "DELETE",
            "-F",
            f"sub_issue_id={child_issue_id}",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode == 0:
        return True

    detail = completed.stderr.strip() or completed.stdout.strip() or "unknown error"
    if "Not Found" in detail:
        return False
    raise SystemExit(
        f"gh command failed ({completed.returncode}): "
        f"api repos/{repo}/issues/{parent_issue_number}/sub_issue --method DELETE -F sub_issue_id={child_issue_id}\n"
        f"{detail}"
    )


def sync_sub_issue_links(
    gh_exe: str,
    repo: str,
    issues: tuple[IssueItem, ...],
    *,
    existing_issue_map: dict[str, ExistingIssue],
    epic_issue_number_map: dict[str, int],
    dry_run: bool,
) -> None:
    sub_issue_api_available = True
    for issue in issues:
        if not sub_issue_api_available:
            break
        existing_issue = existing_issue_map.get(normalize_issue_title(issue.title))
        issue_number = existing_issue.number if existing_issue is not None else None
        parent_issue_number = epic_issue_number_map.get(issue.milestone)
        if issue_number is None or parent_issue_number is None:
            continue
        issue_id = get_issue_database_id(gh_exe, repo, issue_number)

        if dry_run:
            print(
                f"[DRY-RUN] gh api repos/{repo}/issues/{parent_issue_number}/sub_issues "
                f"--method POST -f sub_issue_id={issue_id or '<pending>'}"
            )
            continue

        current_parent_issue_number = get_parent_issue_number(gh_exe, repo, issue_number)
        if current_parent_issue_number == parent_issue_number:
            print(f"sub-issue link exists: {issue.title}")
            continue
        if current_parent_issue_number is not None:
            removed = remove_sub_issue_link(
                gh_exe,
                repo,
                parent_issue_number=current_parent_issue_number,
                child_issue_id=issue_id,
            )
            if not removed:
                print("native sub-issue API unavailable; skipping native hierarchy sync")
                sub_issue_api_available = False
                continue
            print(f"sub-issue removed: {issue.title} from #{current_parent_issue_number}")

        added = try_set_sub_issue_link(
            gh_exe,
            repo,
            parent_issue_number=parent_issue_number,
            child_issue_id=issue_id,
        )
        if not added:
            print("native sub-issue API unavailable; skipping native hierarchy sync")
            sub_issue_api_available = False
            continue
        print(f"sub-issue linked: {issue.title} -> #{parent_issue_number}")


def main() -> int:
    args = parse_args()
    backlog_path = Path(args.backlog_path)
    if not backlog_path.exists():
        raise SystemExit(f"Backlog file not found: {backlog_path}")

    backlog_text = backlog_path.read_text(encoding="utf-8")
    parsed = parse_backlog(backlog_text, source_label=format_backlog_source_label(backlog_path))
    validation_errors = validate_backlog(parsed)
    if validation_errors:
        error_lines = "\n".join(f"- {error}" for error in validation_errors)
        raise SystemExit(f"Backlog validation failed:\n{error_lines}")

    print(f"parsed milestones: {len(parsed.milestones)}")
    print(f"parsed labels: {len(parsed.labels)}")
    print(f"parsed epics: {len(parsed.epics)}")
    print(f"parsed issues: {len(parsed.issues)}")

    gh_exe = ""
    if not args.dry_run:
        gh_exe = resolve_gh_executable()
        print(f"using gh executable: {gh_exe}")
        invoke_gh(gh_exe, "auth", "status")

    ensure_labels(gh_exe, args.repo, parsed.labels, args.dry_run)
    ensure_milestones(gh_exe, args.repo, parsed.milestones, args.dry_run)
    epic_issue_map = ensure_issues(gh_exe, args.repo, parsed.epics, args.dry_run)
    existing_issue_map = ensure_issues(gh_exe, args.repo, parsed.issues, args.dry_run)

    combined_issue_map = dict(epic_issue_map)
    combined_issue_map.update(existing_issue_map)
    catalog_number_map = build_catalog_number_map(parsed.issues, combined_issue_map)
    epic_issue_number_map = build_epic_issue_number_map(parsed.epics, combined_issue_map)
    epic_reference_map = build_epic_reference_map(epic_issue_number_map)

    sync_epic_bodies(
        gh_exe,
        args.repo,
        parsed.epics,
        issues=parsed.issues,
        existing_issue_map=combined_issue_map,
        dry_run=args.dry_run,
    )
    sync_issue_bodies(
        gh_exe,
        args.repo,
        parsed.issues,
        existing_issue_map=combined_issue_map,
        catalog_number_map=catalog_number_map,
        epic_reference_map=epic_reference_map,
        dry_run=args.dry_run,
    )
    sync_sub_issue_links(
        gh_exe,
        args.repo,
        parsed.issues,
        existing_issue_map=combined_issue_map,
        epic_issue_number_map=epic_issue_number_map,
        dry_run=args.dry_run,
    )

    print("backlog creation complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
