from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BACKLOG_PATH = REPO_ROOT / "planning" / "github-issues-backlog.md"
ISSUE_PATTERN = re.compile(
    r"(?ms)^###\s+\d+\)\s+(?P<title>.+?)\r?\n\r?\n(?P<body>.*?)(?=^###\s+\d+\)|^##\s+Suggested Epic Issues)"
)
EPIC_PATTERN = re.compile(r"^\d+\.\s+Epic:\s+(.+?)\s+\(`([^`]+)`\)", re.MULTILINE)


@dataclass(frozen=True)
class IssueItem:
    title: str
    milestone: str
    labels: tuple[str, ...]
    depends_on: str
    description_items: tuple[str, ...]
    acceptance_items: tuple[str, ...]


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
        help="Path to planning/github-issues-backlog.md.",
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


def get_section_body(content: str, start_heading: str, end_heading: str) -> str:
    pattern = re.compile(
        rf"(?ms)^## {re.escape(start_heading)}\s*(?P<body>.*?)(?=^## {re.escape(end_heading)}|\Z)"
    )
    match = pattern.search(content)
    return match.group("body") if match else ""


def parse_bullet_items(section: str) -> tuple[str, ...]:
    return tuple(
        match.group(1).strip()
        for match in re.finditer(r"^\s*-\s+(.+)$", section, re.MULTILINE)
    )


def parse_backlog(backlog_text: str) -> ParsedBacklog:
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
            )
        )

    epic_body = get_section_body(backlog_text, "Suggested Epic Issues", "Suggested Issue Creation Order")
    epics = tuple(
        IssueItem(
            title=epic_match.group(1).strip(),
            milestone=epic_match.group(2).strip(),
            labels=("type:epic",),
            depends_on="none",
            description_items=("Epic created from planning/github-issues-backlog.md",),
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


def build_issue_body(issue: IssueItem) -> str:
    lines = [
        "## Milestone",
        "",
        f"- ``{issue.milestone}``",
        "",
        "## Depends On",
        "",
        f"- {issue.depends_on}",
        "",
        "## Description",
    ]
    lines.extend(f"- {item}" for item in issue.description_items)
    lines.extend(["", "## Acceptance Criteria"])
    lines.extend(f"- {item}" for item in issue.acceptance_items)
    return "\n".join(lines)


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


def ensure_issues(gh_exe: str, repo: str, issues: tuple[IssueItem, ...], dry_run: bool) -> None:
    existing_titles: set[str] = set()
    if not dry_run:
        existing = json.loads(invoke_gh(gh_exe, "issue", "list", "--repo", repo, "--state", "all", "--limit", "500", "--json", "title"))
        existing_titles = {item["title"] for item in existing}

    for issue in issues:
        if not dry_run and issue.title in existing_titles:
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


def main() -> int:
    args = parse_args()
    backlog_path = Path(args.backlog_path)
    if not backlog_path.exists():
        raise SystemExit(f"Backlog file not found: {backlog_path}")

    backlog_text = backlog_path.read_text(encoding="utf-8")
    parsed = parse_backlog(backlog_text)

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
    ensure_issues(gh_exe, args.repo, parsed.epics, args.dry_run)
    ensure_issues(gh_exe, args.repo, parsed.issues, args.dry_run)

    print("backlog creation complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
