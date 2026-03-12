from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


GRAPHQL_ENDPOINT = "https://api.github.com/graphql"
EXPECTED_CONTACT_LINKS = {
    "Private security report": "/security/advisories/new",
    "Contribution guide": "/blob/main/CONTRIBUTING.md",
    "Project backlog": "/blob/main/planning/github-issues-backlog.md",
}
EXPECTED_TEMPLATE_FILES = {
    "bug.yml",
    "chore.yml",
    "docs.yml",
    "epic.yml",
    "feature.yml",
}
QUERY = """
query($owner: String!, $name: String!) {
  repository(owner: $owner, name: $name) {
    url
    defaultBranchRef {
      name
    }
    isBlankIssuesEnabled
    contactLinks {
      name
      about
      url
    }
    issueTemplates {
      name
      about
      title
      filename
    }
  }
}
""".strip()


@dataclass(frozen=True)
class ParsedRepo:
    repo_url: str
    default_branch: str
    blank_issues_enabled: bool
    contact_links: list[dict[str, str]]
    issue_templates: list[dict[str, str]]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate GitHub-recognized issue metadata on the repository default branch."
    )
    parser.add_argument(
        "--repo",
        required=True,
        help="Repository slug in OWNER/REPO format.",
    )
    return parser.parse_args()


def _get_token() -> str:
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if token:
        return token
    raise SystemExit("Missing GH_TOKEN or GITHUB_TOKEN for GitHub GraphQL access.")


def _split_repo(repo: str) -> tuple[str, str]:
    parts = repo.split("/", 1)
    if len(parts) != 2 or not all(parts):
        raise SystemExit(f"Invalid --repo value: {repo!r}. Expected OWNER/REPO.")
    return parts[0], parts[1]


def _run_query(token: str, owner: str, name: str) -> ParsedRepo:
    payload = json.dumps(
        {
            "query": QUERY,
            "variables": {
                "owner": owner,
                "name": name,
            },
        }
    ).encode("utf-8")
    request = Request(
        GRAPHQL_ENDPOINT,
        data=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/vnd.github+json",
            "User-Agent": "etl-identity-engine-issue-metadata-check",
        },
        method="POST",
    )

    try:
        with urlopen(request, timeout=30) as response:
            response_data: dict[str, Any] = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise SystemExit(f"GitHub GraphQL request failed with HTTP {exc.code}.") from exc
    except URLError as exc:
        raise SystemExit(f"GitHub GraphQL request failed: {exc.reason}") from exc

    if response_data.get("errors"):
        raise SystemExit(f"GitHub GraphQL returned errors: {response_data['errors']}")

    repository = response_data["data"]["repository"]
    if repository is None:
        raise SystemExit(f"Repository {owner}/{name} was not found via GitHub GraphQL.")

    default_branch = repository["defaultBranchRef"]["name"]
    return ParsedRepo(
        repo_url=repository["url"],
        default_branch=default_branch,
        blank_issues_enabled=repository["isBlankIssuesEnabled"],
        contact_links=repository["contactLinks"],
        issue_templates=repository["issueTemplates"],
    )


def _validate(parsed: ParsedRepo) -> list[str]:
    errors: list[str] = []

    if parsed.blank_issues_enabled:
        errors.append("blank issues are still enabled on the default branch")

    link_names = {link["name"] for link in parsed.contact_links}
    for expected_name, expected_suffix in EXPECTED_CONTACT_LINKS.items():
        if expected_name not in link_names:
            errors.append(f"missing contact link: {expected_name}")
            continue
        matching = next(link for link in parsed.contact_links if link["name"] == expected_name)
        if not matching["url"].endswith(expected_suffix):
            errors.append(
                f"contact link {expected_name!r} has unexpected URL: {matching['url']}"
            )

    template_files = {template["filename"] for template in parsed.issue_templates}
    missing_templates = sorted(EXPECTED_TEMPLATE_FILES - template_files)
    if missing_templates:
        errors.append(
            "missing issue templates recognized by GitHub: " + ", ".join(missing_templates)
        )

    return errors


def main() -> int:
    args = _parse_args()
    token = _get_token()
    owner, name = _split_repo(args.repo)
    parsed = _run_query(token=token, owner=owner, name=name)
    errors = _validate(parsed)

    print(f"Repository: {owner}/{name}")
    print(f"Default branch: {parsed.default_branch}")
    print(f"Blank issues enabled: {parsed.blank_issues_enabled}")
    print(f"Contact links recognized by GitHub: {len(parsed.contact_links)}")
    print(f"Issue templates recognized by GitHub: {len(parsed.issue_templates)}")

    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1

    print("GitHub recognized the expected issue metadata on the default branch.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
