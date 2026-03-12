from __future__ import annotations

import re
from pathlib import Path

import yaml
import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
ISSUE_TEMPLATE_DIR = REPO_ROOT / ".github" / "ISSUE_TEMPLATE"
CONFIG_PATH = ISSUE_TEMPLATE_DIR / "config.yml"
FORM_PATHS = sorted(path for path in ISSUE_TEMPLATE_DIR.glob("*.yml") if path.name != "config.yml")
EXPECTED_LABELS = {
    "bug.yml": ["type:bug"],
    "chore.yml": ["type:chore"],
    "docs.yml": ["type:docs"],
    "epic.yml": ["type:epic"],
    "feature.yml": ["type:feature"],
}
TOP_LEVEL_FORM_KEYS = {"assignees", "body", "description", "labels", "name", "projects", "title", "type"}
TOP_LEVEL_CONFIG_KEYS = {"blank_issues_enabled", "contact_links"}
INPUT_TYPES = {"checkboxes", "dropdown", "input", "markdown", "textarea", "upload"}
GENERIC_ITEM_KEYS = {"attributes", "id", "type", "validations"}
ALLOWED_ATTRIBUTES = {
    "checkboxes": {"description", "label", "options"},
    "dropdown": {"default", "description", "label", "multiple", "options"},
    "input": {"description", "label", "placeholder", "value"},
    "markdown": {"value"},
    "textarea": {"description", "label", "placeholder", "render", "value"},
    "upload": {"description", "label", "multiple"},
}
ALLOWED_VALIDATIONS = {
    "checkboxes": {"required"},
    "dropdown": {"required"},
    "input": {"required"},
    "markdown": set(),
    "textarea": {"required"},
    "upload": {"required"},
}
ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


def _load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    assert isinstance(data, dict), f"{path.name} must load as a mapping"
    return data


def test_issue_template_inventory_is_expected() -> None:
    actual = {path.name for path in ISSUE_TEMPLATE_DIR.glob("*.yml")}
    assert actual == {
        "bug.yml",
        "chore.yml",
        "config.yml",
        "docs.yml",
        "epic.yml",
        "feature.yml",
    }


@pytest.mark.parametrize("path", sorted(ISSUE_TEMPLATE_DIR.glob("*.yml")), ids=lambda path: path.name)
def test_issue_template_files_have_no_placeholders(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    assert "OWNER/REPO" not in text


def test_issue_template_config_has_required_contact_links() -> None:
    config = _load_yaml(CONFIG_PATH)

    assert set(config).issubset(TOP_LEVEL_CONFIG_KEYS)
    assert config.get("blank_issues_enabled") is False

    contact_links = config.get("contact_links")
    assert isinstance(contact_links, list)
    assert len(contact_links) >= 2
    names: set[str] = set()

    for link in contact_links:
        assert isinstance(link, dict)
        assert set(link).issuperset({"name", "url", "about"})
        assert isinstance(link["name"], str) and link["name"]
        assert isinstance(link["about"], str) and link["about"]
        assert isinstance(link["url"], str) and link["url"].startswith("https://")
        assert link["name"] not in names
        names.add(link["name"])


@pytest.mark.parametrize("path", FORM_PATHS, ids=lambda path: path.name)
def test_issue_forms_parse_and_follow_repo_conventions(path: Path) -> None:
    form = _load_yaml(path)

    assert set(form).issubset(TOP_LEVEL_FORM_KEYS)
    assert isinstance(form.get("name"), str) and form["name"]
    assert isinstance(form.get("description"), str) and form["description"]
    assert isinstance(form.get("title"), str) and form["title"].endswith(": ")
    assert form.get("labels") == EXPECTED_LABELS[path.name]

    body = form.get("body")
    assert isinstance(body, list) and body
    assert any(item.get("type") != "markdown" for item in body)

    seen_ids: set[str] = set()
    seen_labels: set[str] = set()

    for index, item in enumerate(body):
        assert isinstance(item, dict), f"{path.name} body item {index} must be a mapping"
        assert set(item).issubset(GENERIC_ITEM_KEYS)
        assert item.get("type") in INPUT_TYPES

        if item["type"] != "markdown":
            assert isinstance(item.get("id"), str) and item["id"]
            assert ID_PATTERN.fullmatch(item["id"])
            assert item["id"] not in seen_ids
            seen_ids.add(item["id"])
        else:
            assert "id" not in item

        attributes = item.get("attributes")
        assert isinstance(attributes, dict), f"{path.name} body item {index} must define attributes"
        assert set(attributes).issubset(ALLOWED_ATTRIBUTES[item["type"]])

        if item["type"] != "markdown":
            assert isinstance(attributes.get("label"), str) and attributes["label"]
            assert attributes["label"] not in seen_labels
            seen_labels.add(attributes["label"])
        else:
            assert isinstance(attributes.get("value"), str) and attributes["value"].strip()

        if item["type"] == "dropdown":
            options = attributes.get("options")
            assert isinstance(options, list) and options
            assert all(isinstance(option, str) and option for option in options)
            assert len(options) == len(set(options))
            default_index = attributes.get("default")
            if default_index is not None:
                assert isinstance(default_index, int)
                assert 0 <= default_index < len(options)
            multiple = attributes.get("multiple")
            if multiple is not None:
                assert isinstance(multiple, bool)

        if item["type"] == "checkboxes":
            options = attributes.get("options")
            assert isinstance(options, list) and options
            option_labels: set[str] = set()
            for option in options:
                assert isinstance(option, dict)
                assert set(option).issubset({"label", "required"})
                assert isinstance(option.get("label"), str) and option["label"]
                assert option["label"] not in option_labels
                option_labels.add(option["label"])
                if "required" in option:
                    assert isinstance(option["required"], bool)

        if item["type"] in {"input", "textarea", "upload"}:
            description = attributes.get("description")
            if description is not None:
                assert isinstance(description, str)

        if item["type"] == "textarea":
            render = attributes.get("render")
            if render is not None:
                assert isinstance(render, str) and render

        validations = item.get("validations")
        if validations is not None:
            assert isinstance(validations, dict)
            assert set(validations).issubset(ALLOWED_VALIDATIONS[item["type"]])
            if "required" in validations:
                assert isinstance(validations["required"], bool)


def test_issue_form_names_are_unique() -> None:
    names = [_load_yaml(path)["name"] for path in FORM_PATHS]
    assert len(names) == len(set(names))
