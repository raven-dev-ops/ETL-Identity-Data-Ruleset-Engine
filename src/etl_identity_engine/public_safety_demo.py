"""Synthetic CAD/RMS demo builders."""

from __future__ import annotations

from dataclasses import dataclass
from html import escape


@dataclass(frozen=True)
class PublicSafetyDemoResult:
    incident_identity_rows: list[dict[str, str]]
    golden_person_activity_rows: list[dict[str, str]]
    summary: dict[str, object]


def _golden_name_from_row(row: dict[str, str]) -> str:
    return " ".join(
        part
        for part in (
            row.get("golden_first_name", "").strip(),
            row.get("golden_last_name", "").strip(),
        )
        if part
    )


def _stable_person_to_golden_index(golden_rows: list[dict[str, str]]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    conflicts: set[str] = set()
    for row in golden_rows:
        person_entity_id = str(row.get("person_entity_id", "")).strip()
        golden_id = str(row.get("golden_id", "")).strip()
        if not person_entity_id or not golden_id:
            continue
        existing = mapping.get(person_entity_id)
        if existing is None:
            mapping[person_entity_id] = golden_id
            continue
        if existing != golden_id:
            conflicts.add(person_entity_id)

    for person_entity_id in conflicts:
        mapping.pop(person_entity_id, None)
    return mapping


def build_public_safety_demo(
    *,
    incident_rows: list[dict[str, str]],
    incident_link_rows: list[dict[str, str]],
    golden_rows: list[dict[str, str]],
    crosswalk_rows: list[dict[str, str]],
) -> PublicSafetyDemoResult:
    incidents_by_id = {
        str(row.get("incident_id", "")).strip(): row
        for row in incident_rows
        if str(row.get("incident_id", "")).strip()
    }
    golden_by_id = {
        str(row.get("golden_id", "")).strip(): row
        for row in golden_rows
        if str(row.get("golden_id", "")).strip()
    }
    crosswalk_by_source_record_id = {
        str(row.get("source_record_id", "")).strip(): row
        for row in crosswalk_rows
        if str(row.get("source_record_id", "")).strip()
    }
    golden_id_by_person_entity = _stable_person_to_golden_index(golden_rows)

    incident_identity_rows: list[dict[str, str]] = []
    resolved_link_count = 0
    unresolved_link_count = 0

    for link in sorted(
        incident_link_rows,
        key=lambda row: (
            str(row.get("incident_id", "")),
            str(row.get("role", "")),
            str(row.get("source_record_id", "")),
            str(row.get("incident_person_link_id", "")),
        ),
    ):
        incident_id = str(link.get("incident_id", "")).strip()
        source_record_id = str(link.get("source_record_id", "")).strip()
        person_entity_id = str(link.get("person_entity_id", "")).strip()
        incident = incidents_by_id.get(incident_id, {})

        crosswalk = crosswalk_by_source_record_id.get(source_record_id)
        golden_id = ""
        cluster_id = ""
        person_source_system = ""
        golden_row: dict[str, str] = {}

        if crosswalk is not None:
            golden_id = str(crosswalk.get("golden_id", "")).strip()
            cluster_id = str(crosswalk.get("cluster_id", "")).strip()
            person_source_system = str(crosswalk.get("source_system", "")).strip()
            golden_row = golden_by_id.get(golden_id, {})
        elif person_entity_id:
            golden_id = golden_id_by_person_entity.get(person_entity_id, "")
            golden_row = golden_by_id.get(golden_id, {})
            cluster_id = str(golden_row.get("cluster_id", "")).strip()

        if golden_id:
            resolved_link_count += 1
        else:
            unresolved_link_count += 1

        incident_identity_rows.append(
            {
                "incident_id": incident_id,
                "incident_source_system": str(incident.get("source_system", "")).strip(),
                "occurred_at": str(incident.get("occurred_at", "")).strip(),
                "incident_location": str(incident.get("location", "")).strip(),
                "incident_city": str(incident.get("city", "")).strip(),
                "incident_state": str(incident.get("state", "")).strip(),
                "incident_role": str(link.get("role", "")).strip(),
                "person_entity_id": person_entity_id,
                "source_record_id": source_record_id,
                "person_source_system": person_source_system,
                "golden_id": golden_id,
                "cluster_id": cluster_id,
                "golden_first_name": str(golden_row.get("first_name", "")).strip(),
                "golden_last_name": str(golden_row.get("last_name", "")).strip(),
                "golden_dob": str(golden_row.get("dob", "")).strip(),
                "golden_address": str(golden_row.get("address", "")).strip(),
                "golden_phone": str(golden_row.get("phone", "")).strip(),
            }
        )

    grouped_rows: dict[str, list[dict[str, str]]] = {}
    for row in incident_identity_rows:
        golden_id = str(row.get("golden_id", "")).strip()
        if golden_id:
            grouped_rows.setdefault(golden_id, []).append(row)

    golden_person_activity_rows: list[dict[str, str]] = []
    cross_system_golden_person_count = 0

    for golden_id in sorted(grouped_rows):
        rows = grouped_rows[golden_id]
        unique_cad_incidents = {
            row["incident_id"]
            for row in rows
            if row.get("incident_source_system", "").strip().lower() == "cad" and row.get("incident_id", "").strip()
        }
        unique_rms_incidents = {
            row["incident_id"]
            for row in rows
            if row.get("incident_source_system", "").strip().lower() == "rms" and row.get("incident_id", "").strip()
        }
        unique_incidents = {
            row["incident_id"] for row in rows if row.get("incident_id", "").strip()
        }
        unique_source_record_ids = {
            row["source_record_id"] for row in rows if row.get("source_record_id", "").strip()
        }
        roles = sorted({row["incident_role"] for row in rows if row.get("incident_role", "").strip()})
        latest_incident_at = max(
            (row["occurred_at"] for row in rows if row.get("occurred_at", "").strip()),
            default="",
        )
        representative = rows[0]

        if unique_cad_incidents and unique_rms_incidents:
            cross_system_golden_person_count += 1

        golden_person_activity_rows.append(
            {
                "golden_id": golden_id,
                "cluster_id": str(representative.get("cluster_id", "")).strip(),
                "person_entity_id": str(representative.get("person_entity_id", "")).strip(),
                "golden_first_name": str(representative.get("golden_first_name", "")).strip(),
                "golden_last_name": str(representative.get("golden_last_name", "")).strip(),
                "cad_incident_count": str(len(unique_cad_incidents)),
                "rms_incident_count": str(len(unique_rms_incidents)),
                "total_incident_count": str(len(unique_incidents)),
                "linked_source_record_count": str(len(unique_source_record_ids)),
                "roles": ";".join(roles),
                "latest_incident_at": latest_incident_at,
            }
        )

    top_golden_people = [
        {
            "golden_id": row["golden_id"],
            "golden_name": _golden_name_from_row(row),
            "total_incident_count": int(row["total_incident_count"]),
            "cad_incident_count": int(row["cad_incident_count"]),
            "rms_incident_count": int(row["rms_incident_count"]),
        }
        for row in sorted(
            golden_person_activity_rows,
            key=lambda row: (
                -int(row["total_incident_count"]),
                row.get("golden_last_name", ""),
                row.get("golden_first_name", ""),
                row.get("golden_id", ""),
            ),
        )[:5]
    ]

    cad_incident_ids = {
        str(row.get("incident_id", "")).strip()
        for row in incident_rows
        if str(row.get("source_system", "")).strip().lower() == "cad" and str(row.get("incident_id", "")).strip()
    }
    rms_incident_ids = {
        str(row.get("incident_id", "")).strip()
        for row in incident_rows
        if str(row.get("source_system", "")).strip().lower() == "rms" and str(row.get("incident_id", "")).strip()
    }

    cross_system_candidates = [
        row
        for row in golden_person_activity_rows
        if int(row.get("cad_incident_count", "0")) > 0 and int(row.get("rms_incident_count", "0")) > 0
    ]
    single_source_candidates = [
        row
        for row in golden_person_activity_rows
        if (int(row.get("cad_incident_count", "0")) > 0) ^ (int(row.get("rms_incident_count", "0")) > 0)
    ]

    demo_scenarios: list[dict[str, object]] = []
    if top_golden_people:
        top_activity = golden_person_activity_rows[
            next(
                index
                for index, row in enumerate(golden_person_activity_rows)
                if row.get("golden_id", "") == str(top_golden_people[0]["golden_id"])
            )
        ]
        demo_scenarios.append(
            {
                "scenario_id": "highest_activity_identity",
                "title": "Highest Activity Identity",
                "golden_id": top_activity["golden_id"],
                "golden_name": _golden_name_from_row(top_activity),
                "narrative": (
                    "Start here to show the busiest resolved identity in the demo set and explain how "
                    "multiple incident rows roll up to one canonical person."
                ),
                "cad_incident_count": int(top_activity["cad_incident_count"]),
                "rms_incident_count": int(top_activity["rms_incident_count"]),
                "total_incident_count": int(top_activity["total_incident_count"]),
                "latest_incident_at": top_activity["latest_incident_at"],
            }
        )
    if cross_system_candidates:
        cross_system = sorted(
            cross_system_candidates,
            key=lambda row: (
                -int(row.get("total_incident_count", "0")),
                row.get("golden_last_name", ""),
                row.get("golden_first_name", ""),
            ),
        )[0]
        demo_scenarios.append(
            {
                "scenario_id": "cross_system_identity",
                "title": "CAD And RMS On One Identity",
                "golden_id": cross_system["golden_id"],
                "golden_name": _golden_name_from_row(cross_system),
                "narrative": (
                    "Use this person to show the core buyer story: CAD and RMS activity from different "
                    "source records resolve to one golden identity."
                ),
                "cad_incident_count": int(cross_system["cad_incident_count"]),
                "rms_incident_count": int(cross_system["rms_incident_count"]),
                "total_incident_count": int(cross_system["total_incident_count"]),
                "latest_incident_at": cross_system["latest_incident_at"],
            }
        )
    if single_source_candidates:
        single_source = sorted(
            single_source_candidates,
            key=lambda row: (
                -int(row.get("total_incident_count", "0")),
                row.get("golden_last_name", ""),
                row.get("golden_first_name", ""),
            ),
        )[0]
        system_name = "CAD" if int(single_source["cad_incident_count"]) > 0 else "RMS"
        demo_scenarios.append(
            {
                "scenario_id": "single_source_identity",
                "title": f"{system_name}-Only Identity",
                "golden_id": single_source["golden_id"],
                "golden_name": _golden_name_from_row(single_source),
                "narrative": (
                    "Use this person as a contrast case to show that the pipeline preserves identities that "
                    f"only appear in {system_name} without forcing fake cross-system joins."
                ),
                "cad_incident_count": int(single_source["cad_incident_count"]),
                "rms_incident_count": int(single_source["rms_incident_count"]),
                "total_incident_count": int(single_source["total_incident_count"]),
                "latest_incident_at": single_source["latest_incident_at"],
            }
        )

    summary = {
        "incident_count": len(incidents_by_id),
        "incident_person_link_count": len(incident_link_rows),
        "cad_incident_count": len(cad_incident_ids),
        "rms_incident_count": len(rms_incident_ids),
        "resolved_link_count": resolved_link_count,
        "unresolved_link_count": unresolved_link_count,
        "linked_golden_person_count": len(golden_person_activity_rows),
        "cross_system_golden_person_count": cross_system_golden_person_count,
        "demo_scenarios": demo_scenarios,
        "top_golden_people_by_activity": top_golden_people,
    }

    return PublicSafetyDemoResult(
        incident_identity_rows=incident_identity_rows,
        golden_person_activity_rows=golden_person_activity_rows,
        summary=summary,
    )


def build_public_safety_demo_report_markdown(summary: dict[str, object]) -> str:
    top_people = summary.get("top_golden_people_by_activity", [])
    demo_scenarios = summary.get("demo_scenarios", [])
    lines = [
        "# Public Safety Demo Report",
        "",
        "## Summary",
        f"- `incident_count`: `{summary.get('incident_count', 0)}`",
        f"- `incident_person_link_count`: `{summary.get('incident_person_link_count', 0)}`",
        f"- `cad_incident_count`: `{summary.get('cad_incident_count', 0)}`",
        f"- `rms_incident_count`: `{summary.get('rms_incident_count', 0)}`",
        f"- `resolved_link_count`: `{summary.get('resolved_link_count', 0)}`",
        f"- `unresolved_link_count`: `{summary.get('unresolved_link_count', 0)}`",
        f"- `linked_golden_person_count`: `{summary.get('linked_golden_person_count', 0)}`",
        f"- `cross_system_golden_person_count`: `{summary.get('cross_system_golden_person_count', 0)}`",
        "",
        "## What It Demonstrates",
        "- Mock CAD and RMS incidents can be resolved back to a single golden-person identity.",
        "- The crosswalk makes it easy to show how operational incident activity rolls up to one canonical record.",
        "- The activity rollup highlights people who appear across both CAD and RMS sources.",
        "",
        "## Suggested Demo Scenarios",
    ]

    if isinstance(demo_scenarios, list) and demo_scenarios:
        for scenario in demo_scenarios:
            if not isinstance(scenario, dict):
                continue
            lines.append(
                f"- `{scenario.get('title', '')}` for `{scenario.get('golden_id', '')}` "
                f"`{scenario.get('golden_name', '')}`: {scenario.get('narrative', '')}"
            )
    else:
        lines.append("- No demo scenarios were derived from this run.")

    lines.extend(
        [
            "",
        "## Top Golden People By Activity",
        ]
    )

    if isinstance(top_people, list) and top_people:
        for item in top_people:
            lines.append(
                f"- `{item.get('golden_id', '')}` `{item.get('golden_name', '')}`: "
                f"total=`{item.get('total_incident_count', 0)}`, "
                f"cad=`{item.get('cad_incident_count', 0)}`, "
                f"rms=`{item.get('rms_incident_count', 0)}`"
            )
    else:
        lines.append("- No linked golden-person activity was produced.")

    return "\n".join(lines)


def build_public_safety_demo_walkthrough_markdown(summary: dict[str, object]) -> str:
    scenarios = summary.get("demo_scenarios", [])
    lines = [
        "# Public Safety Demo Walkthrough",
        "",
        "Use the dashboard first, then drill into the CSV outputs only if someone wants detail.",
        "",
        "## Sequence",
        "1. Open `public_safety_demo_dashboard.html` and start with the summary cards.",
        "2. Use the cross-system count to establish that some people appear in both CAD and RMS.",
        "3. Open the scenario table below and pick the suggested identity for the buyer conversation.",
        "4. If needed, use `incident_identity_view.csv` to show the raw incident-to-golden joins.",
        "",
        "## Suggested Scenarios",
    ]

    if isinstance(scenarios, list) and scenarios:
        for index, scenario in enumerate(scenarios, start=1):
            if not isinstance(scenario, dict):
                continue
            lines.extend(
                [
                    f"### {index}. {scenario.get('title', '')}",
                    f"- Golden ID: `{scenario.get('golden_id', '')}`",
                    f"- Golden Name: `{scenario.get('golden_name', '')}`",
                    f"- CAD incidents: `{scenario.get('cad_incident_count', 0)}`",
                    f"- RMS incidents: `{scenario.get('rms_incident_count', 0)}`",
                    f"- Total incidents: `{scenario.get('total_incident_count', 0)}`",
                    f"- Latest incident: `{scenario.get('latest_incident_at', '')}`",
                    f"- Talk track: {scenario.get('narrative', '')}",
                    "",
                ]
            )
    else:
        lines.append("- No walkthrough scenarios were generated.")

    lines.extend(
        [
            "## Safety Note",
            "This walkthrough is generated from synthetic demo data only and does not represent live operational records or CJIS compliance by itself.",
        ]
    )
    return "\n".join(lines)


def _summary_card(title: str, value: object, tone: str) -> str:
    return (
        '<article class="metric-card">'
        f'<p class="metric-label">{escape(title)}</p>'
        f'<p class="metric-value metric-{escape(tone)}">{escape(str(value))}</p>'
        "</article>"
    )


def _table_html(
    *,
    title: str,
    subtitle: str,
    headers: list[str],
    rows: list[list[str]],
) -> str:
    header_html = "".join(f"<th>{escape(header)}</th>" for header in headers)
    body_html = "".join(
        "<tr>" + "".join(f"<td>{escape(cell)}</td>" for cell in row) + "</tr>"
        for row in rows
    )
    empty_row_html = '<tr><td colspan="99">No rows</td></tr>'
    return (
        '<section class="panel">'
        f'<div class="panel-header"><h2>{escape(title)}</h2><p>{escape(subtitle)}</p></div>'
        '<div class="table-wrap"><table>'
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{body_html or empty_row_html}</tbody>"
        "</table></div></section>"
    )


def build_public_safety_demo_dashboard_html(
    *,
    summary: dict[str, object],
    golden_person_activity_rows: list[dict[str, str]],
    incident_identity_rows: list[dict[str, str]],
) -> str:
    top_people = summary.get("top_golden_people_by_activity", [])
    demo_scenarios = summary.get("demo_scenarios", [])
    cross_system_rows = sorted(
        [
            row
            for row in golden_person_activity_rows
            if int(row.get("cad_incident_count", "0")) > 0 and int(row.get("rms_incident_count", "0")) > 0
        ],
        key=lambda row: (
            -int(row.get("total_incident_count", "0")),
            row.get("golden_last_name", ""),
            row.get("golden_first_name", ""),
        ),
    )[:8]
    latest_incident_rows = sorted(
        incident_identity_rows,
        key=lambda row: (
            row.get("occurred_at", ""),
            row.get("incident_id", ""),
            row.get("golden_id", ""),
        ),
        reverse=True,
    )[:12]

    metrics_html = "".join(
        [
            _summary_card("Incidents", summary.get("incident_count", 0), "blue"),
            _summary_card("CAD Incidents", summary.get("cad_incident_count", 0), "red"),
            _summary_card("RMS Incidents", summary.get("rms_incident_count", 0), "amber"),
            _summary_card("Resolved Links", summary.get("resolved_link_count", 0), "green"),
            _summary_card("Linked Golden People", summary.get("linked_golden_person_count", 0), "ink"),
            _summary_card("Cross-System People", summary.get("cross_system_golden_person_count", 0), "violet"),
        ]
    )

    top_people_table = _table_html(
        title="Top Golden People By Activity",
        subtitle="The quickest demo story: one resolved person, many operational records.",
        headers=["Golden ID", "Name", "Total", "CAD", "RMS"],
        rows=[
            [
                str(item.get("golden_id", "")),
                str(item.get("golden_name", "")),
                str(item.get("total_incident_count", 0)),
                str(item.get("cad_incident_count", 0)),
                str(item.get("rms_incident_count", 0)),
            ]
            for item in top_people
            if isinstance(item, dict)
        ],
    )
    scenario_table = _table_html(
        title="Suggested Demo Scenarios",
        subtitle="Use these first during the buyer walkthrough.",
        headers=["Scenario", "Golden ID", "Name", "CAD", "RMS", "Total", "Why It Matters"],
        rows=[
            [
                str(item.get("title", "")),
                str(item.get("golden_id", "")),
                str(item.get("golden_name", "")),
                str(item.get("cad_incident_count", 0)),
                str(item.get("rms_incident_count", 0)),
                str(item.get("total_incident_count", 0)),
                str(item.get("narrative", "")),
            ]
            for item in demo_scenarios
            if isinstance(item, dict)
        ],
    )
    cross_system_table = _table_html(
        title="Cross-System People",
        subtitle="Golden people with both CAD and RMS activity in the same demo run.",
        headers=["Golden ID", "Name", "CAD", "RMS", "Total", "Roles", "Latest Incident"],
        rows=[
            [
                row.get("golden_id", ""),
                " ".join(
                    part
                    for part in (row.get("golden_first_name", "").strip(), row.get("golden_last_name", "").strip())
                    if part
                ),
                row.get("cad_incident_count", "0"),
                row.get("rms_incident_count", "0"),
                row.get("total_incident_count", "0"),
                row.get("roles", ""),
                row.get("latest_incident_at", ""),
            ]
            for row in cross_system_rows
        ],
    )
    recent_incident_table = _table_html(
        title="Recent Incident-to-Identity View",
        subtitle="Preview of the joined CAD/RMS incident rows mapped back to the golden person.",
        headers=["Occurred At", "System", "Incident", "Role", "Golden ID", "Golden Person", "Source Record"],
        rows=[
            [
                row.get("occurred_at", ""),
                row.get("incident_source_system", "").upper(),
                row.get("incident_id", ""),
                row.get("incident_role", ""),
                row.get("golden_id", ""),
                " ".join(
                    part
                    for part in (row.get("golden_first_name", "").strip(), row.get("golden_last_name", "").strip())
                    if part
                ),
                row.get("source_record_id", ""),
            ]
            for row in latest_incident_rows
        ],
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Public Safety Identity Demo</title>
  <style>
    :root {{
      --ink: #14213d;
      --ink-soft: #42506b;
      --paper: #f6f2e8;
      --panel: rgba(255, 255, 255, 0.92);
      --line: rgba(20, 33, 61, 0.14);
      --blue: #1d4ed8;
      --red: #b42318;
      --amber: #b45309;
      --green: #146c43;
      --violet: #6d28d9;
      --shadow: 0 18px 40px rgba(20, 33, 61, 0.12);
      --radius: 20px;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Aptos", "Segoe UI Variable Text", "Trebuchet MS", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(29, 78, 216, 0.16), transparent 24rem),
        radial-gradient(circle at bottom right, rgba(180, 83, 9, 0.16), transparent 20rem),
        linear-gradient(160deg, #faf7f2 0%, var(--paper) 60%, #efe6d3 100%);
      min-height: 100vh;
    }}
    main {{
      width: min(1200px, calc(100vw - 2rem));
      margin: 0 auto;
      padding: 2rem 0 3rem;
    }}
    .hero, .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }}
    .hero {{
      padding: 2rem;
      display: grid;
      gap: 1.5rem;
    }}
    .hero-kicker {{
      display: inline-flex;
      width: fit-content;
      padding: 0.35rem 0.75rem;
      border-radius: 999px;
      background: rgba(20, 33, 61, 0.06);
      letter-spacing: 0.08em;
      text-transform: uppercase;
      font-size: 0.75rem;
      font-weight: 700;
    }}
    h1, h2, h3, p {{ margin: 0; }}
    h1 {{
      font-family: Georgia, "Times New Roman", serif;
      font-size: clamp(2.2rem, 5vw, 4rem);
      line-height: 0.95;
      max-width: 14ch;
    }}
    .hero-copy {{
      color: var(--ink-soft);
      font-size: 1.05rem;
      max-width: 66ch;
      line-height: 1.5;
    }}
    .demo-script {{
      display: grid;
      gap: 0.65rem;
      padding: 1rem 1.1rem;
      border-radius: 16px;
      background: rgba(20, 33, 61, 0.045);
      border: 1px solid rgba(20, 33, 61, 0.08);
    }}
    .demo-script strong {{
      font-size: 0.92rem;
      letter-spacing: 0.02em;
    }}
    .demo-script ol {{
      margin: 0;
      padding-left: 1.2rem;
      color: var(--ink-soft);
      display: grid;
      gap: 0.45rem;
    }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 0.9rem;
    }}
    .metric-card {{
      padding: 1rem;
      border-radius: 18px;
      background: rgba(255, 255, 255, 0.84);
      border: 1px solid rgba(20, 33, 61, 0.08);
    }}
    .metric-label {{
      color: var(--ink-soft);
      font-size: 0.78rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 0.35rem;
      font-weight: 700;
    }}
    .metric-value {{
      font-size: 2rem;
      font-weight: 800;
      line-height: 1;
    }}
    .metric-blue {{ color: var(--blue); }}
    .metric-red {{ color: var(--red); }}
    .metric-amber {{ color: var(--amber); }}
    .metric-green {{ color: var(--green); }}
    .metric-violet {{ color: var(--violet); }}
    .metric-ink {{ color: var(--ink); }}
    .dashboard {{
      margin-top: 1.25rem;
      display: grid;
      gap: 1rem;
    }}
    .panel {{
      padding: 1.1rem;
    }}
    .panel-header {{
      display: grid;
      gap: 0.25rem;
      margin-bottom: 0.9rem;
    }}
    .panel-header p {{
      color: var(--ink-soft);
      line-height: 1.45;
    }}
    .table-wrap {{
      overflow-x: auto;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 720px;
    }}
    th, td {{
      padding: 0.72rem 0.75rem;
      border-bottom: 1px solid var(--line);
      text-align: left;
      font-size: 0.94rem;
    }}
    th {{
      font-size: 0.76rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--ink-soft);
    }}
    tbody tr:nth-child(even) {{
      background: rgba(20, 33, 61, 0.025);
    }}
    .footer-note {{
      margin-top: 1rem;
      color: var(--ink-soft);
      font-size: 0.92rem;
    }}
    @media (max-width: 720px) {{
      main {{
        width: min(100vw - 1rem, 1200px);
        padding-top: 1rem;
      }}
      .hero, .panel {{
        border-radius: 16px;
      }}
      .hero {{
        padding: 1.25rem;
      }}
      .panel {{
        padding: 0.95rem;
      }}
    }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <div class="hero-kicker">Mock CAD/RMS Identity Demo</div>
      <div>
        <h1>One person. Many operational records.</h1>
        <p class="hero-copy">
          This dashboard shows how synthetic CAD and RMS incident activity rolls up
          to a single golden-person identity using the ETL identity-resolution
          pipeline. It is meant for demos, not for handling real CJI.
        </p>
      </div>
      <section class="metrics">{metrics_html}</section>
      <section class="demo-script">
        <strong>Fast demo script</strong>
        <ol>
          <li>Start with the cross-system count to show people appearing in both CAD and RMS.</li>
          <li>Open the top-activity table and pick one golden person with both sources.</li>
          <li>Use the recent incident view to show the CAD and RMS rows tied back to that same golden ID.</li>
        </ol>
      </section>
    </section>
    <section class="dashboard">
      {top_people_table}
      {scenario_table}
      {cross_system_table}
      {recent_incident_table}
    </section>
    <p class="footer-note">
      Generated from synthetic inputs only. This dashboard demonstrates the identity-resolution pattern,
      not CJIS compliance by itself.
    </p>
  </main>
</body>
</html>
"""
