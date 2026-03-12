# Public-Safety CAD/RMS ETL Identity Resolution Prototype Repo Design

## Problem framing grounded in CAD and RMS realities

In many public-safety technology stacks, **computer-aided dispatch (CAD)** is positioned as an early intake point for incident information and operational communications, and can feed information onward into one or more **records management systems (RMS)**. This “CAD → RMS” handoff is explicitly described in public functional-specification guidance: CAD systems collect initial incident information and then provide it to one or more RMS systems. citeturn3view3

An RMS, in turn, is commonly characterized as an “agency-wide system” supporting the full records lifecycle—storage, retrieval, retention, manipulation, archiving, and viewing of information tied to law enforcement operations—and is designed to support “single entry” (enter once, reuse across modules) while still enabling multiple reporting mechanisms. citeturn5view0 This “single entry” model is also repeated as a general best practice in RMS functional standards documentation. citeturn6view0

In practice, legacy or multi-vendor environments frequently deviate from that ideal. When multiple modules, agencies, jurisdictions, or upstream sources contribute person-related data, the same real-world person can be represented as multiple near-duplicate records (variant spellings, inconsistent formats, missing identifiers, or stale addresses). Record-linkage theory explicitly assumes that record-generating processes introduce both **errors** and **incompleteness**, and that identical-appearing records can arise for different individuals while different-appearing records can arise for the same individual—exactly the conditions that drive entity-resolution needs. citeturn10view0turn10view1

A purely deterministic approach (e.g., “exact match on full name + DOB”) is often insufficient: when unique identifiers don’t exist, are missing, or contain errors, rigid rules can either create too many false links or miss true matches. The record-linkage literature describes these practical limitations and motivates probabilistic linkage as a way to combine multiple, imperfect signals into a likelihood that two records represent the same entity. citeturn8view1

This is why an ETL-layer identity consistency prototype—built on synthetic data—can be high-value even without any changes to core CAD/RMS products: it targets a widely observed integration pain point (identity fragmentation) at a layer where agencies and vendors commonly implement transformations, standardization, and reporting extracts. citeturn3view3turn6view1

## Architectural constraints, standards alignment, and compliance-safe boundaries

### Boundary conditions for a public, synthetic-only prototype

Your message’s safeguards map well to how criminal-justice information is expected to be protected in real deployments, while keeping this prototype entirely out-of-scope for handling sensitive operational data.

The entity["organization","Criminal Justice Information Services Division","federal program FBI CJIS"] security policy describes its purpose as protecting the full lifecycle of **criminal justice information (CJI)** (creation through dissemination, at rest or in transit) and sets minimum security requirements for entities accessing CJIS services and information. citeturn3view2turn2view3 It is also explicitly stated to be publicly distributable (version 5.0+ may be posted and shared without restriction), which makes it appropriate to cite and reference in your public documentation. citeturn15view0

Your repository should therefore make two things unambiguous:

- The prototype **does not ingest, store, transmit, or require access to** any CJI, production schemas, or agency-controlled infrastructure.
- All datasets are **synthetic from first principles** (not statistically de-identified copies derived from real agency data), to avoid re-identification risk and compliance entanglement.

Those boundaries should be “front page” content in the repo (README + SAFETY.md), because reviewers in public-safety software contexts often need explicit assurances about what is and is not being touched. citeturn15view0

### Data exchange and modeling alignment

Even though the repo is synthetic, it becomes more credible to public-safety stakeholders if terminology and objects resemble broadly used justice information exchange frameworks.

The entity["organization","National Information Exchange Model","justice data exchange standard"] describes its “core” as including widely shared concepts such as **person** and **location**, intended to be reused across domains. citeturn0search6 NIEM guidance emphasizes **harmonization**—choosing a single name/definition/structure for each data element and eliminating duplication—conceptually consistent with your “canonicalization + identity consistency” focus. citeturn0search2 NIEM’s naming and design rules also exist as a normative specification for building conformant models and schemas, which is useful background reference in the repo’s “Standards mapping” doc. citeturn0search30

For person-name modeling detail, NIEM’s public model documentation defines a person-name type as a structured combination of name components (prefix, given name, etc.), which can inform your synthetic schema (even if you don’t implement full NIEM XML). citeturn0search10

## Synthetic data model designed to reproduce ETL identity edge cases

A synthetic dataset for this project should do more than generate random people—it must generate **conflicts with intent**, so that normalization, probabilistic matching, and survivorship produce meaningful before/after quality deltas.

### Core entities and tables

Design the synthetic dataset around four broad tables (CSV + Parquet forms, to mirror typical ETL sources/targets):

- **person_source_records**: one row per person record per “source system” (synthetic CAD extract, synthetic RMS extract, synthetic jail module, etc.).
- **incident_records**: calls-for-service / incident stubs plus timestamps and locations (kept intentionally minimal).
- **incident_person_links**: many-to-many link table (incident_id ↔ person_source_record_id) with simple roles (victim, suspect, witness, reporting party).
- **address_history**: per-person historical addresses with effective start/end dates (to drive “most recent address wins” survivorship).

This structure mirrors the reality that CAD and RMS ecosystems exchange incident and person-associated data and that person identity appears in multiple modules and records. citeturn3view3turn5view1

### Canonical fields and normalization targets

Normalization should explicitly target fields where public-safety data frequently diverges across entry points:

- **Names**: case-folding, punctuation removal, token reordering (“Smith, John”), nickname mapping (Jon ↔ John), middle-name handling, and whitespace normalization.
- **Dates**: output in an unambiguous internet timestamp profile (e.g., RFC 3339 / ISO 8601 profile) to remove locale ambiguity in ETL outputs. RFC 3339 is explicitly defined as a timestamp format and a profile of ISO 8601 for date/time representation. citeturn14search3
- **Addresses**: normalization should support standard abbreviations and consistent formatting. The entity["organization","U.S. Postal Service","address standards US"] defines a “standardized address” as containing required elements and using USPS standard abbreviations, which is exactly what an ETL canonicalization layer attempts to achieve. citeturn14search10turn14search6
- **Phone numbers**: normalize to E.164-like canonical form using a well-known phone parsing/formatting library. entity["company","Google","libphonenumber publisher"]’s libphonenumber repository describes parsing, formatting, and validating international phone numbers (and documents its licensing), making it a defensible dependency choice. citeturn14search9

### Conflict generators (the “why this is realistic” part)

Build deterministic “conflict recipes” into your data generator so reviewers can trace how duplicates were created:

- **Orthographic variants**: “John A. Smith” vs “Jon Smith” vs “Smith, John” (token swap + nickname + middle initial).
- **Partial identifier collisions**: matching DOB + last name, but different address.
- **Address drift**: same person across multiple addresses; one source system stale, one current.
- **Transpositions**: month/day swap in DOB for a subset of records (common ETL ingestion error class).
- **Missingness patterns**: missing middle names, missing apartment/unit, missing phone digits.

These patterns are consistent with record linkage assumptions that errors/incompleteness arise during record generation and entry. citeturn10view0turn10view1

## Matching and “golden record” logic that stays explainable

### Why probabilistic linkage is the right center of gravity

The classic entity["people","Ivan P. Fellegi","record linkage researcher"]–entity["people","Alan B. Sunter","record linkage researcher"] framework formalizes record linkage by defining matched vs unmatched sets, comparison vectors, and linkage rules that consider uncertainty. citeturn10view0turn10view1 Modern summaries reiterate that probabilistic linkage is a response to missing/erroneous unique identifiers and that it combines evidence across multiple fields. citeturn8view1

A practical ETL-layer prototype should therefore:

- Support **exact-match paths** for high-confidence identifiers (e.g., a synthetic “state_id” or “booking_id” field).
- Use **fuzzy similarity** on names/addresses (string distances / token similarity).
- Compute **weighted confidence** (match probability / match weight).
- Enforce clear thresholds:
  - **auto-merge** above a high threshold
  - **manual review queue** in a mid-band
  - **no-link** below a low threshold

This structure mirrors how probabilistic linkage workflows split pairs into links, non-links, and possible links for review, rather than forcing a binary decision under uncertainty. citeturn10view1turn8view2

### Blocking / candidate generation

At scale, naïvely comparing all pairs is too expensive; record linkage practice uses “blocking passes” (multiple passes with different blocking keys) to reduce pair counts while controlling missed matches. The literature explicitly describes using multiple blocking passes (e.g., first pass on ZIP + year-of-birth, second pass on first+last) to manage tradeoffs. citeturn7view0turn8view2 The original Fellegi–Sunter paper also discusses examining alternative blocking procedures as part of linkage operations. citeturn10view2

For a prototype, implement 3–5 blocking rules, each easy to explain, such as:

- last_name_soundex + birth_year  
- postal_code + birth_date  
- first_initial + last_name + birth_month  

### Tooling choice for a public prototype

For a proof-of-concept that you intend to share with product and engineering stakeholders, choosing a recognizable open-source linkage engine strengthens credibility and reduces custom math you have to defend.

**Splink** is described in its documentation and packaging as a Python package for probabilistic record linkage/entity resolution that deduplicates and links records lacking unique identifiers, and it supports Python 3.9+. citeturn11search5turn11search8turn11search12 Its guidance contrasts probabilistic and deterministic linkage as “balance of evidence” across multiple features like name and DOB. citeturn1search13 Splink also provides guidance on evaluation and threshold selection, including clustering evaluation once a linkage threshold is chosen. citeturn1search33

For string similarity comparators in name/address scoring, RapidFuzz documents providing multiple string metrics (including Jaro-Winkler) and being MIT-licensed, which is relevant for a permissively licensed public repo. citeturn11search3turn11search7

### Survivorship (golden record) rules

The survivorship layer should be deterministic—stakeholders in operational public-safety contexts typically need to understand *why* a value was selected for the consolidated record.

Rule families to implement as configuration (YAML), not hard-coded logic:

- **Completeness**: prefer non-null over null; prefer full legal name over initial-only.
- **Freshness**: prefer most recent address by effective date.
- **Source priority**: allow “RMS > CAD > external intake” precedence for certain fields.
- **Validation status**: prefer fields that passed normalization/validation checks.
- **Traceability**: every chosen attribute must store provenance (which source record, what rule fired).

Even in a synthetic prototype, this makes your “golden record” defensible and auditable, and demonstrates how ETL outputs can become “trusted tables” for analytics/BI.

## Public GitHub repository blueprint

This section is a concrete repo design you can implement as a public repository and share with entity["company","ID Networks","public safety software vendor ashtabula oh"], whose own positioning includes public-safety and law-enforcement software (including CAD and RMS), making them a natural reviewer audience for this type of ETL-layer identity prototype. citeturn13search0

### Recommended repo name and one-sentence description

**Repo name:** `synthetic-identity-etl-public-safety`  
**Tagline:** “Synthetic ETL-layer identity resolution + data-quality rules engine prototype for CAD/RMS-style person & incident data.”

### Repository structure (proposed)

```text
synthetic-identity-etl-public-safety/
  README.md
  LICENSE
  CODE_OF_CONDUCT.md
  CONTRIBUTING.md
  SECURITY.md
  SAFETY.md
  CITATION.cff

  docs/
    architecture.md
    data-model.md
    normalization.md
    matching-and-thresholds.md
    survivorship.md
    evaluation-and-metrics.md
    standards-mapping.md
    threat-model.md
    faq.md

  diagrams/
    etl_pipeline.mmd
    entity_resolution_flow.mmd

  data/
    synthetic/
      README.md            # explains generation + guarantees "no real data"
      seeds/
      samples/
        small_demo/        # tiny dataset for quickstart + tests

  config/
    normalization_rules.yml
    blocking_rules.yml
    matching_model_splink.json
    thresholds.yml
    survivorship_rules.yml

  src/
    synthetic_identity_etl/
      __init__.py
      cli.py

      generate/
        synth_generator.py
        conflict_recipes.py
        schemas.py

      normalize/
        names.py
        addresses.py
        dates.py
        phones.py
        identifiers.py
        canonical.py

      link/
        blocking.py
        comparators.py
        splink_runner.py
        clustering.py
        audit_trail.py

      golden/
        survivorship.py
        provenance.py

      quality/
        dq_checks.py
        exception_reports.py
        metrics.py

      io/
        read_sources.py
        write_outputs.py

  notebooks/
    00_quickstart_demo.ipynb
    01_threshold_tuning.ipynb
    02_error_analysis.ipynb

  outputs/
    .gitkeep

  tests/
    test_generate.py
    test_normalize.py
    test_blocking.py
    test_scoring.py
    test_survivorship.py
    test_end_to_end.py

  .github/
    ISSUE_TEMPLATE/
      config.yml
      1-bug.yml
      2-feature.yml
      3-epic.yml
      4-docs.yml
    PULL_REQUEST_TEMPLATE.md
    workflows/
      ci.yml
      lint.yml
      security.yml
```

### Packaging and runtime choices

- **Python baseline**: 3.11 or 3.12 for the repo (while ensuring key dependencies remain compatible); if you adopt Splink, note that Splink states support for Python 3.9+. citeturn11search12
- **Execution model**: a single CLI entrypoint, e.g. `python -m synthetic_identity_etl ...`, that runs the pipeline end-to-end:
  1. generate synthetic sources  
  2. normalize  
  3. candidate generation  
  4. scoring/linkage  
  5. clustering  
  6. survivorship  
  7. outputs + exception reports  

### Repo governance artifacts that matter for public sharing

- **Issue templates**: implement GitHub Issue Forms (YAML) under `.github/ISSUE_TEMPLATE/`, and optionally configure the chooser via `config.yml`. citeturn12search0turn12search8  
- **Labels + milestones**: GitHub supports labels and milestones for categorization and tracking work. citeturn12search5turn12search1  
- **CI**: use GitHub Actions to build/test Python projects; GitHub provides a Python workflow guide, and the widely used `actions/setup-python` action installs Python (and can cache dependencies). citeturn12search2turn12search6

### Licensing and compliance posture for a public repo

For a public prototype with no proprietary dependencies, a permissive license is typical. If you want explicit patent grant language, **Apache 2.0** is often chosen; the Apache Software Foundation publishes the Apache License 2.0 text and identifies the SPDX short identifier as `Apache-2.0`. citeturn12search16turn12search7

Whichever license you choose, include the SPDX identifier in file headers where appropriate; the entity["organization","Linux Foundation","open source foundation"] highlights SPDX identifiers as an important best practice for compliance automation. citeturn12search32 The SPDX license list exists explicitly to provide standardized short identifiers and canonical references. citeturn12search3

## GitHub issues backlog for the full project

Below is a ready-to-use issue backlog, grouped by milestones. Each issue includes a suggested label set and acceptance criteria, so you can paste them into GitHub Issues with minimal editing.

### Milestones

- **M1 — Repo bootstrap & guardrails**
- **M2 — Synthetic data generator**
- **M3 — Normalization layer**
- **M4 — Probabilistic matching & clustering**
- **M5 — Survivorship & golden records**
- **M6 — QA, reporting, docs, and release**

### Labels to create up front

Use consistent prefixes so issues are easy to filter:

- `type:bug`, `type:feature`, `type:docs`, `type:chore`, `type:security`, `type:refactor`
- `area:generate`, `area:normalize`, `area:link`, `area:golden`, `area:quality`, `area:docs`, `area:ci`
- `prio:p0`, `prio:p1`, `prio:p2`
- `status:blocked`, `status:needs-review`, `status:ready`

(GitHub labels are explicitly intended for categorizing issues/PRs and can be managed per repository.) citeturn12search1turn12search5

### Issue set

**Issue: Create repository skeleton + baseline docs**  
Milestone: M1  
Labels: `type:chore`, `area:docs`, `prio:p0`  
Acceptance criteria: Repo contains README, LICENSE placeholder, CONTRIBUTING, CODE_OF_CONDUCT, SECURITY, SAFETY, docs/ + src/ + tests/ scaffolding; README states “synthetic-only” boundary clearly.

**Issue: Add SAFETY.md explaining “no CJI / no agency data” commitment**  
Milestone: M1  
Labels: `type:docs`, `type:security`, `area:docs`, `prio:p0`  
Acceptance criteria: SAFETY.md includes explicit non-goals (no production integrations, no real identities), and explains how synthetic data is generated from first principles.

**Issue: Add GitHub issue templates (YAML Issue Forms) + template chooser config**  
Milestone: M1  
Labels: `type:chore`, `area:ci`, `prio:p1`  
Acceptance criteria: `.github/ISSUE_TEMPLATE/config.yml` + 4 templates (bug, feature, epic, docs). Templates collect environment details, expected/actual behavior, and reproduction steps. (GitHub documents configuring templates via `.github/ISSUE_TEMPLATE` and `config.yml`.) citeturn12search0turn12search8

**Issue: Add GitHub Actions CI for tests + lint**  
Milestone: M1  
Labels: `type:chore`, `area:ci`, `prio:p1`  
Acceptance criteria: CI runs on PRs, installs Python, installs deps, runs unit tests and lint; badge added to README. (GitHub documents Python workflows; `actions/setup-python` installs Python.) citeturn12search2turn12search6

**Issue: Add pre-commit config (formatting, linting, secrets scanning)**  
Milestone: M1  
Labels: `type:chore`, `area:ci`, `prio:p2`  
Acceptance criteria: `.pre-commit-config.yaml` added; documented in CONTRIBUTING.

**Issue: Implement CLI entrypoint `synthetic_identity_etl`**  
Milestone: M1  
Labels: `type:feature`, `area:docs`, `prio:p1`  
Acceptance criteria: `--help` works; CLI supports `run-all`, `generate`, `normalize`, `link`, `golden`, `report`.

**Issue: Define synthetic schema (Person, Incident, Link, AddressHistory)**  
Milestone: M2  
Labels: `type:feature`, `area:generate`, `prio:p0`  
Acceptance criteria: `docs/data-model.md` describes tables, keys, required/optional fields; `schemas.py` defines dataclasses/pydantic models; includes “source_system” dimension.

**Issue: Implement deterministic seed-based synthetic generator for reproducibility**  
Milestone: M2  
Labels: `type:feature`, `area:generate`, `prio:p0`  
Acceptance criteria: Same seed yields identical outputs; generator supports sizes (small_demo, medium, large).

**Issue: Implement conflict recipe engine (name/address/DOB variants)**  
Milestone: M2  
Labels: `type:feature`, `area:generate`, `prio:p0`  
Acceptance criteria: Generator produces configurable % duplicates; each duplicate pair annotated with conflict type(s) for evaluation.

**Issue: Generate incident-to-person relationship records with role distributions**  
Milestone: M2  
Labels: `type:feature`, `area:generate`, `prio:p1`  
Acceptance criteria: Each incident links to 1–N persons; roles assigned; referential integrity validated.

**Issue: Produce “data dictionary” artifact from schema definitions**  
Milestone: M2  
Labels: `type:feature`, `area:docs`, `prio:p2`  
Acceptance criteria: Auto-generated markdown table listing each field, type, description.

**Issue: Add export formats: CSV + Parquet**  
Milestone: M2  
Labels: `type:feature`, `area:generate`, `prio:p1`  
Acceptance criteria: Outputs written to `data/synthetic/<run_id>/`; README explains formats.

**Issue: Implement name normalization (casefold, punctuation, token reorder)**  
Milestone: M3  
Labels: `type:feature`, `area:normalize`, `prio:p0`  
Acceptance criteria: “Smith, John A.” canonicalizes to structured tokens; nickname mapping supported; unit tests for common patterns.

**Issue: Implement date normalization to RFC 3339 timestamps**  
Milestone: M3  
Labels: `type:feature`, `area:normalize`, `prio:p1`  
Acceptance criteria: Multiple input formats normalize to a single canonical string; invalid dates flagged into exceptions. (RFC 3339 defines an ISO 8601 profile for timestamps.) citeturn14search3

**Issue: Implement address normalization module with USPS-style abbreviation mapping**  
Milestone: M3  
Labels: `type:feature`, `area:normalize`, `prio:p1`  
Acceptance criteria: Common street suffix/unit designators normalized; whitespace/punctuation consistent; unit tests include “St.” vs “Street” patterns. (USPS defines standardized addresses using USPS standard abbreviations.) citeturn14search10

**Issue: Implement phone normalization via libphonenumber wrapper (optional dependency)**  
Milestone: M3  
Labels: `type:feature`, `area:normalize`, `prio:p2`  
Acceptance criteria: If installed, canonicalize phone to E.164-like form; otherwise fallback to basic digit normalization. (libphonenumber parses/formats/validates international phone numbers.) citeturn14search9

**Issue: Create canonical “person_fingerprint” fields for blocking**  
Milestone: M3  
Labels: `type:feature`, `area:normalize`, `prio:p1`  
Acceptance criteria: Derived fields (soundex-like last name, birth_year, postal_code) created and stored; documented in normalization.md.

**Issue: Implement DQ checks (completeness, validity, referential integrity)**  
Milestone: M3  
Labels: `type:feature`, `area:quality`, `prio:p1`  
Acceptance criteria: DQ report summarizes missingness and invalid fields; exceptions emitted to `outputs/exceptions/`.

**Issue: Implement blocking rules engine (multi-pass)**  
Milestone: M4  
Labels: `type:feature`, `area:link`, `prio:p0`  
Acceptance criteria: At least 3 blocking passes; outputs candidate pair counts per pass. (Multiple blocking passes are described as standard practice for large linkage projects.) citeturn8view2turn10view2

**Issue: Integrate Splink for probabilistic linkage (primary engine)**  
Milestone: M4  
Labels: `type:feature`, `area:link`, `prio:p0`  
Acceptance criteria: Working Splink run on `small_demo`; config checked into `config/matching_model_splink.json`; produces match probabilities. (Splink is documented as probabilistic linkage for datasets lacking unique identifiers.) citeturn11search5turn11search12

**Issue: Implement string comparators using RapidFuzz (name/address similarity)**  
Milestone: M4  
Labels: `type:feature`, `area:link`, `prio:p1`  
Acceptance criteria: Jaro-Winkler / token ratios used in comparisons; unit tests validate comparator stability. (RapidFuzz provides multiple string metrics and is MIT-licensed.) citeturn11search3

**Issue: Implement clustering from pairwise matches (connected components)**  
Milestone: M4  
Labels: `type:feature`, `area:link`, `prio:p0`  
Acceptance criteria: Candidate pairs above threshold cluster into entity groups; cluster IDs stable and reproducible.

**Issue: Add threshold tuning notebook + evaluation outputs**  
Milestone: M4  
Labels: `type:docs`, `area:link`, `prio:p2`  
Acceptance criteria: Notebook shows how thresholds affect merge/review rates; exports charts to `outputs/`.

**Issue: Implement audit trail for every linked pair**  
Milestone: M4  
Labels: `type:feature`, `area:link`, `prio:p1`  
Acceptance criteria: Output includes which fields agreed/disagreed, weights or similarity scores, and final decision.

**Issue: Implement survivorship rules engine (config-driven)**  
Milestone: M5  
Labels: `type:feature`, `area:golden`, `prio:p0`  
Acceptance criteria: YAML-defined rules choose authoritative values; rule order deterministic; unit tests cover precedence.

**Issue: Generate golden person table + crosswalk table (source_id → golden_id)**  
Milestone: M5  
Labels: `type:feature`, `area:golden`, `prio:p0`  
Acceptance criteria: `golden_persons.parquet` + `person_crosswalk.parquet` produced; includes provenance fields.

**Issue: Implement address-history survivorship (“most recent wins” w/ tie-breakers)**  
Milestone: M5  
Labels: `type:feature`, `area:golden`, `prio:p1`  
Acceptance criteria: Uses effective dates; stable tie-breaker (source priority, completeness).

**Issue: Implement exception routing (manual review queue)**  
Milestone: M5  
Labels: `type:feature`, `area:quality`, `prio:p1`  
Acceptance criteria: Mid-confidence clusters routed to `review_queue.csv` with details for human review.

**Issue: Create before/after metrics (duplication rate, completeness, consistency)**  
Milestone: M6  
Labels: `type:feature`, `area:quality`, `prio:p0`  
Acceptance criteria: Metrics include baseline duplicates vs post-merge, % nulls reduced, # exceptions; summarized in markdown + CSV.

**Issue: Produce “implementation memo” doc for internal adaptation**  
Milestone: M6  
Labels: `type:docs`, `area:docs`, `prio:p1`  
Acceptance criteria: `docs/implementation_memo.md` explains how to swap synthetic sources for real ones without changing core logic (while warning about compliance and approvals).

**Issue: Add Mermaid architecture diagrams (ETL pipeline + linkage flow)**  
Milestone: M6  
Labels: `type:docs`, `area:docs`, `prio:p2`  
Acceptance criteria: `diagrams/*.mmd` render in GitHub; included in README and docs/architecture.md.

**Issue: Add “Standards mapping” doc (NIEM-inspired naming + rationale)**  
Milestone: M6  
Labels: `type:docs`, `area:docs`, `prio:p2`  
Acceptance criteria: `docs/standards-mapping.md` references NIEM “person/location” concepts and harmonization rationale. citeturn0search6turn0search2

**Issue: Prepare v0.1.0 public release checklist**  
Milestone: M6  
Labels: `type:chore`, `area:docs`, `prio:p1`  
Acceptance criteria: Release notes, reproducible demo command, sample outputs committed (small only), and a “known limitations” section.

**Issue: Security & privacy review for public repo (no secrets, no sensitive examples)**  
Milestone: M6  
Labels: `type:security`, `area:docs`, `prio:p0`  
Acceptance criteria: Confirm no credentials; confirm synthetic data is generated not derived; confirm SAFETY.md and SECURITY.md accurately describe scope; confirm no references that imply handling CJI. citeturn15view0

### Issue templates (what each YAML form should ask)

- **Bug**: steps to reproduce, expected vs actual, dataset size, commit hash, logs excerpt.
- **Feature**: problem statement, proposed behavior, config impacts, acceptance criteria.
- **Epic**: goal, non-goals, deliverables, risks, dependencies.
- **Docs**: page(s) impacted, what’s unclear, suggested wording.

(GitHub documents both issue templates and YAML issue forms syntax; keeping your forms structured dramatically improves triage when you share the repo publicly.) citeturn12search0turn12search8