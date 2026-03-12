# Pipeline Architecture

## Objective

This prototype models an ETL identity-resolution workflow for multi-source operational data. The goal is to ingest inconsistent person records, normalize them, identify likely duplicates, and produce trusted golden records plus an exception queue for manual review.

## Processing Stages

### 1. Source Ingestion
Input datasets arrive from multiple source systems such as CAD, RMS, booking, citation, or reporting platforms. In this prototype, all sources are synthetic CSV files.

Example inputs:
- `person_source_a.csv`
- `person_source_b.csv`
- `person_source_c.csv`

### 2. Normalization Layer
Raw source data is standardized into canonical comparison formats.

Typical operations:
- trim whitespace
- standardize date values
- strip punctuation from names
- normalize address suffixes
- reduce phone values to digits-only formats

Output:
- `normalized_person_records`

### 3. Candidate Generation
The pipeline reduces comparison cost by generating duplicate candidates using blocking keys.

Example blocking keys:
- date of birth
- state
- phonetic last-name bucket

Output:
- candidate record pairs eligible for scoring

### 4. Matching and Scoring
Candidate pairs are scored using a weighted ruleset.

Signals may include:
- exact DOB match
- name similarity
- phonetic last-name match
- address similarity
- phone match
- external reference match

Output:
- match score
- rule trace / explainability metadata
- merge decision or review recommendation

### 5. Survivorship / Golden Record Layer
For records selected for merge, survivorship rules determine which fields become authoritative.

Example:
- full legal name wins over abbreviated name
- canonical ISO DOB wins over ambiguous format
- most recent normalized address wins over shorter variant

Output:
- `golden_person_records`

### 6. Exception and Audit Outputs
Borderline or conflicting cases are routed to manual review.

Outputs:
- exception queue
- audit log
- summary metrics

## High-Level Flow

```text
Synthetic Sources
    ↓
Ingestion
    ↓
Normalization
    ↓
Candidate Generation
    ↓
Weighted Match Scoring
    ↓
Merge / Review Decision
    ↓
Survivorship Rules
    ↓
Golden Records + Exception Queue + Audit Log
```

## Suggested Initial Repo Layout

```text
ETL-Identity-Data-Ruleset-Engine/
├── README.md
├── data/
│   └── synthetic_sources/
├── docs/
│   └── pipeline_architecture.md
├── rules/
│   └── identity_matching_rules.yaml
└── scripts/
    └── generate_synthetic_data.py
```

## Minimum Demo Workflow

1. Generate synthetic source records.
2. Normalize the records into canonical comparison fields.
3. Build candidate pairs using DOB + phonetic surname blocking.
4. Score pairs using weighted matching rules.
5. Route records into one of three outcomes:
   - auto-merge
   - manual review
   - no-match
6. Produce golden records and an audit report.

## Why This Matters

This design is intentionally constrained and explainable. It is suited to ETL-heavy environments where teams need data-quality gains without rewriting upstream systems, and where traceability matters as much as accuracy.
