#!/usr/bin/env python3
"""
test_sql_eicu.py — Inventory test for eICU BigQuery tables

For each table referenced in the METRE eICU extraction pipeline (extract_sql.py),
this script queries the specific columns used by the METRE code with LIMIT 10.
Results are saved as CSVs in test_results/; errors are recorded.
A TABLE_INVENTORY_EICU.md summary is generated at the end.

Usage:
    python test_sql_eicu.py
"""

import argparse
import os
import sys
from datetime import datetime

from google.cloud import bigquery
import pandas as pd

from constants import PROJECT_ID


# ---------------------------------------------------------------------------
# Table probes: (table_fqn, [columns], source_function, notes)
#
# Columns are the specific columns referenced in METRE/extract_sql.py eICU
# queries — drawn from SELECT lists, JOINs, WHERE clauses, and downstream
# code that accesses specific column names after the query returns.
# ---------------------------------------------------------------------------

EICU_TABLE_PROBES = [
    # -----------------------------------------------------------------------
    # BASE TABLES (eicu_crd)
    # -----------------------------------------------------------------------
    (
        "eicu_crd.patient",
        [
            "patientunitstayid", "gender", "age", "ethnicity",
            "hospitaldischargestatus", "unitdischargeoffset",
            "hospitaladmitoffset", "hospitaldischargeoffset",
            "hospitaladmitsource", "unitdischargelocation",
            "unitdischargestatus", "hospitaldischargeyear", "hospitalid",
        ],
        "get_patient_group_eicu",
        "Core patient table — demographics, outcomes, LOS",
    ),
    (
        "eicu_crd.lab",
        [
            "patientunitstayid", "labname", "labresultoffset",
            "labresultrevisedoffset", "labresult",
        ],
        "query_bg_eicu / query_lab_eicu / query_labmakeup_eicu / get_group_id_eicu (ARF)",
        "Lab results — blood gas, chemistry, CBC, inflammation, etc.",
    ),
    (
        "eicu_crd.nursecharting",
        [
            "patientunitstayid", "nursingchartoffset",
            "nursingchartentryoffset", "nursingchartcelltypevallabel",
            "nursingchartcelltypevalname", "nursingchartvalue",
            "nursingchartcelltypecat",
        ],
        "query_vital_eicu",
        "Nurse charting — vitals (HR, BP, temp, SpO2, RR)",
    ),
    (
        "eicu_crd.microlab",
        [
            "patientunitstayid", "culturetakenoffset", "culturesite",
            "organism", "antibiotic", "sensitivitylevel",
        ],
        "query_microlab_eicu",
        "Microbiology — culture sites, organisms, sensitivity",
    ),
    (
        "eicu_crd.respiratorycare",
        [
            "patientunitstayid", "priorventstartoffset", "priorventendoffset",
        ],
        "query_vent_eicu / get_group_id_eicu (ARF)",
        "Respiratory care — ventilation start/end offsets",
    ),
    (
        "eicu_crd.respiratorycharting",
        [
            "patientunitstayid", "respchartoffset",
            "respchartvalue", "respchartvaluelabel",
        ],
        "query_tidalvol_eicu",
        "Respiratory charting — tidal volume observations",
    ),
    (
        "eicu_crd.vitalperiodic",
        ["patientunitstayid", "observationoffset", "cvp"],
        "query_cvp_eicu",
        "Periodic vitals — central venous pressure",
    ),
    (
        "eicu_crd.medication",
        [
            "patientunitstayid", "drugname", "drugordercancelled",
            "drugstartoffset", "drugstopoffset",
        ],
        "query_anti_eicu",
        "Medications — used for antibiotic extraction",
    ),
    (
        "eicu_crd.intakeoutput",
        ["patientunitstayid", "intakeoutputoffset", "cellpath"],
        "query_crrt/rbc/ffp/pll/colloid/crystalloid_eicu",
        "Intake/output — CRRT, transfusions, fluid boluses",
    ),
    (
        "eicu_crd.diagnosis",
        ["patientunitstayid", "icd9code"],
        "query_comorbidity_eicu / get_group_id_eicu (CHF, COPD)",
        "Diagnoses — ICD-9 codes for comorbidity and cohort selection",
    ),

    # -----------------------------------------------------------------------
    # DERIVED TABLES (eicu_crd_derived)
    # -----------------------------------------------------------------------
    (
        "eicu_crd_derived.icustay_detail",
        [
            "patientunitstayid", "unitdischargeoffset", "unitadmitoffset",
        ],
        "get_group_id_eicu / query_vent_eicu / query_med_eicu / query_anti_eicu / etc.",
        "ICU stay detail — used as JOIN anchor in most eICU queries",
    ),
    (
        "eicu_crd_derived.pivoted_med",
        [
            "patientunitstayid", "drugorderoffset", "drugstopoffset",
            "dopamine", "norepinephrine", "epinephrine",
            "vasopressin", "phenylephrine", "dobutamine",
            "milrinone", "heparin",
        ],
        "query_med_eicu / get_group_id_eicu (Shock)",
        "Pivoted medications — vasopressors, inotropes, heparin",
    ),
    (
        "eicu_crd_derived.pivoted_gcs",
        ["patientunitstayid", "chartoffset", "gcs"],
        "query_gcs_eicu",
        "Glasgow Coma Scale",
    ),
    (
        "eicu_crd_derived.pivoted_uo",
        ["patientunitstayid", "chartoffset", "urineoutput"],
        "query_uo_eicu",
        "Urine output",
    ),
    (
        "eicu_crd_derived.pivoted_weight",
        ["patientunitstayid", "chartoffset", "weight"],
        "query_weight_eicu",
        "Patient weight",
    ),
]


def probe_table(client, table_fqn, columns):
    """
    Run SELECT <columns> FROM <table> LIMIT 10.
    Returns (dataframe_or_None, error_message_or_None).
    """
    cols_str = ", ".join(columns)
    sql = f"SELECT {cols_str} FROM `physionet-data.{table_fqn}` LIMIT 10"
    try:
        df = client.query(sql).result().to_dataframe()
        return df, None
    except Exception as e:
        return None, str(e)


def main():
    parser = argparse.ArgumentParser(
        description="Probe eICU BigQuery tables referenced by METRE"
    )
    parser.add_argument(
        "--project_id",
        type=str,
        default=PROJECT_ID,
        help="BigQuery billing project ID",
    )
    args = parser.parse_args()

    os.environ["GOOGLE_CLOUD_PROJECT"] = args.project_id
    client = bigquery.Client(project=args.project_id)

    os.makedirs("test_results", exist_ok=True)

    results = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

    print(f"eICU Table Inventory Test — {timestamp}")
    print(f"Project: {args.project_id}")
    print(f"Probing {len(EICU_TABLE_PROBES)} tables...\n")

    for i, (table_fqn, columns, source_fn, notes) in enumerate(EICU_TABLE_PROBES, 1):
        short_name = table_fqn.replace(".", "__")
        print(f"[{i:2d}/{len(EICU_TABLE_PROBES)}] {table_fqn}")
        print(f"       Columns: {', '.join(columns)}")

        df, error = probe_table(client, table_fqn, columns)

        if error is None:
            csv_path = f"test_results/{short_name}.csv"
            df.to_csv(csv_path, index=False)
            status = "YES"
            row_count = len(df)
            col_list = list(df.columns)
            error_short = ""
            print(f"       -> OK ({row_count} rows, saved to {csv_path})\n")
        else:
            status = "NO"
            row_count = 0
            col_list = []
            if "Not found" in error:
                error_short = "Table not found"
            elif "not found inside" in error:
                error_short = "Column not found: " + error.split("Name ")[1].split(" not found")[0] if "Name " in error else error[:120]
            elif "Access Denied" in error:
                error_short = "Access Denied"
            else:
                error_short = error[:120]
            print(f"       -> FAILED: {error_short}\n")

        results.append({
            "table": table_fqn,
            "columns_requested": columns,
            "source_function": source_fn,
            "notes": notes,
            "status": status,
            "rows_returned": row_count,
            "columns_found": col_list,
            "error": error_short,
            "error_full": error or "",
        })

    # -----------------------------------------------------------------------
    # Write TABLE_INVENTORY_EICU.md
    # -----------------------------------------------------------------------
    n_ok = sum(1 for r in results if r["status"] == "YES")
    n_fail = sum(1 for r in results if r["status"] == "NO")

    lines = [
        f"# eICU Table Inventory",
        f"",
        f"**Generated:** {timestamp}  ",
        f"**Project:** `{args.project_id}`  ",
        f"**Result:** {n_ok} tables accessible, {n_fail} failed  ",
        f"",
        f"## Summary",
        f"",
        f"| # | Table | Accessible? | Columns Tested | Source Function | Error | Notes |",
        f"|---|-------|:---:|---|---|---|---|",
    ]

    for i, r in enumerate(results, 1):
        cols_str = ", ".join(r["columns_requested"])
        found = "YES" if r["status"] == "YES" else "NO"
        error_cell = r["error"] if r["error"] else "—"
        lines.append(
            f"| {i} | `{r['table']}` | {found} | {cols_str} | {r['source_function']} | {error_cell} | {r['notes']} |"
        )

    lines += [
        "",
        "## Details",
        "",
    ]

    for r in results:
        icon = "+" if r["status"] == "YES" else "x"
        lines.append(f"### [{icon}] `{r['table']}`")
        lines.append(f"")
        lines.append(f"- **Status:** {r['status']}")
        lines.append(f"- **Source:** `{r['source_function']}`")
        lines.append(f"- **Columns requested:** `{', '.join(r['columns_requested'])}`")
        if r["status"] == "YES":
            lines.append(f"- **Columns returned:** `{', '.join(r['columns_found'])}`")
            lines.append(f"- **Sample rows:** {r['rows_returned']} (see `test_results/`)")
        else:
            lines.append(f"- **Error:** {r['error_full']}")
        lines.append(f"- **Notes:** {r['notes']}")
        lines.append("")

    md_path = "TABLE_INVENTORY_EICU.md"
    with open(md_path, "w") as f:
        f.write("\n".join(lines))

    print("=" * 60)
    print(f"Done. {n_ok} OK, {n_fail} FAILED.")
    print(f"  CSVs:     test_results/")
    print(f"  Report:   {md_path}")


if __name__ == "__main__":
    main()
