#!/usr/bin/env python3
"""
test_sql_mimic.py — Inventory test for MIMIC-IV 3.1 BigQuery tables

For each table referenced in the METRE extraction pipeline (extract_sql.py),
this script queries the specific columns used by the METRE code with LIMIT 10.
Results are saved as CSVs in test_results/; errors (missing tables/columns)
are recorded. A TABLE_INVENTORY.md summary is generated at the end.

Usage:
    python test_sql_mimic.py --project_id <your-project-id>
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
# Columns are the specific columns referenced in METRE/extract_sql.py queries
# for each table — drawn from SELECT lists, JOINs, WHERE clauses, and
# downstream code that accesses specific column names after the query returns.
# ---------------------------------------------------------------------------

MIMIC_TABLE_PROBES = [
    # -----------------------------------------------------------------------
    # DERIVED TABLES
    # -----------------------------------------------------------------------
    (
        "mimiciv_3_1_derived.sepsis3",
        ["stay_id"],
        "get_group_id (sepsis_3)",
        "Used to identify sepsis-3 cohort stay IDs",
    ),
    (
        "mimiciv_3_1_derived.icustay_detail",
        [
            "subject_id", "hadm_id", "stay_id", "gender", "admission_age",
            "race", "hospital_expire_flag", "hospstay_seq", "icustay_seq",
            "los_icu", "admittime", "dischtime", "icu_intime", "icu_outtime",
        ],
        "get_patient_group",
        "Core cohort table — used in nearly every query as a JOIN anchor",
    ),
    (
        "mimiciv_3_1_derived.bg",
        ["subject_id", "charttime", "aado2_calc", "specimen"],
        "query_bg_mimic",
        "Blood gas. Query uses b.* then drops aado2_calc and specimen",
    ),
    (
        "mimiciv_3_1_derived.blood_differential",
        ["subject_id", "charttime", "specimen_id"],
        "query_blood_diff_mimic",
        "Query uses b.* ; specimen_id dropped downstream",
    ),
    (
        "mimiciv_3_1_derived.cardiac_marker",
        ["subject_id", "charttime", "specimen_id", "troponin_t"],
        "query_cardiac_marker_mimic",
        "Query uses b.* ; troponin_t cast to numeric, specimen_id dropped",
    ),
    (
        "mimiciv_3_1_derived.coagulation",
        ["subject_id", "charttime", "specimen_id"],
        "query_coagulation_mimic",
        "Query uses b.* ; specimen_id dropped downstream",
    ),
    (
        "mimiciv_3_1_derived.complete_blood_count",
        ["subject_id", "charttime", "specimen_id", "hematocrit", "hemoglobin", "wbc"],
        "query_cbc_mimic",
        "Query uses b.* ; hematocrit/hemoglobin renamed, wbc dropped",
    ),
    (
        "mimiciv_3_1_derived.enzyme",
        ["subject_id", "charttime", "specimen_id", "ck_mb"],
        "query_enzyme_mimic",
        "Query uses b.* ; ck_mb dropped downstream",
    ),
    (
        "mimiciv_3_1_derived.gcs",
        ["subject_id", "stay_id", "charttime", "gcs"],
        "query_gcs_mimic",
        "Glasgow Coma Scale — explicit column SELECT",
    ),
    (
        "mimiciv_3_1_derived.inflammation",
        ["subject_id", "hadm_id", "charttime", "crp"],
        "query_inflammation_mimic",
        "CRP inflammation marker — explicit column SELECT",
    ),
    (
        "mimiciv_3_1_derived.urine_output_rate",
        ["stay_id", "charttime", "weight", "uo"],
        "query_uo_mimic",
        "Urine output rate — explicit column SELECT",
    ),
    (
        "mimiciv_3_1_derived.ventilation",
        ["stay_id", "starttime", "endtime"],
        "query_vent_mimic / get_group_id (ARF)",
        "Ventilation episodes",
    ),
    (
        "mimiciv_3_1_derived.antibiotic",
        ["stay_id", "starttime", "stoptime", "antibiotic", "route"],
        "query_antibiotics_mimic",
        "Antibiotic administration",
    ),
    (
        "mimiciv_3_1_derived.vasoactive_agent",
        [
            "stay_id", "starttime", "endtime",
            "norepinephrine", "epinephrine", "dopamine",
            "vasopressin", "phenylephrine", "dobutamine", "milrinone",
        ],
        "query_vasoactive_mimic / get_group_id (Shock)",
        "Vasopressor/inotrope administration",
    ),
    (
        "mimiciv_3_1_derived.charlson",
        [
            "subject_id", "hadm_id",
            "myocardial_infarct", "congestive_heart_failure",
            "peripheral_vascular_disease", "cerebrovascular_disease",
            "dementia", "chronic_pulmonary_disease", "rheumatic_disease",
            "peptic_ulcer_disease", "mild_liver_disease",
            "diabetes_without_cc", "diabetes_with_cc", "paraplegia",
            "renal_disease", "malignant_cancer", "severe_liver_disease",
            "metastatic_solid_tumor", "aids",
        ],
        "query_comorbidity_mimic / get_group_id (CHF, COPD)",
        "Charlson comorbidity index components",
    ),
    (
        "mimiciv_3_1_derived.crrt",
        ["stay_id", "charttime"],
        "query_crrt_mimic",
        "Continuous renal replacement therapy",
    ),

    # -----------------------------------------------------------------------
    # ICU TABLES
    # -----------------------------------------------------------------------
    (
        "mimiciv_3_1_icu.chartevents",
        ["subject_id", "stay_id", "charttime", "itemid", "value", "valuenum", "valueuom"],
        "query_vitals_mimic / query_chart_lab_mimic / get_group_id (ARF)",
        "Chart events — vitals, flowsheet data",
    ),
    (
        "mimiciv_3_1_icu.icustays",
        ["subject_id", "hadm_id", "stay_id", "intime", "outtime"],
        "get_patient_group (readmission subquery)",
        "ICU stay times — used for readmission_30 calculation",
    ),
    (
        "mimiciv_3_1_icu.inputevents",
        [
            "stay_id", "starttime", "endtime", "itemid",
            "amount", "amountuom", "rateuom", "rate",
            "patientweight", "statusdescription",
        ],
        "query_rbc/pll/ffp_trans_mimic, query_colloid/crystalloid_mimic",
        "Input events — transfusions, colloids, crystalloids, (heparin fix)",
    ),

    # -----------------------------------------------------------------------
    # HOSP TABLES
    # -----------------------------------------------------------------------
    (
        "mimiciv_3_1_hosp.admissions",
        ["hadm_id", "admission_type", "insurance", "deathtime", "discharge_location"],
        "get_patient_group",
        "Hospital admissions — demographics and outcomes",
    ),
    (
        "mimiciv_3_1_hosp.labevents",
        ["subject_id", "hadm_id", "charttime", "specimen_id", "itemid", "value", "valueuom", "valuenum"],
        "query_chemistry_mimic / query_chart_lab_mimic / get_group_id (ARF)",
        "Lab results",
    ),
    (
        "mimiciv_3_1_hosp.patients",
        ["subject_id", "anchor_year", "anchor_year_group"],
        "query_anchor_year_mimic",
        "Patient demographics — anchor year for date shifting",
    ),

    # -----------------------------------------------------------------------
    # KNOWN-MISSING (old pre-3.1 schema) — expect these to FAIL
    # -----------------------------------------------------------------------
    (
        "mimic_derived.culture",
        ["subject_id", "charttime"],
        "query_culture_mimic (STUBBED)",
        "REMOVED in 3.1 — culture table no longer exists",
    ),
    (
        "mimic_derived.heparin",
        ["subject_id", "starttime", "endtime"],
        "query_heparin_mimic (STUBBED)",
        "REMOVED in 3.1 — heparin table no longer exists",
    ),

    # -----------------------------------------------------------------------
    # POTENTIAL REPLACEMENT TABLE (for culture)
    # -----------------------------------------------------------------------
    (
        "mimiciv_3_1_hosp.microbiologyevents",
        ["subject_id", "charttime", "spec_type_desc", "org_name", "test_name", "interpretation"],
        "(proposed replacement for culture)",
        "Could replace mimic_derived.culture — see SCHEMA_MIGRATION_NOTES.md",
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
        description="Probe MIMIC-IV 3.1 BigQuery tables referenced by METRE"
    )
    parser.add_argument(
        "--project_id",
        type=str,
        default=PROJECT_ID,
        help="BigQuery billing project ID",
    )
    args = parser.parse_args()

    # Authenticate
    os.environ["GOOGLE_CLOUD_PROJECT"] = args.project_id
    client = bigquery.Client(project=args.project_id)

    # Create output directory
    os.makedirs("test_results", exist_ok=True)

    results = []  # list of dicts for the markdown report
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

    print(f"MIMIC-IV 3.1 Table Inventory Test — {timestamp}")
    print(f"Project: {args.project_id}")
    print(f"Probing {len(MIMIC_TABLE_PROBES)} tables...\n")

    for i, (table_fqn, columns, source_fn, notes) in enumerate(MIMIC_TABLE_PROBES, 1):
        short_name = table_fqn.replace(".", "__")
        print(f"[{i:2d}/{len(MIMIC_TABLE_PROBES)}] {table_fqn}")
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
            # Shorten the error for the markdown table
            if "Not found" in error:
                error_short = "Table not found"
            elif "Unrecognized name" in error:
                # Extract the column name from the error
                error_short = error.split("Unrecognized name:")[0] + "Column error"
                if "Unrecognized name:" in error:
                    error_short = "Column not found: " + error.split("Unrecognized name:")[1].split(";")[0].strip()
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
    # Write TABLE_INVENTORY.md
    # -----------------------------------------------------------------------
    n_ok = sum(1 for r in results if r["status"] == "YES")
    n_fail = sum(1 for r in results if r["status"] == "NO")

    lines = [
        f"# MIMIC-IV 3.1 Table Inventory",
        f"",
        f"**Generated:** {timestamp}  ",
        f"**Project:** `{args.project_id}`  ",
        f"**Result:** {n_ok} tables accessible, {n_fail} failed  ",
        f"",
        f"## Summary",
        f"",
        f"| # | Table | Found in 3.1? | Columns Tested | Source Function | Error | Notes |",
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

    md_path = "TABLE_INVENTORY.md"
    with open(md_path, "w") as f:
        f.write("\n".join(lines))

    print("=" * 60)
    print(f"Done. {n_ok} OK, {n_fail} FAILED.")
    print(f"  CSVs:     test_results/")
    print(f"  Report:   {md_path}")


if __name__ == "__main__":
    main()
