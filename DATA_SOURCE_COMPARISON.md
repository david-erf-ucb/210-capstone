# Data Source Comparison: MIMIC-IV 3.1 vs eICU

This document maps each information type to its source tables and join keys in both MIMIC-IV 3.1 and eICU, based on the METRE extraction pipeline (`METRE/extract_sql.py` and `METRE/extract_database.py`).

Use this as a reference when combining datasets (TODO item 4) or building new queries.

## Key Differences

- **Patient ID**: MIMIC uses `subject_id` / `hadm_id` / `stay_id`; eICU uses `patientunitstayid` only.
- **Time representation**: MIMIC uses absolute timestamps (`charttime`, `starttime`); eICU uses minute offsets from ICU admission (`chartoffset`, `labresultoffset`, etc.).
- **Race/Ethnicity**: MIMIC column is `race`; eICU column is `ethnicity`.
- **Sepsis-3**: MIMIC has a derived `sepsis3` table; eICU relies on a pre-computed CSV (`resources/eicu_sepsis_3_id.csv`).
- **Culture data**: MIMIC's `mimic_derived.culture` was removed in v3.1 (replacement: `microbiologyevents`); eICU has `eicu_crd.microlab`.
- **Heparin**: MIMIC's `mimic_derived.heparin` was removed in v3.1 (replacement: query `inputevents`); eICU gets heparin from `pivoted_med`.

## Source Mapping

| Information Type | MIMIC Table(s) | MIMIC Join Key | eICU Table(s) | eICU Join Key | Notes |
|---|---|---|---|---|---|
| **Patient cohort / demographics** | `mimiciv_3_1_derived.icustay_detail` | `stay_id`, `hadm_id`, `subject_id` | `eicu_crd.patient` | `patientunitstayid` | MIMIC also joins `admissions` for insurance, deathtime, etc. eICU has mortality derived inline via `hospitaldischargestatus` / `unitdischargestatus`. |
| **Readmission (30-day)** | `mimiciv_3_1_icu.icustays` (self-join) | `stay_id`, `subject_id` | N/A | — | eICU pipeline does not compute 30-day readmission. |
| **Blood gas** | `mimiciv_3_1_derived.bg` | `subject_id` -> `icustay_detail` | `eicu_crd.lab` (filtered by labname: paO2, paCO2, pH, FiO2, anion gap, Base Excess, PEEP) | `patientunitstayid` | eICU pivots lab rows into columns in the query itself. |
| **Vitals** | `mimiciv_3_1_icu.chartevents` (custom pivot query) | `stay_id` -> `icustay_detail` | `eicu_crd.nursecharting` (custom pivot query) | `patientunitstayid` | Both build HR, BP, RR, SpO2, temp from raw charted values. eICU has invasive + non-invasive BP separated. |
| **Blood differential** | `mimiciv_3_1_derived.blood_differential` | `subject_id` -> `icustay_detail` | `eicu_crd.lab` (part of `query_lab_eicu`: bands, basos, eos, lymphs, monos, polys) | `patientunitstayid` | MIMIC has a dedicated derived table; eICU extracts from general lab table. |
| **Cardiac markers** | `mimiciv_3_1_derived.cardiac_marker` | `subject_id` -> `icustay_detail` | `eicu_crd.lab` (part of `query_lab_eicu`: troponin-T, CPK-MB) | `patientunitstayid` | MIMIC has dedicated derived table; eICU extracts from general lab. |
| **Chemistry (BUN, creatinine, etc.)** | `mimiciv_3_1_hosp.labevents` (custom pivot by itemid) | `subject_id` -> `icustay_detail` | `eicu_crd.lab` (part of `query_lab_eicu`: albumin, BUN, calcium, chloride, creatinine, glucose, bicarbonate, sodium, potassium, etc.) | `patientunitstayid` | MIMIC uses `itemid` codes; eICU uses `labname` strings. |
| **Coagulation** | `mimiciv_3_1_derived.coagulation` | `subject_id` -> `icustay_detail` | `eicu_crd.lab` (part of `query_lab_eicu`: PT-INR, PTT, fibrinogen, PT) | `patientunitstayid` | MIMIC has dedicated derived table. |
| **Complete blood count** | `mimiciv_3_1_derived.complete_blood_count` | `subject_id` -> `icustay_detail` | `eicu_crd.lab` (part of `query_lab_eicu`: WBC, Hct, Hgb, platelets, MCH, MCHC, MCV, RBC, RDW) | `patientunitstayid` | MIMIC has dedicated derived table. |
| **Culture / microbiology** | `mimic_derived.culture` (REMOVED in 3.1); replacement: `mimiciv_3_1_hosp.microbiologyevents` | `subject_id` -> `icustay_detail` | `eicu_crd.microlab` | `patientunitstayid` | MIMIC culture is currently STUBBED — needs restoration. eICU microlab is functional. |
| **Enzymes (ALT, AST, ALP, etc.)** | `mimiciv_3_1_derived.enzyme` | `subject_id` -> `icustay_detail` | `eicu_crd.lab` (part of `query_lab_eicu`: ALT, AST, alkaline phos., amylase, CPK) | `patientunitstayid` | MIMIC has dedicated derived table. |
| **GCS** | `mimiciv_3_1_derived.gcs` | `stay_id` -> `icustay_detail` | `eicu_crd_derived.pivoted_gcs` | `patientunitstayid` | Both have dedicated GCS tables. |
| **Inflammation (CRP)** | `mimiciv_3_1_derived.inflammation` | `subject_id` -> `icustay_detail` | `eicu_crd.lab` (part of `query_lab_eicu`: CRP) | `patientunitstayid` | MIMIC has dedicated derived table. |
| **Urine output** | `mimiciv_3_1_derived.urine_output_rate` | `stay_id` -> `icustay_detail` | `eicu_crd_derived.pivoted_uo` | `patientunitstayid` | Both have dedicated UO tables. |
| **Weight** | N/A (not extracted separately for MIMIC) | — | `eicu_crd_derived.pivoted_weight` | `patientunitstayid` | eICU extracts weight as a time series; MIMIC gets weight via `urine_output_rate`. |
| **CVP** | N/A (not extracted separately for MIMIC) | — | `eicu_crd.vitalperiodic` | `patientunitstayid` | CVP only extracted for eICU. |
| **Additional chart/lab items** | `mimiciv_3_1_icu.chartevents` + `mimiciv_3_1_hosp.labevents` (using item lists from `resources/`) | `stay_id`, `hadm_id` | `eicu_crd.lab` (`query_labmakeup_eicu`: urinary creatinine, magnesium, phosphate, WBC urine) | `patientunitstayid` | MIMIC uses itemid-based chart/lab extraction; eICU uses labname-based extraction for a smaller set. |
| **Tidal volume** | N/A (not extracted separately for MIMIC) | — | `eicu_crd.respiratorycharting` | `patientunitstayid` | Only extracted for eICU. |
| **Ventilation** | `mimiciv_3_1_derived.ventilation` | `stay_id` -> `icustay_detail` | `eicu_crd.respiratorycare` + `eicu_crd_derived.icustay_detail` | `patientunitstayid` | MIMIC has a derived table with start/end times; eICU computes from `priorventstartoffset` / `priorventendoffset`. |
| **Antibiotics** | `mimiciv_3_1_derived.antibiotic` | `stay_id` -> `icustay_detail` | `eicu_crd.medication` (filtered by long regex drug name list) | `patientunitstayid` | MIMIC has a clean derived table; eICU requires pattern-matching across drug names. |
| **Vasopressors / inotropes** | `mimiciv_3_1_derived.vasoactive_agent` | `stay_id` -> `icustay_detail` | `eicu_crd_derived.pivoted_med` | `patientunitstayid` | Same drugs in both: dopamine, epinephrine, norepinephrine, phenylephrine, vasopressin, dobutamine, milrinone. |
| **Heparin** | `mimic_derived.heparin` (REMOVED in 3.1); replacement: query `mimiciv_3_1_icu.inputevents` by itemid | `stay_id` | `eicu_crd_derived.pivoted_med` (heparin column) | `patientunitstayid` | MIMIC heparin is currently STUBBED — needs restoration via inputevents. eICU heparin is functional via pivoted_med. |
| **CRRT** | `mimiciv_3_1_derived.crrt` | `stay_id` -> `icustay_detail` | `eicu_crd.intakeoutput` (filtered by `cellpath` containing "crrt") | `patientunitstayid` | MIMIC has a derived table; eICU uses pattern matching on I/O records. |
| **RBC transfusion** | `mimiciv_3_1_icu.inputevents` (itemids: 225168, 226368, 227070) | `stay_id` -> `icustay_detail` | `eicu_crd.intakeoutput` (filtered by `cellpath` containing "rbc" or "red blood cell") | `patientunitstayid` | Both use item/pattern filtering on I/O tables. |
| **Platelet transfusion** | `mimiciv_3_1_icu.inputevents` (itemids: 225170, 226369, 227071) | `stay_id` -> `icustay_detail` | `eicu_crd.intakeoutput` (filtered by `cellpath` containing "platelet") | `patientunitstayid` | Same approach as RBC. |
| **FFP transfusion** | `mimiciv_3_1_icu.inputevents` (itemids: 220970, 226367, 227072) | `stay_id` -> `icustay_detail` | `eicu_crd.intakeoutput` (filtered by `cellpath` containing "plasma" or "ffp") | `patientunitstayid` | Same approach as RBC. |
| **Colloid bolus** | `mimiciv_3_1_icu.inputevents` (itemids: albumin, hetastarch, dextran) | `stay_id` -> `icustay_detail` | `eicu_crd.intakeoutput` (filtered by `cellpath` containing "colloid") | `patientunitstayid` | MIMIC has rate-based filtering; eICU uses simple pattern match. |
| **Crystalloid bolus** | `mimiciv_3_1_icu.inputevents` (itemids: NaCl, LR, D5, etc.) | `stay_id` -> `icustay_detail` | `eicu_crd.intakeoutput` (filtered by `cellpath` containing "crystalloid") | `patientunitstayid` | MIMIC has rate-based filtering; eICU uses simple pattern match. |
| **Comorbidities (Charlson)** | `mimiciv_3_1_derived.charlson` | `hadm_id` -> `icustay_detail` | `eicu_crd.diagnosis` (ICD-9 code-based Charlson calculation in SQL) | `patientunitstayid` | MIMIC has a pre-computed Charlson table; eICU computes Charlson from raw ICD-9 codes inline. |
| **Anchor year / time context** | `mimiciv_3_1_hosp.patients` (`anchor_year`, `anchor_year_group`) | `subject_id` -> `icustay_detail` | `eicu_crd.patient` (`hospitaldischargeyear`) | `patientunitstayid` | MIMIC uses anchor year for de-identification; eICU has actual discharge year. |
