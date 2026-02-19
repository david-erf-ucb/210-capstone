# MIMIC-IV 3.1 Table Inventory

**Generated:** 2026-02-11 12:02  
**Project:** `icu-project-486401`  
**Result:** 23 tables accessible, 2 failed  

## Summary

| # | Table | Found in 3.1? | Columns Tested | Source Function | Error | Notes |
|---|-------|:---:|---|---|---|---|
| 1 | `mimiciv_3_1_derived.sepsis3` | YES | stay_id | get_group_id (sepsis_3) | — | Used to identify sepsis-3 cohort stay IDs |
| 2 | `mimiciv_3_1_derived.icustay_detail` | YES | subject_id, hadm_id, stay_id, gender, admission_age, race, hospital_expire_flag, hospstay_seq, icustay_seq, los_icu, admittime, dischtime, icu_intime, icu_outtime | get_patient_group | — | Core cohort table — used in nearly every query as a JOIN anchor |
| 3 | `mimiciv_3_1_derived.bg` | YES | subject_id, charttime, aado2_calc, specimen | query_bg_mimic | — | Blood gas. Query uses b.* then drops aado2_calc and specimen |
| 4 | `mimiciv_3_1_derived.blood_differential` | YES | subject_id, charttime, specimen_id | query_blood_diff_mimic | — | Query uses b.* ; specimen_id dropped downstream |
| 5 | `mimiciv_3_1_derived.cardiac_marker` | YES | subject_id, charttime, specimen_id, troponin_t | query_cardiac_marker_mimic | — | Query uses b.* ; troponin_t cast to numeric, specimen_id dropped |
| 6 | `mimiciv_3_1_derived.coagulation` | YES | subject_id, charttime, specimen_id | query_coagulation_mimic | — | Query uses b.* ; specimen_id dropped downstream |
| 7 | `mimiciv_3_1_derived.complete_blood_count` | YES | subject_id, charttime, specimen_id, hematocrit, hemoglobin, wbc | query_cbc_mimic | — | Query uses b.* ; hematocrit/hemoglobin renamed, wbc dropped |
| 8 | `mimiciv_3_1_derived.enzyme` | YES | subject_id, charttime, specimen_id, ck_mb | query_enzyme_mimic | — | Query uses b.* ; ck_mb dropped downstream |
| 9 | `mimiciv_3_1_derived.gcs` | YES | subject_id, stay_id, charttime, gcs | query_gcs_mimic | — | Glasgow Coma Scale — explicit column SELECT |
| 10 | `mimiciv_3_1_derived.inflammation` | YES | subject_id, hadm_id, charttime, crp | query_inflammation_mimic | — | CRP inflammation marker — explicit column SELECT |
| 11 | `mimiciv_3_1_derived.urine_output_rate` | YES | stay_id, charttime, weight, uo | query_uo_mimic | — | Urine output rate — explicit column SELECT |
| 12 | `mimiciv_3_1_derived.ventilation` | YES | stay_id, starttime, endtime | query_vent_mimic / get_group_id (ARF) | — | Ventilation episodes |
| 13 | `mimiciv_3_1_derived.antibiotic` | YES | stay_id, starttime, stoptime, antibiotic, route | query_antibiotics_mimic | — | Antibiotic administration |
| 14 | `mimiciv_3_1_derived.vasoactive_agent` | YES | stay_id, starttime, endtime, norepinephrine, epinephrine, dopamine, vasopressin, phenylephrine, dobutamine, milrinone | query_vasoactive_mimic / get_group_id (Shock) | — | Vasopressor/inotrope administration |
| 15 | `mimiciv_3_1_derived.charlson` | YES | subject_id, hadm_id, myocardial_infarct, congestive_heart_failure, peripheral_vascular_disease, cerebrovascular_disease, dementia, chronic_pulmonary_disease, rheumatic_disease, peptic_ulcer_disease, mild_liver_disease, diabetes_without_cc, diabetes_with_cc, paraplegia, renal_disease, malignant_cancer, severe_liver_disease, metastatic_solid_tumor, aids | query_comorbidity_mimic / get_group_id (CHF, COPD) | — | Charlson comorbidity index components |
| 16 | `mimiciv_3_1_derived.crrt` | YES | stay_id, charttime | query_crrt_mimic | — | Continuous renal replacement therapy |
| 17 | `mimiciv_3_1_icu.chartevents` | YES | subject_id, stay_id, charttime, itemid, value, valuenum, valueuom | query_vitals_mimic / query_chart_lab_mimic / get_group_id (ARF) | — | Chart events — vitals, flowsheet data |
| 18 | `mimiciv_3_1_icu.icustays` | YES | subject_id, hadm_id, stay_id, intime, outtime | get_patient_group (readmission subquery) | — | ICU stay times — used for readmission_30 calculation |
| 19 | `mimiciv_3_1_icu.inputevents` | YES | stay_id, starttime, endtime, itemid, amount, amountuom, rateuom, rate, patientweight, statusdescription | query_rbc/pll/ffp_trans_mimic, query_colloid/crystalloid_mimic | — | Input events — transfusions, colloids, crystalloids, (heparin fix) |
| 20 | `mimiciv_3_1_hosp.admissions` | YES | hadm_id, admission_type, insurance, deathtime, discharge_location | get_patient_group | — | Hospital admissions — demographics and outcomes |
| 21 | `mimiciv_3_1_hosp.labevents` | YES | subject_id, hadm_id, charttime, specimen_id, itemid, value, valueuom, valuenum | query_chemistry_mimic / query_chart_lab_mimic / get_group_id (ARF) | — | Lab results |
| 22 | `mimiciv_3_1_hosp.patients` | YES | subject_id, anchor_year, anchor_year_group | query_anchor_year_mimic | — | Patient demographics — anchor year for date shifting |
| 23 | `mimic_derived.culture` | NO | subject_id, charttime | query_culture_mimic (STUBBED) | 403 Access Denied: Table physionet-data:mimic_derived.culture: User does not have permission to query table physionet-da | REMOVED in 3.1 — culture table no longer exists |
| 24 | `mimic_derived.heparin` | NO | subject_id, starttime, endtime | query_heparin_mimic (STUBBED) | 403 Access Denied: Table physionet-data:mimic_derived.heparin: User does not have permission to query table physionet-da | REMOVED in 3.1 — heparin table no longer exists |
| 25 | `mimiciv_3_1_hosp.microbiologyevents` | YES | subject_id, charttime, spec_type_desc, org_name, test_name, interpretation | (proposed replacement for culture) | — | Could replace mimic_derived.culture — see SCHEMA_MIGRATION_NOTES.md |

## Details

### [+] `mimiciv_3_1_derived.sepsis3`

- **Status:** YES
- **Source:** `get_group_id (sepsis_3)`
- **Columns requested:** `stay_id`
- **Columns returned:** `stay_id`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Used to identify sepsis-3 cohort stay IDs

### [+] `mimiciv_3_1_derived.icustay_detail`

- **Status:** YES
- **Source:** `get_patient_group`
- **Columns requested:** `subject_id, hadm_id, stay_id, gender, admission_age, race, hospital_expire_flag, hospstay_seq, icustay_seq, los_icu, admittime, dischtime, icu_intime, icu_outtime`
- **Columns returned:** `subject_id, hadm_id, stay_id, gender, admission_age, race, hospital_expire_flag, hospstay_seq, icustay_seq, los_icu, admittime, dischtime, icu_intime, icu_outtime`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Core cohort table — used in nearly every query as a JOIN anchor

### [+] `mimiciv_3_1_derived.bg`

- **Status:** YES
- **Source:** `query_bg_mimic`
- **Columns requested:** `subject_id, charttime, aado2_calc, specimen`
- **Columns returned:** `subject_id, charttime, aado2_calc, specimen`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Blood gas. Query uses b.* then drops aado2_calc and specimen

### [+] `mimiciv_3_1_derived.blood_differential`

- **Status:** YES
- **Source:** `query_blood_diff_mimic`
- **Columns requested:** `subject_id, charttime, specimen_id`
- **Columns returned:** `subject_id, charttime, specimen_id`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Query uses b.* ; specimen_id dropped downstream

### [+] `mimiciv_3_1_derived.cardiac_marker`

- **Status:** YES
- **Source:** `query_cardiac_marker_mimic`
- **Columns requested:** `subject_id, charttime, specimen_id, troponin_t`
- **Columns returned:** `subject_id, charttime, specimen_id, troponin_t`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Query uses b.* ; troponin_t cast to numeric, specimen_id dropped

### [+] `mimiciv_3_1_derived.coagulation`

- **Status:** YES
- **Source:** `query_coagulation_mimic`
- **Columns requested:** `subject_id, charttime, specimen_id`
- **Columns returned:** `subject_id, charttime, specimen_id`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Query uses b.* ; specimen_id dropped downstream

### [+] `mimiciv_3_1_derived.complete_blood_count`

- **Status:** YES
- **Source:** `query_cbc_mimic`
- **Columns requested:** `subject_id, charttime, specimen_id, hematocrit, hemoglobin, wbc`
- **Columns returned:** `subject_id, charttime, specimen_id, hematocrit, hemoglobin, wbc`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Query uses b.* ; hematocrit/hemoglobin renamed, wbc dropped

### [+] `mimiciv_3_1_derived.enzyme`

- **Status:** YES
- **Source:** `query_enzyme_mimic`
- **Columns requested:** `subject_id, charttime, specimen_id, ck_mb`
- **Columns returned:** `subject_id, charttime, specimen_id, ck_mb`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Query uses b.* ; ck_mb dropped downstream

### [+] `mimiciv_3_1_derived.gcs`

- **Status:** YES
- **Source:** `query_gcs_mimic`
- **Columns requested:** `subject_id, stay_id, charttime, gcs`
- **Columns returned:** `subject_id, stay_id, charttime, gcs`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Glasgow Coma Scale — explicit column SELECT

### [+] `mimiciv_3_1_derived.inflammation`

- **Status:** YES
- **Source:** `query_inflammation_mimic`
- **Columns requested:** `subject_id, hadm_id, charttime, crp`
- **Columns returned:** `subject_id, hadm_id, charttime, crp`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** CRP inflammation marker — explicit column SELECT

### [+] `mimiciv_3_1_derived.urine_output_rate`

- **Status:** YES
- **Source:** `query_uo_mimic`
- **Columns requested:** `stay_id, charttime, weight, uo`
- **Columns returned:** `stay_id, charttime, weight, uo`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Urine output rate — explicit column SELECT

### [+] `mimiciv_3_1_derived.ventilation`

- **Status:** YES
- **Source:** `query_vent_mimic / get_group_id (ARF)`
- **Columns requested:** `stay_id, starttime, endtime`
- **Columns returned:** `stay_id, starttime, endtime`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Ventilation episodes

### [+] `mimiciv_3_1_derived.antibiotic`

- **Status:** YES
- **Source:** `query_antibiotics_mimic`
- **Columns requested:** `stay_id, starttime, stoptime, antibiotic, route`
- **Columns returned:** `stay_id, starttime, stoptime, antibiotic, route`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Antibiotic administration

### [+] `mimiciv_3_1_derived.vasoactive_agent`

- **Status:** YES
- **Source:** `query_vasoactive_mimic / get_group_id (Shock)`
- **Columns requested:** `stay_id, starttime, endtime, norepinephrine, epinephrine, dopamine, vasopressin, phenylephrine, dobutamine, milrinone`
- **Columns returned:** `stay_id, starttime, endtime, norepinephrine, epinephrine, dopamine, vasopressin, phenylephrine, dobutamine, milrinone`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Vasopressor/inotrope administration

### [+] `mimiciv_3_1_derived.charlson`

- **Status:** YES
- **Source:** `query_comorbidity_mimic / get_group_id (CHF, COPD)`
- **Columns requested:** `subject_id, hadm_id, myocardial_infarct, congestive_heart_failure, peripheral_vascular_disease, cerebrovascular_disease, dementia, chronic_pulmonary_disease, rheumatic_disease, peptic_ulcer_disease, mild_liver_disease, diabetes_without_cc, diabetes_with_cc, paraplegia, renal_disease, malignant_cancer, severe_liver_disease, metastatic_solid_tumor, aids`
- **Columns returned:** `subject_id, hadm_id, myocardial_infarct, congestive_heart_failure, peripheral_vascular_disease, cerebrovascular_disease, dementia, chronic_pulmonary_disease, rheumatic_disease, peptic_ulcer_disease, mild_liver_disease, diabetes_without_cc, diabetes_with_cc, paraplegia, renal_disease, malignant_cancer, severe_liver_disease, metastatic_solid_tumor, aids`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Charlson comorbidity index components

### [+] `mimiciv_3_1_derived.crrt`

- **Status:** YES
- **Source:** `query_crrt_mimic`
- **Columns requested:** `stay_id, charttime`
- **Columns returned:** `stay_id, charttime`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Continuous renal replacement therapy

### [+] `mimiciv_3_1_icu.chartevents`

- **Status:** YES
- **Source:** `query_vitals_mimic / query_chart_lab_mimic / get_group_id (ARF)`
- **Columns requested:** `subject_id, stay_id, charttime, itemid, value, valuenum, valueuom`
- **Columns returned:** `subject_id, stay_id, charttime, itemid, value, valuenum, valueuom`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Chart events — vitals, flowsheet data

### [+] `mimiciv_3_1_icu.icustays`

- **Status:** YES
- **Source:** `get_patient_group (readmission subquery)`
- **Columns requested:** `subject_id, hadm_id, stay_id, intime, outtime`
- **Columns returned:** `subject_id, hadm_id, stay_id, intime, outtime`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** ICU stay times — used for readmission_30 calculation

### [+] `mimiciv_3_1_icu.inputevents`

- **Status:** YES
- **Source:** `query_rbc/pll/ffp_trans_mimic, query_colloid/crystalloid_mimic`
- **Columns requested:** `stay_id, starttime, endtime, itemid, amount, amountuom, rateuom, rate, patientweight, statusdescription`
- **Columns returned:** `stay_id, starttime, endtime, itemid, amount, amountuom, rateuom, rate, patientweight, statusdescription`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Input events — transfusions, colloids, crystalloids, (heparin fix)

### [+] `mimiciv_3_1_hosp.admissions`

- **Status:** YES
- **Source:** `get_patient_group`
- **Columns requested:** `hadm_id, admission_type, insurance, deathtime, discharge_location`
- **Columns returned:** `hadm_id, admission_type, insurance, deathtime, discharge_location`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Hospital admissions — demographics and outcomes

### [+] `mimiciv_3_1_hosp.labevents`

- **Status:** YES
- **Source:** `query_chemistry_mimic / query_chart_lab_mimic / get_group_id (ARF)`
- **Columns requested:** `subject_id, hadm_id, charttime, specimen_id, itemid, value, valueuom, valuenum`
- **Columns returned:** `subject_id, hadm_id, charttime, specimen_id, itemid, value, valueuom, valuenum`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Lab results

### [+] `mimiciv_3_1_hosp.patients`

- **Status:** YES
- **Source:** `query_anchor_year_mimic`
- **Columns requested:** `subject_id, anchor_year, anchor_year_group`
- **Columns returned:** `subject_id, anchor_year, anchor_year_group`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Patient demographics — anchor year for date shifting

### [x] `mimic_derived.culture`

- **Status:** NO
- **Source:** `query_culture_mimic (STUBBED)`
- **Columns requested:** `subject_id, charttime`
- **Error:** 403 Access Denied: Table physionet-data:mimic_derived.culture: User does not have permission to query table physionet-data:mimic_derived.culture, or perhaps it does not exist.; reason: accessDenied, message: Access Denied: Table physionet-data:mimic_derived.culture: User does not have permission to query table physionet-data:mimic_derived.culture, or perhaps it does not exist.

Location: US
Job ID: 1d9a9b14-b4eb-4198-9fca-229ecd302b95

- **Notes:** REMOVED in 3.1 — culture table no longer exists

### [x] `mimic_derived.heparin`

- **Status:** NO
- **Source:** `query_heparin_mimic (STUBBED)`
- **Columns requested:** `subject_id, starttime, endtime`
- **Error:** 403 Access Denied: Table physionet-data:mimic_derived.heparin: User does not have permission to query table physionet-data:mimic_derived.heparin, or perhaps it does not exist.; reason: accessDenied, message: Access Denied: Table physionet-data:mimic_derived.heparin: User does not have permission to query table physionet-data:mimic_derived.heparin, or perhaps it does not exist.

Location: US
Job ID: adac2bf2-2415-40ff-954c-d9dce0269964

- **Notes:** REMOVED in 3.1 — heparin table no longer exists

### [+] `mimiciv_3_1_hosp.microbiologyevents`

- **Status:** YES
- **Source:** `(proposed replacement for culture)`
- **Columns requested:** `subject_id, charttime, spec_type_desc, org_name, test_name, interpretation`
- **Columns returned:** `subject_id, charttime, spec_type_desc, org_name, test_name, interpretation`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Could replace mimic_derived.culture — see SCHEMA_MIGRATION_NOTES.md
