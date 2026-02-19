# eICU Table Inventory

**Generated:** 2026-02-12 13:34  
**Project:** `icu-project-486401`  
**Result:** 15 tables accessible, 0 failed  

## Summary

| # | Table | Accessible? | Columns Tested | Source Function | Error | Notes |
|---|-------|:---:|---|---|---|---|
| 1 | `eicu_crd.patient` | YES | patientunitstayid, gender, age, ethnicity, hospitaldischargestatus, unitdischargeoffset, hospitaladmitoffset, hospitaldischargeoffset, hospitaladmitsource, unitdischargelocation, unitdischargestatus, hospitaldischargeyear, hospitalid | get_patient_group_eicu | — | Core patient table — demographics, outcomes, LOS |
| 2 | `eicu_crd.lab` | YES | patientunitstayid, labname, labresultoffset, labresultrevisedoffset, labresult | query_bg_eicu / query_lab_eicu / query_labmakeup_eicu / get_group_id_eicu (ARF) | — | Lab results — blood gas, chemistry, CBC, inflammation, etc. |
| 3 | `eicu_crd.nursecharting` | YES | patientunitstayid, nursingchartoffset, nursingchartentryoffset, nursingchartcelltypevallabel, nursingchartcelltypevalname, nursingchartvalue, nursingchartcelltypecat | query_vital_eicu | — | Nurse charting — vitals (HR, BP, temp, SpO2, RR) |
| 4 | `eicu_crd.microlab` | YES | patientunitstayid, culturetakenoffset, culturesite, organism, antibiotic, sensitivitylevel | query_microlab_eicu | — | Microbiology — culture sites, organisms, sensitivity |
| 5 | `eicu_crd.respiratorycare` | YES | patientunitstayid, priorventstartoffset, priorventendoffset | query_vent_eicu / get_group_id_eicu (ARF) | — | Respiratory care — ventilation start/end offsets |
| 6 | `eicu_crd.respiratorycharting` | YES | patientunitstayid, respchartoffset, respchartvalue, respchartvaluelabel | query_tidalvol_eicu | — | Respiratory charting — tidal volume observations |
| 7 | `eicu_crd.vitalperiodic` | YES | patientunitstayid, observationoffset, cvp | query_cvp_eicu | — | Periodic vitals — central venous pressure |
| 8 | `eicu_crd.medication` | YES | patientunitstayid, drugname, drugordercancelled, drugstartoffset, drugstopoffset | query_anti_eicu | — | Medications — used for antibiotic extraction |
| 9 | `eicu_crd.intakeoutput` | YES | patientunitstayid, intakeoutputoffset, cellpath | query_crrt/rbc/ffp/pll/colloid/crystalloid_eicu | — | Intake/output — CRRT, transfusions, fluid boluses |
| 10 | `eicu_crd.diagnosis` | YES | patientunitstayid, icd9code | query_comorbidity_eicu / get_group_id_eicu (CHF, COPD) | — | Diagnoses — ICD-9 codes for comorbidity and cohort selection |
| 11 | `eicu_crd_derived.icustay_detail` | YES | patientunitstayid, unitdischargeoffset, unitadmitoffset | get_group_id_eicu / query_vent_eicu / query_med_eicu / query_anti_eicu / etc. | — | ICU stay detail — used as JOIN anchor in most eICU queries |
| 12 | `eicu_crd_derived.pivoted_med` | YES | patientunitstayid, drugorderoffset, drugstopoffset, dopamine, norepinephrine, epinephrine, vasopressin, phenylephrine, dobutamine, milrinone, heparin | query_med_eicu / get_group_id_eicu (Shock) | — | Pivoted medications — vasopressors, inotropes, heparin |
| 13 | `eicu_crd_derived.pivoted_gcs` | YES | patientunitstayid, chartoffset, gcs | query_gcs_eicu | — | Glasgow Coma Scale |
| 14 | `eicu_crd_derived.pivoted_uo` | YES | patientunitstayid, chartoffset, urineoutput | query_uo_eicu | — | Urine output |
| 15 | `eicu_crd_derived.pivoted_weight` | YES | patientunitstayid, chartoffset, weight | query_weight_eicu | — | Patient weight |

## Details

### [+] `eicu_crd.patient`

- **Status:** YES
- **Source:** `get_patient_group_eicu`
- **Columns requested:** `patientunitstayid, gender, age, ethnicity, hospitaldischargestatus, unitdischargeoffset, hospitaladmitoffset, hospitaldischargeoffset, hospitaladmitsource, unitdischargelocation, unitdischargestatus, hospitaldischargeyear, hospitalid`
- **Columns returned:** `patientunitstayid, gender, age, ethnicity, hospitaldischargestatus, unitdischargeoffset, hospitaladmitoffset, hospitaldischargeoffset, hospitaladmitsource, unitdischargelocation, unitdischargestatus, hospitaldischargeyear, hospitalid`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Core patient table — demographics, outcomes, LOS

### [+] `eicu_crd.lab`

- **Status:** YES
- **Source:** `query_bg_eicu / query_lab_eicu / query_labmakeup_eicu / get_group_id_eicu (ARF)`
- **Columns requested:** `patientunitstayid, labname, labresultoffset, labresultrevisedoffset, labresult`
- **Columns returned:** `patientunitstayid, labname, labresultoffset, labresultrevisedoffset, labresult`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Lab results — blood gas, chemistry, CBC, inflammation, etc.

### [+] `eicu_crd.nursecharting`

- **Status:** YES
- **Source:** `query_vital_eicu`
- **Columns requested:** `patientunitstayid, nursingchartoffset, nursingchartentryoffset, nursingchartcelltypevallabel, nursingchartcelltypevalname, nursingchartvalue, nursingchartcelltypecat`
- **Columns returned:** `patientunitstayid, nursingchartoffset, nursingchartentryoffset, nursingchartcelltypevallabel, nursingchartcelltypevalname, nursingchartvalue, nursingchartcelltypecat`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Nurse charting — vitals (HR, BP, temp, SpO2, RR)

### [+] `eicu_crd.microlab`

- **Status:** YES
- **Source:** `query_microlab_eicu`
- **Columns requested:** `patientunitstayid, culturetakenoffset, culturesite, organism, antibiotic, sensitivitylevel`
- **Columns returned:** `patientunitstayid, culturetakenoffset, culturesite, organism, antibiotic, sensitivitylevel`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Microbiology — culture sites, organisms, sensitivity

### [+] `eicu_crd.respiratorycare`

- **Status:** YES
- **Source:** `query_vent_eicu / get_group_id_eicu (ARF)`
- **Columns requested:** `patientunitstayid, priorventstartoffset, priorventendoffset`
- **Columns returned:** `patientunitstayid, priorventstartoffset, priorventendoffset`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Respiratory care — ventilation start/end offsets

### [+] `eicu_crd.respiratorycharting`

- **Status:** YES
- **Source:** `query_tidalvol_eicu`
- **Columns requested:** `patientunitstayid, respchartoffset, respchartvalue, respchartvaluelabel`
- **Columns returned:** `patientunitstayid, respchartoffset, respchartvalue, respchartvaluelabel`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Respiratory charting — tidal volume observations

### [+] `eicu_crd.vitalperiodic`

- **Status:** YES
- **Source:** `query_cvp_eicu`
- **Columns requested:** `patientunitstayid, observationoffset, cvp`
- **Columns returned:** `patientunitstayid, observationoffset, cvp`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Periodic vitals — central venous pressure

### [+] `eicu_crd.medication`

- **Status:** YES
- **Source:** `query_anti_eicu`
- **Columns requested:** `patientunitstayid, drugname, drugordercancelled, drugstartoffset, drugstopoffset`
- **Columns returned:** `patientunitstayid, drugname, drugordercancelled, drugstartoffset, drugstopoffset`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Medications — used for antibiotic extraction

### [+] `eicu_crd.intakeoutput`

- **Status:** YES
- **Source:** `query_crrt/rbc/ffp/pll/colloid/crystalloid_eicu`
- **Columns requested:** `patientunitstayid, intakeoutputoffset, cellpath`
- **Columns returned:** `patientunitstayid, intakeoutputoffset, cellpath`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Intake/output — CRRT, transfusions, fluid boluses

### [+] `eicu_crd.diagnosis`

- **Status:** YES
- **Source:** `query_comorbidity_eicu / get_group_id_eicu (CHF, COPD)`
- **Columns requested:** `patientunitstayid, icd9code`
- **Columns returned:** `patientunitstayid, icd9code`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Diagnoses — ICD-9 codes for comorbidity and cohort selection

### [+] `eicu_crd_derived.icustay_detail`

- **Status:** YES
- **Source:** `get_group_id_eicu / query_vent_eicu / query_med_eicu / query_anti_eicu / etc.`
- **Columns requested:** `patientunitstayid, unitdischargeoffset, unitadmitoffset`
- **Columns returned:** `patientunitstayid, unitdischargeoffset, unitadmitoffset`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** ICU stay detail — used as JOIN anchor in most eICU queries

### [+] `eicu_crd_derived.pivoted_med`

- **Status:** YES
- **Source:** `query_med_eicu / get_group_id_eicu (Shock)`
- **Columns requested:** `patientunitstayid, drugorderoffset, drugstopoffset, dopamine, norepinephrine, epinephrine, vasopressin, phenylephrine, dobutamine, milrinone, heparin`
- **Columns returned:** `patientunitstayid, drugorderoffset, drugstopoffset, dopamine, norepinephrine, epinephrine, vasopressin, phenylephrine, dobutamine, milrinone, heparin`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Pivoted medications — vasopressors, inotropes, heparin

### [+] `eicu_crd_derived.pivoted_gcs`

- **Status:** YES
- **Source:** `query_gcs_eicu`
- **Columns requested:** `patientunitstayid, chartoffset, gcs`
- **Columns returned:** `patientunitstayid, chartoffset, gcs`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Glasgow Coma Scale

### [+] `eicu_crd_derived.pivoted_uo`

- **Status:** YES
- **Source:** `query_uo_eicu`
- **Columns requested:** `patientunitstayid, chartoffset, urineoutput`
- **Columns returned:** `patientunitstayid, chartoffset, urineoutput`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Urine output

### [+] `eicu_crd_derived.pivoted_weight`

- **Status:** YES
- **Source:** `query_weight_eicu`
- **Columns requested:** `patientunitstayid, chartoffset, weight`
- **Columns returned:** `patientunitstayid, chartoffset, weight`
- **Sample rows:** 10 (see `test_results/`)
- **Notes:** Patient weight
