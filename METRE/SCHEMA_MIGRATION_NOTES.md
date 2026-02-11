# METRE Schema Migration: MIMIC-IV 2.x → 3.1

## ⚠️ IMPORTANT: SKIPPED FEATURES

**The following features are SKIPPED in this extraction due to MIMIC-IV 3.1 schema changes:**

### 1. CULTURE DATA - NOT EXTRACTED
- **Missing table**: `mimiciv_derived.culture` (removed in v3.1)
- **Impact on output**: The following columns will be empty/zero:
  - `specimen_culture` (14 categorical culture site features)
  - `positive_culture` (binary: was culture positive?)
  - `has_sensitivity` (binary: was sensitivity testing done?)
  - `screen` (binary: was this a screening culture?)
- **Research impact**: Cannot analyze infection patterns, culture positivity rates, or antibiotic sensitivity data
- **Fix available**: Query `mimiciv_3_1_hosp.microbiologyevents` instead (see below)

### 2. HEPARIN DATA - NOT EXTRACTED  
- **Missing table**: `mimiciv_derived.heparin` (removed in v3.1)
- **Impact on output**: The `heparin` intervention column will always be 0
- **Research impact**: Cannot identify patients receiving heparin anticoagulation
- **Fix available**: Query `mimiciv_3_1_icu.inputevents` with heparin itemids (see below)

---

## Completed Fixes

| Old | New | Status |
|-----|-----|--------|
| `physionet-data.mimic_derived.*` | `physionet-data.mimiciv_3_1_derived.*` | ✅ Done |
| `physionet-data.mimic_icu.*` | `physionet-data.mimiciv_3_1_icu.*` | ✅ Done |
| `physionet-data.mimic_hosp.*` | `physionet-data.mimiciv_3_1_hosp.*` | ✅ Done |
| `physionet-data.mimic_core.*` | `physionet-data.mimiciv_3_1_hosp.*` | ✅ Done |
| `i.ethnicity` | `i.race` | ✅ Done |
| `df[col].iteritems()` | `df[col].items()` | ✅ Done |
| `bg.specimen` column | Excluded from numeric aggregation | ✅ Done |

---

## How to Restore Skipped Features

### Restoring Culture Data

Replace the stub in `extract_sql.py:query_culture_mimic()` with:

```sql
SELECT 
    m.subject_id,
    m.charttime,
    m.spec_type_desc as specimen,
    CASE WHEN m.test_name LIKE '%SCREEN%' THEN 1 ELSE 0 END as screen,
    CASE WHEN m.org_name IS NOT NULL AND m.org_name != 'NEGATIVE' THEN 1 ELSE 0 END as positive_culture,
    CASE WHEN m.interpretation IS NOT NULL THEN 1 ELSE 0 END as has_sensitivity,
    i.hadm_id,
    i.stay_id,
    i.icu_intime
FROM `physionet-data.mimiciv_3_1_hosp.microbiologyevents` m
INNER JOIN `physionet-data.mimiciv_3_1_derived.icustay_detail` i 
    ON i.subject_id = m.subject_id
WHERE m.subject_id IN ({icuids})
    AND m.charttime BETWEEN i.icu_intime AND i.icu_outtime
```

**Note**: The `specimen` to culture site mapping in `mimic_culturesite_map.json` may need updating for new specimen type descriptions.

### Restoring Heparin Data

Replace the stub in `extract_sql.py:query_heparin_mimic()` with:

```sql
SELECT 
    ie.subject_id,
    ie.starttime,
    ie.endtime,
    i.hadm_id,
    i.stay_id,
    i.icu_intime,
    i.icu_outtime
FROM `physionet-data.mimiciv_3_1_icu.inputevents` ie
INNER JOIN `physionet-data.mimiciv_3_1_derived.icustay_detail` i 
    ON i.stay_id = ie.stay_id
WHERE ie.itemid IN (225152, 225975, 229597, 230044)
    AND ie.amount > 0
    AND ie.subject_id IN ({ids})
    AND ie.starttime < i.icu_outtime
    AND ie.endtime > i.icu_intime
```

Heparin itemids from `d_items`:
- 225152: Heparin Sodium (Prophylaxis)
- 225975: Heparin Sodium  
- 229597: Heparin (IABP)
- 230044: Heparin Flush (10 units/ml) - may want to exclude flushes

---

## Tables Verified Working in v3.1

- ✅ sepsis3
- ✅ icustay_detail  
- ✅ bg (minus specimen column)
- ✅ blood_differential
- ✅ cardiac_marker
- ✅ coagulation
- ✅ complete_blood_count
- ✅ enzyme
- ✅ gcs
- ✅ inflammation
- ✅ urine_output_rate
- ✅ ventilation
- ✅ antibiotic
- ✅ vasoactive_agent
- ✅ charlson
- ✅ crrt
- ✅ icustays
- ✅ chartevents
- ✅ inputevents
- ✅ admissions
- ✅ labevents
- ✅ patients
