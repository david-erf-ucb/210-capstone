# MIMIC / eICU Project — Scoping TODO

## 0. Access & Setup
- [ ] Make sure Bilal has access to the BigQuery project
- [ ] Set up GitHub repository

## 1. eICU Data Connection
- [ ] Get eICU data connected via BigQuery
- [ ] Begin extracting eICU data and perform any necessary updates
- [ ] Validate eICU extraction against existing METRE JSON configs (`json_files/eicu_*`) to ensure column ordering and outlier thresholds carry over

## 2. MIMIC Data Quality
- [ ] Explore anomalies in MIMIC data — e.g. high ventilation rates (may require some research)
- [ ] Pick 5 "case studies" to look into deeply and potentially show the class

## 3. MIMIC v3.1 Updates
- [ ] Finish remaining updates for MIMIC v3.1
- [ ] Restore culture data (currently stubbed out) — see `METRE/SCHEMA_MIGRATION_NOTES.md` for query template using `microbiologyevents`
- [ ] Restore heparin data (currently stubbed out) — see `METRE/SCHEMA_MIGRATION_NOTES.md` for query template using `inputevents`

## 4. Combining Datasets
- [ ] Look into combining eICU and MIMIC data (variable mapping, schema alignment, etc.)

## 5. Panel Data
- [ ] Create panel data for combined data source (or one of them individually)

## 6. Modeling Prep
- [ ] Decide on training and holdout datasets
- [ ] Define evaluation metrics and baseline model approach (existing model code in `METRE/training/` to build on)

## 7. Front-End Dashboard
- [ ] Build a front-end for displaying model results
- [ ] Incorporate real-time elements to simulate how the dashboard would update with new observations
