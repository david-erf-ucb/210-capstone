"""Pre-fetch all eICU BigQuery queries into cache without pandas processing."""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
from google.cloud import bigquery
from extract_database import cached_query, get_patient_group_eicu
from extract_sql import (
    query_bg_eicu, query_lab_eicu, query_vital_eicu, query_microlab_eicu,
    query_gcs_eicu, query_uo_eicu, query_weight_eicu, query_cvp_eicu,
    query_labmakeup_eicu, query_tidalvol_eicu, query_vent_eicu,
    query_med_eicu, query_anti_eicu, query_crrt_eicu,
    query_rbc_trans_eicu, query_ffp_trans_eicu, query_pll_trans_eicu,
    query_colloid_eicu, query_crystalloid_eicu, query_comorbidity_eicu,
)

CACHE_DIR = './cache/eICU_Generic/raw'
client = bigquery.Client()

# Load patient to get icuids_to_keep
class Args:
    database = 'eICU'
    patient_group = 'Generic'
    age_min = 18
    los_min = 4
    los_max = 240
    time_window = 60

args = Args()
patient = cached_query(CACHE_DIR, 'patient', get_patient_group_eicu, args, client)
young_age = [str(i) for i in range(args.age_min)]
patient = patient.loc[~patient.loc[:, 'age'].isin(young_age)]
icuids_to_keep = set([str(s) for s in patient['patientunitstayid']])
tw_in_min = args.time_window

queries = [
    ('bg', query_bg_eicu, [client, icuids_to_keep]),
    ('lab', query_lab_eicu, [client, icuids_to_keep]),
    ('vital', query_vital_eicu, [client, icuids_to_keep]),
    ('microlab', query_microlab_eicu, [client, icuids_to_keep]),
    ('gcs', query_gcs_eicu, [client, icuids_to_keep]),
    ('uo', query_uo_eicu, [client, icuids_to_keep]),
    ('weight', query_weight_eicu, [client, icuids_to_keep]),
    ('cvp', query_cvp_eicu, [client, icuids_to_keep]),
    ('labmakeup', query_labmakeup_eicu, [client, icuids_to_keep]),
    ('tidal_vol', query_tidalvol_eicu, [client, icuids_to_keep]),
    ('vent', query_vent_eicu, [client, icuids_to_keep, tw_in_min]),
    ('antibiotics', query_anti_eicu, [client, icuids_to_keep, tw_in_min]),
    ('crrt', query_crrt_eicu, [client, icuids_to_keep, tw_in_min]),
    ('rbc_trans', query_rbc_trans_eicu, [client, icuids_to_keep, tw_in_min]),
    ('ffp_trans', query_ffp_trans_eicu, [client, icuids_to_keep, tw_in_min]),
    ('pll_trans', query_pll_trans_eicu, [client, icuids_to_keep, tw_in_min]),
    ('colloid', query_colloid_eicu, [client, icuids_to_keep, tw_in_min]),
    ('crystalloid', query_crystalloid_eicu, [client, icuids_to_keep, tw_in_min]),
    ('comorbidity', query_comorbidity_eicu, [client, icuids_to_keep]),
]

med_names = ['dopamine', 'epinephrine', 'norepinephrine', 'phenylephrine',
             'vasopressin', 'dobutamine', 'milrinone', 'heparin']
for m in med_names:
    queries.append((f'med_{m}', query_med_eicu, [client, icuids_to_keep, m, tw_in_min]))

print(f"Will fetch {len(queries)} queries (skipping cached ones)\n")
for name, fn, fn_args in queries:
    cached_query(CACHE_DIR, name, fn, *fn_args)

print("\nAll eICU queries cached!")
