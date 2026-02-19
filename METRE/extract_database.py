# Set up Google big query
from google.cloud import bigquery
import os
import json
import pickle
import numpy as np
import pandas as pd
from extraction_utils import *
from extract_sql import *

# Note: For local execution, authenticate via:
#   gcloud auth application-default login
# The BigQuery client will automatically use these credentials.


# ---------------------------------------------------------------------------
# Caching helpers -- query BigQuery once, store results as parquet
# ---------------------------------------------------------------------------

def cached_query(cache_dir, name, query_fn, *args, force=False, **kwargs):
    """Run *query_fn* and cache the resulting DataFrame as parquet.

    On subsequent calls the parquet file is loaded instead of re-querying
    BigQuery, unless *force* is True.
    """
    path = os.path.join(cache_dir, f"{name}.parquet")
    if not force and os.path.exists(path):
        print(f"  [CACHE HIT]  {name}  <-  {path}")
        return pd.read_parquet(path)
    print(f"  [QUERYING]   {name}  from BigQuery ...")
    df = query_fn(*args, **kwargs)
    os.makedirs(cache_dir, exist_ok=True)
    df.to_parquet(path)
    print(f"  [CACHED]     {name}  ->  {path}")
    return df


def _save_params(cache_dir, args):
    """Persist the extraction parameters so we can detect mismatches later."""
    params = {
        'database': args.database,
        'patient_group': args.patient_group,
        'age_min': args.age_min,
        'los_min': args.los_min,
        'los_max': args.los_max,
        'time_window': args.time_window,
    }
    os.makedirs(cache_dir, exist_ok=True)
    path = os.path.join(cache_dir, '_params.json')
    with open(path, 'w') as f:
        json.dump(params, f, indent=2)


def _check_params(cache_dir, args):
    """Warn (but don't block) if cached data was generated with different params."""
    path = os.path.join(cache_dir, '_params.json')
    if not os.path.exists(path):
        return
    with open(path) as f:
        cached = json.load(f)
    current = {
        'database': args.database,
        'patient_group': args.patient_group,
        'age_min': args.age_min,
        'los_min': args.los_min,
        'los_max': args.los_max,
        'time_window': args.time_window,
    }
    if cached != current:
        print("WARNING: Cached data was generated with different parameters!")
        print(f"  Cached:  {cached}")
        print(f"  Current: {current}")
        print("  Use --force_query to re-extract, or delete the cache directory.\n")


def extract_mimic(args):
    os.environ["GOOGLE_CLOUD_PROJECT"] = args.project_id
    client = bigquery.Client(project=args.project_id)
    # MIMIC-IV id
    ID_COLS = ['subject_id', 'hadm_id', 'stay_id']
    # datatime format to hour
    to_hours = lambda x: max(0, x.days * 24//args.time_window + x.seconds // (3600 * args.time_window))

    # --- cache setup ---
    raw_dir = os.path.join(args.cache_dir, f"MIMIC_{args.patient_group}", "raw")
    force = args.force_query
    _check_params(os.path.join(args.cache_dir, f"MIMIC_{args.patient_group}"), args)
    _save_params(os.path.join(args.cache_dir, f"MIMIC_{args.patient_group}"), args)

    # get group id, could be sepsis3, ARF, shock, COPD, CHF
    patient = cached_query(raw_dir, 'patient', get_patient_group, args, client, force=force)
    print("Patient icu info query done, start querying variables in Dynamic table")
    # get icu stay id and subject id
    icuids_to_keep = patient['stay_id']
    icuids_to_keep = set([str(s) for s in icuids_to_keep])
    subject_to_keep = patient['subject_id']
    subject_to_keep = set([str(s) for s in subject_to_keep])
    # create template fill_df with time window for each stay based on icu in/out time
    patient.set_index('stay_id', inplace=True)
    patient['max_hours'] = (patient['icu_outtime'] - patient['icu_intime']).apply(to_hours)
    missing_hours_fill = range_unnest(patient, 'max_hours', out_col_name='hours_in', reset_index=True)
    missing_hours_fill['tmp'] = np.NaN
    fill_df = patient.reset_index()[ID_COLS].join(missing_hours_fill.set_index('stay_id'), on='stay_id')
    fill_df.set_index(ID_COLS + ['hours_in'], inplace=True)

    # start with mimic_derived_data
    # query bg table
    bg = cached_query(raw_dir, 'bg', query_bg_mimic, client, subject_to_keep, force=force)
    # initial process bg table
    bg['hours_in'] = (bg['charttime'] - bg['icu_intime']).apply(to_hours)
    bg.drop(columns=['charttime', 'icu_intime', 'aado2_calc', 'specimen'], inplace=True) # aado2_calc, specimen not used
    bg = process_query_results(bg, fill_df)

    # query vital sign
    vitalsign = cached_query(raw_dir, 'vitalsign', query_vitals_mimic, client, icuids_to_keep, force=force)
    # temperature/glucose is a repeat name but different itemid, rename for now and combine later
    vitalsign.rename(columns={'temperature': 'temp_vital'}, inplace=True)
    vitalsign.rename(columns={'glucose': 'glucose_vital'}, inplace=True)
    vitalsign['hours_in'] = (vitalsign['charttime'] - vitalsign['icu_intime']).apply(to_hours)
    vitalsign.drop(columns=['charttime', 'icu_intime', 'temperature_site'], inplace=True) # temperature_site is not used
    vitalsign = process_query_results(vitalsign, fill_df)

    # query blood differential
    blood_diff = cached_query(raw_dir, 'blood_diff', query_blood_diff_mimic, client, subject_to_keep, force=force)
    blood_diff['hours_in'] = (blood_diff['charttime'] - blood_diff['icu_intime']).apply(to_hours)
    blood_diff.drop(columns=['charttime', 'icu_intime', 'specimen_id'], inplace=True)
    blood_diff = process_query_results(blood_diff, fill_df)

    # query cardiac marker
    cardiac_marker = cached_query(raw_dir, 'cardiac_marker', query_cardiac_marker_mimic, client, subject_to_keep, force=force)
    cardiac_marker['troponin_t'].replace(to_replace=[None], value=np.nan, inplace=True)
    cardiac_marker['troponin_t'] = pd.to_numeric(cardiac_marker['troponin_t'])
    cardiac_marker['hours_in'] = (cardiac_marker['charttime'] - cardiac_marker['icu_intime']).apply(to_hours)
    cardiac_marker.drop(columns=['charttime', 'icu_intime', 'specimen_id'], inplace=True)
    cardiac_marker = process_query_results(cardiac_marker, fill_df)

    # query chemistry
    chemistry = cached_query(raw_dir, 'chemistry', query_chemistry_mimic, client, subject_to_keep, force=force)
    # rename glucose into glucose_chem and others
    chemistry.rename(columns={'glucose': 'glucose_chem'}, inplace=True)
    chemistry.rename(columns={'bicarbonate': 'bicarbonate_chem'}, inplace=True)
    chemistry.rename(columns={'chloride': 'chloride_chem'}, inplace=True)
    chemistry.rename(columns={'calcium': 'calcium_chem'}, inplace=True)
    chemistry.rename(columns={'potassium': 'potassium_chem'}, inplace=True)
    chemistry.rename(columns={'sodium': 'sodium_chem'}, inplace=True)
    chemistry['hours_in'] = (chemistry['charttime'] - chemistry['icu_intime']).apply(to_hours)
    chemistry.drop(columns=['charttime', 'icu_intime', 'specimen_id'], inplace=True)
    chemistry = process_query_results(chemistry, fill_df)

    # query coagulation
    coagulation = cached_query(raw_dir, 'coagulation', query_coagulation_mimic, client, subject_to_keep, force=force)
    coagulation['hours_in'] = (coagulation['charttime'] - coagulation['icu_intime']).apply(to_hours)
    coagulation.drop(columns=['charttime', 'icu_intime', 'specimen_id'], inplace=True)
    coagulation = process_query_results(coagulation, fill_df)

    # query cbc
    cbc = cached_query(raw_dir, 'cbc', query_cbc_mimic, client, subject_to_keep, force=force)
    cbc.rename(columns={'hematocrit': 'hematocrit_cbc'}, inplace=True)
    cbc.rename(columns={'hemoglobin': 'hemoglobin_cbc'}, inplace=True)
    # also drop wbc since it's a repeat 51301
    cbc['hours_in'] = (cbc['charttime'] - cbc['icu_intime']).apply(to_hours)
    cbc.drop(columns=['charttime', 'icu_intime', 'specimen_id', 'wbc'], inplace=True)
    cbc = process_query_results(cbc, fill_df)

    # query culture
    culture = cached_query(raw_dir, 'culture', query_culture_mimic, client, subject_to_keep, force=force)
    # MIMIC-IV 3.1: culture table no longer exists, query returns empty DataFrame
    # Create placeholder with expected structure when skipped
    if culture.empty:
        # Create empty culture DataFrame with expected multi-level columns
        # Use float dtype for numeric indicators so pd.get_dummies won't consume them
        culture_cols = pd.MultiIndex.from_tuples([
            ('specimen_culture', 'last'),
            ('screen', 'last'),
            ('positive_culture', 'last'),
            ('has_sensitivity', 'last')
        ])
        culture = pd.DataFrame(index=fill_df.index, columns=culture_cols)
        culture[('screen', 'last')] = culture[('screen', 'last')].astype(float)
        culture[('positive_culture', 'last')] = culture[('positive_culture', 'last')].astype(float)
        culture[('has_sensitivity', 'last')] = culture[('has_sensitivity', 'last')].astype(float)
    else:
        culture.rename(columns={'specimen': 'specimen_culture'}, inplace=True)
        culture['hours_in'] = (culture['charttime'] - culture['icu_intime']).apply(to_hours)
        culture.drop(columns=['charttime', 'icu_intime'], inplace=True)
        culture = culture.groupby(ID_COLS + ['hours_in']).agg(['last'])
        culture = culture.reindex(fill_df.index)

    # query enzyme
    enzyme = cached_query(raw_dir, 'enzyme', query_enzyme_mimic, client, subject_to_keep, force=force)
    # also drop ck_mb since it's a repeat 50911
    enzyme['hours_in'] = (enzyme['charttime'] - enzyme['icu_intime']).apply(to_hours)
    enzyme.drop(columns=['charttime', 'icu_intime', 'specimen_id', 'ck_mb'], inplace=True)
    enzyme = process_query_results(enzyme, fill_df)

    # query gcs
    gcs = cached_query(raw_dir, 'gcs', query_gcs_mimic, client, icuids_to_keep, force=force)
    gcs['hours_in'] = (gcs['charttime'] - gcs['icu_intime']).apply(to_hours)
    gcs.drop(columns=['charttime', 'icu_intime'], inplace=True)
    gcs = process_query_results(gcs, fill_df)

    # query inflammation
    inflammation = cached_query(raw_dir, 'inflammation', query_inflammation_mimic, client, subject_to_keep, force=force)
    inflammation['hours_in'] = (inflammation['charttime'] - inflammation['icu_intime']).apply(to_hours)
    inflammation.drop(columns=['charttime', 'icu_intime'], inplace=True)
    inflammation = process_query_results(inflammation, fill_df)

    # query uo
    uo = cached_query(raw_dir, 'uo', query_uo_mimic, client, icuids_to_keep, force=force)
    uo['hours_in'] = (uo['charttime'] - uo['icu_intime']).apply(to_hours)
    uo.drop(columns=['charttime', 'icu_intime'], inplace=True)
    uo = process_query_results(uo, fill_df)

    # join and save
    # use MIMIC-Extract way to query other itemids that was present in MIMIC-Extract
    # load resources
    chartitems_to_keep = pd.read_excel('./resources/chartitems_to_keep_0505.xlsx')
    lab_to_keep = pd.read_excel('./resources/labitems_to_keep_0505.xlsx')
    var_map = pd.read_csv('./resources/Chart_makeup_0505 - var_map0505.csv')
    chart_items = chartitems_to_keep['chartitems_to_keep'].tolist()
    lab_items = lab_to_keep['labitems_to_keep'].tolist()
    chart_items = set([str(i) for i in chart_items])
    lab_items = set([str(i) for i in lab_items])

    # additional chart and lab
    chart_lab = cached_query(raw_dir, 'chart_lab', query_chart_lab_mimic, client, icuids_to_keep, chart_items, lab_items, force=force)
    chart_lab['value'] = pd.to_numeric(chart_lab['value'], 'coerce')
    chart_lab = chart_lab.set_index('stay_id').join(patient[['icu_intime']])
    chart_lab['hours_in'] = (chart_lab['charttime'] - chart_lab['icu_intime']).apply(to_hours)
    chart_lab.drop(columns=['charttime', 'icu_intime', 'valueuom'], inplace=True)  # valueuom is string, can't aggregate
    chart_lab.set_index('itemid', append=True, inplace=True)
    var_map.set_index('itemid', inplace=True)
    chart_lab = chart_lab.join(var_map, on='itemid').set_index(['LEVEL1', 'LEVEL2'], append=True)
    chart_lab.index.names = ['stay_id', chart_lab.index.names[1], chart_lab.index.names[2], chart_lab.index.names[3]]
    group_item_cols = ['LEVEL2']
    chart_lab = chart_lab.groupby(ID_COLS + group_item_cols + ['hours_in']).agg(['mean', 'count'])

    chart_lab.columns = chart_lab.columns.droplevel(0)
    chart_lab.columns.names = ['Aggregation Function']
    chart_lab = chart_lab.unstack(level=group_item_cols)
    chart_lab.columns = chart_lab.columns.reorder_levels(order=group_item_cols + ['Aggregation Function'])

    chart_lab = chart_lab.reindex(fill_df.index)
    chart_lab = chart_lab.sort_index(axis=1, level=0)
    new_cols = chart_lab.columns.reindex(['mean', 'count'], level=1)
    chart_lab = chart_lab.reindex(columns=new_cols[0])

    # join all dataframes
    total = bg.join(
        [vitalsign, blood_diff, cardiac_marker, chemistry, coagulation, cbc, culture, enzyme, gcs, inflammation, uo])

    # start combining and drop some columns either due to redundancy or not well-populated
    # drop some columns (not well-populated or already dependent on existing columns )
    columns_to_drop = ['rdwsd', 'aado2', 'pao2fio2ratio', 'carboxyhemoglobin',
                       'methemoglobin', 'globulin', 'd_dimer', 'thrombin', 'basophils_abs', 'eosinophils_abs',
                       'lymphocytes_abs', 'monocytes_abs', 'neutrophils_abs']
    for c in columns_to_drop:
        total.drop(c, axis=1, level=0, inplace=True)

    idx = pd.IndexSlice
    chart_lab.loc[:, idx[:, ['count']]] = chart_lab.loc[:, idx[:, ['count']]].fillna(0)
    total.loc[:, idx[:, ['count']]] = total.loc[:, idx[:, ['count']]].fillna(0)

    # combine columns since they were from different itemids but have the same semantics
    names_to_combine = [
        ['so2', 'spo2'], ['fio2', 'fio2_chartevents'], ['bicarbonate', 'bicarbonate_chem'],
        ['hematocrit', 'hematocrit_cbc'], ['hemoglobin', 'hemoglobin_cbc'], ['chloride', 'chloride_chem'],
        ['glucose', 'glucose_chem'], ['glucose', 'glucose_vital'],
        ['temperature', 'temp_vital'], ['sodium', 'sodium_chem'], ['potassium', 'potassium_chem']
    ]
    for names in names_to_combine:
        original = total.loc[:, idx[names[0], ['mean', 'count']]].copy(deep=True)
        makeups = total.loc[:, idx[names[1], ['mean', 'count']]].copy(deep=True)
        filled = combine_cols(makeups, original)
        total.loc[:, idx[names[0], ['mean', 'count']]] = filled.loc[:, ['mean', 'count']].values
        total.drop(names[1], axis=1, level=0, inplace=True)

    # MIMIC-IV 3.1: Only do culture site mapping if culture data was actually extracted
    # (culture table no longer exists in v3.1, so specimen_culture will be all NaN)
    if not total[('specimen_culture', 'last')].isna().all():
        with open('./json_files/mimic_culturesite_map.json') as f:
            csite_map = json.load(f)
        total.loc[:, idx['specimen_culture', ['last']]] = pd.Series(
            np.squeeze(total.loc[:, idx['specimen_culture', ['last']]].values)).map(csite_map).values

    # drop Eosinophils
    chart_lab.drop('Eosinophils', axis=1, level=0, inplace=True)
    # combine in chart_lab table
    names = ['Phosphate', 'Phosphorous']
    original = chart_lab.loc[:, idx[names[0], ['mean', 'count']]].copy(deep=True)
    makeups = chart_lab.loc[:, idx[names[1], ['mean', 'count']]].copy(deep=True)
    filled = combine_cols(makeups, original)
    chart_lab.loc[:, idx[names[0], ['mean', 'count']]] = filled.loc[:, ['mean', 'count']].values
    chart_lab.drop(names[1], axis=1, level=0, inplace=True)

    names = ['Potassium', 'Potassium serum']
    original = chart_lab.loc[:, idx[names[0], ['mean', 'count']]].copy(deep=True)
    makeups = chart_lab.loc[:, idx[names[1], ['mean', 'count']]].copy(deep=True)
    filled = combine_cols(makeups, original)
    chart_lab.loc[:, idx[names[0], ['mean', 'count']]] = filled.loc[:, ['mean', 'count']].values
    chart_lab.drop(names[1], axis=1, level=0, inplace=True)

    # Combine between chartlab and total table
    with open('./json_files/mimic_to_combine_1.json') as f:
        names_list = json.load(f)
    for names in names_list:
        original = total.loc[:, idx[names[0], ['mean', 'count']]].copy(deep=True)
        makeups = chart_lab.loc[:, idx[names[1], ['mean', 'count']]].copy(deep=True)
        filled = combine_cols(makeups, original)
        total.loc[:, idx[names[0], ['mean', 'count']]] = filled.loc[:, ['mean', 'count']].values
        chart_lab.drop(names[1], axis=1, level=0, inplace=True)

    # In eicu mbp contains both invasive and non-invasive, so combine them for mimic_iv
    names_list = [['dbp', 'Diastolic blood pressure'], ['dbp_ni', 'Diastolic blood pressure'],
                  ['mbp', 'Mean blood pressure'], ['mbp_ni', 'Mean blood pressure'],
                  ['sbp', 'Systolic blood pressure'], ['sbp_ni', 'Systolic blood pressure']]
    for names in names_list:
        original = total.loc[:, idx[names[0], ['count', 'mean']]].copy(deep=True)
        makeups = chart_lab.loc[:, idx[names[1], ['count', 'mean']]].copy(deep=True)
        filled = combine_cols(makeups, original)
        total.loc[:, idx[names[0], ['count', 'mean']]] = filled.loc[:, ['count', 'mean']].values
        # Xm.drop(names[1], axis=1, level=0, inplace=True)
    chart_lab.drop('Mean blood pressure', axis=1, level=0, inplace=True)
    chart_lab.drop('Diastolic blood pressure', axis=1, level=0, inplace=True)
    chart_lab.drop('Systolic blood pressure', axis=1, level=0, inplace=True)

    with open('./json_files/mimic_to_drop_1.json') as f:
        columns_to_drop = json.load(f)
    for c in columns_to_drop:
        chart_lab.drop(c, axis=1, level=0, inplace=True)
    vital = total.join(chart_lab)
    # Done dropping and combining

    # screen and positive culture needs impute, they are last columns but with float data type
    vital_encode = pd.get_dummies(vital)
    # When culture is skipped (all NaN), pd.get_dummies creates no dummy columns
    # and the MultiIndex is preserved.  Flatten to a regular Index of tuples so
    # that downstream string-keyed culture-site columns can be added correctly.
    if isinstance(vital_encode.columns, pd.MultiIndex):
        vital_encode.columns = vital_encode.columns.to_flat_index()

    # MIMIC-IV 3.1: Culture columns may be all NaN if culture table was skipped
    # Create mask columns (will be all 0 if culture data was skipped)
    vital_encode[('positive_culture', 'mask')] = (~vital_encode[('positive_culture', 'last')].isnull()).astype(float)
    vital_encode[('screen', 'mask')] = (~vital_encode[('screen', 'last')].isnull()).astype(float)
    vital_encode[('has_sensitivity', 'mask')] = (~vital_encode[('has_sensitivity', 'last')].isnull()).astype(float)
    # X_encode.fillna(value=0, inplace=True)
    # vital_encode.fillna(value=0, inplace=True)

    col = vital_encode.columns.to_list()
    col.insert(col.index(('screen', 'last')) + 1, ('screen', 'mask'))
    col.insert(col.index(('positive_culture', 'last')) + 1, ('positive_culture', 'mask'))
    col.insert(col.index(('has_sensitivity', 'last')) + 1, ('has_sensitivity', 'mask'))

    vital_final = vital_encode[col[:-3]].copy()

    # check if any culture site is missing and fill in empty
    # MIMIC-IV 3.1: If culture was skipped, all 14 sites will be missing - add them all as zeros
    col_encode = [str(c) for c in vital_final.columns.to_list()]
    csite_col = [int(i.split('cul_site')[-1]) for i in col_encode if "cul_site" in i]
    if len(csite_col) < 14:
        # find out which is missing
        missing_site = [i for i in range(14) if i not in csite_col]
        missing_col_name = ["('specimen_culture', 'last')_cul_site" + str(i) for i in missing_site]
        for col in missing_col_name:
            vital_final[col] = 0
    with open('./json_files/mimic_col_order.json') as f:
        mimic_col_order_raw = json.load(f)
    mimic_col_order = [tuple(c) if isinstance(c, list) else c for c in mimic_col_order_raw]
    vital_final = vital_final[mimic_col_order]
    print('Start querying variables in the Intervention table')
    ####### Done vital table #######

    # start query intervention
    vent_data = cached_query(raw_dir, 'vent', query_vent_mimic, client, icuids_to_keep, force=force)
    vent_data = compile_intervention(vent_data, 'vent', args.time_window)

    ids_with = vent_data['stay_id']
    ids_with = set(map(int, ids_with))
    ids_all = set(map(int, icuids_to_keep))
    ids_without = (ids_all - ids_with)
    novent_data = patient.copy(deep=True)
    novent_data = novent_data.reset_index()
    novent_data = novent_data.set_index('stay_id')
    novent_data = novent_data.iloc[novent_data.index.isin(ids_without)]
    novent_data = novent_data.reset_index()
    novent_data = novent_data[['subject_id', 'hadm_id', 'stay_id', 'max_hours']]
    # novent_data['max_hours'] = novent_data['stay_id'].map(icustay_timediff)
    novent_data = novent_data.groupby('stay_id')
    novent_data = novent_data.apply(add_blank_indicators)
    novent_data.rename(columns={'on': 'vent'}, inplace=True)
    novent_data = novent_data.reset_index()

    # Concatenate all the data vertically
    intervention = pd.concat([vent_data[['subject_id', 'hadm_id', 'stay_id', 'hours_in', 'vent']],
                              novent_data[['subject_id', 'hadm_id', 'stay_id', 'hours_in', 'vent']]],
                             axis=0)

    # query antibiotics
    antibiotics = cached_query(raw_dir, 'antibiotics', query_antibiotics_mimic, client, icuids_to_keep, force=force)
    antibiotics = compile_intervention(antibiotics, 'antibiotics', args.time_window)
    intervention = intervention.merge(
        antibiotics[['subject_id', 'hadm_id', 'stay_id', 'hours_in', 'antibiotic', 'route']],
        on=['subject_id', 'hadm_id', 'stay_id', 'hours_in'],
        how='left'
    )

    # vaso agents
    column_names = ['dopamine', 'epinephrine', 'norepinephrine', 'phenylephrine', 'vasopressin', 'dobutamine',
                    'milrinone']
    for c in column_names:
        # TOTAL VASOPRESSOR DATA
        new_data = cached_query(raw_dir, f'vasoactive_{c}', query_vasoactive_mimic, client, icuids_to_keep, c, force=force)
        new_data = compile_intervention(new_data, c, args.time_window)
        intervention = intervention.merge(
            new_data[['subject_id', 'hadm_id', 'stay_id', 'hours_in', c]],
            on=['subject_id', 'hadm_id', 'stay_id', 'hours_in'],
            how='left'
        )

    # heparin (stubbed in MIMIC-IV 3.1 -- table no longer exists)
    heparin = cached_query(raw_dir, 'heparin', query_heparin_mimic, client, subject_to_keep, force=force)
    if heparin.empty:
        heparin = pd.DataFrame(columns=['subject_id', 'hadm_id', 'stay_id', 'hours_in', 'heparin'])
    else:
        heparin = compile_intervention(heparin, 'heparin', args.time_window)
    intervention = intervention.merge(
        heparin[['subject_id', 'hadm_id', 'stay_id', 'hours_in', 'heparin']],
        on=['subject_id', 'hadm_id', 'stay_id', 'hours_in'],
        how='left'
    )

    # crrt
    crrt = cached_query(raw_dir, 'crrt', query_crrt_mimic, client, icuids_to_keep, force=force)
    crrt = compile_intervention(crrt, 'crrt', args.time_window)
    intervention = intervention.merge(
        crrt[['subject_id', 'hadm_id', 'stay_id', 'hours_in', 'crrt']],
        on=['subject_id', 'hadm_id', 'stay_id', 'hours_in'],
        how='left'
    )

    # rbc transfusion
    rbc_trans = cached_query(raw_dir, 'rbc_trans', query_rbc_trans_mimic, client, icuids_to_keep, force=force)
    rbc_trans = compile_intervention(rbc_trans, 'rbc_trans', args.time_window)
    intervention = intervention.merge(
        rbc_trans[['subject_id', 'hadm_id', 'stay_id', 'hours_in', 'rbc_trans']],
        on=['subject_id', 'hadm_id', 'stay_id', 'hours_in'],
        how='left'
    )

    # platelets transfusion
    platelets_trans = cached_query(raw_dir, 'pll_trans', query_pll_trans_mimic, client, icuids_to_keep, force=force)
    platelets_trans = compile_intervention(platelets_trans, 'platelets_trans', args.time_window)
    intervention = intervention.merge(
        platelets_trans[['subject_id', 'hadm_id', 'stay_id', 'hours_in', 'platelets_trans']],
        on=['subject_id', 'hadm_id', 'stay_id', 'hours_in'],
        how='left'
    )

    # ffp transfusion
    ffp_trans = cached_query(raw_dir, 'ffp_trans', query_ffp_trans_mimic, client, icuids_to_keep, force=force)
    ffp_trans = compile_intervention(ffp_trans, 'ffp_trans', args.time_window)
    intervention = intervention.merge(
        ffp_trans[['subject_id', 'hadm_id', 'stay_id', 'hours_in', 'ffp_trans']],
        on=['subject_id', 'hadm_id', 'stay_id', 'hours_in'],
        how='left'
    )

    # other infusion
    colloid_bolus = cached_query(raw_dir, 'colloid', query_colloid_mimic, client, icuids_to_keep, force=force)
    colloid_bolus = compile_intervention(colloid_bolus, 'colloid_bolus', args.time_window)
    intervention = intervention.merge(
        colloid_bolus[['subject_id', 'hadm_id', 'stay_id', 'hours_in', 'colloid_bolus']],
        on=['subject_id', 'hadm_id', 'stay_id', 'hours_in'],
        how='left'
    )

    # other infusion
    crystalloid_bolus = cached_query(raw_dir, 'crystalloid', query_crystalloid_mimic, client, icuids_to_keep, force=force)
    crystalloid_bolus = compile_intervention(crystalloid_bolus, 'crystalloid_bolus', args.time_window)
    intervention = intervention.merge(
        crystalloid_bolus[['subject_id', 'hadm_id', 'stay_id', 'hours_in', 'crystalloid_bolus']],
        on=['subject_id', 'hadm_id', 'stay_id', 'hours_in'],
        how='left')

    # Process the Intervention table
    intervention.drop('route', axis=1, inplace=True) # drop route column
    # for each column, astype to int and fill na with 0
    intervention = intervention.fillna(0)
    intervention.loc[:, 'antibiotic'] = intervention.loc[:, 'antibiotic'].mask(intervention.loc[:, 'antibiotic'] != 0,
                                                                               1).values
    for i in range(5, 20):
        intervention.iloc[:, i] = intervention.iloc[:, i].astype(int)
    intervention.set_index(ID_COLS + ['hours_in'], inplace=True)
    intervention.sort_index(level=['stay_id', 'hours_in'], inplace=True)
    # Finish processing the Intervention table
    print('Start querying variables in the Static table')

    # static info
    #  query patients anchor year and comorbidity
    anchor_year = cached_query(raw_dir, 'anchor_year', query_anchor_year_mimic, client, icuids_to_keep, force=force)
    comorbidity = cached_query(raw_dir, 'comorbidity', query_comorbidity_mimic, client, icuids_to_keep, force=force)
    patient.reset_index(inplace=True)
    patient.set_index(ID_COLS, inplace=True)
    comorbidity.set_index(ID_COLS, inplace=True)
    anchor_year.set_index(ID_COLS, inplace=True)
    static = patient.join([comorbidity, anchor_year['anchor_year_group']])

    if args.exit_point == 'Raw':
        print('Exit point is after querying raw records, saving results...')
        os.makedirs(args.output_dir, exist_ok=True)
        vital_final.to_parquet(os.path.join(args.output_dir, 'MEEP_MIMIC_vital.parquet'))
        static.to_parquet(os.path.join(args.output_dir, 'MEEP_MIMIC_static.parquet'))
        intervention.to_parquet(os.path.join(args.output_dir, 'MEEP_MIMIC_inv.parquet'))
        return

    # remove outliers
    total_cols = vital_final.columns.tolist()
    mean_col = [i for i in total_cols if 'mean' in i]
    X_mean = vital_final.loc[:, mean_col]

    if not args.no_removal:
        print('Performing outlier removal')
        with open("./json_files/mimic_outlier_high.json") as f:
            range_dict_high  = json.load(f)
        with open("./json_files/mimic_outlier_low.json") as f:
            range_dict_low = json.load(f)
        for var_to_remove in range_dict_high:
            remove_outliers_h(vital_final, X_mean, var_to_remove, range_dict_high[var_to_remove])
        for var_to_remove in range_dict_low:
            remove_outliers_l(vital_final, X_mean, var_to_remove, range_dict_low[var_to_remove])
    else:
        print('Skipped outlier removal')

    if args.exit_point == 'Outlier_removal':
        print('Exit point is after removing outliers, saving results...')
        os.makedirs(args.output_dir, exist_ok=True)
        vital_final.to_parquet(os.path.join(args.output_dir, 'MEEP_MIMIC_vital.parquet'))
        static.to_parquet(os.path.join(args.output_dir, 'MEEP_MIMIC_static.parquet'))
        intervention.to_parquet(os.path.join(args.output_dir, 'MEEP_MIMIC_inv.parquet'))
        return

    # normalize
    print('Start normalization and data imputation ')
    count_col = [i for i in total_cols if 'count' in i]
    col_means, col_stds = vital_final.loc[:, mean_col].mean(axis=0), vital_final.loc[:, mean_col].std(axis=0)
    # saving col_means and col_stds for eicu normalization
    df_mean_std = col_means.to_frame('mean').join(col_stds.to_frame('std'))
    os.makedirs(args.output_dir, exist_ok=True)
    df_mean_std.to_parquet(os.path.join(args.output_dir, 'MIMIC_mean_std_stats.parquet'))
    vital_final.loc[:, mean_col] = (vital_final.loc[:, mean_col] - col_means) / col_stds
    icustay_means = vital_final.loc[:, mean_col].groupby(ID_COLS).mean()
    # impute
    vital_final.loc[:, mean_col] = vital_final.loc[:, mean_col].groupby(ID_COLS).fillna(method='ffill').groupby(
        ID_COLS).fillna(
        icustay_means).fillna(0)
    # 0 or 1
    vital_final.loc[:, count_col] = (vital_final.loc[:, count_col] > 0).astype(float)
    # at this satge only 3 last columns has nan values
    vital_final = vital_final.fillna(0)
    # convert to int in stead of int64 which will be problematic for hdf saving
    vital_final[('screen', 'last')] = vital_final[('screen', 'last')].astype('uint8')
    vital_final[('positive_culture', 'last')] = vital_final[('positive_culture', 'last')].astype('uint8')
    vital_final[('has_sensitivity', 'last')] = vital_final[('has_sensitivity', 'last')].astype('uint8')

    if args.exit_point == 'Impute':
        print('Exit point is after data imputation, saving results...')
        os.makedirs(args.output_dir, exist_ok=True)
        vital_final.to_parquet(os.path.join(args.output_dir, 'MEEP_MIMIC_vital.parquet'))
        static.to_parquet(os.path.join(args.output_dir, 'MEEP_MIMIC_static.parquet'))
        intervention.to_parquet(os.path.join(args.output_dir, 'MEEP_MIMIC_inv.parquet'))
        return

    # split data
    stays_v = set(vital_final.index.get_level_values(2).values)
    stays_static = set(static.index.get_level_values(2).values)
    stays_int = set(intervention.index.get_level_values(2).values)
    assert stays_v == stays_static, "Subject ID pools differ!"
    assert stays_v == stays_int, "Subject ID pools differ!"
    train_frac, dev_frac, test_frac = 0.7, 0.1, 0.2
    SEED = 41
    np.random.seed(SEED)
    subjects, N = np.random.permutation(list(stays_v)), len(stays_v)
    N_train, N_dev, N_test = int(train_frac * N), int(dev_frac * N), int(test_frac * N)
    train_stay = list(stays_v)[:N_train]
    dev_stay = list(stays_v)[N_train:N_train + N_dev]
    test_stay = list(stays_v)[N_train + N_dev:]
    def convert_dtype(df):
        names = df.columns.to_list()
        dtypes = df.dtypes.to_list()
        for i, col in enumerate(df.columns.to_list()):
            if dtypes[i] == pd.Int64Dtype():
                df.loc[:, col] = df.loc[:, col].astype(float)
        return df
    static = convert_dtype(static)

    [(vital_train, vital_dev, vital_test), (Y_train, Y_dev, Y_test), (static_train, static_dev, static_test)] = [
        [df[df.index.get_level_values(2).isin(s)] for s in (train_stay, dev_stay, test_stay)] \
        for df in (vital_final, intervention, static)]

    if args.exit_point == 'All':
        print('Exit point is after all steps, including train-val-test splitting, saving results...')
        split_dir = os.path.join(args.output_dir, 'MIMIC_split')
        os.makedirs(split_dir, exist_ok=True)
        vital_train.to_parquet(os.path.join(split_dir, 'vital_train.parquet'))
        vital_dev.to_parquet(os.path.join(split_dir, 'vital_dev.parquet'))
        vital_test.to_parquet(os.path.join(split_dir, 'vital_test.parquet'))
        Y_train.to_parquet(os.path.join(split_dir, 'inv_train.parquet'))
        Y_dev.to_parquet(os.path.join(split_dir, 'inv_dev.parquet'))
        Y_test.to_parquet(os.path.join(split_dir, 'inv_test.parquet'))
        static_train.to_parquet(os.path.join(split_dir, 'static_train.parquet'))
        static_dev.to_parquet(os.path.join(split_dir, 'static_dev.parquet'))
        static_test.to_parquet(os.path.join(split_dir, 'static_test.parquet'))
    return


def extract_eicu(args):

    os.environ["GOOGLE_CLOUD_PROJECT"] = args.project_id
    client = bigquery.Client(project=args.project_id)
    ID_COLS = ['patientunitstayid']
    # minutes to hour
    to_hours = lambda x: int(x // (60 * args.time_window))
    tw_in_min = 60 * args.time_window

    # --- cache setup ---
    raw_dir = os.path.join(args.cache_dir, f"eICU_{args.patient_group}", "raw")
    force = args.force_query
    _check_params(os.path.join(args.cache_dir, f"eICU_{args.patient_group}"), args)
    _save_params(os.path.join(args.cache_dir, f"eICU_{args.patient_group}"), args)

    # get patient group
    patient = cached_query(raw_dir, 'patient', get_patient_group_eicu, args, client, force=force)
    print("Patient icu info query done, start querying variables in Dynamic table")
    patient['unitadmitoffset'] = 0
    young_age = [str(i) for i in range(args.age_min)]
    patient = patient.loc[~patient.loc[:, 'age'].isin(young_age)]
    icuids_to_keep = patient['patientunitstayid']
    icuids_to_keep = set([str(s) for s in icuids_to_keep])
    patient.set_index('patientunitstayid', inplace=True)
    patient['max_hours'] = (patient['unitdischargeoffset'] - patient['unitadmitoffset']).apply(to_hours)
    missing_hours_fill = range_unnest(patient, 'max_hours', out_col_name='hours_in', reset_index=True)
    missing_hours_fill['tmp'] = np.NaN
    fill_df = patient.reset_index()[ID_COLS].join(missing_hours_fill.set_index('patientunitstayid'),
                                                  on='patientunitstayid')
    fill_df.set_index(ID_COLS + ['hours_in'], inplace=True)

    # ---- chunked vital processing to limit memory ----
    import gc
    N_CHUNKS = 20
    all_stay_ids = sorted(fill_df.index.get_level_values('patientunitstayid').unique())
    chunks = [all_stay_ids[i::N_CHUNKS] for i in range(N_CHUNKS)]

    # Pre-load JSON config files (small, reused per chunk)
    with open("./json_files/eicu_empty_columns.json") as f:
        columns_to_make = json.load(f)
    with open("./json_files/eicu_empty_culture.json") as f:
        empty_culture = json.load(f)
    with open("./json_files/eicu_col_order.json") as f:
        col = json.load(f)
    breakpoint1 = col.index('positive')
    breakpoint2 = col.index("('culturesite', 'last')_culturesite0")
    col_ready = []
    for i in range(breakpoint1):
        col_ready.append((col[i], 'mean'))
        col_ready.append((col[i], 'count'))
    for i in range(breakpoint1, breakpoint2):
        col_ready.append((col[i], 'last'))
        col_ready.append((col[i], 'mask'))
    for i in range(breakpoint2, len(col)):
        col_ready.append(col[i])

    chunk_dir = os.path.join(raw_dir, '_vital_chunks')
    os.makedirs(chunk_dir, exist_ok=True)

    for ci, chunk_ids in enumerate(chunks):
        chunk_path = os.path.join(chunk_dir, f'chunk_{ci:03d}.parquet')
        if os.path.exists(chunk_path):
            print(f'  Vital chunk {ci+1}/{N_CHUNKS} already on disk, skipping.')
            continue

        print(f'  Processing vital chunk {ci+1}/{N_CHUNKS} ({len(chunk_ids)} stays)...')
        chunk_set = set(chunk_ids)
        chunk_fill = fill_df[fill_df.index.get_level_values('patientunitstayid').isin(chunk_set)]

        def _read_and_filter(name):
            df = pd.read_parquet(os.path.join(raw_dir, f'{name}.parquet'))
            df = df[df['patientunitstayid'].isin(chunk_set)]
            for c in df.columns:
                if df[c].dtype == object and c not in ('patientunitstayid',):
                    df[c] = pd.to_numeric(df[c], errors='coerce')
            return df

        bg = fill_query(_read_and_filter('bg'), chunk_fill, tw_in_min)
        lab = fill_query(_read_and_filter('lab'), chunk_fill, tw_in_min)
        vital_raw = _read_and_filter('vital')
        vital_raw.drop('entryoffset', axis=1, inplace=True)
        vital_raw = fill_query(vital_raw, chunk_fill, tw_in_min)

        microlab = _read_and_filter('microlab')
        microlab['hours_in'] = microlab['culturetakenoffset'].floordiv(60)
        microlab.drop(columns=['culturetakenoffset'], inplace=True)
        microlab.reset_index(inplace=True)
        microlab = microlab.groupby(ID_COLS + ['hours_in']).agg(['last'])
        microlab = microlab.reindex(chunk_fill.index)

        gcs = fill_query(_read_and_filter('gcs'), chunk_fill, tw_in_min)
        uo = fill_query(_read_and_filter('uo'), chunk_fill, tw_in_min)
        weight = fill_query(_read_and_filter('weight'), chunk_fill, tw_in_min)

        cvp_raw = _read_and_filter('cvp')
        cvp_raw.loc[:, 'cvp'] = cvp_raw.loc[:, 'cvp'].astype(float)
        cvp_raw = fill_query(cvp_raw, chunk_fill, tw_in_min, time='observationoffset')

        vital_c = bg.join([lab, vital_raw, gcs, uo, weight, cvp_raw, microlab])
        del bg, lab, vital_raw, gcs, uo, weight, cvp_raw, microlab

        labmakeup = fill_query(_read_and_filter('labmakeup'), chunk_fill, tw_in_min)
        tidal_vol_obs = fill_query(_read_and_filter('tidal_vol'), chunk_fill, tw_in_min)
        vital_c = vital_c.join([labmakeup, tidal_vol_obs])
        del labmakeup, tidal_vol_obs

        idx = pd.IndexSlice
        vital_c.loc[:, idx[:, 'count']] = vital_c.loc[:, idx[:, 'count']].fillna(0)

        original = vital_c.loc[:, idx['ibp_systolic', ['mean', 'count']]].copy(deep=True)
        makeups = vital_c.loc[:, idx['nibp_systolic', ['mean', 'count']]].copy(deep=True)
        filled = combine_cols(makeups, original)
        vital_c.loc[:, idx['ibp_systolic', ['mean', 'count']]] = filled.loc[:, ['mean', 'count']].values

        original = vital_c.loc[:, idx['ibp_diastolic', ['mean', 'count']]].copy(deep=True)
        makeups = vital_c.loc[:, idx['nibp_diastolic', ['mean', 'count']]].copy(deep=True)
        filled = combine_cols(makeups, original)
        vital_c.loc[:, idx['ibp_diastolic', ['mean', 'count']]] = filled.loc[:, ['mean', 'count']].values

        original = vital_c.loc[:, idx['ibp_mean', ['mean', 'count']]].copy(deep=True)
        makeups = vital_c.loc[:, idx['nibp_mean', ['mean', 'count']]].copy(deep=True)
        filled = combine_cols(makeups, original)
        vital_c.loc[:, idx['ibp_mean', ['mean', 'count']]] = filled.loc[:, ['mean', 'count']].values

        vital_c.drop('basedeficit', axis=1, level=0, inplace=True)
        vital_c.drop('index', axis=1, level=0, inplace=True)
        vital_c = pd.get_dummies(vital_c)
        vital_c[('positive', 'mask')] = (~vital_c[('positive', 'last')].isnull()).astype(float)
        vital_c[('screen', 'mask')] = (~vital_c[('screen', 'last')].isnull()).astype(float)
        vital_c[('has_sensitivity', 'mask')] = (~vital_c[('has_sensitivity', 'last')].isnull()).astype(float)

        for c_name in columns_to_make:
            vital_c[(c_name, 'mean')] = chunk_fill.values
            vital_c[(c_name, 'count')] = 0
        for c_name in empty_culture:
            vital_c[c_name] = 0

        vital_c = vital_c.reindex(columns=col_ready, fill_value=0)
        vital_c.to_parquet(chunk_path)
        del vital_c, chunk_fill
        gc.collect()
        print(f'  Chunk {ci+1}/{N_CHUNKS} done -> {chunk_path}')

    # vital chunks stay on disk -- reassembled lazily at save time
    _vital_chunk_dir = chunk_dir
    _vital_n_chunks = N_CHUNKS
    print('Start querying variables in the Intervention table')

    # Intervention table
    # ventilation
    vent = cached_query(raw_dir, 'vent', query_vent_eicu, client, icuids_to_keep, tw_in_min, force=force)
    vent_data = process_inv(vent, 'vent')
    ids_with = vent_data['patientunitstayid']
    ids_with = set(map(int, ids_with))
    ids_all = set(map(int, icuids_to_keep))
    ids_without = (ids_all - ids_with)

    # patient.set_index('patientunitstayid', inplace=True)
    icustay_timediff_tmp = patient['unitdischargeoffset'] - patient['unitadmitoffset']
    icustay_timediff = pd.Series([timediff // tw_in_min
                                  for timediff in icustay_timediff_tmp], index=patient.index.values)
    # Create a new fake dataframe with blanks on all vent entries
    out_data = fill_df.copy(deep=True)
    out_data = out_data.reset_index()
    out_data = out_data.set_index('patientunitstayid')
    out_data = out_data.iloc[out_data.index.isin(ids_without)]
    out_data = out_data.reset_index()
    out_data = out_data[['patientunitstayid']]
    out_data['max_hours'] = out_data['patientunitstayid'].map(icustay_timediff)

    # Create all 0 column for vent
    out_data = out_data.groupby('patientunitstayid')
    out_data = out_data.apply(add_blank_indicators_e)
    out_data.rename(columns={'on': 'vent'}, inplace=True)

    out_data = out_data.reset_index()
    intervention = pd.concat([vent_data[['patientunitstayid', 'hours_in', 'vent']],
                              out_data[['patientunitstayid', 'hours_in', 'vent']]],
                             axis=0)

    # vasoactive drugs
    column_names = ['dopamine', 'epinephrine', 'norepinephrine', 'phenylephrine', 'vasopressin', 'dobutamine',
                    'milrinone', 'heparin']

    for c in column_names:
        med = cached_query(raw_dir, f'med_{c}', query_med_eicu, client, icuids_to_keep, c, tw_in_min, force=force)
        # 'epinephrine',  'dopamine', 'norepinephrine', 'phenylephrine', \
        #    'vasopressin', 'dobutamine', 'milrinone',  'heparin',
        med = process_inv(med, c)
        intervention = intervention.merge(
            med[['patientunitstayid', 'hours_in', c]],
            on=['patientunitstayid', 'hours_in'],
            how='left'
        )

    # antibiotics
    anti = cached_query(raw_dir, 'antibiotics', query_anti_eicu, client, icuids_to_keep, tw_in_min, force=force)
    anti = process_inv(anti, 'antib')
    intervention = intervention.merge(
        anti[['patientunitstayid', 'hours_in', 'antib']],
        on=['patientunitstayid', 'hours_in'],
        how='left'
    )

    # crrt
    crrt = cached_query(raw_dir, 'crrt', query_crrt_eicu, client, icuids_to_keep, tw_in_min, force=force)
    crrt = process_inv(crrt, 'crrt')
    intervention = intervention.merge(
        crrt[['patientunitstayid', 'hours_in', 'crrt']],
        on=['patientunitstayid', 'hours_in'],
        how='left'
    )

    # rbc transfusion
    rbc = cached_query(raw_dir, 'rbc_trans', query_rbc_trans_eicu, client, icuids_to_keep, tw_in_min, force=force)
    rbc = process_inv(rbc, 'rbc')
    intervention = intervention.merge(
        rbc[['patientunitstayid', 'hours_in', 'rbc']],
        on=['patientunitstayid', 'hours_in'],
        how='left'
    )

    # ffp transfusion
    ffp = cached_query(raw_dir, 'ffp_trans', query_ffp_trans_eicu, client, icuids_to_keep, tw_in_min, force=force)
    ffp = process_inv(ffp, 'ffp')
    intervention = intervention.merge(
        ffp[['patientunitstayid', 'hours_in', 'ffp']],
        on=['patientunitstayid', 'hours_in'],
        how='left'
    )

    # platelets transfusion
    platelets = cached_query(raw_dir, 'pll_trans', query_pll_trans_eicu, client, icuids_to_keep, tw_in_min, force=force)
    platelets = process_inv(platelets, 'platelets')
    intervention = intervention.merge(
        platelets[['patientunitstayid', 'hours_in', 'platelets']],
        on=['patientunitstayid', 'hours_in'],
        how='left'
    )

    #colloid
    colloid = cached_query(raw_dir, 'colloid', query_colloid_eicu, client, icuids_to_keep, tw_in_min, force=force)
    colloid = process_inv(colloid, 'colloid')
    intervention = intervention.merge(
        colloid[['patientunitstayid', 'hours_in', 'colloid']],
        on=['patientunitstayid', 'hours_in'],
        how='left'
    )

    #crystalloid
    crystalloid = cached_query(raw_dir, 'crystalloid', query_crystalloid_eicu, client, icuids_to_keep, tw_in_min, force=force)
    crystalloid = process_inv(crystalloid, 'crystalloid')
    intervention = intervention.merge(
        crystalloid[['patientunitstayid', 'hours_in', 'crystalloid']],
        on=['patientunitstayid', 'hours_in'],
        how='left'
    )

    # for each column, astype to int and fill na with 0
    intervention = intervention.fillna(0)
    for i in range(3, 18):
        intervention.iloc[:, i] = intervention.iloc[:, i].astype(int)

    intervention.set_index(ID_COLS + ['hours_in'], inplace=True)
    intervention.sort_index(level=['patientunitstayid', 'hours_in'], inplace=True)

    # reorder intervention columns
    with open("./json_files/eicu_inv_col_order.json") as f:
        new_col = json.load(f)
    intervention = intervention.loc[:, new_col]
    print('Start querying variables in the Static table')

    # static query
    # commo
    commo = cached_query(raw_dir, 'comorbidity', query_comorbidity_eicu, client, icuids_to_keep, force=force)
    commo.set_index('patientunitstayid', inplace=True)
    static = patient.join(commo)
    static_col = static.columns.tolist()
    static_col.remove('hospitalid')
    static_col.append('hospitalid')
    static = static[static_col]

    if args.exit_point == 'Raw':
        print('Exit point is after querying raw records, saving results...')
        os.makedirs(args.output_dir, exist_ok=True)
        intervention.to_parquet(os.path.join(args.output_dir, 'MEEP_eICU_inv.parquet'))
        static.to_parquet(os.path.join(args.output_dir, 'MEEP_eICU_static.parquet'))
        # Stream vital chunks to output without loading all into memory
        import pyarrow as pa
        import pyarrow.parquet as pq
        vital_out = os.path.join(args.output_dir, 'MEEP_eICU_vital.parquet')
        # Read first chunk to establish the canonical schema
        schema = pq.read_schema(os.path.join(_vital_chunk_dir, 'chunk_000.parquet'))
        writer = pq.ParquetWriter(vital_out, schema)
        for i in range(_vital_n_chunks):
            tbl = pq.read_table(os.path.join(_vital_chunk_dir, f'chunk_{i:03d}.parquet'))
            tbl = tbl.cast(schema)
            writer.write_table(tbl)
            del tbl
        writer.close()
        print(f'  Vital written from {_vital_n_chunks} chunks -> {vital_out}')
        return

    # For later exit points, we need vital in memory
    vital = pd.concat([pd.read_parquet(os.path.join(_vital_chunk_dir, f'chunk_{i:03d}.parquet'))
                        for i in range(_vital_n_chunks)])
    vital.sort_index(inplace=True)

    total_cols = vital.columns.tolist()
    mean_col = [i for i in total_cols if 'mean' in i]
    X_mean = vital.loc[:, mean_col]

    if not args.no_removal:
        print('Performing outlier removal')
        with open("./json_files/eicu_outlier_high.json") as f:
            range_dict_high = json.load(f)
        with open("./json_files/eicu_outlier_low.json") as f:
            range_dict_low = json.load(f)
        for var_to_remove in range_dict_high:
            remove_outliers_h(vital, X_mean, var_to_remove, range_dict_high[var_to_remove])
        for var_to_remove in range_dict_low:
            remove_outliers_l(vital, X_mean, var_to_remove, range_dict_low[var_to_remove])
    else:
        print('Skipped outlier removal')
    del X_mean

    if args.exit_point == 'Outlier_removal':
        print('Exit point is after removing outliers, saving results...')
        os.makedirs(args.output_dir, exist_ok=True)
        intervention.to_parquet(os.path.join(args.output_dir, 'MEEP_eICU_inv.parquet'))
        static.to_parquet(os.path.join(args.output_dir, 'MEEP_eICU_static.parquet'))
        vital.to_parquet(os.path.join(args.output_dir, 'MEEP_eICU_vital.parquet'))
        return

    # read_mimic col means col stds
    print('Start normalization and data imputation ')
    # normalize
    count_col = [i for i in total_cols if 'count' in i]
    # # fix fio2 column by x100
    # vital.loc[:, [('fio2', 'mean')]] = vital.loc[:, [('fio2', 'mean')]] * 100
    # col_means, col_stds = vital.loc[:, mean_col].mean(axis=0), vital.loc[:, mean_col].std(axis=0)
    # first use mimic mean to normorlize
    if args.norm_eicu == 'MIMIC':
        mimic_mean_std = pd.read_parquet(os.path.join(args.output_dir, 'MIMIC_mean_std_stats.parquet'))
        col_means, col_stds = mimic_mean_std.loc[:, 'mean'], mimic_mean_std.loc[:, 'std']
        col_means.index = mean_col
        col_stds.index = mean_col
    else:
        col_means, col_stds = vital.loc[:, mean_col].mean(axis=0), vital.loc[:, mean_col].std(axis=0)
    vital.loc[:, mean_col] = (vital.loc[:, mean_col] - col_means) / col_stds
    icustay_means = vital.loc[:, mean_col].groupby(ID_COLS).mean()
    # impute
    vital.loc[:, mean_col] = vital.loc[:, mean_col].groupby(ID_COLS).fillna(method='ffill').groupby(ID_COLS).fillna(
        icustay_means).fillna(0)
    # 0 or 1
    vital.loc[:, count_col] = (vital.loc[:, count_col] > 0).astype(float)
    # at this satge only 3 last columns has nan values
    vital = vital.fillna(0)
    vital[('screen', 'last')] = vital[('screen', 'last')].astype('uint8')
    vital[('positive', 'last')] = vital[('positive', 'last')].astype('uint8')
    vital[('has_sensitivity', 'last')] = vital[('has_sensitivity', 'last')].astype('uint8')

    if args.exit_point == 'Impute':
        print('Exit point is after data imputation, saving results...')
        os.makedirs(args.output_dir, exist_ok=True)
        intervention.to_parquet(os.path.join(args.output_dir, 'MEEP_eICU_inv.parquet'))
        static.to_parquet(os.path.join(args.output_dir, 'MEEP_eICU_static.parquet'))
        vital.to_parquet(os.path.join(args.output_dir, 'MEEP_eICU_vital.parquet'))
        return

    # split data
    stays_v = set(vital.index.get_level_values(0).values)
    stays_static = set(static.index.get_level_values(0).values)
    stays_int = set(intervention.index.get_level_values(0).values)
    assert stays_v == stays_static, "Stay ID pools differ!"
    assert stays_v == stays_int, "Stay ID pools differ!"
    train_frac, dev_frac, test_frac = 0.7, 0.1, 0.2
    SEED = 41
    np.random.seed(SEED)
    subjects, N = np.random.permutation(list(stays_v)), len(stays_v)
    N_train, N_dev, N_test = int(train_frac * N), int(dev_frac * N), int(test_frac * N)
    train_stay = list(stays_v)[:N_train]
    dev_stay = list(stays_v)[N_train:N_train + N_dev]
    test_stay = list(stays_v)[N_train + N_dev:]
    def convert_dtype(df):
        names = df.columns.to_list()
        dtypes = df.dtypes.to_list()
        for i, col in enumerate(df.columns.to_list()):
            if dtypes[i] == pd.Int64Dtype():
                df.loc[:, col] = df.loc[:, col].astype(float)
        return df
    static = convert_dtype(static)

    [(vital_train, vital_dev, vital_test), (Y_train, Y_dev, Y_test), (static_train, static_dev, static_test)] = [
        [df[df.index.get_level_values(0).isin(s)] for s in (train_stay, dev_stay, test_stay)] \
        for df in (vital, intervention, static)]

    if args.exit_point == 'All':
        print('Exit point is after all steps, including train-val-test splitting, saving results...')
        split_dir = os.path.join(args.output_dir, 'eICU_split')
        os.makedirs(split_dir, exist_ok=True)
        vital_train.to_parquet(os.path.join(split_dir, 'vital_train.parquet'))
        vital_dev.to_parquet(os.path.join(split_dir, 'vital_dev.parquet'))
        vital_test.to_parquet(os.path.join(split_dir, 'vital_test.parquet'))
        Y_train.to_parquet(os.path.join(split_dir, 'inv_train.parquet'))
        Y_dev.to_parquet(os.path.join(split_dir, 'inv_dev.parquet'))
        Y_test.to_parquet(os.path.join(split_dir, 'inv_test.parquet'))
        static_train.to_parquet(os.path.join(split_dir, 'static_train.parquet'))
        static_dev.to_parquet(os.path.join(split_dir, 'static_dev.parquet'))
        static_test.to_parquet(os.path.join(split_dir, 'static_test.parquet'))
    return

