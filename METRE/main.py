import argparse
import os
import sys
from extract_database import *

# Allow importing from project root (parent directory)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from constants import PROJECT_ID, DEFAULT_AGE_MIN, DEFAULT_LOS_MIN, DEFAULT_LOS_MAX, DEFAULT_TIME_WINDOW

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Parse to query MIMIC/eICU data")
    parser.add_argument("--database", type=str, default='MIMIC', choices=['MIMIC', 'eICU'])
    parser.add_argument("--project_id", type=str, default=PROJECT_ID,
                        help='Specify the Bigquery billing project')
    parser.add_argument("--age_min", type=int, default=DEFAULT_AGE_MIN, help='Min patient age to query')
    parser.add_argument("--los_min", type=int, default=DEFAULT_LOS_MIN, help='Min ICU LOS in hour')
    parser.add_argument("--los_max", type=int, default=DEFAULT_LOS_MAX, help='Max ICU LOS in hour')
    parser.add_argument("--patient_group", type=str, default='Generic', choices=['Generic', 'sepsis_3', 'ARF', 'shock', 'COPD', 'CHF'],
                        help='Specific groups to extract')
    parser.add_argument("--custom_id", action='store_true', default=False, help="Whether use custom stay ids")
    parser.add_argument('--customid_dir', required='--custom_id' in sys.argv, help="Specify custom id dir")
    parser.add_argument("--exit_point", type=str, default='All', choices=['All', 'Raw', 'Outlier_removal', 'Impute'],
                        help='Where to stop the pipeline')
    parser.add_argument("--no_removal", action='store_true', default=False, help="When set to True, no outlier removal")
    parser.add_argument("--norm_eicu", type=str, default='MIMIC', choices=['MIMIC', 'eICU'],
                        help="Whether use MIMIC mean and std to standardize eICU variables")
    parser.add_argument("--time_window", type=int, default=DEFAULT_TIME_WINDOW, help='Time window to aggregate the data')
    parser.add_argument("--output_dir", type=str, default='./output')
    parser.add_argument("--cache_dir", type=str, default='./cache',
                        help='Directory to store cached BigQuery results (avoids re-querying)')
    parser.add_argument("--force_query", action='store_true', default=False,
                        help='Bypass cache and re-fetch all data from BigQuery')
    args = parser.parse_args()
    if args.database == 'MIMIC':
        extract_mimic(args)
    elif args.database == 'eICU':
        extract_eicu(args)

