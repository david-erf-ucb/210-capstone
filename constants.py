# constants.py — Shared project configuration
# Import from here instead of hardcoding values in scripts.

# BigQuery billing project ID
PROJECT_ID = "icu-project-486401"

# BigQuery data source project (where MIMIC/eICU tables live)
PHYSIONET_PROJECT = "physionet-data"

# Default METRE pipeline settings
DEFAULT_AGE_MIN = 18
DEFAULT_LOS_MIN = 24   # hours
DEFAULT_LOS_MAX = 240  # hours
DEFAULT_TIME_WINDOW = 1
