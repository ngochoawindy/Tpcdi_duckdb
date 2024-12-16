#!/bin/bash

# Default scale value
scale=3

# Parse command line arguments
for arg in "$@"
do
    case $arg in
        --scale=*)
        scale="${arg#*=}"
        shift
        ;;
    esac
done

echo "Scale: $scale"

cd python

# Run create staging and DW schemas
echo 'Creating staging schema...'
python3 create_staging_schema.py --scale $scale

echo 'Creating data warehouse schema...'
python3 create_dw_schema.py --scale $scale

# HISTORICAL PROCESS
cd historical

# Run the main historical script
echo 'Running historical main process...'
python3 main_historical.py --scale $scale
echo "Historical Load completed for scale $scale."
# INCREMENTAL PROCESS
cd ../incremental

# Run the main incremental script
echo 'Running incremental update 1 main process...'
python3 main_incremental.py --scale $scale --phase 1
echo "Incremental Update Phase 1 completed for scale $scale"

echo 'Running incremental update 2 main process...'
python3 main_incremental.py --scale $scale --phase 2
echo "Incremental Update Phase 1 completed for scale $scale"
