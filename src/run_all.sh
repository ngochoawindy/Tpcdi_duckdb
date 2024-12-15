#!/bin/bash

# Default scale value
scale=1

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

echo $scale

cd python

echo 'create_staging_schema'
python create_staging_schema.py --scale $scale
echo 'create_dw_schema.py'
python create_dw_schema.py --scale $scale
echo 'main.py'
python main.py --scale $scale


