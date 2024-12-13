# Tpcdi_duckdb

## Generate Source Data and Copy to Staging Area 

Generate Data using DIGen:

```bash
java -jar DIGen.jar -sf 3 -o /generated_data/scale_3
```

Copy all generated data to folder /staging_area

## Initialization Phase 

### Create Staging Schema 

```bash
cd src/python
python3 create_staging_schema.py --scale $scale
```


### Create Data Warehouse Schema 
In the folder src/python:

```bash
python3 create_dw_schema.py --scale $scale
```

## Historical Load Phase 

### Load Data to Staging Schema
1. Load txt, csv, finwire: load_staging.py (later will be called in main and track the time): load_staging.py
2. For FINWIRE (2.2.8): there are 3 file fields needed to be separated:CMP, SEC, FIN and load: process_finwire.py
3. Parse customerMgmt.xml to .csv and load to staging: load_staging_customermgmt.py

Staging Schema Load: DONE !

### Transform and Load to Data Warehouse schema