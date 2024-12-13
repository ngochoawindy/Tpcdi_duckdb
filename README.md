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
1. Load txt, csv, finwire: load_staging.py (later will be called in main and track the time)
2. 

### Transform and Load to Data Warehouse schema