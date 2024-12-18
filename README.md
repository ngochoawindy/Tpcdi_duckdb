# TPC-DI Benchmark Using DuckDB

This project implements the TPC-DI benchmark using DuckDB, an open-source relational database management system. The benchmark is implemented with four different scale factors and the system performance is evaluated on three phases: Historical Load Phase, Incremental Update Phase 1, and Incremental Update Phase 2, carried out in this order.

## Set Up

1. Clone the repository
   ```bash
   git clone https://github.com/ngochoawindy/Tpcdi_duckdb
   cd Tpcdi_duckdb
    ```
2. Install DuckDB
   ```bash
   pip install duckdb
   ```
3. Download TPC-DS Tools
   - Go to Go to [TPC-DI Official Site](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp) and download `TPC-DI_Tools_v1.1.0.zip`.
   - Unzip the zip file to a folder named tpcdi-kit

## Usage

### Generate Source Data and Copy to Staging Area 
- Go to folder tools and run the command to generate data using DIGen: 
```bash
java -jar DIGen.jar -sf 3 -o /generated_data/scale_3
java -jar DIGen.jar -sf 5 -o /generated_data/scale_5
java -jar DIGen.jar -sf 7 -o /generated_data/scale_7
java -jar DIGen.jar -sf 9 -o /generated_data/scale_9
```
- Copy all generated data to folder /staging_area/scale_{sc} (sc = 3, 5, 7, 9) 

### Initialization Phase 

- Create Staging Schema 

```bash
cd src/python
python3 create_staging_schema.py --scale $scale
```
- Create Data Warehouse Schema 
In the folder src/python:

```bash
python3 create_dw_schema.py --scale $scale
```

### Running the Benchmark
The run_all.sh script, located in the src folder, automates the benchmark (historical load, incremental update 1, incremental update 2). Run it as follows:
```bash
cd scr
bash run_all.sh $scale
```
