import time
import pandas as pd
import duckdb
import argparse
from run_transform import *
from load_staging_incremental import load_staging_incremental

parser = argparse.ArgumentParser(description="Incremental Update for TPC-DI benchmarking")
parser.add_argument('--scale', '-s', help="Scale factor (3, 5, 7, 9)", required=True, choices=['3', '5', '7', '9'])
parser.add_argument('--phase', '-p', help="Incremental Update Phase (1,2)", required=True, choices=['1', '2'])

args = parser.parse_args()
scale = args.scale
phase = args.phase

con = duckdb.connect(f'../../../database/sc_{scale}.db')

id = int(phase) + 1

time_load={}
print('start loading staging')
time_load_staging_incremental_start=time.time()
load_staging_incremental(con, scale, id)
time_load_staging_incremental_end=time.time()
time_load['load_staging_incremental']=time_load_staging_incremental_end-time_load_staging_incremental_start
print('end loading staging')

print('start transforming and loading datawarehouse')

run_functions = [    
    run_TransformProspect,
    run_TransformDimCustomer,
    run_TransformDimAccount,    
    run_TransformDimTrade,    
    run_TransformFactCashBalances,    
    run_TransformFactMarketHistory,
    run_TransformFactWatches
]

time_dw={}
time_load_datawarehouse_start=time.time()
for run_func in run_functions:
    time_func=run_func(con, id)
    time_dw.update(time_func)
time_load_datawarehouse_end=time.time()
time_load['load_datawarehouse']=time_load_datawarehouse_end-time_load_datawarehouse_start

time_load_df=pd.DataFrame(list(time_load.items()), columns=['Load_Type', 'Time'])
time_load_df.to_csv('../../result/Inc_'+phase+'_time_staging_DW_'+scale+'.csv', index=False)


time_dw_df=pd.DataFrame(list(time_dw.items()), columns=['Function', 'Time'])
time_dw_df.to_csv('../../result/Inc_'+phase+'_time_DW_'+scale+'.csv', index=False)