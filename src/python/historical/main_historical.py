import time
import pandas as pd
import duckdb
import argparse
from run_transform import *
from load_staging import load_staging
from load_staging_customermgmt import parse_load_customer_mgmt
from process_finwire import process_finwire

parser = argparse.ArgumentParser(description="Historical Load for TPC-DI benchmarking")
parser.add_argument('--scale', '-s', help="Scale factor (3, 5, 7, 9)", required=True, choices=['3', '5', '7', '9'])
scale = parser.parse_args().scale

con = duckdb.connect(f'../../../database/sc_{scale}.db')


time_load={}
print('start loading staging')
time_load_staging_start=time.time()
load_staging(con, scale)
print('start process finwire')
process_finwire(con)
print('start parse load customer')
parse_load_customer_mgmt(con, scale)
time_load_staging_end=time.time()
time_load['load_staging']=time_load_staging_end-time_load_staging_start
print('end loading staging')
print('start load datawarehouse')

run_functions = [
    run_TransformTradeType,
    run_TransformStatusType,
    run_TransformTaxRate,
    run_TransformIndustry,
    run_TransformDimDate,
    run_TransformDimTime,
    run_TransformDimCompany,
    run_TransformDimBroker,
    run_TransformProspect,
    run_TransformDimCustomer,
    run_TransformDimAccount,
    run_TransformDimSecurity,
    run_TransformDimTrade,
    run_TransformFinancial,
    run_TransformFactCashBalances,
    run_TransformFactHoldings,
    run_TransformFactWatches,
    run_TransformFactMarketHistory
]

# time_all={}
# # Run all tests
# for test_func in test_functions:
#     time_func = test_func(con)
#     time_all.update(time_func)
#     print(f'{test_func.__name__}', time_func)
#
# import pandas as pd
# time_df=pd.DataFrame(list(time_all.items()), columns=['Function', 'Time'])
# time_df.to_csv('../result/time_df.csv', index=False)
time_dw={}
time_load_datawarehouse_start=time.time()
for run_func in run_functions:
    time_func=run_func(con)
    time_dw.update(time_func)
time_load_datawarehouse_end=time.time()
time_load['load_datawarehouse']=time_load_datawarehouse_end-time_load_datawarehouse_start

time_load_df=pd.DataFrame(list(time_load.items()), columns=['Load_Type', 'Time'])
time_load_df.to_csv('../../result/His_time_staging_DW_'+scale+'.csv', index=False)


time_dw_df=pd.DataFrame(list(time_dw.items()), columns=['Function', 'Time'])
time_dw_df.to_csv('../../result/His_time_DW_'+scale+'.csv', index=False)