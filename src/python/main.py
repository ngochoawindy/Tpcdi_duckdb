import duckdb
import argparse
from test_file import *

parser = argparse.ArgumentParser(description="Data Warehouse Schema for TPC-DI benchmarking")
parser.add_argument('--scale', '-s', help="Scale factor (3, x, x, x)", required=True, choices=['3', 'x', 'x', 'x'])
scale = parser.parse_args().scale

con = duckdb.connect(f'../../database/sc_{scale}.db')

test_functions = [
    test_TransformTradeType,
    test_TransformStatusType,
    test_TransformTaxRate,
    test_TransformIndustry,
    test_TransformDimDate,
    test_TransformDimTime,
    test_TransformDimCompany,
    test_TransformDimBroker,
    test_TransformProspect,
    test_TransformDimCustomer,
    test_TransformDimAccount,
    test_TransformDimSecurity,
    test_TransformDimTrade,
    test_TransformFinancial,
    test_TransformFactCashBalances,
    test_TransformFactHoldings,
    test_TransformFactWatches,
    test_TransformFactMarketHistory
]

time_all={}
# Run all tests
for test_func in test_functions:
    time_func = test_func(con)
    time_all.update(time_func)
    print(f'{test_func.__name__}', time_func)

import pandas as pd
time_df=pd.DataFrame(list(time_all.items()), columns=['Function', 'Time'])
time_df.to_csv('../result/time_df.csv', index=False)
