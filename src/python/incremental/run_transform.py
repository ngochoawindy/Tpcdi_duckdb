import duckdb
import time
from transform import *

def run_TransformProspect(con, id):
    sql_prospect = '../../sql/incremental/1_prospect.sql'
    transform = TransformProspect(con, sql_prospect)
    res = {}
    time_start = time.time()
    transform.transform_load_master_prospect(id)
    transform_load_master_prospect_end = time.time()
    res["transform_load_master_prospect"] =transform_load_master_prospect_end - time_start
    return res


def run_TransformDimCustomer(con, id):
    sql_dim_customer = '../../sql/incremental/2_dimcustomer.sql'    
    transform = TransformDimCustomer(con, sql_dim_customer)
    res = {}
    time_start = time.time()
    transform.transform_load_master_dimcustomer(id)
    transform_load_master_dimcustomer_end = time.time()
    res["transform_load_master_dimcustomer"] =transform_load_master_dimcustomer_end - time_start
    return res


def run_TransformDimAccount(con, id):
    sql_dimaccount = '../../sql/incremental/3_dimaccount.sql'
    transform = TransformDimAccount(con, sql_dimaccount)
    res = {}
    time_start = time.time()
    transform.transform_load_master_dimaccount(id)
    transform_load_master_dimaccount_end = time.time()
    res["transform_load_master_dimaccount"] =transform_load_master_dimaccount_end - time_start
    return res


def run_TransformDimTrade(con, id):    
    sql_dimtrade = '../../sql/incremental/4_dimtrade.sql'
    sql_dimtrade_message = '../sql/incremental/dimessages_dimtrade.sql'
    transform = TransformDimTrade(con, sql_dimtrade)

    res = {}
    time_start = time.time()
    transform.transform_load_master_dimtrade(id)
    transform_load_master_dimtrade_end = time.time()
    res["transform_load_master_dimtrade"] =transform_load_master_dimtrade_end - time_start

    return res


def run_TransformFactCashBalances(con, id):
    sql_fact_cash_balances = '../../sql/incremental/5_factcashbalances.sql'
    transform = TransformFactCashBalances(con, sql_fact_cash_balances)
    res = {}
    time_start = time.time()
    transform.transform_load_fact_cash_balances(id)
    transform_load_fact_cash_balances_end = time.time()
    res["transform_load_fact_cash_balances"] =transform_load_fact_cash_balances_end - time_start
    return res

def run_TransformFactWatches(con, id):
    sql_fact_watches = '../../sql/incremental/7_factwatches.sql'
    transform = TransformFactWatches(con, sql_fact_watches)
    res = {}
    time_start = time.time()
    transform.transform_load_fact_watches(id)
    transform_load_fact_watches_end = time.time()
    res["transform_load_fact_watches"] =transform_load_fact_watches_end - time_start
    return res


def run_TransformFactMarketHistory(con, id):
    sql_fact_market_history = '../../sql/incremental/6_factmarkethistory.sql'    
    transform = TransformFactMarketHistory(con, sql_fact_market_history)
    res = {}
    time_start = time.time()
    transform.transform_load_fact_market_history(id)
    transform_load_fact_market_history_end = time.time()
    res["transform_load_fact_market_history"] =transform_load_fact_market_history_end - time_start
    
    return res
