import duckdb
import time
from tranform import *


def run_TransformTradeType(con):
    sql_trade_type = '../sql/load_tradetype.sql'
    transform = TransformTradeType(con, sql_trade_type)
    res = {}
    time_start = time.time()
    transform.transform_load_master_tradetype()
    transform_load_master_tradetype_end = time.time()
    res["transform_load_master_tradetype"] =transform_load_master_tradetype_end - time_start
    return res


def run_TransformStatusType(con):
    sql_status_type = '../sql/statustype.sql'
    transform = TransformStatusType(con, sql_status_type)
    res = {}
    time_start = time.time()
    transform.transform_load_master_statustype()
    transform_load_master_statustype_end = time.time()
    res["transform_load_master_statustype"] =transform_load_master_statustype_end - time_start
    return res


def run_TransformTaxRate(con):
    sql_tax_rate = '../sql/taxrate.sql'
    transform = TransformTaxRate(con, sql_tax_rate)
    res = {}
    time_start = time.time()
    transform.transform_load_master_taxrate()
    transform_load_master_taxrate_end = time.time()
    res["transform_load_master_taxrate"] =transform_load_master_taxrate_end - time_start
    return res


def run_TransformIndustry(con):
    sql_industry = '../sql/industry.sql'
    transform = TransformIndustry(con, sql_industry)
    res = {}
    time_start = time.time()
    transform.transform_load_master_industry()
    transform_load_master_industry_end = time.time()
    res["transform_load_master_industry"] =transform_load_master_industry_end - time_start
    return res


def run_TransformDimDate(con):
    sql_dimdate = '../sql/dimdate.sql'
    transform = TransformDimDate(con, sql_dimdate)
    res = {}
    time_start = time.time()
    transform.transform_load_master_dimdate()
    transform_load_master_dimdate_end = time.time()
    res["transform_load_master_dimdate"] =transform_load_master_dimdate_end - time_start
    return res


def run_TransformDimTime(con):
    sql_dimtime = '../sql/dimtime.sql'
    transform = TransformDimTime(con, sql_dimtime)
    res = {}
    time_start = time.time()
    transform.transform_load_master_dimtime()
    transform_load_master_dimtime_end = time.time()
    res["transform_load_master_dimtime"] =transform_load_master_dimtime_end - time_start
    return res


def run_TransformDimCompany(con):
    sql_dimcompany = '../sql/dimcompany.sql'
    sql_dimmessage_dimcompany = '../sql/dimcompany_dimessages.sql'
    transform = TransformDimCompany(con, sql_dimcompany, sql_dimmessage_dimcompany)
    res = {}
    time_start = time.time()
    transform.transform_load_master_dimcompany()
    transform_load_master_dimcompany_end = time.time()
    res["transform_load_master_dimcompany"] =transform_load_master_dimcompany_end - time_start

    load_master_dimmessage_dimcompany_start = time.time()
    transform.transform_load_master_dimmessage_dimcompany()
    load_master_dimmessage_dimcompany_end = time.time()
    res["transform_load_master_dimmessage_dimcompany"] = load_master_dimmessage_dimcompany_end - load_master_dimmessage_dimcompany_start
    return res


def run_TransformDimBroker(con):
    sql_dimbroker = '../sql/dimbroker.sql'
    transform = TransformDimBroker(con, sql_dimbroker)
    res = {}
    time_start = time.time()
    transform.transform_load_master_dimbroker()
    transform_load_master_dimbroker_end = time.time()
    res["transform_load_master_dimbroker"] =transform_load_master_dimbroker_end - time_start
    return res


def run_TransformProspect(con):
    sql_prospect = '../sql/prospect.sql'
    transform = TransformProspect(con, sql_prospect, None)
    res = {}
    time_start = time.time()
    transform.transform_load_master_prospect()
    transform_load_master_prospect_end = time.time()
    res["transform_load_master_prospect"] =transform_load_master_prospect_end - time_start
    return res


def run_TransformDimCustomer(con):
    sql_dim_customer = '../sql/dimcustomer.sql'
    sql_dim_customer_message = '../sql/dimcustomer_dimmessage.sql'
    transform = TransformDimCustomer(con, sql_dim_customer, sql_dim_customer_message)
    res = {}
    time_start = time.time()
    transform.transform_load_master_dimcustomer()
    transform_load_master_dimcustomer_end = time.time()
    res["transform_load_master_dimcustomer"] =transform_load_master_dimcustomer_end - time_start

    load_master_dimessages_dimcustomer_start = time.time()
    transform.load_master_dimessages_dimcustomer()
    load_master_dimessages_dimcustomer_end = time.time()
    res["load_master_dimessages_dimcustomer"] = load_master_dimessages_dimcustomer_end - load_master_dimessages_dimcustomer_start

    sql_update_prospect = '../sql/update_prospect.sql'
    transform = TransformProspect(con, None, sql_update_prospect)
    update_master_prospect_start = time.time()
    transform.update_master_prospect()
    update_master_prospect_end = time.time()
    res["update_master_prospect"] =update_master_prospect_end - update_master_prospect_start
    return res


def run_TransformDimAccount(con):
    sql_dimaccount = '../sql/dimaccount.sql'
    transform = TransformDimAccount(con, sql_dimaccount)
    res = {}
    time_start = time.time()
    transform.transform_load_master_dimaccount()
    transform_load_master_dimaccount_end = time.time()
    res["transform_load_master_dimaccount"] =transform_load_master_dimaccount_end - time_start
    return res


def run_TransformDimSecurity(con):
    sql_dimsecurity = '../sql/dimsecurity.sql'
    transform = TransformDimSecurity(con, sql_dimsecurity)
    res = {}
    time_start = time.time()
    transform.transform_load_master_dimsecurity()
    transform_load_master_dimsecurity_end = time.time()
    res["transform_load_master_dimsecurity"] =transform_load_master_dimsecurity_end - time_start
    return res


def run_TransformDimTrade(con):
    sql_trade_joined = '../sql/trade_joined.sql'
    sql_dimtrade = '../sql/dimtrade.sql'
    sql_dimtrade_message = '../sql/dimessages_dimtrade.sql'
    transform = TransformDimTrade(con, sql_dimtrade, sql_dimtrade_message,)

    res = {}


    time_start = time.time()
    transform.transform_load_master_dimtrade()
    transform_load_master_dimtrade_end = time.time()
    res["transform_load_master_dimtrade"] =transform_load_master_dimtrade_end - time_start

    load_master_dimessages_dimtrade_start = time.time()
    transform.load_master_dimessages_dimtrade()
    load_master_dimessages_dimtrade_end = time.time()
    res["load_master_dimessages_dimtrade"] =  load_master_dimessages_dimtrade_end - load_master_dimessages_dimtrade_start
    return res


def run_TransformFinancial(con):
    sql_financial = '../sql/financial.sql'
    transform = TransformFinancial(con, sql_financial)
    res = {}
    time_start = time.time()
    transform.transform_load_financial()
    transform_load_financial_end = time.time()
    res["transform_load_financial"] =transform_load_financial_end - time_start
    return res


def run_TransformFactCashBalances(con):
    sql_fact_cash_balances = '../sql/fact_cash_balances.sql'
    transform = TransformFactCashBalances(con, sql_fact_cash_balances)
    res = {}
    time_start = time.time()
    transform.transform_load_fact_cash_balances()
    transform_load_fact_cash_balances_end = time.time()
    res["transform_load_fact_cash_balances"] =transform_load_fact_cash_balances_end - time_start
    return res


def run_TransformFactHoldings(con):
    sql_fact_holdings = '../sql/factholdings.sql'
    transform = TransformFactHoldings(con, sql_fact_holdings)
    res = {}
    time_start = time.time()
    transform.transform_load_fact_holdings()
    transform_load_fact_holdings_end = time.time()
    res["transform_load_fact_holdings"] =transform_load_fact_holdings_end - time_start
    return res


def run_TransformFactWatches(con):
    sql_fact_watches = '../sql/factwatches.sql'
    transform = TransformFactWatches(con, sql_fact_watches)
    res = {}
    time_start = time.time()
    transform.transform_load_fact_watches()
    transform_load_fact_watches_end = time.time()
    res["transform_load_fact_watches"] =transform_load_fact_watches_end - time_start
    return res


def run_TransformFactMarketHistory(con):
    sql_fact_market_history = '../sql/factmarkethistory.sql'
    sql_fact_market_history_message = '../sql/factmarkethistory_dimessages.sql'
    transform = TransformFactMarketHistory(con, sql_fact_market_history, sql_fact_market_history_message)
    res = {}
    time_start = time.time()
    transform.transform_load_fact_market_history()
    transform_load_fact_market_history_end = time.time()
    res["transform_load_fact_market_history"] =transform_load_fact_market_history_end - time_start

    load_fact_market_history_message_start = time.time()
    transform.load_fact_market_history_message()
    load_fact_market_history_message_end = time.time()
    res["load_fact_market_history_message"] = load_fact_market_history_message_end - load_fact_market_history_message_start
    return res


# if __name__ == '__main__':
#     con = duckdb.connect('../../database/sc_3.db')
#     # List of all test functions
#     test_functions =[
#         run_TransformTradeType,
#         run_TransformStatusType,
#         run_TransformTaxRate,
#         run_TransformIndustry,
#         run_TransformDimDate,
#         run_TransformDimTime,
#         run_TransformDimCompany,
#         run_TransformDimBroker,
#         run_TransformProspect,
#         run_TransformDimCustomer,
#         run_TransformDimAccount,
#         run_TransformDimSecurity,
#         run_TransformDimTrade,
#         run_TransformFinancial,
#         run_TransformFactCashBalances,
#         run_TransformFactHoldings,
#         run_TransformFactWatches,
#         run_TransformFactMarketHistory
#     ]

#     # Run all tests
#     for test_func in test_functions:
#         print(f'{test_func.__name__}', test_func(con))
