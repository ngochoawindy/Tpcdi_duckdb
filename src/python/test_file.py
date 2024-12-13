import duckdb
import time
from tranform import TransformDimCustomer, TransformDimBroker,TransformFactCashBalances, TransformFinancial

con = duckdb.connect('/database/sc_3.db')


def test_TransformDimCustomer():
    sql_dim_customer = '/home/sakana/BDMA/DataWarehouse/TPC-DI/Tpcdi_duckdb/src/sql/4_dim_customer.sql'
    sql_dim_customer_message = '/home/sakana/BDMA/DataWarehouse/TPC-DI/Tpcdi_duckdb/src/sql/4_dimcustomer_message.sql'

    transform = TransformDimCustomer(con, sql_dim_customer, sql_dim_customer_message)
    res = {}
    time_start = time.time()
    transform.transform_load_master_dimcustomer()
    transform_load_master_dimcustomer_end = time.time()
    res["transform_load_master_dimcustomer"] = [transform_load_master_dimcustomer_end - time_start]
    load_master_dimessages_dimcustomer_end = time.time()
    transform.load_master_dimessages_dimcustomer()
    res["load_master_dimessages_dimcustomer"] = [
        load_master_dimessages_dimcustomer_end - transform_load_master_dimcustomer_end]
    return res
    
    
def test_TransformDimBroker():
    sql_dimbroker = '/home/sakana/BDMA/DataWarehouse/TPC-DI/Tpcdi_duckdb/src/sql/2_dim_broker.sql'

    transform = TransformDimBroker(con, sql_dimbroker)
    res = {}
    time_start = time.time()
    transform.transform_load_master_dimbroker()
    transform_load_master_dimbroker_end = time.time()
    res["transform_load_master_dimbroker"] = [transform_load_master_dimbroker_end - time_start]
    return res


def test_TransformFactCashBalances():
    sql_fact_cash_balances = '/home/sakana/BDMA/DataWarehouse/TPC-DI/Tpcdi_duckdb/src/sql/9_fact_cash_balances.sql'

    transform = TransformFactCashBalances(con, sql_fact_cash_balances)
    res = {}
    time_start = time.time()
    transform.transform_load_fact_cash_balances()
    transform_load_fact_cash_balances_end = time.time()
    res["transform_load_fact_cash_balances"] = [transform_load_fact_cash_balances_end - time_start]
    return res


def test_TransformFinancial():
    sql_financial = '/home/sakana/BDMA/DataWarehouse/TPC-DI/Tpcdi_duckdb/src/sql/14_financial.sql'

    transform = TransformFinancial(con, sql_financial)
    res = {}
    time_start = time.time()
    transform.transform_load_financial()
    transform_load_financial_end = time.time()
    res["transform_load_financial"] = [transform_load_financial_end - time_start]
    return res



if __name__ == '__main__':
    print('table_2_test_TransformDimBroker', test_TransformDimBroker())
    print('table_4_test_TransformDimCustomer', test_TransformDimCustomer())
    print('table_9_test_TransformFactCastBalances', test_TransformFactCashBalances())
    print('table_14_test_TransformFinancial', test_TransformFinancial())
