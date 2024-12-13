'''Tabel 2: DimBroker'''
class TransformDimBroker:
    def __init__(self, con, sql_dimbroker):
        self.con = con
        self.sql_dim_customer = sql_dimbroker

    def transform_load_master_dimbroker(self):
        with open(self.sql_dim_customer, 'r') as sql_file:
            transform_load_master_dimbroker = sql_file.read()
        self.con.sql(transform_load_master_dimbroker)



'''
Tabel 4: DimCustomer, DimMessage
'''

class TransformDimCustomer:
    def __init__(self, con, sql_dim_customer, sql_dim_customer_message):
        self.con = con
        self.sql_dim_customer = sql_dim_customer
        self.sql_dim_customer_message = sql_dim_customer_message

    def transform_load_master_dimcustomer(self):
        with open(self.sql_dim_customer, 'r') as sql_file:
            transform_load_master_dimcustomer = sql_file.read()
        self.con.sql(transform_load_master_dimcustomer)

    def load_master_dimessages_dimcustomer(self):
        with open(self.sql_dim_customer_message, 'r') as sql_file:
            load_master_dimessages_dimcustomer = sql_file.read()
        self.con.sql(load_master_dimessages_dimcustomer)



'''Tabel 9: FactCashBalances'''
class TransformFactCashBalances:
    def __init__(self, con, sql_fact_cash_balances):
        self.con = con
        self.sql_fact_cash_balances = sql_fact_cash_balances

    def transform_load_fact_cash_balances(self):
        with open(self.sql_fact_cash_balances, 'r') as sql_file:
            transform_load_fact_cash_balances = sql_file.read()
        self.con.sql(transform_load_fact_cash_balances)


'''Tabel 14: Financial'''
class TransformFinancial:
    def __init__(self, con, sql_financial):
        self.con = con
        self.sql_financial = sql_financial

    def transform_load_financial(self):
        with open(self.sql_financial, 'r') as sql_file:
            transform_load_financial = sql_file.read()
        self.con.sql(transform_load_financial)