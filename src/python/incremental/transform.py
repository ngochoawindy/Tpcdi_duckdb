'''Transform & load master.prospect'''
class TransformProspect:
    def __init__(self, con, sql_prospect):
        self.con = con
        self.sql_prospect = sql_prospect        

    def transform_load_master_prospect(self, id):
        with open(self.sql_prospect, 'r') as sql_file:
            transform_load_master_prospect = sql_file.read()
            transform_load_master_prospect = transform_load_master_prospect.replace('var_id', str(id))
            transform_load_master_prospect = transform_load_master_prospect.replace('pre_id', str(id-1))
        self.con.sql(transform_load_master_prospect)    

'''Transform & load master.customer'''
class TransformDimCustomer:
    def __init__(self, con, sql_dim_customer):
        self.con = con
        self.sql_dim_customer = sql_dim_customer        

    def transform_load_master_dimcustomer(self, id):
        with open(self.sql_dim_customer, 'r') as sql_file:
            transform_load_master_dimcustomer = sql_file.read()
            transform_load_master_dimcustomer = transform_load_master_dimcustomer.replace('var_id', str(id))
        self.con.sql(transform_load_master_dimcustomer)

'''Transform & load master.dimaccount'''
class TransformDimAccount:
    def __init__(self, con, sql_dimaccount):
        self.con = con
        self.sql_dimaccount = sql_dimaccount

    def transform_load_master_dimaccount(self, id):
        with open(self.sql_dimaccount, 'r') as sql_file:
            transform_load_master_dimaccount = sql_file.read()
            transform_load_master_dimaccount = transform_load_master_dimaccount.replace('var_id', str(id))
        self.con.sql(transform_load_master_dimaccount)

'''Transform & load master.dimtrade'''
class TransformDimTrade:
    def __init__(self, con, sql_dimtrade):
        self.con = con
        self.sql_dimtrade = sql_dimtrade
        
    def transform_load_master_dimtrade(self, id):
        with open(self.sql_dimtrade, 'r') as sql_file:
            transform_load_master_dimtrade = sql_file.read()
            transform_load_master_dimtrade = transform_load_master_dimtrade.replace('var_id', str(id))
        self.con.sql(transform_load_master_dimtrade)

'''FactCashBalances'''
class TransformFactCashBalances:
    def __init__(self, con, sql_fact_cash_balances):
        self.con = con
        self.sql_fact_cash_balances = sql_fact_cash_balances

    def transform_load_fact_cash_balances(self, id):
        with open(self.sql_fact_cash_balances, 'r') as sql_file:
            transform_load_fact_cash_balances = sql_file.read()
            transform_load_fact_cash_balances = transform_load_fact_cash_balances.replace('var_id', str(id))
        self.con.sql(transform_load_fact_cash_balances)

'''FactMarketHistory'''
class TransformFactMarketHistory:
    def __init__(self, con, sql_fact_market_history):
        self.con = con
        self.sql_fact_market_history = sql_fact_market_history        

    def transform_load_fact_market_history(self, id):
        with open(self.sql_fact_market_history, 'r') as sql_file:
            transform_load_fact_market_history = sql_file.read()
            transform_load_fact_market_history = transform_load_fact_market_history.replace('var_id', str(id))
        self.con.sql(transform_load_fact_market_history)
    
'''FactWatches'''
class TransformFactWatches:
    def __init__(self, con, sql_fact_watches):
        self.con = con
        self.sql_fact_watches = sql_fact_watches

    def transform_load_fact_watches(self, id):
        with open(self.sql_fact_watches, 'r') as sql_file:
            transform_load_fact_watches = sql_file.read()
            transform_load_fact_watches = transform_load_fact_watches.replace('var_id', str(id))
        self.con.sql(transform_load_fact_watches)