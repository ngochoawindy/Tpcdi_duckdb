'''master.tradetype, master.statustype, master.taxrate, master.industry'''
class TransformTradeType:
    def __init__(self, con, sql_trade_type):
        self.con = con
        self.sql_trade_type = sql_trade_type

    def transform_load_master_tradetype(self):
        with open(self.sql_trade_type, 'r') as sql_file:
            transform_load_master_tradetype = sql_file.read()
        self.con.sql(transform_load_master_tradetype)

class TransformStatusType:
    def __init__(self, con, sql_status_type):
        self.con = con
        self.sql_status_type = sql_status_type

    def transform_load_master_statustype(self):
        with open(self.sql_status_type, 'r') as sql_file:
            transform_load_master_statustype = sql_file.read()
        self.con.sql(transform_load_master_statustype)

class TransformTaxRate:
    def __init__(self, con, sql_tax_rate):
        self.con = con
        self.sql_tax_rate = sql_tax_rate

    def transform_load_master_taxrate(self):
        with open(self.sql_tax_rate, 'r') as sql_file:
            transform_load_master_taxrate = sql_file.read()
        self.con.sql(transform_load_master_taxrate)

class TransformIndustry:
    def __init__(self, con, sql_industry):
        self.con = con
        self.sql_industry = sql_industry

    def transform_load_master_industry(self):
        with open(self.sql_industry, 'r') as sql_file:
            transform_load_master_industry = sql_file.read()
        self.con.sql(transform_load_master_industry)


'''Transform & load master.dimdate, master.dimtime, master.dimcompany'''
class TransformDimDate:
    def __init__(self, con, sql_dimdate):
        self.con = con
        self.sql_dimdate = sql_dimdate

    def transform_load_master_dimdate(self):
        with open(self.sql_dimdate, 'r') as sql_file:
            transform_load_master_dimdate = sql_file.read()
        self.con.sql(transform_load_master_dimdate)

class TransformDimTime:
    def __init__(self, con, sql_dimtime):
        self.con = con
        self.sql_dimtime = sql_dimtime

    def transform_load_master_dimtime(self):
        with open(self.sql_dimtime, 'r') as sql_file:
            transform_load_master_dimtime = sql_file.read()
        self.con.sql(transform_load_master_dimtime)

class TransformDimCompany:
    def __init__(self, con, sql_dimcompany, sql_dimmessage_dimcompany):
        self.con = con
        self.sql_dimcompany = sql_dimcompany
        self.sql_dimmessage_dimcompany = sql_dimmessage_dimcompany

    def transform_load_master_dimcompany(self):
        with open(self.sql_dimcompany, 'r') as sql_file:
            transform_load_master_dimcompany = sql_file.read()
        self.con.sql(transform_load_master_dimcompany)

    def transform_load_master_dimmessage_dimcompany(self):
        with open(self.sql_dimmessage_dimcompany, 'r') as sql_file:
            transform_load_master_dimmessage_dimcompany = sql_file.read()
        self.con.sql(transform_load_master_dimmessage_dimcompany)


'''Transform & load master.dimbroker, prospect, customer, dimessages_customer'''
class TransformDimBroker:
    def __init__(self, con, sql_dimbroker):
        self.con = con
        self.sql_dim_customer = sql_dimbroker

    def transform_load_master_dimbroker(self):
        with open(self.sql_dim_customer, 'r') as sql_file:
            transform_load_master_dimbroker = sql_file.read()
        self.con.sql(transform_load_master_dimbroker)


class TransformProspect:
    def __init__(self, con, sql_prospect, sql_update_prospect):
        self.con = con
        self.sql_prospect = sql_prospect
        self.sql_update_prospect= sql_update_prospect

    def transform_load_master_prospect(self):
        with open(self.sql_prospect, 'r') as sql_file:
            transform_load_master_prospect = sql_file.read()
        self.con.sql(transform_load_master_prospect)

    def update_master_prospect(self):
        with open(self.sql_update_prospect, 'r') as sql_file:
            update_prospect = sql_file.read()
        self.con.sql(update_prospect)


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

'''DimAccount'''
class TransformDimAccount:
    def __init__(self, con, sql_dimaccount):
        self.con = con
        self.sql_dimaccount = sql_dimaccount

    def transform_load_master_dimaccount(self):
        with open(self.sql_dimaccount, 'r') as sql_file:
            transform_load_master_dimaccount = sql_file.read()
        self.con.sql(transform_load_master_dimaccount)

'''DimSecurity'''
class TransformDimSecurity:
    def __init__(self, con, sql_dimsecurity):
        self.con = con
        self.sql_dimsecurity = sql_dimsecurity

    def transform_load_master_dimsecurity(self):
        with open(self.sql_dimsecurity, 'r') as sql_file:
            transform_load_master_dimsecurity = sql_file.read()
        self.con.sql(transform_load_master_dimsecurity)

'''DimTrade'''
class TransformDimTrade:
    def __init__(self, con, sql_dimtrade, sql_dimtrade_message):
        self.con = con
        self.sql_dimtrade = sql_dimtrade
        self.sql_dimtrade_message = sql_dimtrade_message

    def transform_load_master_dimtrade(self):
        with open(self.sql_dimtrade, 'r') as sql_file:
            transform_load_master_dimtrade = sql_file.read()
        self.con.sql(transform_load_master_dimtrade)

    def load_master_dimessages_dimtrade(self):
        with open(self.sql_dimtrade_message, 'r') as sql_file:
            load_master_dimessages_dimtrade = sql_file.read()
        self.con.sql(load_master_dimessages_dimtrade)


'''Financial'''
class TransformFinancial:
    def __init__(self, con, sql_financial):
        self.con = con
        self.sql_financial = sql_financial

    def transform_load_financial(self):
        with open(self.sql_financial, 'r') as sql_file:
            transform_load_financial = sql_file.read()
        self.con.sql(transform_load_financial)

'''FactCashBalances'''
class TransformFactCashBalances:
    def __init__(self, con, sql_fact_cash_balances):
        self.con = con
        self.sql_fact_cash_balances = sql_fact_cash_balances

    def transform_load_fact_cash_balances(self):
        with open(self.sql_fact_cash_balances, 'r') as sql_file:
            transform_load_fact_cash_balances = sql_file.read()
        self.con.sql(transform_load_fact_cash_balances)


'''FactHoldings'''
class TransformFactHoldings:
    def __init__(self, con, sql_fact_holdings):
        self.con = con
        self.sql_fact_holdings = sql_fact_holdings

    def transform_load_fact_holdings(self):
        with open(self.sql_fact_holdings, 'r') as sql_file:
            transform_load_fact_holdings = sql_file.read()
        self.con.sql(transform_load_fact_holdings)


'''FactWatches'''
class TransformFactWatches:
    def __init__(self, con, sql_fact_watches):
        self.con = con
        self.sql_fact_watches = sql_fact_watches

    def transform_load_fact_watches(self):
        with open(self.sql_fact_watches, 'r') as sql_file:
            transform_load_fact_watches = sql_file.read()
        self.con.sql(transform_load_fact_watches)



'''FactMarketHistory'''
class TransformFactMarketHistory:
    def __init__(self, con, sql_fact_market_history, sql_fact_market_history_message):
        self.con = con
        self.sql_fact_market_history = sql_fact_market_history
        self.sql_fact_market_history_message = sql_fact_market_history_message

    def transform_load_fact_market_history(self):
        with open(self.sql_fact_market_history, 'r') as sql_file:
            transform_load_fact_market_history = sql_file.read()
        self.con.sql(transform_load_fact_market_history)

    def load_fact_market_history_message(self):
        with open(self.sql_fact_market_history_message, 'r') as sql_file:
            load_fact_market_history_message = sql_file.read()
        self.con.sql(load_fact_market_history_message)


