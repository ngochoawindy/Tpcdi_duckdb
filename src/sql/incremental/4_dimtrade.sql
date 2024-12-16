insert into master.dimtrade(
    tradeid, cashflag, quantity, bidprice, executedby, tradeprice, fee, commission, tax,
    status, type, sk_securityid, sk_companyid, sk_accountid, sk_customerid, sk_brokerid,
    sk_createdateid, sk_createtimeid, sk_closedateid, sk_closetimeid, batchid
)
select     
    t.t_id as tradeid, 
    t.t_is_cash as cashflag, 
    t.t_qty as quantity, 
    t.t_bid_price as bidprice, 
    t.t_exec_name as executedby, 
    t.t_trade_price as tradeprice,
    t.t_chrg as fee, 
    t.t_comm as commission, 
    t.t_tax as tax, 
    st.st_name as status, 
    tt.tt_name as type, 
    s.sk_securityid as sk_securityid, 
    s.sk_companyid as sk_companyid, 
    a.sk_accountid as sk_accountid, 
    c.sk_customerid as sk_customerid, 
    a.sk_brokerid as sk_brokerid,     
    cast(extract(epoch from t.t_dts::date) as numeric) as sk_createdateid, 
    cast(extract(epoch from t.t_dts::time) as numeric) as sk_createtimeid,     
    NULL as sk_closedateid, 
    NULL as sk_closetimeid, 
    var_id as batchid,    
from staging.trade1 t
left join master.statustype st on t.t_st_id = st.st_id
left join master.tradetype tt on t.t_tt_id = tt.tt_id
left join master.dimsecurity s on t.t_s_symb = s.symbol and s.iscurrent = 1
left join master.dimaccount a on t.t_ca_id = a.accountid and a.iscurrent = 1
left join master.dimcustomer c on t.t_id = c.customerid and c.iscurrent = 1
where t.cdc_flag = 'I';

-- Update existing records (cdc_flag = 'U')
update master.dimtrade
set 
    cashflag = t.t_is_cash,
    quantity = t.t_qty,
    bidprice = t.t_bid_price,
    executedby = t.t_exec_name,
    tradeprice = t.t_trade_price,
    fee = t.t_chrg,
    commission = t.t_comm,
    tax = t.t_tax,
    status = st.st_name,
    type = tt.tt_name,
    sk_securityid = s.sk_securityid,
    sk_companyid = s.sk_companyid,
    sk_accountid = a.sk_accountid,
    sk_customerid = c.sk_customerid,
    sk_brokerid = a.sk_brokerid,    
    sk_closedateid = case 
        when t.t_st_id in ('CMPT', 'CNCL') then cast(extract(epoch from t.t_dts::date) as numeric)
        else NULL 
    end,
    sk_closetimeid = case 
        when t.t_st_id in ('CMPT', 'CNCL') then cast(extract(epoch from t.t_dts::time) as numeric)
        else NULL 
    end  
from staging.trade1 t
left join master.statustype st on t.t_st_id = st.st_id
left join master.tradetype tt on t.t_tt_id = tt.tt_id
left join master.dimsecurity s on t.t_s_symb = s.symbol and s.iscurrent = 1
left join master.dimaccount a on t.t_ca_id = a.accountid and a.iscurrent = 1
left join master.dimcustomer c on t.t_id = c.customerid and c.iscurrent = 1
where t.cdc_flag = 'U'
and master.dimtrade.tradeid = t.t_id;

-- Report invalid commission and fee cases in dimessages
insert into master.dimessages
select 
    now(),
    var_id,
    'DimTrade' as messagesource,
    'Alert' as messagetype,
    'Invalid trade commission' as messagetext,
    concat('t_id = ', t.t_id, ', t_comm = ', t.t_comm) as messagedata
from staging.trade1 t
where t.t_comm is not null
and t.t_comm > (t.t_trade_price * t.t_qty);

insert into master.dimessages 
select 
    now(),
    var_id,
    'DimTrade' as messagesource,
    'Alert' as messagetype,
    'Invalid trade fee' as messagetext,
    concat('t_id = ', t.t_id, ', t_chrg = ', t.t_chrg) as messagedata
from staging.trade1 t
where t.t_chrg is not null
and t.t_chrg > (t.t_trade_price * t.t_qty);