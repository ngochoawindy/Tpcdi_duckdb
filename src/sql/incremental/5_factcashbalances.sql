insert into master.factcashbalances (
    sk_customerid, sk_accountid, sk_dateid, cash, batchid
)
select    
    a.sk_customerid as sk_customerid,    
    a.sk_accountid as sk_accountid,    
    d.sk_dateid as sk_dateid,    
    coalesce(f.cash, 0) + sum(ct.ct_amt) as cash,    
    var_id as batchid    
from 
    staging.cashtransaction1 ct
join 
    master.dimaccount a on ct.ct_ca_id = a.accountid and a.iscurrent = 1
join 
    master.dimdate d on ct.ct_dts::date = d.datevalue
left join 
    master.factcashbalances f on f.sk_accountid = a.sk_accountid and f.sk_dateid = d.sk_dateid
group by 
    a.sk_customerid,
    a.sk_accountid,
    d.sk_dateid,
    f.cash
