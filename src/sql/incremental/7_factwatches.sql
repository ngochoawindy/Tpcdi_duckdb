insert into master.factwatches
with watches as (
    select w1.w_c_id, 
           TRIM(w1.w_s_symb) as w_s_symb, 
           w1.w_dts::date as dateplaced, 
           w2.w_dts::date as dateremoved
    from staging.watchhistory1 w1
    join staging.watchhistory1 w2 
        on w1.w_c_id = w2.w_c_id
        and w1.w_s_symb = w2.w_s_symb
        and w1.w_action = 'ACTV'
        and w2.w_action = 'CNCL'
)

select 
    c.sk_customerid as sk_customerid,
    s.sk_securityid as sk_securityid,
    CAST(EXTRACT(YEAR FROM w.dateplaced) * 10000 + EXTRACT(MONTH FROM w.dateplaced) * 100 + EXTRACT(DAY FROM w.dateplaced) AS NUMERIC) as sk_dateid_dateplaced,
    CAST(EXTRACT(YEAR FROM w.dateremoved) * 10000 + EXTRACT(MONTH FROM w.dateremoved) * 100 + EXTRACT(DAY FROM w.dateremoved) AS NUMERIC) as sk_dateid_dateremoved,
    var_id as batchid
from watches w
join master.dimcustomer c
    on w.w_c_id = c.customerid
join master.dimsecurity s
    on w.w_s_symb = s.symbol
    and s.iscurrent = 1  
join master.dimdate d1
    on w.dateplaced = d1.datevalue
join master.dimdate d2
    on w.dateremoved = d2.datevalue;