-- Insert New Records (cdc_flag = 'I')
insert into master.dimaccount 
select 
    row_number() over(order by a.ca_id) as sk_accountid,
    a.ca_id as accountid,
    b.sk_brokerid as sk_brokerid,
    c.sk_customerid as sk_customerid,
    st.st_name as status,
    a.ca_name as accountdesc,
    a.ca_tax_st as taxstatus,    
    1 as iscurrent, 
    var_id as batchid, 
    bd.batchdate::date as effectivedate, 
    '9999-12-31'::date as enddate
from staging.account a cross join staging.batchdate bd
left join master.dimbroker b on a.ca_b_id = b.brokerid and b.iscurrent = 1
left join master.dimcustomer c on a.ca_c_id = c.customerid and c.iscurrent = 1
left join master.statustype st on a.ca_st_id = st.st_id
where a.cdc_flag = 'I';

update master.dimaccount
set iscurrent = 0,
    enddate = bd.batchdate::date  
FROM
    staging.batchdate bd
where accountid in (
    select ca_id
    from staging.account
    where cdc_flag = 'U'
)
and iscurrent = 1;

insert into master.dimaccount 
select 
    row_number() over(order by a.ca_id) as sk_accountid,
    a.ca_id as accountid,
    b.sk_brokerid as sk_brokerid,
    c.sk_customerid as sk_customerid,
    st.st_name as status,
    a.ca_name as accountdesc,
    a.ca_tax_st as taxstatus,    
    1 as iscurrent, 
    var_id as batchid, 
    bd.batchdate::date as effectivedate, 
    '9999-12-31'::date as enddate
from staging.account a cross join staging.batchdate bd
left join master.dimbroker b on a.ca_b_id = b.brokerid and b.iscurrent = 1
left join master.dimcustomer c on a.ca_c_id = c.customerid and c.iscurrent = 1
left join master.statustype st on a.ca_st_id = st.st_id
where a.cdc_flag = 'U';

-- Handle Customer Status Change to "inactive"
update master.dimaccount
set status = 'INACTIVE'
where sk_customerid in (
    select sk_customerid
    from master.dimcustomer
    where status = 'INACTIVE' and iscurrent = 1
)
and iscurrent = 1;
