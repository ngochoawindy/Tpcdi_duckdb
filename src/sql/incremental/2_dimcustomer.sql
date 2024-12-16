-- Insert New Customers (CDC_FLAG = 'I')

INSERT INTO master.dimcustomer (
    sk_customerid, customerid, taxid, lastname, firstname, middleinitial, gender, tier, dob,
    addressline1, addressline2, postalcode, city, stateprov, country,
    phone1, phone2, phone3, email1, email2, status, nationaltaxratedesc, nationaltaxrate,
    localtaxratedesc, localtaxrate, agencyid, creditrating, networth, marketingnameplate,
    iscurrent, batchid, effectivedate, enddate
)
SELECT
    row_number() over(order by c.c_id) as sk_customerid,
    c.c_id as customerid,
    c.c_tax_id as taxid,
    c.c_l_name as lastname,
    c.c_f_name as firstname,
    c.c_m_name as middleinitial,    
    case when c.c_gndr IN ('M', 'F') then upper(c.c_gndr) else 'U' end as gender,
    c.c_tier as tier,
    c.c_dob as dob,
    c.c_adline1 as addressline1,
    c.c_adline2 as addressline2,
    c.c_zipcode as postalcode,
    c.c_city as city,
    c.c_state_prov as stateprov,
    c.c_ctry as country,    
    case 
        when c.c_ctry_1 is not null and c.c_area_1 is not null and c.c_local_1 is not null then 
            concat('+', c.c_ctry_1, ' (', c.c_area_1, ') ', c.c_local_1)
        when c.c_ctry_1 is null and c.c_area_1 is not null and c.c_local_1 is not null then
            concat('(', c.c_area_1, ') ', c.c_local_1)
        when c.c_area_1 is null and c.c_local_1 is not null then
            c.c_local_1
        else null
    end as phone1,
    case 
        when c.c_ctry_2 is not null and c.c_area_2 is not null and c.c_local_2 is not null then 
            concat('+', c.c_ctry_2, ' (', c.c_area_2, ') ', c.c_local_2)
        when c.c_ctry_2 is null and c.c_area_2 is not null and c.c_local_2 is not null then
            concat('(', c.c_area_2, ') ', c.c_local_2)
        when c.c_area_2 is null and c.c_local_2 is not null then
            c.c_local_2
        else null
    end as phone2,
    case 
        when c.c_ctry_3 is not null and c.c_area_3 is not null and c.c_local_3 is not null then 
            concat('+', c.c_ctry_3, ' (', c.c_area_3, ') ', c.c_local_3)
        when c.c_ctry_3 is null and c.c_area_3 is not null and c.c_local_3 is not null then
            concat('(', c.c_area_3, ') ', c.c_local_3)
        when c.c_area_3 is null and c.c_local_3 is not null then
            c.c_local_3
        else null
    end as phone3,
    c.c_email_1 as email1,
    c.c_email_2 as email2,    
    st.st_name as status,    
    tx1.tx_name as nationaltaxratedesc,
    tx1.tx_rate as nationaltaxrate,    
    tx2.tx_name as localtaxratedesc,
    tx2.tx_rate as localtaxrate,   
    p.agencyid,
    p.creditrating,
    p.networth,
    p.marketingnameplate,
    TRUE as iscurrent,  
    var_id as batchid,  
    bd.batchdate::date as effectivedate,  
    '9999-12-31'::date as enddate  
FROM
    staging.customer c
cross join staging.batchdate bd
left join master.statustype st ON c.c_st_id = st.st_id
left join master.taxrate tx1 ON c.c_nat_tx_id = tx1.tx_id
left join master.taxrate tx2 ON c.c_lcl_tx_id = tx2.tx_id
left join master.prospect p ON
    upper(c.c_f_name) = upper(p.firstname) and
    upper(c.c_l_name) = upper(p.lastname) and
    upper(c.c_adline1) = upper(p.addressline1) and
    upper(c.c_adline2) = upper(p.addressline2) and
    upper(c.c_zipcode) = upper(p.postalcode)
WHERE
    c.cdc_flag = 'I'; 


UPDATE master.dimcustomer
SET
    iscurrent = FALSE,
    enddate = bd.batchdate::date  
FROM
    staging.batchdate bd
WHERE
    master.dimcustomer.customerid IN (
        SELECT c_id FROM staging.customer c WHERE c.cdc_flag = 'U'
    );


INSERT INTO master.dimcustomer (
    sk_customerid, customerid, taxid, lastname, firstname, middleinitial, gender, tier, dob,
    addressline1, addressline2, postalcode, city, stateprov, country,
    phone1, phone2, phone3, email1, email2, status, nationaltaxratedesc, nationaltaxrate,
    localtaxratedesc, localtaxrate, agencyid, creditrating, networth, marketingnameplate,
    iscurrent, batchid, effectivedate, enddate
)
SELECT
    row_number() over(order by c.c_id) as sk_customerid,
    c.c_id as customerid,
    c.c_tax_id as taxid,
    c.c_l_name as lastname,
    c.c_f_name as firstname,
    c.c_m_name as middleinitial,    
    case when c.c_gndr IN ('M', 'F') then upper(c.c_gndr) else 'U' end as gender,
    c.c_tier as tier,
    c.c_dob as dob,
    c.c_adline1 as addressline1,
    c.c_adline2 as addressline2,
    c.c_zipcode as postalcode,
    c.c_city as city,
    c.c_state_prov as stateprov,
    c.c_ctry as country,    
    case 
        when c.c_ctry_1 is not null and c.c_area_1 is not null and c.c_local_1 is not null then 
            concat('+', c.c_ctry_1, ' (', c.c_area_1, ') ', c.c_local_1)
        when c.c_ctry_1 is null and c.c_area_1 is not null and c.c_local_1 is not null then
            concat('(', c.c_area_1, ') ', c.c_local_1)
        when c.c_area_1 is null and c.c_local_1 is not null then
            c.c_local_1
        else null
    end as phone1,
    
    case 
        when c.c_ctry_2 is not null and c.c_area_2 is not null and c.c_local_2 is not null then 
            concat('+', c.c_ctry_2, ' (', c.c_area_2, ') ', c.c_local_2)
        when c.c_ctry_2 is null and c.c_area_2 is not null and c.c_local_2 is not null then
            concat('(', c.c_area_2, ') ', c.c_local_2)
        when c.c_area_2 is null and c.c_local_2 is not null then
            c.c_local_2
        else null
    end as phone2,
    case 
        when c.c_ctry_3 is not null and c.c_area_3 is not null and c.c_local_3 is not null then 
            concat('+', c.c_ctry_3, ' (', c.c_area_3, ') ', c.c_local_3)
        when c.c_ctry_3 is null and c.c_area_3 is not null and c.c_local_3 is not null then
            concat('(', c.c_area_3, ') ', c.c_local_3)
        when c.c_area_3 is null and c.c_local_3 is not null then
            c.c_local_3
        else null
    end as phone3,
    c.c_email_1 as email1,
    c.c_email_2 as email2,
    st.st_name as status,    
    tx1.tx_name as nationaltaxratedesc,
    tx1.tx_rate as nationaltaxrate,    
    tx2.tx_name as localtaxratedesc,
    tx2.tx_rate as localtaxrate,    
    p.agencyid,
    p.creditrating,
    p.networth,
    p.marketingnameplate,
    TRUE as iscurrent,  
    var_id as batchid,  
    bd.batchdate::date as effectivedate,  
    '9999-12-31' as enddate  
FROM
    staging.customer c cross join staging.batchdate bd
left join master.statustype st ON c.c_st_id = st.st_id
left join master.taxrate tx1 ON c.c_nat_tx_id = tx1.tx_id
left join master.taxrate tx2 ON c.c_lcl_tx_id = tx2.tx_id
left join master.prospect p ON
    upper(c.c_f_name) = upper(p.firstname) and
    upper(c.c_l_name) = upper(p.lastname) and
    upper(c.c_adline1) = upper(p.addressline1) and
    upper(c.c_adline2) = upper(p.addressline2) and
    upper(c.c_zipcode) = upper(p.postalcode)
WHERE
    c.cdc_flag = 'U';  


-- Report to Dimessages
INSERT INTO master.dimessages (messagesource, messagetype, messagetext, messagedata)
SELECT
    'DimCustomer' as messagesource,
    'Alert' as messagetype,
    'Invalid customer tier' as messagetext,
    concat('C_ID = ', c.c_id, ', C_TIER = ', c.c_tier) as messagedata
FROM staging.customer c
WHERE c.c_tier NOT IN (1, 2, 3);  

INSERT INTO master.dimessages (messagesource, messagetype, messagetext, messagedata)
SELECT
    'DimCustomer' as messagesource,
    'Alert' as messagetype,
    'DOB out of range' as messagetext,
    concat('C_ID = ', c.c_id, ', C_DOB = ', c.c_dob) as messagedata
FROM staging.customer c
WHERE c.c_dob < (select * from staging.batchdate) - interval '100 years'
	or c.c_dob > (select * from staging.batchdate);
