-- Delete existing data for batch ID 2
DELETE FROM master.prospect WHERE batchid = var_id;

CREATE TEMPORARY TABLE prospect_counts AS
WITH date_record_id AS (
    SELECT dd.sk_dateid
    FROM master.dimdate dd
    INNER JOIN staging.batchdate bd
        ON dd.datevalue = bd.batchdate
),
existing_prospects AS (
    SELECT 
        p.agencyid,
        p.lastname,
        p.firstname,
        p.middleinitial,
        p.gender,
        p.addressline1,
        p.addressline2,
        p.postalcode,
        p.city,
        p.state,
        p.country,
        p.phone,
        p.income,
        p.numbercars,
        p.numberchildren,
        p.maritalstatus,
        p.age,
        p.creditrating,
        p.ownorrentflag,
        p.employer,
        p.numbercreditcards,
        p.networth,
        pp.sk_updatedateid
    FROM staging.prospect p
    LEFT JOIN master.prospect pp
        ON p.agencyid = pp.agencyid
        AND pp.batchid = pre_id
)
SELECT
    COUNT(*) AS source_rows,
    SUM(CASE 
            WHEN pp.agencyid IS NULL THEN 1  -- Row is new
            ELSE 0
        END) AS inserted_rows,
    SUM(CASE
            WHEN pp.agencyid IS NOT NULL 
                 AND (
                    UPPER(pp.lastname) != UPPER(p.lastname)
                    OR UPPER(pp.firstname) != UPPER(p.firstname)
                    OR UPPER(pp.middleinitial) != UPPER(p.middleinitial)
                    OR UPPER(pp.gender) != UPPER(p.gender)
                    OR UPPER(pp.addressline1) != UPPER(p.addressline1)
                    OR UPPER(pp.addressline2) != UPPER(p.addressline2)
                    OR UPPER(pp.postalcode) != UPPER(p.postalcode)
                    OR UPPER(pp.city) != UPPER(p.city)
                    OR UPPER(pp.state) != UPPER(p.state)
                    OR UPPER(pp.country) != UPPER(p.country)
                    OR UPPER(pp.phone) != UPPER(p.phone)
                    OR pp.income != p.income
                    OR pp.numbercars != p.numbercars
                    OR pp.numberchildren != p.numberchildren
                    OR pp.maritalstatus != p.maritalstatus
                    OR pp.age != p.age
                    OR pp.creditrating != p.creditrating
                    OR pp.ownorrentflag != p.ownorrentflag
                    OR UPPER(pp.employer) != UPPER(p.employer)
                    OR pp.numbercreditcards != p.numbercreditcards
                    OR pp.networth != p.networth
                ) 
            THEN 1
            ELSE 0
        END) AS updated_rows
FROM staging.prospect p
LEFT JOIN master.prospect pp
    ON p.agencyid = pp.agencyid
    AND pp.batchid = pre_id;

-- Insert new and update existing rows in the master.prospect table
INSERT INTO master.prospect (
    agencyid, lastname, firstname, middleinitial, gender, addressline1, addressline2,
    postalcode, city, state, country, phone, income, numbercars, numberchildren,
    maritalstatus, age, creditrating, ownorrentflag, employer, numbercreditcards,
    networth, sk_recorddateid, sk_updatedateid, iscustomer, marketingnameplate, batchid
)
SELECT
    p.agencyid,
    p.lastname,
    p.firstname,
    p.middleinitial,
    p.gender,
    p.addressline1,
    p.addressline2,
    p.postalcode,
    p.city,
    p.state,
    p.country,
    p.phone,
    p.income,
    p.numbercars,
    p.numberchildren,
    p.maritalstatus,
    p.age,
    p.creditrating,
    p.ownorrentflag,
    p.employer,
    p.numbercreditcards,
    p.networth,
    dri.sk_dateid AS sk_recorddateid,
    COALESCE(pp.sk_updatedateid, dri.sk_dateid) AS sk_updatedateid,
    CASE 
        WHEN EXISTS (
            SELECT 1
            FROM master.dimcustomer c
            WHERE UPPER(c.firstname) = UPPER(p.firstname)
              AND UPPER(c.lastname) = UPPER(p.lastname)
              AND UPPER(c.addressline1) = UPPER(p.addressline1)
              AND UPPER(c.addressline2) = UPPER(p.addressline2)
              AND UPPER(c.postalcode) = UPPER(p.postalcode)
              AND c.status = 'ACTIVE'
        ) THEN TRUE
        ELSE FALSE
    END AS iscustomer,
    CASE
        WHEN p.networth > 1000000 OR p.income > 200000 THEN 'HighValue'
        WHEN p.numberchildren > 3 OR p.numbercreditcards > 5 THEN 'Expenses'
        WHEN p.age > 45 THEN 'Boomer'
        WHEN p.income < 50000 OR p.creditrating < 600 OR p.networth < 100000 THEN 'MoneyAlert'
        WHEN p.numbercars > 3 OR p.numbercreditcards > 7 THEN 'Spender'
        WHEN p.age < 25 AND p.networth > 1000000 THEN 'Inherited'
        ELSE NULL
    END AS marketingnameplate,
    var_id AS batchid
FROM staging.prospect p
CROSS JOIN (
    SELECT dd.sk_dateid
    FROM master.dimdate dd
    INNER JOIN staging.batchdate bd
        ON dd.datevalue = bd.batchdate
) dri
LEFT JOIN (
    SELECT 
        agencyid, 
        lastname, 
        firstname, 
        middleinitial, 
        gender, 
        addressline1, 
        addressline2,
        postalcode, 
        city, 
        state, 
        country, 
        phone, 
        income, 
        numbercars, 
        numberchildren,
        maritalstatus, 
        age, 
        creditrating, 
        ownorrentflag, 
        employer, 
        numbercreditcards, 
        networth, 
        sk_updatedateid
    FROM master.prospect
    WHERE batchid = pre_id
) pp
    ON p.agencyid = pp.agencyid;

-- Dimessages
INSERT INTO master.dimessages 
SELECT now(), var_id, 'Prospect', 'Source rows', 'Status', source_rows
FROM prospect_counts;

INSERT INTO master.dimessages 
SELECT now(), var_id, 'Prospect', 'Inserted rows','Status', inserted_rows
FROM prospect_counts;

INSERT INTO master.dimessages 
SELECT now(), var_id,'Prospect', 'Updated rows', 'Status', updated_rows
FROM prospect_counts;

DROP TABLE prospect_counts;
