import duckdb
import argparse

parser = argparse.ArgumentParser(description="Set up the DuckDB database - Staging Schema for TPC-DI benchmarking")
parser.add_argument('--scale', '-s', help="Scale factor (3, 5, 7, 9)", required=True, choices=['3', '5', '7', '9'])
scale = parser.parse_args().scale

con = duckdb.connect(f'../../database/sc_{scale}.db')

query = '''
-- For Historical Load
drop schema if exists staging cascade;
CREATE SCHEMA staging;

drop table if exists staging.batchdate;
create table staging.batchdate(
	batchdate date not null	
);

drop table if exists staging.cashtransaction;
create table staging.cashtransaction(
	ct_ca_id numeric(11) not null check(ct_ca_id >= 0),
	ct_dts timestamp not null,
	ct_amt numeric(10, 2) not null,
	ct_name char(100) not null
);

drop table if exists staging.customermgmt;
create table staging.customermgmt(
	--action element
	actiontype char(9) check(actiontype in ('NEW','ADDACCT','UPDCUST','UPDACCT','CLOSEACCT','INACT')),
	actionts varchar check(length(actionts) > 0),
	--action.customer element
	c_id numeric(11) not null check(c_id >= 0),
	c_tax_id char(20) check((actiontype = 'NEW' and length(c_tax_id) > 0) or (actiontype != 'NEW')),
	c_gndr char(1) check(length(c_gndr) > 0),
	c_tier numeric(1) check(c_tier >= 0),
	c_dob date check((actiontype = 'NEW' and c_dob is not null) or (actiontype != 'NEW')),
	--action.customer.name element
	c_l_name char(25) check((actiontype = 'NEW' and length(c_l_name) > 0) or (actiontype != 'NEW')),
	c_f_name char(20) check((actiontype = 'NEW' and length(c_f_name) > 0) or (actiontype != 'NEW')),
	c_m_name char(1),
	--action.customer.address element
	c_adline1 char(80) check((actiontype = 'NEW' and length(c_adline1) > 0) or (actiontype != 'NEW')),
	c_adline2 char(80),
	c_zipcode char(12) check((actiontype = 'NEW' and length(c_zipcode) > 0) or (actiontype != 'NEW')),
	c_city char(25) check((actiontype = 'NEW' and length(c_city) > 0) or (actiontype != 'NEW')),
	c_state_prov char(20) check((actiontype = 'NEW' and length(c_state_prov) > 0) or (actiontype != 'NEW')),
	c_ctry char(24),
	--action.customer.contactinfo element
	c_prim_email char(50),
	c_alt_email char(50),
	--action.customer.contactinfo.phone element
	--phone1
	c_p_1_ctry_code char(20),
	c_p_1_area_code char(20),
	c_p_1_local char(20),
	c_p_1_ext char(20),
	--phone2
	c_p_2_ctry_code char(20),
	c_p_2_area_code char(20),
	c_p_2_local char(20),
	c_p_2_ext char(20),
	--phone3
	c_p_3_ctry_code char(20),
	c_p_3_area_code char(20),
	c_p_3_local char(20),
	c_p_3_ext char(20),
	--action.customer.taxinfo element
	c_lcl_tx_id char(4),
	c_nat_tx_id char(4),
	--action.customer.account attribute
	ca_id numeric(11),
	ca_tax_st numeric(1) check((actiontype = 'NEW' and ca_tax_st >= 0) or (actiontype != 'NEW')),
	--action.customer.account element
	ca_b_id numeric(11) check((actiontype = 'NEW' and ca_b_id >= 0) or (actiontype != 'NEW')),
	ca_name char(50)	
);

drop table if exists staging.dailymarket;
create table staging.dailymarket(
	dm_date date not null,
	dm_s_symb char(15) not null,
	dm_close numeric(8, 2) not null,
	dm_high numeric(8, 2) not null,
	dm_low numeric(8, 2) not null,
	dm_vol numeric(12) not null check(dm_vol >= 0)
);

drop table if exists staging.date;
create table staging.date(
	sk_dateid numeric(11) not null check(sk_dateid >= 0),
	datevalue char(20) not null,
	datedesc char(20) not null,
	calendaryearid numeric(4) not null check(calendaryearid >= 0),
	calendaryeardesc char(20) not null,
	calendarqtrid numeric(5) not null check(calendarqtrid >= 0),
	calendarqtrdesc char(20) not null,
	calendarmonthid numeric(6) not null check(calendarmonthid >= 0),
	calendarmonthdesc char(20) not null,	
	calendarweekid numeric(6) not null check(calendarweekid >= 0),
	calendarweekdesc char(20) not null,	
	dayofweeknum numeric(1) not null check(dayofweeknum >= 0),
	dayofweekdesc char(10) not null,	
	fiscalyearid numeric(4) not null check(fiscalyearid >= 0),
	fiscalyeardesc char(20) not null,	
	fiscalqtrid numeric(5) not null check(fiscalqtrid >= 0),
	fiscalqtrdesc char(20) not null,	
	holidayflag boolean
);

drop table if exists staging.finwire;
create table staging.finwire(
	text varchar
);

drop table if exists staging.finwire_cmp;
create table staging.finwire_cmp(
	pts char(15) check(length(pts) > 0),
	rectype char(3) check(length(rectype) > 0),
	companyname char(60) check(length(companyname) > 0),
	cik char(10) check(length(cik) > 0),
	status char(4) check(length(status) > 0),
	industryid char(2) check(length(industryid) > 0),
	sprating char(4) check(length(sprating) > 0),
	foundingdate char(8),
	addressline1 char(80) check(length(addressline1) > 0),
	addressline2 char(80),
	postalcode char(12) check(length(postalcode) > 0),
	city char(25) check(length(city) > 0),
	stateprovince char(20) check(length(stateprovince) > 0),
	country char(24),
	ceoname char(46) check(length(ceoname) > 0),
	description char(150) check(length(description) > 0)
);

drop table if exists staging.finwire_sec;
create table staging.finwire_sec(
	pts char(15) check(length(pts) > 0),
	rectype char(3) check(length(rectype) > 0),
	symbol char(15) check(length(symbol) > 0),
	issuetype char(6) check(length(issuetype) > 0),
	status char(4) check(length(status) > 0),
	name char(70) check(length(name) > 0),
	exid char(6) check(length(exid) > 0),
	shout char(13) check(length(shout) > 0),
	firsttradedate char(8) check(length(firsttradedate) > 0),
	firsttradeexchg char(8) check(length(firsttradeexchg) > 0),
	dividend char(12) check(length(dividend) > 0),
	conameorcik char(60) check(length(conameorcik) > 0)
);

drop table if exists staging.finwire_fin;
create table staging.finwire_fin(
	pts char(15) check(length(pts) > 0),
	rectype char(3) check(length(rectype) > 0),
	year char(4) check(length(year) > 0),
	quarter char(1) check(length(quarter) > 0),
	qtrstartdate char(8) check(length(qtrstartdate) > 0),
	postingdate char(8) check(length(postingdate) > 0),
	revenue char(17) check(length(revenue) > 0),
	earnings char(17) check(length(earnings) > 0),
	eps char(12) check(length(eps) > 0),
	dilutedeps char(12) check(length(dilutedeps) > 0),
	margin char(12) check(length(margin) > 0),
	inventory char(17) check(length(inventory) > 0),
	assets char(17) check(length(assets) > 0),
	liability char(17) check(length(liability) > 0),
	shout char(13) check(length(shout) > 0),
	dilutedshout char(13) check(length(dilutedshout) > 0),
	conameorcik char(60) check(length(conameorcik) > 0)
);

drop table if exists staging.holdinghistory;
create table staging.holdinghistory(
	hh_h_t_id numeric(15) not null check(hh_h_t_id >= 0),
	hh_t_id numeric(15) not null check(hh_t_id >= 0),
	hh_before_qty numeric(6) not null check(hh_before_qty >= 0),
	hh_after_qty numeric(6) not null check(hh_after_qty >= 0)
);

drop table if exists staging.hr;
create table staging.hr(
	employeeid numeric(11) not null check(employeeid >= 0),
	managerid numeric(11) not null check(managerid >= 0),
	employeefirstname char(30) not null,
	employeelastname char(30) not null,
	employeemi char(1),
	employeejobcode numeric(3) check(employeejobcode >= 0),
	employeebranch char(30),
	employeeoffice char(10),
	employeephone char(14)	
);

drop table if exists staging.industry;
create table staging.industry(
	in_id char(2) not null,
	in_name char(50) not null,
	in_sc_id char(4) not null	
);

drop table if exists staging.prospect;
create table staging.prospect(
	agencyid char(30) not null,
	lastname char(30) not null,
	firstname char(30) not null,
	middleinitial char(1),
	gender char(1),
	addressline1 char(80),
	addressline2 char(80),
	postalcode char(12),
	city char(25) not null,
	state char(20) not null,
	country char(24),
	phone char(30),
	income numeric(9) check(income >= 0),
	numbercars numeric(2) check(numbercars >= 0),
	numberchildren numeric(2) check(numberchildren >= 0),
	maritalstatus char(1),
	age numeric(3) check(age >= 0),
	creditrating numeric(4) check(creditrating >= 0),
	ownorrentflag char(1),
	employer char(30),
	numbercreditcards numeric(2) check(numbercreditcards >= 0),
	networth numeric(12) check(networth >= 0)	
);

drop table if exists staging.statustype;
create table staging.statustype(
	st_id char(4) not null,
	st_name char(10) not null	
);

drop table if exists staging.taxrate;
create table staging.taxrate(
	tx_id char(4) not null,
	tx_name char(50) not null,
	tx_rate numeric(6,5) not null check(tx_rate >= 0)
);

drop table if exists staging.time;
create table staging.time(
	sk_timeid numeric(11) not null check(sk_timeid >= 0),
	timevalue char(20) not null,
	hourid numeric(2) not null check(hourid >= 0),
	hourdesc char(20) not null,
	minuteid numeric(2) not null check(minuteid >= 0),
	minutedesc char(20) not null,
	secondid numeric(2) not null check(secondid >= 0),
	seconddesc char(20) not null,
	markethoursflag boolean,
	officehoursflag boolean
);

drop table if exists staging.tradehistory;
create table staging.tradehistory(
	th_t_id numeric(15) not null check(th_t_id >= 0),
	th_dts timestamp not null,
	th_st_id char(4) not null	
);

drop table if exists staging.trade;
create table staging.trade(
	t_id numeric(15) not null check(t_id >= 0),
	t_dts timestamp not null,
	t_st_id char(4) not null,
	t_tt_id char(3) not null,
	t_is_cash integer check(t_is_cash in (0, 1)),
	t_s_symb char(15) not null,
	t_qty numeric(6) check(t_qty >= 0),
	t_bid_price numeric(8,2) check(t_bid_price >= 0),
	t_ca_id numeric(11) not null check(t_ca_id >= 0),
	t_exec_name char(49) not null,
	t_trade_price numeric(8,2) check((t_st_id = 'CMPT' and t_trade_price >= 0) or (t_st_id != 'CMPT' and t_trade_price is null)),
	t_chrg numeric(10,2) check((t_st_id = 'CMPT' and t_chrg >= 0) or (t_st_id != 'CMPT' and t_chrg is null)),
	t_comm numeric(10,2) check((t_st_id = 'CMPT' and t_comm >= 0) or (t_st_id != 'CMPT' and t_comm is null)),
	t_tax numeric(10,2) check((t_st_id = 'CMPT' and t_tax >= 0) or (t_st_id != 'CMPT' and t_tax is null))
);

drop table if exists staging.tradetype;
create table staging.tradetype(
	tt_id char(3) not null,
	tt_name char(12) not null,
	tt_is_sell numeric(1) not null check(tt_is_sell >= 0),
	tt_is_mrkt numeric(1) not null check(tt_is_mrkt >= 0)	
);

drop table if exists staging.watchhistory;
create table staging.watchhistory(
	w_c_id numeric(11) not null check(w_c_id >= 0),
	w_s_symb char(15) not null,
	w_dts timestamp not null,
	w_action char(4) check(w_action in ('ACTV', 'CNCL'))	
);

drop table if exists staging.audit;
create table staging.audit(
	dataset char(20) not null,
	batchid numeric(5) check(batchid >= 0),
	date date,
	attribute char(50) not null,
	value numeric(15),
	dvalue numeric(15,5)	
);

drop table if exists staging.trade_joined;
CREATE TABLE staging.trade_joined(
	t_id INTEGER NOT NULL,
	t_dts DATETIME NOT NULL,
	t_st_id CHAR(4) NOT NULL,
	t_tt_id CHAR(3) NOT NULL,
	t_is_cash BOOLEAN,
	t_s_symb CHAR(15) NOT NULL,
	t_qty INTEGER DEFAULT NULL,
	t_bid_price DECIMAL DEFAULT NULL,
	t_ca_id INTEGER NOT NULL,
	t_exec_name CHAR(49) NOT NULL,
	t_trade_price DECIMAL NULL DEFAULT NULL,
	t_chrg DECIMAL NULL DEFAULT NULL,
	t_comm DECIMAL NULL DEFAULT NULL,
	t_tax DECIMAL NULL DEFAULT NULL,
	th_t_id INTEGER NOT NULL,
	th_dts DATETIME NOT NULL,
	th_st_id CHAR(4) NOT NULL,
	PRIMARY KEY(t_id, th_st_id)
);

-- For Incremental Update 
drop table if exists staging.customer;
CREATE TABLE staging.customer (
        cdc_flag CHAR(1) NOT NULL,
        cdc_dsn BIGINT NOT NULL,
        c_id INT NOT NULL,
        c_tax_id CHAR(20) NOT NULL,
        c_st_id CHAR(4),
        c_l_name CHAR(25) NOT NULL,
        c_f_name CHAR(20) NOT NULL,
        c_m_name CHAR(1),
        c_gndr CHAR(1),
        c_tier NUMERIC(1),
        c_dob DATE NOT NULL,
        c_adline1 CHAR(80) NOT NULL,
        c_adline2 CHAR(80),
        c_zipcode CHAR(12) NOT NULL,
        c_city CHAR(25) NOT NULL,
        c_state_prov CHAR(20) NOT NULL,
        c_ctry CHAR(24),
        c_ctry_1 CHAR(3),
        c_area_1 CHAR(3),
        c_local_1 CHAR(10),
        c_ext_1 CHAR(5),
        c_ctry_2 CHAR(3),
        c_area_2 CHAR(3),
        c_local_2 CHAR(10),
        c_ext_2 CHAR(5),
        c_ctry_3 CHAR(3),
        c_area_3 CHAR(3),
        c_local_3 CHAR(10),
        c_ext_3 CHAR(5),
        c_email_1 CHAR(50),
        c_email_2 CHAR(50),
        c_lcl_tx_id CHAR(4) NOT NULL,
        c_nat_tx_id CHAR(4) NOT NULL
    );
    
drop table if exists staging.account;
CREATE TABLE staging.account (
    cdc_flag CHAR(1) CHECK (cdc_flag IN ('I', 'U')) NOT NULL, 
    cdc_dsn BIGINT NOT NULL,                                  
    ca_id BIGINT NOT NULL,                                  
    ca_b_id BIGINT NOT NULL,                                  
    ca_c_id BIGINT NOT NULL,                                  
    ca_name CHAR(50),                                         
    ca_tax_st NUMERIC(1) CHECK (ca_tax_st IN (0, 1, 2)),      
    ca_st_id CHAR(4) CHECK (ca_st_id IN ('ACTV', 'INAC'))     
);

drop table if exists staging.trade1 ;
create table staging.trade1 (
    cdc_flag   text check (cdc_flag in ('I', 'U')),               
    cdc_dsn    bigint not null,                                   
    t_id       numeric(15) not null check(t_id >= 0),
    t_dts      timestamp not null,                                
    t_st_id    char(4) not null,                                  
    t_tt_id    char(3) not null,                                  
    t_is_cash  integer check(t_is_cash in (0, 1)),
    t_s_symb   char(15) not null,                                 
    t_qty      numeric(6) check(t_qty >= 0),
    t_bid_price numeric(8,2) check(t_bid_price >= 0),
    t_ca_id    numeric(11) not null check(t_ca_id >= 0),
    t_exec_name char(49) not null,                                
    t_trade_price numeric(8,2) check((t_st_id = 'CMPT' and t_trade_price >= 0) or (t_st_id != 'CMPT' and t_trade_price is null)),
    t_chrg numeric(10,2) check((t_st_id = 'CMPT' and t_chrg >= 0) or (t_st_id != 'CMPT' and t_chrg is null)),
	t_comm numeric(10,2) check((t_st_id = 'CMPT' and t_comm >= 0) or (t_st_id != 'CMPT' and t_comm is null)),
	t_tax numeric(10,2) check((t_st_id = 'CMPT' and t_tax >= 0) or (t_st_id != 'CMPT' and t_tax is null))
);

drop table if exists staging.cashtransaction1;
create table staging.cashtransaction1(
    cdc_flag varchar(1) not null,          
    cdc_dsn bigint not null, 
	ct_ca_id numeric(11) not null check(ct_ca_id >= 0),
	ct_dts timestamp not null,
	ct_amt numeric(10, 2) not null,
	ct_name char(100) not null
);

drop table if exists staging.dailymarket1;
create table staging.dailymarket1(
    cdc_flag varchar(1) not null,          
    cdc_dsn bigint not null, 
	dm_date date not null,
	dm_s_symb char(15) not null,
	dm_close numeric(8, 2) not null,
	dm_high numeric(8, 2) not null,
	dm_low numeric(8, 2) not null,
	dm_vol numeric(12) not null check(dm_vol >= 0)
);

drop table if exists staging.watchhistory1;
create table staging.watchhistory1(
    cdc_flag varchar(1) not null,          
    cdc_dsn bigint not null, 
	w_c_id numeric(11) not null check(w_c_id >= 0),
	w_s_symb char(15) not null,
	w_dts timestamp not null,
	w_action char(4) check(w_action in ('ACTV', 'CNCL'))	
);
'''
print(f'Creating Staging Schema for scale factor {scale}')
con.sql(query)
print(f'Finished setting up Staging Schema.')