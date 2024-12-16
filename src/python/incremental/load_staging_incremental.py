import duckdb

# Later need to move create table customer to create staging schema 

def load_staging_incremental(con, scale, id):
    query = f'''

    DELETE FROM staging.prospect;
    COPY staging.prospect FROM '../../../staging_area/scale_{scale}/Batch{id}/Prospect.csv' delimiter ',' CSV;
    
    DELETE FROM staging.customer; 
    COPY staging.customer FROM '../../../staging_area/scale_{scale}/Batch{id}/Customer.txt' (delimiter '|', HEADER false);

    DELETE FROM staging.account;
    COPY staging.account FROM '../../../staging_area/scale_{scale}/Batch{id}/Account.txt' (delimiter '|', HEADER false);
        
    DELETE FROM staging.trade1;
    COPY staging.trade1 FROM '../../../staging_area/scale_{scale}/Batch{id}/Trade.txt' delimiter '|' null as '';
    
    delete from staging.cashtransaction1;
    COPY staging.cashtransaction1 FROM '../../../staging_area/scale_{scale}/Batch{id}/CashTransaction.txt' delimiter '|';

    delete from staging.dailymarket1;
    COPY staging.dailymarket1 FROM '../../../staging_area/scale_{scale}/Batch{id}/DailyMarket.txt' (delimiter '|', HEADER false);

    delete from staging.watchhistory1;
    COPY staging.watchhistory1 FROM '../../../staging_area/scale_{scale}/Batch{id}/WatchHistory.txt' (delimiter '|', HEADER false);
    
    '''
    con.sql(query)
