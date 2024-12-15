import duckdb


def clean_field(field):
    """Replace empty fields with NULL."""
    trimmed_field = field.strip()
    return None if trimmed_field == '' else trimmed_field

def process_finwire(con):    
    insert_cmp = """
        INSERT INTO staging.finwire_cmp (
            pts, 
            rectype, 
            companyname, 
            cik, 
            status, 
            industryid, 
            sprating,
            foundingdate, 
            addressline1,
            addressline2, 
            postalcode, 
            city, 
            stateprovince, 
            country, 
            ceoname, 
            description
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    insert_sec = """
        INSERT INTO staging.finwire_sec (
            pts, 
            rectype, 
            symbol, 
            issuetype, 
            status, 
            name, 
            exid, 
            shout,
            firsttradedate, 
            firsttradeexchg, 
            dividend, 
            conameorcik
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    insert_fin = """
        INSERT INTO staging.finwire_fin (
            pts, 
            rectype,
            year, 
            quarter, 
            qtrstartdate, 
            postingdate, 
            revenue,
            earnings, 
            eps, 
            dilutedeps, 
            margin, 
            inventory, 
            assets,
            liability, 
            shout, 
            dilutedshout, 
            conameorcik
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    
    fetch_query = "SELECT text FROM staging.finwire;"
    
    truncate_cmp = "TRUNCATE TABLE staging.finwire_cmp;"
    truncate_sec = "TRUNCATE TABLE staging.finwire_sec;"
    truncate_fin = "TRUNCATE TABLE staging.finwire_fin;"
    
    try:
        cursor = con.cursor()
        # Truncate tables
        cursor.execute(truncate_cmp)
        cursor.execute(truncate_sec)
        cursor.execute(truncate_fin)

        cursor.execute(fetch_query)
        records = cursor.fetchall()

        # Process each record
        for record in records:
            raw_text = record[0]  # The raw text of the record
            rec_type = raw_text[15:18].strip()  # Extract rectype 

            if rec_type == 'CMP':
                parsed_data = (
                    clean_field(raw_text[0:15]),   # pts
                    rec_type,
                    clean_field(raw_text[18:78]),  # companyname
                    clean_field(raw_text[78:88]),  # cik
                    clean_field(raw_text[88:92]),  # status
                    clean_field(raw_text[92:94]),  # industryid
                    clean_field(raw_text[94:98]),  # sprating
                    clean_field(raw_text[98:106]), # foundingdate
                    clean_field(raw_text[106:186]),# addrline1
                    clean_field(raw_text[186:266]),# addrline2
                    clean_field(raw_text[266:278]),# postalcode
                    clean_field(raw_text[278:303]),# city
                    clean_field(raw_text[303:323]),# stateprovince
                    clean_field(raw_text[323:347]),# country
                    clean_field(raw_text[347:393]),# ceoname
                    clean_field(raw_text[393:543]) # description
                )
                cursor.execute(insert_cmp, parsed_data)

            elif rec_type == 'SEC':
                parsed_data = (
                    clean_field(raw_text[0:15]),   # pts
                    rec_type,
                    clean_field(raw_text[18:33]),  # symbol
                    clean_field(raw_text[33:39]),  # issuetype
                    clean_field(raw_text[39:43]),  # status
                    clean_field(raw_text[43:113]), # name
                    clean_field(raw_text[113:119]),# exid
                    clean_field(raw_text[119:132]),# shout
                    clean_field(raw_text[132:140]),# firsttradedate
                    clean_field(raw_text[140:148]),# firsttradeexchg
                    clean_field(raw_text[148:160]),# dividend
                    clean_field(raw_text[160:220]) # conameorcik
                )
                cursor.execute(insert_sec, parsed_data)

            elif rec_type == 'FIN':
                parsed_data = (
                    clean_field(raw_text[0:15]),   # pts
                    rec_type,
                    clean_field(raw_text[18:22]),  # year
                    clean_field(raw_text[22:23]),  # quarter
                    clean_field(raw_text[23:31]),  # qtrstartdate
                    clean_field(raw_text[31:39]),  # postingdate
                    clean_field(raw_text[39:56]),  # revenue
                    clean_field(raw_text[56:73]),  # earnings
                    clean_field(raw_text[73:85]),  # eps
                    clean_field(raw_text[85:97]),  # dilutedeps
                    clean_field(raw_text[97:109]), # margin
                    clean_field(raw_text[109:126]),# inventory
                    clean_field(raw_text[126:143]),# assets
                    clean_field(raw_text[143:160]),# liabilities
                    clean_field(raw_text[160:173]),# shout
                    clean_field(raw_text[173:186]),# dilutedshout
                    clean_field(raw_text[186:246]) # conameorcik
                )
                cursor.execute(insert_fin, parsed_data)

        print("Records processed successfully!")

    except Exception as e:
        print(f"Error processing records: {e}")
    
    finally:
        cursor.close()