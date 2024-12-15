import xmltodict
import pandas as pd
import numpy as np
import duckdb

def parse_load_customer_mgmt(con, scale):

    with open(f'../../staging_area/scale_{scale}/Batch1/CustomerMgmt.xml') as fd:
        doc = xmltodict.parse(fd.read())
    
    df = pd.DataFrame(columns=np.arange(0, 36))

    # Extract actions from the parsed XML data
    actions = doc['TPCDI:Actions']
    action = actions['TPCDI:Action']

    # Iterate through each action
    for a in action:
        rows = {}

        # action element
        rows.update({
            0: [a.get('@ActionType')],
            1: [a.get('@ActionTS')]
        })

        # action.customer element
        customer = a.get('Customer')
        rows.update({
            2: [customer.get('@C_ID') if customer else None],
            3: [customer.get('@C_TAX_ID') if customer else None],
            4: [customer.get('@C_GNDR') if customer else None],
            5: [customer.get('@C_TIER') if customer else None],
            6: [customer.get('@C_DOB') if customer else None]
        })

        # action.customer.name element
        name = customer.get('Name') if customer else None
        rows.update({
            7: [name.get('C_L_NAME') if name else None],
            8: [name.get('C_F_NAME') if name else None],
            9: [name.get('C_M_NAME') if name else None]
        })

        # action.customer.address element
        address = customer.get('Address') if customer else None
        rows.update({
            10: [address.get('C_ADLINE1') if address else None],
            11: [address.get('C_ADLINE2') if address else None],
            12: [address.get('C_ZIPCODE') if address else None],
            13: [address.get('C_CITY') if address else None],
            14: [address.get('C_STATE_PROV') if address else None],
            15: [address.get('C_CTRY') if address else None]
        })

        # action.customer.contactinfo element
        contactinfo = customer.get('ContactInfo') if customer else None
        rows.update({
            16: [contactinfo.get('C_PRIM_EMAIL') if contactinfo else None],
            17: [contactinfo.get('C_ALT_EMAIL') if contactinfo else None]
        })

        for i in range(1, 4):  # Iterate for phone_1, phone_2, phone_3
            phone = contactinfo.get(f'C_PHONE_{i}') if contactinfo else None
            rows.update({
                18 + (i-1)*4: [phone.get('C_CTRY_CODE') if phone else None],
                19 + (i-1)*4: [phone.get('C_AREA_CODE') if phone else None],
                20 + (i-1)*4: [phone.get('C_LOCAL') if phone else None],
                21 + (i-1)*4: [phone.get('C_EXT') if phone else None]
            })

        # action.customer.taxinfo element
        taxinfo = customer.get('TaxInfo') if customer else None
        rows.update({
            30: [taxinfo.get('C_LCL_TX_ID') if taxinfo else None],
            31: [taxinfo.get('C_NAT_TX_ID') if taxinfo else None]
        })

        # action.customer.account attribute
        account = customer.get('Account') if customer else None
        rows.update({
            32: [account.get('@CA_ID') if account else None],
            33: [account.get('@CA_TAX_ST') if account else None],
            34: [account.get('CA_B_ID') if account else None],
            35: [account.get('CA_NAME') if account else None]
        })

        # Append to dataframe
        df = pd.concat([df, pd.DataFrame.from_dict(rows)], axis=0)

    # Replace NaN and "None" 
    df.replace(to_replace=np.NaN, value="", inplace=True)
    df.replace(to_replace="None", value="", inplace=True)

    # Write the dataframe to a CSV file
    df.to_csv(f'../../staging_area/scale_{scale}/Batch1/CustomerMgmt.csv', index=False)
    print('Customer Management data converted from XML to CSV')

    # Load to staging 
    query = f'''
    DELETE FROM staging.customermgmt;

    -- Assuming you have headers in your CSV file. If not, use HEADER FALSE.
    COPY staging.customermgmt FROM '../../staging_area/scale_{scale}/Batch1/CustomerMgmt.csv' WITH CSV HEADER;    

    '''
    con.sql(query)
    print('Load Customermgmt')

