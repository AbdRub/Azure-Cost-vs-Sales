def main():
    import requests as rs
    from zipfile import ZipFile
    from io import BytesIO
    import pandas as pd
    from azure.storage.blob import ContainerClient,BlobServiceClient
    from io import StringIO
    import numpy as np
    import json
    import datetime as dt
    import warnings
    import duckdb as db
    import time

    def query(sqlString:str)->None:
    
        try:
            return cxn.query(sqlString).show(max_width = 100000, max_rows = 100000)
        except Exception as e:
            return e
    
    def getInvoices():
        
        try:
            relativeInvoicesURL = '/v1/invoices'
            response = json.loads(
            rs.get(
                f"{base_url}{relativeInvoicesURL}",
                headers=HTTPheaders)
            .content)

            return response['items']
        
        except Exception as e:
            return e
        
    def getOneTimeInvoiceLineItems(invoiceID:str)->dict:
        
        try:
            newCommerceOneTimeBillingURL = f'/v1/invoices/OneTime-{invoiceID}/lineitems/OneTime/BillingLineItems?size=5000'

            response = json.loads(
                rs.get(
                        f"{base_url}{newCommerceOneTimeBillingURL}",
                        headers=HTTPheaders)
                .content) if rs.get(
                        f"{base_url}{newCommerceOneTimeBillingURL}",
                        headers=HTTPheaders).status_code == 200 else None # if response status code = 200
            
            return response['items']
        
        except Exception as e:
            return e

    # Set display options

    pd.set_option('max_colwidth', None)

    warnings.filterwarnings('ignore')

    pd.set_option('display.max_columns', None)

    pd.set_option('display.max_rows', None)

    try:
        cxn = db.connect('./DB/dbfile')
        print( 'connected to Db')
    except Exception as e:
        print(e)

    
    time.sleep(1)

    # Parse Secrets

    try:
        secrets = json.load(open(file='./secrets.json'))
        print('Secrets file found.')
    except:
        print('Secrets file not found or some error in file. Make sure the file exists in the same directory as the code.')

    if secrets:
        refresh_token = secrets['refresh_token']

        app_id = secrets['app_id']

        app_secret = secrets['app_secret']

        request_body = {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "scope": "openid",
                "resource": "https://api.partnercenter.microsoft.com",
                "client_id": app_id,
                "client_secret": app_secret,
            }

        base_url = (
        'https://api.partnercenter.microsoft.com'
        )


        isAccessTokenObtained = 0


    try:
        req = rs.post(
                    "https://login.windows.net/6e75cca6-47f0-47a3-a928-9d5315750bd9/oauth2/token"
                    ,data = request_body)
        
        access_token = json.loads(req.text)['access_token']
        isAccessTokenObtained = 1
        print('Refresh Token valid, access token obtained. \nAccess token : ',access_token[:20]+'...')
    except:
        print('Refresh token expired, or no internet connection. \nUnable to obtain access token')
        print(json.loads(req.text))
        

    if isAccessTokenObtained == 1:
            
        HTTPheaders = {'Authorization': 'Bearer ' + access_token}

        print('Fetching list of incvoices...')
        allInvoicesDF = pd.DataFrame(getInvoices())
        print('Invoices fetched...')


        latestOfficeInvoice = cxn.query(f"select id from allInvoicesDF where id like 'G%' order by invoicedate desc limit 1 ")
        print(latestOfficeInvoice)
        latestOfficeInvoice = str(latestOfficeInvoice).replace("┌────────────┐\n│     id     │\n│  varchar   │\n├────────────┤\n│ ",'').replace(" │\n└────────────┘\n",'')
        print('latest invoice: ',latestOfficeInvoice)

        # previousInvoice = cxn.query("select id from invoices where id like 'G%' order by invoicedate  desc limit 1 offset 1 ")
        # previousInvoice = str(previousInvoice).replace("┌────────────┐\n│     id     │\n│  varchar   │\n├────────────┤\n│ ",'').replace(" │\n└────────────┘\n",'')
        # print('prv invoice: ',previousInvoice)
        
        latestMonthDF = pd.DataFrame(getOneTimeInvoiceLineItems(latestOfficeInvoice))
        print(latestMonthDF.head())
        
        requiredColumns = """
        customerId,
        customerName,
        customerDomainName,
        invoiceNumber,
        orderDate,
        invoiceNumber,
        substring(skuName,0,30) skuName,
        subscriptionId,
        chargeType,
        effectiveUnitPrice,
        quantity,
        subtotal,
        taxTotal,
        totalForCustomer,
        chargeStartDate,
        chargeEndDate,
        referenceId,
        billableQuantity,
        monthname(cast(chargeStartDate as datetime)) || cast(year(cast(chargeStartDate as datetime))as varchar)  invoiceMonth,
        subscriptionStartDate,
        subscriptionEndDate,
        monthname(cast(subscriptionstartdate as datetime)) || cast(year(cast(subscriptionstartdate as datetime))as varchar) as  mn
        """ # concat monthname and year

        cxn.query(f"create temp table office_temp as select {requiredColumns} from latestMonthDF where productid like 'CFQ%'")
        cxn.query(f"""
 
            with
                t0 as (
                select {requiredColumns}
                from office_temp  
                --and customerDomainName = 'domainName'
                --and subscriptionid = '12266bbd-6d4a-422f-d413-4c688cc48550'
                )
                ,
                
                t1 as                                            /* club rows that have similar charge types and reference IDs. */

                (select customerid,customerdomainname,max(cast(orderDate as datetime)) orderdate1, last(invoicenumber order by orderdate) newInvoiceNumber
                ,subscriptionid,date_trunc('day',cast(chargestartdate as datetime)) truncatedchargestartdate
                , chargeenddate,chargetype
                ,first(billablequantity order by totalforcustomer desc) fbqty , last(billablequantity order by totalforcustomer desc) lbqty
                ,fbqty - lbqty QtyAdded
                ,case when qtyadded = 0 then fbqty else qtyadded end as QtyAdded_adjusted
                ,round(sum(totalforcustomer),2) newTotal
                ,case when newTotal < 0 then -(abs(QtyAdded_adjusted)) else QtyAdded_adjusted end as QtyAdded_adjustedForNegativity
                ,referenceid
                ,mn
                from t0  
                group by customerid,customerdomainname,subscriptionid,truncatedchargestartdate, chargeenddate,chargetype,referenceid,mn
                order by subscriptionid,orderdate1)
                ,
                final as 
                (
                select customerid,customerdomainname,orderdate1,newinvoicenumber,subscriptionid,truncatedchargestartdate chargestartdate,chargeenddate
                ,date_diff('day',cast(truncatedchargestartdate as date),cast(chargeenddate as date)) noOfDays
                ,chargetype,QtyAdded_adjustedForNegativity qty,newtotal,referenceid,mn
                from t1

                )
                select * from final
                """).to_csv('test.csv')
        
        return None
    
if __name__ == "__main__":
    main()
