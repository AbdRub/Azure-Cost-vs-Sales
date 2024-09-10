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
        cxn = db.connect('')
        print( 'connected to Db')
    except Exception as e:
        print(e)

    
    time.sleep(1)

    # Parse Secrets

    try:
        secrets = json.load(open(file='../secrets.json'))
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


        latestOfficeInvoice = cxn.query(f"select id,billingPeriodStartDate from allInvoicesDF where id like 'G%' order by invoicedate desc limit 1 ")
        df=latestOfficeInvoice.to_df()
        latestOfficeInvoice,billingMonth = (df['id'].iloc[0],df['billingPeriodStartDate'].iloc[0])

        
        latestMonthDF = pd.DataFrame(getOneTimeInvoiceLineItems(latestOfficeInvoice))

        
        requiredColumns = """
        customerId,
        customerName,
        customerDomainName,
        invoiceNumber,
        orderDate,
        substring(skuName,0,30) skuName,
        skuid,
        productname,
        productid,
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
        subscriptionStartDate,
        subscriptionEndDate,
        monthname(cast(subscriptionstartdate as datetime)) || cast(year(cast(subscriptionstartdate as datetime))as varchar) as  mn
        """ # concat monthname and year

        cxn.query(f"create temp table office_temp as select {requiredColumns} from latestMonthDF where productid like 'CFQ%'")
        cxn.query(f"""
 
            with 
                t0 as (
                select {requiredColumns}
                from office_temp where productid like 'CFQ%'  
                --and customerDomainName = 'yaxiso365.onmicrosoft.com'
                --and customerDomainName = 'annet50.onmicrosoft.com'
                --and subscriptionid = '12266bbd-6d4a-422f-d413-4c688cc48550'
                )
                ,
                
                t1 as                                            /* club rows that have similar charge types and reference IDs. */

                (select customername,customerid,customerdomainname,skuname,skuid,productname,max(cast(orderDate as datetime)) orderdate1
                ,last(invoicenumber order by orderdate) newInvoiceNumber
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
                group by customername,customerid,customerdomainname,subscriptionid,skuname,skuid,productname,truncatedchargestartdate, chargeenddate,chargetype,referenceid,mn
                order by subscriptionid,orderdate1)
                ,

                t2 as

                (select *
                ,sum(QtyAdded_adjustedForNegativity) over(partition by customerid,customerdomainname, subscriptionid, mn ,cast(chargeenddate as date) order by orderdate1 rows between unbounded preceding and  0 following) as finalLicenseQty
                ,truncatedchargestartdate newSDate
                , lead(truncatedchargestartdate) over(partition by customerid, customerdomainname,subscriptionid,mn order by orderdate1 ) adjustedChargeStartDate 
                ,case when adjustedChargeStartDate is null then date_add(cast(chargeenddate as date), interval 1 day) 
                else adjustedChargeStartDate end as  lead_adjustedChargeStartDate,
                from t1
                    order by subscriptionid,orderdate1 )
                ,

                subscriptionWiseBreakdown as
                
                    (select customerid,customerdomainname,subscriptionid,round(sum(newTotal),2) sm from t2 group by 1,2,3 order by 4 desc)
                ,

                final as 
                (
                select customername,customerid,customerDomainName,newInvoiceNumber as invoiceId,orderDate1 orderDate,subscriptionId,skuname,skuid,productname,newSdate as chargeStartDate, cast(lead_adjustedChargeStartDate as date) as chargeEndDate,chargeType, finalLicenseQty Qty, newTotal Amount
                --, mn billingMonth
                from t2
                order by subscriptionid,orderdate
                )
                
                select *,'{billingMonth}' as invoiceMonth from final 
                
                
            """).to_csv('../test.csv')
        
        return None
    
if __name__ == "__main__":
    main()
