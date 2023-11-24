import requests as rs
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from azure.storage.blob import ContainerClient,BlobServiceClient
from io import StringIO
import numpy as np
import calendar
import json
import datetime as dt
import warnings
import time

warnings.filterwarnings('ignore')

pd.set_option('display.max_columns', None)

pd.set_option('display.max_rows', None)

secrets = json.load(open(file='./secrets.json'))

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

def getAccessToken(request_body):
    req = rs.post(
                  "https://login.windows.net/6e75cca6-47f0-47a3-a928-9d5315750bd9/oauth2/token"
                  ,data = request_body)
    access_token = json.loads(req.text)['access_token']
    return access_token


access_token = getAccessToken(request_body = request_body)

headers = {'Authorization': 'Bearer ' + access_token}

def getInvoices (base_url):
    relative_url = '/v1/invoices'

    response = json.loads(
    rs.get(
            f"{base_url}{relative_url}",
           headers=headers)
    .content)

    main_df=pd.DataFrame() # Main DataFrame, currently empty

    columns = list(response['items'][0].keys())

    for row in response['items']:
        row_df = pd.DataFrame.from_dict([row]) #Each row gets converted to a DataFrame
        main_df = pd.concat([main_df,row_df],ignore_index=True) #Then gets appended to the Main DataFrame
        
    return main_df

invoices_df = getInvoices(base_url = base_url)
invoices_df = invoices_df[invoices_df['invoiceType']=='OneTime']

invoiceIdList = list(invoices_df['id'])
invoiceDateList = list(invoices_df['invoiceDate'])

zipp = zip(invoiceIdList,invoiceDateList)
# latestInvoice,latestInvoiceDate = list(zipp)[-1] 

def getInvoiceLineItems(base_url, invoiceId):
    relative_url = f'/v1/invoices/{invoiceId}/lineitems/OneTime/BillingLineItems?size=5000'

    response = json.loads(
        rs.get(
                f"{base_url}{relative_url}",
               headers=headers)
        .content)

    main_df=pd.DataFrame()

    columns = list(response['items'][0].keys())

    for row in response['items']:
        sub_df = pd.DataFrame.from_dict([row])
        main_df = pd.concat([main_df,sub_df],ignore_index=True)
    return main_df
    

invoice_df = getInvoiceLineItems(base_url = base_url, invoiceId = 'G020077203')
invoice_df.head()

print(invoice_df[
    (invoice_df['customerName'].str.startswith('TATA'))
          ][
    ['productName','skuName','termAndBillingCycle','totalForCustomer','chargeStartDate','chargeEndDate']
        ].sort_values('productName'))




