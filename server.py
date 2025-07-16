from requests_oauthlib import OAuth1
import requests
import sys
import glob
import time
import os
import json
import pandas as pd
from datetime import datetime, timedelta
from alive_progress import alive_bar
import schedule 


mage_auth = OAuth1(
    client_key=os.environ['MAGE_CONSUMER_KEY'],
    client_secret=os.environ['MAGE_CONSUMER_SECRET'],
    resource_owner_key=os.environ['MAGE_TOKEN_ID'],
    resource_owner_secret=os.environ['MAGE_TOKEN_SECRET'],
    signature_method="HMAC-SHA256",
)

ns_auth = OAuth1(
    client_key=os.environ['NS_CONSUMER_KEY'],
    client_secret=os.environ['NS_CONSUMER_SECRET'],
    resource_owner_key=os.environ['NS_TOKEN_ID'],
    resource_owner_secret=os.environ['NS_TOKEN_SECRET'],
    realm=os.environ['NS_REALM'],
    signature_method="HMAC-SHA256",
)



mage_headers = {"Content-Type": "application/json"}
ns_headers = {"Content-Type": "application/json", "prefer": "transient"}

def getMissingCosts():
    print("Getting Magento Missing Costs...")
    currentPage = 1
    lastPage = False
    all_missing_df = pd.DataFrame()
    while lastPage == False:
        ALLPRODUCTS_URL = os.environ['MAGE_URL'] + "/rest/V1/products/?searchCriteria[filter_groups][1][filters][0][field]=cost&searchCriteria[filter_groups][1][filters][0][condition_type]=null&searchCriteria[filter_groups][2][filters][0][field]=status&searchCriteria[filter_groups][2][filters][0][condition_type]=eq&searchCriteria[filter_groups][2][filters][0][value]=1&fields=items[sku,id]&searchCriteria[page_size]=500&searchCriteria[currentPage]=" + str(currentPage)
        products_response = requests.request("GET", ALLPRODUCTS_URL, auth=mage_auth, headers=mage_headers)
        products_data = json.loads(products_response.text)
        product_paged_df = pd.json_normalize(products_data,"items")

        if product_paged_df.empty:
            print("Last page!")
            lastPage = True
        else:
            all_missing_df = pd.concat([all_missing_df, product_paged_df])
            missing_count = all_missing_df.shape[0]
            print("Magento Results Retrieved: " + str(missing_count))
            currentPage += 1

    return all_missing_df



def getNSCosts():
    print("Getting NS Item Costs...")
    offset = 0
    totalResults = 0
    lastPage = False
    all_costs_df = pd.DataFrame()

    while lastPage == False:

        NS_QUERY_URL = os.environ['NS_URL'] + "services/rest/query/v1/suiteql?limit=1000&offset=" + str(offset)
        NS_QUERY = {"q": "select itemid, round(lastpurchaseprice,2) as lastpurchaseprice from item where isOnline = 'T' and itemType = 'InvtPart'"}
        costs_response = requests.post(NS_QUERY_URL, auth=ns_auth, headers=ns_headers, json=NS_QUERY)
        costs_data = json.loads(costs_response.text)
        responseCount = costs_data["count"]
        totalResults += responseCount
        print("NS Results Retrieved: " + str(totalResults))
        if responseCount < 1000:
            lastPage = True
        else:
            offset += 1000

        costs_paged_df = pd.json_normalize(costs_data,"items")
        all_costs_df = pd.concat([all_costs_df, costs_paged_df])
    return all_costs_df

def getNSKitCosts():
    print("Getting NS Kit Costs...")
    offset = 0
    totalResults = 0
    lastPage = False
    all_costs_df = pd.DataFrame()
    while lastPage == False:

        NS_QUERY_URL = os.environ['NS_URL'] + "services/rest/query/v1/suiteql?limit=1000&offset=" + str(offset)
        NS_QUERY = {"q": "select itemid, sum(lastpurchaseprice) as lastpurchaseprice from (select i.itemid, ic.itemid as component_id, ic.lastpurchaseprice from item i right join itemmember im on i.id = im.parentitem right join item ic on im.item = ic.id where i.itemType = 'Kit' and i.isOnline = 'T') group by itemid"}        
        costs_response = requests.post(NS_QUERY_URL, auth=ns_auth, headers=ns_headers, json=NS_QUERY)
        costs_data = json.loads(costs_response.text)
        responseCount = costs_data["count"]
        print("Items on this NS page: " + str(responseCount))
        if responseCount < 1000:
            lastPage = True
        else:
            offset += 1000

        costs_paged_df = pd.json_normalize(costs_data,"items")
        all_costs_df = pd.concat([all_costs_df, costs_paged_df])
    return all_costs_df



def generateCostJSON(costs_df):
    json_body = [
    {
        "product": {
            "sku": str(row['itemid']),
            "custom_attributes": [
                {
                    "attribute_code": "cost",
                    "value": str(row['lastpurchaseprice'])
                }
            ]
        }
    }
    for _, row in costs_df.iterrows()
    ]

    return json_body


def getUploadProcess(bulk_uuid):
    STATUS_URL = os.environ['MAGE_URL'] + "/rest/V1/bulk/" + bulk_uuid + "/status"
    status_response = requests.request("GET", STATUS_URL, auth=mage_auth, headers=mage_headers)
    status_data = json.loads(status_response.text)
    status_data_df = pd.json_normalize(status_data,"operations_list")
    return status_data_df


def processCosts():

    #Get all item costs from NS
    ns_costs = getNSCosts()
    #Get all kit costs from NS
    ns_kit_costs = getNSKitCosts()
    #Merge into one table
    ns_costs=pd.concat([ns_costs, ns_kit_costs], ignore_index=True)
    #Get all items on Magento with no cost value
    mage_missing_costs = getMissingCosts()
    #Filter cost table to only SKUs with missing costs
    filtered_costs = ns_costs[ns_costs['itemid'].isin(mage_missing_costs['sku'])]
    #Upload if any exist
    if filtered_costs.shape[0] > 0 :

        print("Uploading " + str(filtered_costs.shape[0]) + " cost records")
        #Generate upload JSON body from table
        upload_body = generateCostJSON(filtered_costs)
        MAGE_UPLOAD_URL = os.environ['MAGE_URL'] + "/rest/async/bulk/V1/products"
        #Post to Magento API
        upload_response = requests.post(MAGE_UPLOAD_URL, auth=mage_auth, headers=mage_headers, json=upload_body)
        upload_response_body = json.loads(upload_response.text)
        upload_uuid = upload_response_body["bulk_uuid"]
        print("Upload UUID " + upload_uuid)
    else :
        print("No new costs to upload!")



print("NS Mage Sync app started, loading schedules...")
print(os.environ)




