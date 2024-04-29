from pymongo import MongoClient

import requests

from datetime import datetime
 
client = MongoClient('mongodb://localhost:27017/')

db = client['Tw']
 
 
try:

    products_data = []
 
    collection_names = db.list_collection_names()
    
    for collection_name in collection_names:

        collection = db[collection_name]
 
        # Fetch documents directly from MongoDB and convert created_at to datetime

        product_docs = list(collection.find({}, {"_id": 0, "buildurl": 1, "created_at": 1}).sort("created_at", -1).limit(5))

        products_data.append({

            'name': collection_name,

            'urls': [doc['buildurl'] for doc in product_docs]

        })
 
except Exception as e:

    print('Error fetching product data:', e)
 
 
print(products_data)
 
def fetch_and_store_data(url, collection_name, pn):

    try:

        response = requests.get(url)

        response.raise_for_status()

        data_array = response.json()
 
        # Define the schema for the build

        build_schema = {

            "bid": str,

            "test_result_id": str,

            "created_at": str,

            "result": str,

             "log": str,

            "test_case_id": str,

            "Name": str,

            "branch_id": str,

            "branch_name": str,

            "build_id": str,

            "build_name": str,

            "team_id": str,

            "team_name": str,

            "stage": str,

            "critical": str,

            "test_duration": str,

            "Url": str

        }

        pndb = f"Tw_{pn}"

        db = client[pndb]

        collection = db[collection_name]
 
        for element in data_array:

            # Convert created_at string to datetime object

            element['created_at'] = datetime.strptime(element['created_at'], "%Y-%m-%d %H:%M:%S")

            # Insert document into collection

            collection.insert_one(element)
 
        print(f"Fetched and saved documents from URL: {url} into collection {collection_name}")
 
    except Exception as e:

        print(f"Error fetching or storing data from {url}: {e}")
 
def main():

    try:

        for product_data in products_data:

            product_name = product_data['name']

            urls = product_data['urls']

            collection_names = [f"Tw_build{i+1}" for i in range(len(urls))]

            for url, collection_name in zip(urls, collection_names):

                fetch_and_store_data(url, collection_name, product_name)
 
    except Exception as e:

        print('Error:', e)

main()


import pymongo

import requests
 
 
from links import cortina_urls,  mxl_urls,  broadcom_urls, tw_urls
 
client = pymongo.MongoClient('mongodb://localhost:27017/')

db = client['Tw']
 
 
mxl=mxl_urls

cortina=cortina_urls

tw=tw_urls

broadcom=broadcom_urls
 
 
 
for url in tw:

    try:

        response = requests.get(url)

        dataArray = response.json()
 
        collectionName = url.split('/')[5] 

        branchSchema = {

            'bid': {'type': int, 'required': False},

            'name': {'type': str, 'required': False},

            'created_at': {'type': str, 'required': False},

            'branch_name': {'type': str, 'required': False},

            'branch_id': {'type': int, 'required': False},

            'status': {'type': str, 'required': False},

            'pipeline_status': {'type': str, 'required': False},

            'Url': {'type': str, 'required': False},

            'buildurl': {'type': str, 'required': False}

        }
 
       

        collection = db[collectionName]
 
        

        for element in dataArray:

            newBranch = {

                'bid': element['build_id'],

                'name': element['name'],

                'created_at': element['created_at'],

                'branch_name': element['branch_name'],

                'branch_id': element['branch_id'],

                'status': element['status'],

                'pipeline_status': element['pipeline_status'],

                'Url': url,

                'buildurl': f"http://skydocker.adtran.com/api/build/{element['build_id']}/results"

            }

            result = collection.insert_one(newBranch)

            print(f"Document saved successfully in collection {collectionName}: {result.inserted_id}")
 
        print(f"Fetched and saved documents from URL: {url} into collection {collectionName}")
 
    except Exception as e:

        print(f"Error fetching JSON: {e}")
