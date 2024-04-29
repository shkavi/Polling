from pymongo import MongoClient
import requests
from datetime import datetime
import sys
from links import product_links

client = MongoClient('mongodb://localhost:27017')

# Function to fetch and store data from the API
def fetch_and_store_data(url, collection_name):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data_array = response.json()

        db = client[collection_name.split('_')[0]]  # Use product name as database name
        collection = db[collection_name]

        for element in data_array:
            # Convert created_at string to datetime object
            if 'created_at' in element:
                element['created_at'] = datetime.strptime(element['created_at'], "%Y-%m-%d %H:%M:%S")

            # Insert document into collection
            collection.insert_one(element)

        print(f"Fetched and saved documents from URL: {url} into collection {collection_name}")

    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occurred while fetching data from {url}: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
    except Exception as e:
        print(f"Error storing data into collection {collection_name}: {e}")

# Function to fetch and update data for all products
def fetch_and_update_all():
    try:
        for product_name, urls in product_links.items():
            for i, url in enumerate(urls, 1):
                collection_name = f"{product_name}_build{i}"
                fetch_and_store_data(url, collection_name)

    except Exception as e:
        print('Error:', e)

# Main function
if __name__ == "__main__":
    fetch_and_update_all()
