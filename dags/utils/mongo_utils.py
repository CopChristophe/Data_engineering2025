from pymongo import MongoClient

def fetch_from_mongo(url):
    """Fetches HTML content from MongoDB for processing."""
    mongo_client = MongoClient(host="mongo-db", port=27017, username="root", password="example")
    db = mongo_client["files_db"]
    collection = db["files"]

    file_data = collection.find_one({"url": url})
    if file_data and "content" in file_data:
        print(f"Fetched content for URL: {url}")
        return file_data["content"]
    else:
        print(f"No content found for URL: {url}")
        return None