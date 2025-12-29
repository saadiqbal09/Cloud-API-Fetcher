import requests
import json

def fetch_api_data(api_url):
    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            api_data = response.json()
            print(json.dumps(api_data,indent=4))
        else:
            print(f"unable to fetch data status code: {response.status_code}")
    except Exception as e :
        print(f"AN error : {str(e)}")
api_url = "https://jsonplaceholder.typicode.com/posts"
fetch_api_data(api_url)
