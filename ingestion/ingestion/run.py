import requests, os
from dotenv import load_dotenv
import json

load_dotenv()

url = "https://soccer-football-info.p.rapidapi.com/live/full/"

querystring = {"l":"en_US","f":"json","e":"no"}

headers = {
	"x-rapidapi-key": os.getenv("API_KEY"),
	"x-rapidapi-host": "soccer-football-info.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

# ensure the folder /storage exists
os.makedirs('../../storage', exist_ok=True)
with open('../../storage/file.json', 'w') as f:
    json.dump(response.json()['result'], f, indent=2)
    
f.close()