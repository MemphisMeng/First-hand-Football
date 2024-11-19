import requests, os
from dotenv import load_dotenv
import json

def fetch():
	load_dotenv()

	url = "https://soccer-football-info.p.rapidapi.com/live/full/"

	querystring = {"l":"en_US","f":"json","e":"no"}

	headers = {
		"x-rapidapi-key": os.getenv("API_KEY"),
		"x-rapidapi-host": "soccer-football-info.p.rapidapi.com"
	}

	response = requests.get(url, headers=headers, params=querystring)
	return response.json()['result']