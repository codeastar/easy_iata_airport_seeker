import requests
import os, sys, json, time
from tinydb import TinyDB, Query

db = TinyDB('skyscanner_iata.json')
Profile = db.table('Profile')
Countries = db.table('Countries')
Airports = db.table('Airports')

ENDPOINT_PREFIX = "https://skyscanner-skyscanner-flight-search-v1.p.rapidapi.com/apiservices/"
MARKET = "US"
CURRENCY = "USD"
SLEEP_BUFFER = 5

def initProfileDB():
  if "SKYSCAN_RAPID_API_KEY" in os.environ:
    API_KEY = os.environ['SKYSCAN_RAPID_API_KEY']   
    Profile.upsert({'api_key':API_KEY}, Query().api_key.exists())
  else: 
    API_KEY = Profile.search(Query().api_key)
    if API_KEY == []: 
      sys.exit("No API key found")
    API_KEY = API_KEY[0]['api_key']

  profile_dict = {
    "API_KEY": API_KEY,
  }
  return profile_dict    

def handleAPIException(responseText, apiname):
      print(json.dumps(json.loads(responseText), indent=3, sort_keys=True))
      sys.exit(f"API exception on [{apiname}]")    

def getCountries(headers):
    country_list = []
    ss_countries = Countries.search(Query())
    request_start_time=0
    if len(ss_countries) == 0:
        request_start_time = time.time()
        url = ENDPOINT_PREFIX+f"reference/v1.0/countries/en-US"
        response = requests.request("GET", url, headers=headers)
        if response.status_code != 200: handleAPIException(response.text, "getCountries")
        ss_countries = json.loads(response.text)
        Countries.insert_multiple(ss_countries["Countries"])
        ss_countries = ss_countries["Countries"]
    for country in ss_countries:
        country_list.append(country['Name'])    
    return country_list, request_start_time    

def getIataCode(place_string, request_time_list):
    url = ENDPOINT_PREFIX+f"autosuggest/v1.0/{MARKET}/{CURRENCY}/en-US/"
    querystring = {"query":place_string}
    request_start_time = time.time()
    request_time_list.append(request_start_time)
    if (len(request_time_list) % 40 == 0): 
        the_first_request_time = request_time_list[0]
        second_from_1st_request = round(time.time() - the_first_request_time)
        print(f"Hit the 40th request mark, wait for 60-{second_from_1st_request} seconds")
        time.sleep(60-second_from_1st_request+SLEEP_BUFFER)
        request_time_list =[]
    response = requests.request("GET", url, headers=headers, params=querystring)
    if response.status_code != 200: handleAPIException(response.text, "getIataCode")
    place_json = json.loads(response.text)    
    for place in place_json["Places"]:
        if ((len(place['PlaceId']) == 7) and (place['CountryName']==place_string)):
            print(f"{place['PlaceName']}, {place['CountryName']} - {place['PlaceId'][:3]}")
            place_dict = {
                "PlaceName": place['PlaceName'],
                "CountryName": place['CountryName'],
                "Iata": place['PlaceId'][:3], 
            }
            Airports.upsert(place_dict, Query().Iata == place['PlaceId'][:3])
    return request_time_list        

print("Start processing your request...")
profile_dict = initProfileDB()
headers = {
    'x-rapidapi-host': "skyscanner-skyscanner-flight-search-v1.p.rapidapi.com",
    'x-rapidapi-key': profile_dict["API_KEY"]
    }    
airports = Airports.search(Query().Iata)
if airports == []: 
    print("No Airport found, start requesting from Skyscanner API...")
    request_time_list = []
    country_list, request_start_time = getCountries(headers)
    if (request_start_time > 0): request_time_list.append(request_start_time)
    for country in country_list:
        request_time_list = getIataCode(country, request_time_list)

print("Got Airport from DB")
print(Airports.search(Query()))
