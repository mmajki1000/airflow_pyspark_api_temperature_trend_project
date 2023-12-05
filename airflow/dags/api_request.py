import requests
import json

# import city parameters from .json file. It is needed for API request


def get_params():
    f = open('./data/raw/city.json')
    params = json.load(f)
    s_date = params['s_date']
    e_date = params['e_date']
    lat = params['lat']
    lon = params['lon']
    # request string creation (open source weather API), it returns daily max temp for provided date range
    client = r'https://archive-api.open-meteo.com/v1/'
    endpoint = f'era5?latitude={lat}&longitude={lon}&start_date={s_date}&end_date={e_date}&daily=temperature_2m_max'
    sensor = f'era5?latitude={lat}&longitude={lon}&start_date="2022-01-01"&end_date="2022-01-02"&daily=temperature_2m_max'
    url = client + endpoint
    return url, sensor


url = get_params()[0]

# API call function
def get_data(url):
    try:
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()


    except requests.exceptions.HTTPError as e:
        print(e.response.text)
    
    # save data to .json

    with open('./data/raw/input_data.json', 'w') as outfile:
        json.dump(data, outfile, indent=4) 


get_data(url)


