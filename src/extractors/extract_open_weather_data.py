import requests
import json
from utils.load_variables import get_env
from utils.logging_config import logger
from typing import Dict


open_weather_api:str = get_env('OPEN_WEATHER_API')

def get_open_weather_location_zip_code_data(zip_code: str) -> Dict:
    open_weather_location_data_url: str = f'http://api.openweathermap.org/geo/1.0/zip?zip={zip_code}&appid={open_weather_api}'

    try:
        logger.info('Extracting location data from OpenWeather API...')
        response = requests.get(url=open_weather_location_data_url, timeout=5)
        response.raise_for_status() # if status is not 200

        logger.info('Data extracted successfully')
        return response.json()
        
    except requests.exceptions.HTTPError as http_error:
        logger.error(f'HTTP error: {http_error}')
        raise

    except requests.exceptions.RequestException as req_error:
        logger.error(f'Request failed: {req_error}')
        raise
        
def get_open_weather_data(zip_code: str) -> Dict:

    # get zip code location data
    data: dict = get_open_weather_location_zip_code_data(zip_code=zip_code)
    lat = data['lat']
    lon = data['lon']

    open_weather_data_url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={open_weather_api}'

    try:
        logger.info('Extracting location data from OpenWeather API...')
        reponse = requests.get(url=open_weather_data_url, timeout=5)
        reponse.raise_for_status()
        
        logger.info('Data extracted successfully...')
        return reponse.json()
    
    except requests.exceptions.HTTPError as http_error:
        logger.error(f'HTTP error: {http_error}')
        raise 

    except requests.exceptions.RequestException as req_error:
        logger.info(f'Request failed: {req_error}')
        raise


# test the function to make sure it fetches the right amount of data
# zip_code = '98404'
# data = get_open_weather_data(zip_code=zip_code)

# print(json.dumps(data, indent=4))


