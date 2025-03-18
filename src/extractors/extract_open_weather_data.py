import requests
import json
from utils.load_variables import get_env
from utils.logging_config import logger
from typing import Dict
from datetime import datetime
class OpenWeatherAPI:
    """Open weather API class"""
    def __init__(self):
        self.open_weather_api = get_env('OPEN_WEATHER_API')
        self.lat = None
        self.lon = None
    
    def __get_open_weather_location_zip_code_data(self, zip_code: str) -> Dict:
        open_weather_location_data_url: str = f'http://api.openweathermap.org/geo/1.0/zip?zip={zip_code}&appid={self.open_weather_api}'

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
    
    def get_daily_open_weather_data(self, zip_code:str ) -> Dict:
        """Fetch daily data"""
        # get zip code location data
        data: dict = self.__get_open_weather_location_zip_code_data(zip_code=zip_code)
        self.lat = data['lat']
        self.lon = data['lon']

        open_weather_data_url = f'https://api.openweathermap.org/data/2.5/weather?lat={self.lat}&lon={self.lon}&appid={self.open_weather_api}'

        try:
            logger.info('Extracting location data from OpenWeather API...')
            reponse = requests.get(url=open_weather_data_url, timeout=5)
            reponse.raise_for_status()
            
            logger.info('Data extracted successfully...')
            extracted_at = datetime.now()
            
            # add an approximate timestamp for when the data was extracted from the API
            data_extracted = reponse.json()
            data_extracted["extracted_at"] = str(extracted_at)
            
            return data_extracted
        
        except requests.exceptions.HTTPError as http_error:
            logger.error(f'HTTP error: {http_error}')
            raise 

        except requests.exceptions.RequestException as req_error:
            logger.info(f'Request failed: {req_error}')
            raise 
    
    # def get_historical_open_weather_data(self, zip_code: str) -> Dict:
    #     """Fetch historical data"""
    #     open_weather_data_url = f'https://history.openweathermap.org/data/2.5/history/city?lat={self.lat}&lon={self.lon}&type=hour&appid={self.open_weather_api}'
        
    #     try:
    #         response = requests.get(url=open_weather_data_url, timeout=5)
    #         response.raise_for_status() #if status != 200, raise exception
            
    #         return response.json()

    #     except requests.exceptions.HTTPError as http_err:
    #         logger.info(f'HTTP error: {http_err}')
    #         raise 
    #     except requests.exceptions.RequestException as req_error:
    #         logger.info(f'Request error: {req_error}')
    #         raise
            

# test the function to make sure it fetches the right amount of data

# zip_code = '94804'
# data_obj = OpenWeatherAPI()
# data = data_obj.get_daily_open_weather_data(zip_code=zip_code)
# data2= data_obj.get_historical_open_weather_data(zip_code=zip_code)

# with open('./data/raw/historical_weather_data.json', 'w+') as f:
#     json.dump(data, f)

# print(json.dumps(data, indent=4))
# print(json.dumps(data2, indent=4))