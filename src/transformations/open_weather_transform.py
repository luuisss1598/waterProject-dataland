import pandas as pd 
from typing import Any, NewType
from extractors.extract_open_weather_data import OpenWeatherAPI
from utils.logging_config import logger
import json
from utils.md5 import generate_md5_key


"""
Function to transform data from OpenWeather API
"""
Dataframe = NewType('Dataframe', pd.DataFrame)

def transform_open_weather_data_from_api(extracted_data_open_weather) -> Dataframe:
    # build the dataframe, but we need to flatten out the json data first
    logger.info('Converting JSON into Pandas Dataframe..')
    try:
        # create a unique value for the each row generated from the API before creating dataframe
        _pk = generate_md5_key(extracted_data_open_weather['id'], extracted_data_open_weather['extracted_at'])
        extracted_data_open_weather['_pk'] = _pk
        
        df = pd.DataFrame([extracted_data_open_weather])
        return df
    
    except Exception as err:
        logger.error('Error creating Pandas Dataframe')
        raise

# open_weather = OpenWeatherAPI()

# zip_code = '94804'
# transformed_data = open_weather.get_daily_open_weather_data(zip_code=zip_code)
# data = transform_open_weather_data_from_api(extracted_data_open_weather=transformed_data)

# # print(json.dumps(transformed_data, indent=4))
# print(data)
