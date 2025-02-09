
# encoding: utf-8

import sys
import os
print(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))
from dotenv import load_dotenv
load_dotenv("./vulcan.env")
import re
import pandas as pd
import requests
import datetime
import os
os.environ['USE_PYGEOS'] = '0'
import geopandas
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
from translate import Translator
import time
start_time = time.time()




# designate the path of font
font_path = os.getenv("TaipeiSansTCBeta-Regular.ttf")
# setting Matplotlib to use designated font
plt.rcParams['font.family'] = FontProperties(fname=font_path).get_name()


class WebAPIJson:
    def __init__(self, url: str):
        # Initialize the class with the JSON data retrieved from the given URL
        # self.json_data is a type of JSON object
        self.json_data = requests.get(url).json()

class CWBD0047(WebAPIJson):
    def __init__(self, county_code:int = 63):
        """
        county_code: int, default 63. From 1 to 85. +4.
        """
        ### Government Open Data Platform
        url = f'https://opendata.cwb.gov.tw/api/v1/rest/datastore/F-D0047-{county_code:03d}?Authorization=CWB-4864F9F8-9D54-432C-876B-1C8053C39EAE&format=JSON&sort=time'
        super().__init__(url)
        self.get_level2_data()

    def get_level2_data(self):
        self.cwb_data = []

        # Get all elements from 'locations' to a list
        self.town_list = self.json_data['records']['locations'][0]['location']

        for town in self.town_list:
            town_name = town['locationName']
            town_code = town['geocode']
            # Get time sequence data from 'weatherElement'
            # next(): Return the next item from the iterator
            weather_description = next(element for element in town['weatherElement'] if element['elementName'] == 'WeatherDescription')['time']

            for weather in weather_description:
                start_time = datetime.datetime.strptime(weather['startTime'], '%Y-%m-%d %H:%M:%S')
                end_time = datetime.datetime.strptime(weather['endTime'], '%Y-%m-%d %H:%M:%S')
                value = weather['elementValue'][0]['value']

                # Split the value by '。' and remove the last element
                value = value.split('。')
                value.pop()

                # Extract rainfall_probability from the 2nd element of value
                # group() conbine the elements extracted by search()
                rainfall_probability = int(re.search(r'\d+', value[1]).group())

                # Extract temperature from the 3rd element of value
                temperature = int(re.search(r'\d+', value[2]).group())

                # Extract relative_humidity from the 6th element of value
                relative_humidity = int(re.search(r'\d+', value[5]).group())

                # Create a dictionary with all the data
                value_dict = {'town_name': town_name,'town_code': town_code , 'start_time': start_time, 'end_time': end_time,
                              'rainfall_probability': rainfall_probability, 'temperature': temperature,
                              'relative_humidity': relative_humidity, 'weather': value[0], 'wind': value[3], 'comfort': value[4]}
                
                # Update the dictionary with town_name, start_time, end_time
                value_dict.update({'town_name': town_name, 'start_time': start_time, 'end_time': end_time})

                # deposit the dictionary into the cwb_data list inorder to transform to dataframe
                self.cwb_data.append(value_dict)

        # Convert the list to a DataFrame
        # Extend the value element to columns
        self.cwb_data = pd.DataFrame(self.cwb_data)
        # Drop data from the DataFrame if the end_time is less than the current time
        # Only chose the data which column['end_time'] is biger than the current time
        self.cwb_data = self.cwb_data[self.cwb_data['end_time'] > datetime.datetime.now()]
        # self.cwb_data.reset_index(drop=True, inplace=True)  # 重新設置索引，以匹配過濾後的 DataFrame
        # keep the last data
        self.cwb_data = self.cwb_data.loc[self.cwb_data.groupby('town_code')['end_time'].idxmin()]
        



if __name__ == '__main__':

    df_weather = pd.DataFrame()
    # Get weather data from API
    for i in range(1, 89, 4):
        #get weather data from API
        cwb_obj = CWBD0047(i)
        df_weather = df_weather._append(cwb_obj.cwb_data)

    df_weather.to_csv('./weather.csv',index=False,encoding='utf-8-sig')
        # #Upload the data to the DynamoDB table
        # self.upload_weather()




print("--- %s seconds ---" % (time.time() - start_time))

