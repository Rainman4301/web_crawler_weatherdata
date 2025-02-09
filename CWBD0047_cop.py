#encoding=utf-8


import sys
import os
file_path = os.path.dirname(__file__)
sys.path.append(os.path.join(file_path, os.path.pardir))
from dotenv import load_dotenv
load_dotenv(os.path.join(file_path,"vulcan.env"))
import pandas as pd
import requests
import json
from decimal import Decimal
import boto3
import os
os.environ['USE_PYGEOS'] = '0'
import geopandas
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import matplotlib.font_manager as fm
import io
import time
start_time = time.time()
import shutil
from shapely.geometry import Point




def get_shapefile(file_name):

    # Read the shapefile and decode it using UTF-8
    geodata = geopandas.read_file( os.path.join(file_path,f'taiwan/{file_name}'), encoding='utf-8')
    
    return geodata



class WeatherData():

    def __init__(self):
       
        #only taiwan shp
        self.geo_data = None
        #weather plus taiwan shp
        self.df_weather = None
        self.AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
        self.AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1', aws_access_key_id=self.AWS_ACCESS_KEY, aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY)

    def upload_weather(self):
        # Upload the data to the DynamoDB table
        table = self.dynamodb.Table('weather')
        for index, row in self.df_weather.iterrows():
            data = json.loads(row.to_json(), parse_float=Decimal)
            # overwrite the data if the same key exists
            table.put_item(Item=data)

    
    def main(self):
    
        self.df_weather = pd.DataFrame()
        self.df_weather = pd.read_csv(    os.path.join(file_path,'weather.csv') ,encoding='utf-8-sig' )

        

        #conbin df_weather and geodata
        self.geodata = get_shapefile("TOWN_MOI_1120317.shp")
        # Merge the geodata and the weather data
        self.df_weather['town_code'] = self.df_weather['town_code'].astype('int64')
        self.geodata['TOWNCODE'] = self.geodata['TOWNCODE'].astype('int64')


        self.df_weather = pd.merge(self.geodata, self.df_weather, left_on='TOWNCODE', right_on='town_code', how='left')
        # Convert geodata to geopandas, with the geometry column as 'polygon'
        self.df_weather = geopandas.GeoDataFrame(self.df_weather, geometry='geometry')


        # output df_weather to shpfile
        self.df_weather.to_file(os.path.join(file_path,'qgis_project','df_weather.shp'), encoding='utf-8')


        return self.df_weather
        

        
        


def generate_weather_plots(geodata, timestamp, location,taiwan_geo):
    
    # Font
    f = fm.FontProperties(fname=os.path.join(file_path, 'TaipeiSansTCBeta-Regular.ttf'))


    # Create a directory with the timestamp as the name
    output_folder =  os.path.join(file_path,f'picture/{timestamp}')  
    os.makedirs(output_folder, exist_ok=True)

    # Get the start_time and end_time from the first row of the DataFrame
    start_time = geodata.iloc[0]['start_time']
    end_time = geodata.iloc[0]['end_time']

    start_time = start_time.strftime('%Y%m%d %H')
    end_time = end_time.strftime('%H')

    

    if  location: 
        
        # 依照location參數的經緯度座標，過濾出geodata中geometry包含該座標的行政區資料
        # 創建一個 Point 對象表示經緯度座標
        point = Point(location[0], location[1])

        # 使用 GeoDataFrame 的 contains 函式進行空間運算
        specific_area = geodata[geodata.geometry.contains(point)]

        
    

    # Set the title of each subplot, Set the variables to be plotted
    variables = ['temperature', 'rainfall_probability', 'relative_humidity']
    titles = ['Temperature', 'Rainfall', 'Humidity']

    vmin_values = [0, 0, 0]
    vmax_values = [40, 100, 100]

    # Create a single figure with three subplots
    fig, axs = plt.subplots(1, 4, figsize=(24, 7))

    for i, ax in enumerate(axs.flatten()):


        # 如果location參數為空，則前三張圖為台灣全圖，最後一張圖為台灣地圖
        if not location:
            if i < len(variables):
                ax.set_title(f'Taiwan Weather \n {start_time}h - {end_time}h \n {titles[i]}', fontsize=20, fontproperties=f)
                # Column means that the subplot will draw the chart by means of the geodata's data related to the column name "variables[i]"
                taiwan_geo.plot(ax=ax, column=variables[i], legend=True, cmap='coolwarm', vmin=vmin_values[i], vmax=vmax_values[i], edgecolor='black')
            elif i == 3:  # 將第四張圖設置在 3 索引位置
                ax.set_title(f'{start_time}h - {end_time}h ', fontsize=20, fontproperties=f)
                # Column means that the subplot will draw the chart by means of the geodata's data related to the column name "variables[i]"
                taiwan_geo.plot(ax=ax, edgecolor='black')

                ax.text(1, 0.7,"輸入詳細位址\n能更清楚查詢到資料喔!", transform=ax.transAxes, fontsize=12, color='black', fontweight='bold',fontproperties = f)
                
            

            



        else:

            if i < len(variables):
                ax.set_title(f'  {geodata.iloc[0]["COUNTYNAME"]} Weather\n  {start_time}h - {end_time}h\n  {titles[i]}', fontsize=20,fontproperties = f)
                
                # Column means that the subplot will draw the chart by means of the geodata's data related to the column name "variables[i]"
                geodata.plot(ax=ax, column=variables[i], legend=True, cmap='coolwarm', vmin=vmin_values[i], vmax=vmax_values[i], edgecolor='black')
                # geodata.apply(lambda x: ax.text(x.geometry.centroid.x, x.geometry.centroid.y, f"{x['TOWNNAME']}\n{x[variables[i]]}", ha='center'), axis=1)
                for x in geodata.itertuples():
                    x_coord = x.geometry.centroid.x
                    y_coord = x.geometry.centroid.y

                    var = str(variables[i])
                    # label = f"{x.TOWNNAME}\n{x['var']}"
                    label = f"{x.TOWNNAME}\n{x._asdict()[var]}"

                    ax.annotate(
                        label,
                        (x_coord, y_coord),
                        textcoords="offset points",
                        xytext=(0, 10),
                        ha='center',
                        fontsize=8,
                        fontproperties = f,
                        arrowprops=dict(facecolor='black', arrowstyle='->')
                    )
                

            elif(i == 3 ):

                # 創建一個包含單一座標點的 GeoSeries
                address_geo = geopandas.GeoSeries(point)
                
                # 第四張圖以specific_area作圖
                ax.set_title(f' {geodata.iloc[0]["COUNTYNAME"]}" "{specific_area.iloc[0]["TOWNNAME"]}事件發生地', fontsize=20,fontproperties = f)

                for geometry in specific_area.geometry:
                    if geometry.geom_type == 'Polygon':
                        x, y = geometry.exterior.xy
                        ax.fill(x, y, color='blue', alpha=0.5)
                        ax.plot(x, y, color='black')  # 如果您需要邊界
                    elif geometry.geom_type == 'MultiPolygon':
                        for polygon in geometry:
                            x, y = polygon.exterior.xy
                            ax.fill(x, y, color='blue', alpha=0.5)
                            ax.plot(x, y, color='black')  # 如果您需要邊界
                            
              # 繪製指定地址的紅色星星
                address_geo.plot(ax=ax, marker='*', color='red', markersize=100, label='Address')
                # 顯示圖例
                ax.legend()
                
                ax.text(1, 0.7, f'  {geodata.iloc[i]["weather"]}\n  {geodata.iloc[i]["wind"]}\n  {geodata.iloc[i]["comfort"]}\n', transform=ax.transAxes, fontsize=12, color='black', fontweight='bold',fontproperties = f)
                
                break


        
        

        special_bound = ["宜蘭縣", "屏東縣", "高雄市","基隆市"]
 
        if geodata.iloc[0]['COUNTYNAME'] in special_bound and location:
            if geodata.iloc[0]['COUNTYNAME'] == "宜蘭縣":
                ax.set_xlim(121.2, 122.05)
                ax.set_ylim(24.25, 25.1)
            elif geodata.iloc[0]['COUNTYNAME'] == "屏東縣":
                ax.set_xlim(120.3083, 120.5635)
                ax.set_ylim(22.3964, 22.9581)
            elif geodata.iloc[0]['COUNTYNAME'] == "高雄市":
                ax.set_xlim(120.15, 121.05)
                ax.set_ylim(22.45, 23.5)
            elif geodata.iloc[0]['COUNTYNAME'] == "基隆市":
                ax.set_xlim(121.58, 121.83)
                ax.set_ylim(25.01, 25.2)
        elif(location):
            
            # Calculate the bounding box of the "geometry" column (Polygon) in the GeoDataFrame
            bbox = geodata['geometry'].total_bounds
            # Set the x and y axis limits using the bounding box values
            ax.set_xlim(bbox[0], bbox[2])
            ax.set_ylim(bbox[1], bbox[3])

        elif(not location):

            ax.set_xlim(119.5,122.5)
            ax.set_ylim(21.7, 25.7)







    # Save the single figure with subplots as an image file
    plt.tight_layout()
    image_file = io.BytesIO()
    plt.savefig(image_file, format='png')
    plt.close()

    # Move the file pointer to the beginning of the file
    image_file.seek(0)

    # Output the image file to the local folder inside the timestamp folder
    with open(f'{output_folder}/weather_plots_{timestamp}.png', 'wb') as f:
        f.write(image_file.read())






def check_new_data(object):

    table_dctr = object.dynamodb.Table('data_geoinfo')
    response = table_dctr.scan()
    items_dctr = response['Items']
    items_dctr = pd.DataFrame(items_dctr)

    table_wd = object.dynamodb.Table('data_weather')
    response = table_wd.scan()
    items_wd = response['Items']
    items_wd = pd.DataFrame(items_wd)

    # compare two dataframe and find out the new data by timestamp
    #items_dctr = items_dctr[~items_dctr['timestamp'].isin(items_wd['timestamp'])]
    #change the 台 to 臺
    items_dctr['county'] = items_dctr['county'].str.replace('台','臺')
    items_dctr = pd.DataFrame(items_dctr)
    return items_dctr
    
    
    
    
    
    
def data_to_dataweather(weather_data,row,url,index):

    index = str(index)

    '''
    table_wd = weather_data.dynamodb.Table('data_weather')     
    
    item ={'UUID':row["UUID"],
           'timestamp':row["timestamp"],
           'address':row['address'],
           'checked':True,
           'message':row['address']+"所在地區未來三小時內天氣預測圖如下",
           'url':url}
    table_wd.put_item(Item=item)

    '''


    weather_list = ["W1","F1","F2","A1","A2-1","A2-2","A3","B1","B2","B3"]
    earthquake_list = ["W2","C1","C2","C3"]




    # upload data to data_dmm_to_client table
    table_wd = weather_data.dynamodb.Table('data_dmm_to_client')     
      
    description = ""
    if(row['type'] in earthquake_list):
        description = "所在地區有感地震資訊如下"
    elif(row['type'] in weather_list):
        description = "所在地區未來三小時內天氣預測圖如下"

    item ={'UUID':row["UUID"],
        'timestamp':int(str(row["timestamp"])+index),
        'message':f'{row["address"]} {description}',
        'url':url
        }
    
    table_wd.put_item(Item=item)
    




    
    


def upload_S3_dynamodb(weather_data,check,i,index):


    #upload the folder with  the picture in 'picture' folder to AWS S3 & dynamodb 
    s3 = boto3.resource('s3',aws_access_key_id=weather_data.AWS_ACCESS_KEY, aws_secret_access_key=weather_data.AWS_SECRET_ACCESS_KEY)
    bucket = s3.Bucket('summer004')
    # walking through the folder of picture
    for root, dirs, files in os.walk(os.path.join(file_path,f'picture/{check.iloc[i]["timestamp"]}')):
        
        for file in files:
            
            #upload the picture to S3
            local_file_path = os.path.join(root, file)

            #check.iloc[i]["timestamp"]} 這個是資料夾名稱
            #file 這個是檔案名稱
            s3_key = f'{check.iloc[i]["timestamp"]}/{file}'  # 指定上傳後的完整路徑，格式為 '{timestamp}/{file}'
            
            bucket.upload_file(local_file_path, s3_key)   
            url = f'https://summer004.s3-ap-northeast-1.amazonaws.com/{check.iloc[i]["timestamp"]}/{file}'
            print(url)

            #upload the url to dynamodb
            data_to_dataweather(weather_data,check.iloc[i],url,index)

    


    
    #delete the file in local
    # 使用 shutil.rmtree() 函数删除文件夹及其内容
    try:
        shutil.rmtree(  os.path.join(file_path,f'picture/{check.iloc[i]["timestamp"]}')   )
        # print(f"Folder '{folder_path}' and its contents have been removed.")
    except Exception as e:
        print(f"An error occurred while removing the folder: {e}")

    
            
    print(f'upload {len(files)} new weather plots to S3')
        



class EarthquakeData():

    def __init__(self):
        self.url = 'https://opendata.cwb.gov.tw/api/v1/rest/datastore/E-A0015-001?Authorization=CWB-F7D84390-2893-4D9F-88F3-BB982C10DDB0'
        self.earthquakedata = None
        self.stationdata = None
        self.get_earthquake_data()
        self.get_station_data()

    def get_earthquake_data(self):
    
        response = requests.get(self.url)
        data = response.json()
        return data['records']['Earthquake']


    def get_station_data(self):
        
        response = requests.get(self.url)
        data = response.json()
        stations = {}
        for earthquake in data['records']['Earthquake']:
            for area in earthquake['Intensity']['ShakingArea']:
                for station in area['EqStation']:
                    station_id = station['StationID'] # 測站編號
                    if station_id not in stations:
                        stations[station_id] = {
                            'latitude': station['StationLatitude'],
                            'longitude': station['StationLongitude'],
                            'intensity': []
                        }
                    stations[station_id]['intensity'].append({
                        'area': area['AreaDesc'],
                        'intensity': station['SeismicIntensity']
                    })
        return stations
    


    def plot_earthquake_map(self, earthquake, station_data, taiwanshp, timestamp,address):


        # Font
        f = fm.FontProperties(fname=os.path.join(file_path,'TaipeiSansTCBeta-Regular.ttf'))    




        # Create a directory with the timestamp as the name
        output_folder = os.path.join(file_path,f'picture/{timestamp}')      
        os.makedirs(output_folder, exist_ok=True)




        # plot the earthquake map
        titleA = f"Earthquake No: {earthquake['EarthquakeNo']} - FocalDepth: {earthquake['EarthquakeInfo']['FocalDepth']} km\nReportColor: {earthquake['ReportColor']} - Epicenter Location: {earthquake['EarthquakeInfo']['Epicenter']['Location']}\nTime: {earthquake['EarthquakeInfo']['OriginTime']}"
        titleB =f"{address.iloc[0]['COUNTYNAME']} situation "
        fig, axs = plt.subplots(1,2,figsize=(17, 10))
        # 定義顏色映射字典，這裡只是示例，您可以根據需求定義更多的顏色
        color_map = {
            1: 'green',
            2: 'yellow',
            3: 'orange',
            4: 'red'
        }

        # 創建一字典來保存每個座標對應的最大震度值
        max_intensity_dict = {}

        for i in range(len(axs)):
            
            ax = axs[i]
            if(i == 0):
                ax.set_title(titleA,fontproperties = f)
                taiwanshp.plot(ax=ax, color='white', edgecolor='black')  # Set map color to white   
                


                # 繪製測站
                for station_id, station_info in station_data.items():
                    latitude = station_info['latitude']
                    longitude = station_info['longitude']
                    intensity_list = station_info['intensity']
                    x, y = longitude, latitude
                    # ax.plot(x, y, 'ro', markersize=2)


                    # 找到該座標對應的最大震度值
                    max_intensity = max(intensity_value['intensity'] for intensity_value in intensity_list)
                    
                    # 如果該座標已經存在於字典中，則比較並更新最大震度值
                    if (x, y) in max_intensity_dict:
                        max_intensity_dict[(x, y)] = max(max_intensity_dict[(x, y)], max_intensity)
                    else:
                        max_intensity_dict[(x, y)] = max_intensity

                    # 在字典中只保留最大震度值的座標
                    unique_coords = max_intensity_dict.keys()

                    # 加入測站的震度標籤
                    for intensity_value in unique_coords:
                        label = max_intensity_dict[(x, y)]
                        label = int(label[0])
                        color = color_map.get(label, 'black')
                        ax.annotate(label, (x, y), xytext=(1, 1), textcoords='offset points', fontsize=8, color=color,fontproperties = f)    
                
                
                # 繪製地震位置的紅色星星
                epicenter_longitude = earthquake['EarthquakeInfo']['Epicenter']['EpicenterLongitude']
                epicenter_latitude = earthquake['EarthquakeInfo']['Epicenter']['EpicenterLatitude']
                ax.plot(epicenter_longitude, epicenter_latitude, 'r*', markersize=10, label='Earthquake Epicenter')
                # 繪製指定地址地區的多邊形位置並上色
                address.plot(ax=ax, color='lightcoral', alpha=0.5)
                # setting the range of whole Taiwan
                ax.set_xlim(119, 123)
                ax.set_ylim(21.5, 25.7)
                 # 顯示圖例
                ax.legend()
            
            else:
                ax.set_title(titleB,fontproperties = f)
                # 繪製指定地址地區的多邊形位置並上色
                address.plot(ax=ax, color='white', edgecolor='black', alpha=0.5)


              
                # 繪製測站
                for station_id, station_info in station_data.items():
                    latitude = station_info['latitude']
                    longitude = station_info['longitude']
                    intensity_list = station_info['intensity']
                    x, y = longitude, latitude

                    # 判斷座標是否在 address 的邊界內
                    if address.geometry.contains(Point(x, y)).any():

                        # 在字典中只保留最大震度值的座標
                        unique_coords = max_intensity_dict.keys()
                        
                        # 加入測站的震度標籤
                        for intensity_value in unique_coords:
                            label = max_intensity_dict[(x, y)]
                            label = int(label[0])
                            color = color_map.get(label, 'black')
                            ax.annotate(label, (x, y), xytext=(1, 1), textcoords='offset points', fontsize=10, color=color,fontproperties = f)    
                
                
                # 繪製地震位置的紅色星星
                epicenter_longitude = earthquake['EarthquakeInfo']['Epicenter']['EpicenterLongitude']
                epicenter_latitude = earthquake['EarthquakeInfo']['Epicenter']['EpicenterLatitude']
                ax.plot(epicenter_longitude, epicenter_latitude, 'r*', markersize=10, label='Earthquake Epicenter')

                address.apply(lambda x: ax.text(x.geometry.centroid.x, x.geometry.centroid.y, f"{x['TOWNNAME']}\n", ha='center', fontsize=8,fontproperties = f), axis=1)

                # setting the range of the map depends on the address
                bbox = address['geometry'].total_bounds
                ax.set_xlim(bbox[0], bbox[2])
                ax.set_ylim(bbox[1], bbox[3])  

                # 顯示圖例
                ax.legend()


        plt.tight_layout()
        # Save the current subplot as a separate image file
        image_file = io.BytesIO()
        plt.savefig(image_file, format='png')
        plt.close()

        # Move the file pointer to the beginning of the file
        image_file.seek(0)

        # Output the image file to the local folder inside the timestamp folder
        with open(f'{output_folder}/earthquake_map_{timestamp}.png', 'wb') as f:
            f.write(image_file.read())

                

    
    def main(self):

        self.earthquakedata = self.get_earthquake_data()
        self.earthquakedata = self.earthquakedata[0] 


        self.stationdata = self.get_station_data()
        # 取得最新的地震資料
         



def main():

    weather_data = WeatherData()
    #我想寫一個檢查的機制判斷AWS中dynamodb中data_command_to_rosa table中是否有data_weather table中還未有的新資料
    #如果有新資料就使用data_command_to_rosa table 中 address 欄位的資料來決定下面weather_geo 要保留的資料 並且把輸出的天氣圖存入AWS中的S3中並獲得他的url
    check =  check_new_data(weather_data)
    check = check.reset_index(drop=True)
    weather_list = ["W1","F1","F2","A1","A2-1","A2-2","A3","B1","B2","B3"]
    earthquake_list = ["W2","C1","C2","C3"]
    index = 3



    if(check.empty == False): 
        
        weather_geo =  weather_data.main()


        for i in range(len(check)):

            weather_geo_match = pd.DataFrame()
            type = check.iloc[i]['type']
            type =type.split(',')
            type = ["W1","W2"]
            
            
            # any(item in weather_list for item in type)
            if(any(item in weather_list for item in type)):

                for county in weather_geo['COUNTYNAME'].unique():

                    if(county.find(check.iloc[i]['county']) != -1):
                        weather_geo_match = weather_geo_match._append(weather_geo[weather_geo['COUNTYNAME'] == county])
                        break
                # weather_geo_match = weather_geo
                

                if not weather_geo_match.empty:
                    
                    # change weather_geo_match start_time and end_time to datetime type
                    weather_geo_match['start_time'] = pd.to_datetime(weather_geo_match['start_time'])
                    weather_geo_match['end_time'] = pd.to_datetime(weather_geo_match['end_time'])
                    weather_geo_match = geopandas.GeoDataFrame(weather_geo_match, geometry='geometry')
                    weather_geo_match = weather_geo_match.reset_index(drop=True)
                    
                    # get the address's location
                    location = [check.iloc[i]['longitude'],check.iloc[i]['latitude']]

                    # location =[]
                    generate_weather_plots(weather_geo_match , check.iloc[i]['timestamp'],location,weather_geo)
                    upload_S3_dynamodb(weather_data,check,i,index)
                    index += 1

                else:
                    print("No matching data found for address:", check.iloc[i]['address'])




            # any(item in earthquake_list for item in type)
            if(any(item in earthquake_list for item in type)):


                for county in weather_geo['COUNTYNAME'].unique():

                    if(county.find(check.iloc[i]['county']) != -1):
                        weather_geo_match = weather_geo_match._append(weather_geo[weather_geo['COUNTYNAME'] == county])
                        break

                weather_geo_match = geopandas.GeoDataFrame(weather_geo_match,geometry='geometry')
                weather_geo_match = weather_geo_match.reset_index(drop=True)
            
                # output the earthquake data
                taiwan_shp = get_shapefile("COUNTY_MOI_1090820.shp")
                earthquake = EarthquakeData()
                earthquake.main()
                earthquake.plot_earthquake_map(earthquake.earthquakedata,earthquake.stationdata,taiwan_shp,check.iloc[i]['timestamp'],weather_geo_match) 
                upload_S3_dynamodb(weather_data,check,i,index)


    else:


        pass          
    


if __name__ == '__main__':

    main()


    
    
    print("--- %s seconds ---" % (time.time() - start_time))
            

            







  

    



