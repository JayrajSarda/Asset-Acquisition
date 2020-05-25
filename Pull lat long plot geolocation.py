import geopandas
import geoplot
import time
import requests
import json
import pandas as pd
import matplotlib.pyplot as plt
import geopy
import folium
import webbrowser
from tqdm import tqdm
from folium.plugins import FastMarkerCluster
# Function to geth the latitudes and longitudes of the address(property)
def lat_long_pull(csv_read,token,csv_write):
    # Reading the csv and loading the access tokens of the locationIQ API
    data = pd.read_csv(csv_read)
    YOUR_PRIVATE_TOKEN = token
    lat = []
    long = []
    # Looping on the dataframe and calling the locationIQ API
    for x,rows in tqdm(data.iterrows()):
        state = str(rows['state'])
        # Concadinating the address fields of the data frame to input into locationIQ API
        address = rows['adress']+","+rows['city']+","+state+","+str(rows['zip'])
        SEARCH_STRING = address
        # Calling the LocationIQ API and pusing the token(Access key) and SearchString(Address)
        resp = requests.get(f'https://us1.locationiq.com/v1/search.php?key={YOUR_PRIVATE_TOKEN}&q={SEARCH_STRING}&format=json')
        # Pushing the contents of the resultant data into a variable "x"
        x=resp.content
        loaded_json = json.loads(x)
        # The resultant file is a list of json, so I am getting the required json file
        out = loaded_json[0]
        # Appending the latitude and longitude data into lists(lat,long)
        lat.append(out['lat'])
        long.append(out['lon'])
        # The API allows 2 records per second so I used sleep function to makesure it is followed
        time.sleep(1)
        #print("in")
    #print(lat,long)
    # Coverting the lists(lat,long) into series so that I can push into the dataframe
    data['lat']=pd.Series(lat)
    data['lon']=pd.Series(long)
    # Exporting the CSV
    data.to_csv (csv_write, index = False, header=True)
# Function to plot the latitudes and longitudes on a map
def geo_location(read_long_csv,lat_long):
    # Reading the latitudes and longitudes csv file
    df = pd.read_csv(read_long_csv)
    # Using Folium package to plot the lat and long
    folium_map = folium.Map(location=lat_long,
                            zoom_start=2,
                            tiles='CartoDB dark_matter')
    FastMarkerCluster(data=list(zip(df['lat'].values, df['lon'].values))).add_to(folium_map)
    folium.LayerControl().add_to(folium_map)
    # Saving the map in a html file
    folium_map.save(outfile='map.html')
    out = "map.html"
    # opening the map html file on browser
    webbrowser.open(out, new=2)
    # returning the plotted map
    return folium_map
def main():
    lat_long_pull("D:\Capstone Project\Dataset\Shrunken_Data.csv","b142105a208be4",'D:\Capstone Project\Dataset\export_shurenk_dat_lon_lat_data.csv')
    time.sleep(5)
    geo_location("D:\Capstone Project\Dataset\export_shurenk_dat_lon_lat_data.csv",[36.778259,-119.417931])