import time
import requests
import json
import pandas as pd
from tqdm import tqdm
def filter_flag_condi(dat_main,dat_vac,dat_taxdel):
    # Creating empty lists to store the outputs
    list_flag_count = []
    list_vac = list(dat_vac["APN"])
    list_taxdel = list(dat_taxdel["APN_D"])
    # Creating empty flag columns in the dataset
    dat_main['VAC'] = pd.Series()
    dat_main['Tax Del'] = pd.Series()
    # Coppying the columns from the source dataset to duplicate in the output dataset
    list_col = list(dat_main.columns)
    # creating the output dataframe
    filter_dat = pd.DataFrame(columns=list_col)
    # Looping on the source dataset
    for x, rows in dat_main.iterrows():
        # Initially the fields "VAC" and "Tax Del" are set to "False"
        rows["VAC"] = "False"
        rows["Tax Del"] = "False"
        # Checking if the property is in vacant dataset
        if rows["apn"] in list_vac:
            rows["VAC"] = "True"
        # Checking if the property is in taxdeliquent dataset
        if rows["apn"] in list_taxdel:
            rows["Tax Del"] = "True"
        # Appending the records that are either vacant or taxdeliquent to new dataframe
        if rows["VAC"] == "True" or rows["Tax Del"] == "True":
            filter_dat = filter_dat.append(rows)
        # conditions to check the count of flag conditions and appending to a list "list_flag_count"
        if rows["VAC"] == "True" and rows["Tax Del"] == "True":
            list_flag_count.append(2)
        elif rows["VAC"] == "True" or rows["Tax Del"] == "True":
            list_flag_count.append(1)
        else:
            list_flag_count.append(0)
    # Pushing the flag count list to the output dataframe
    filter_dat["Flag Count"] = pd.Series(list_flag_count)
    # Returning the dataframe
    return filter_dat
def lat_long_pull(data,token):
    # Reading the csv and loading the access tokens of the locationIQ API
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
        # The API allows 2 records per second so I used sleep function to make sure it is followed
        time.sleep(1)
    # Coverting the lists(lat,long) into series so that I can push into the dataframe
    data['lat']=lat
    data['lon']=long
    return data
def main():
    dat_main = pd.read_csv(r"D:\Capstone Project\Dataset\Workon dat\titaniumapril2020.csv")
    dat_vac = pd.read_csv(r"D:\Capstone Project\Dataset\Workon dat\VC VACANT MAY 2020.csv")
    dat_taxdel = pd.read_csv(r"D:\Capstone Project\Dataset\Workon dat\Combined.csv")
    out_put = filter_flag_condi(dat_main, dat_vac, dat_taxdel)
    final_out = lat_long_pull(out_put, "b142105a208be4")
    final_out.to_csv(r"D:\Capstone Project\Dataset\Workon dat\data_out.csv")
main()
