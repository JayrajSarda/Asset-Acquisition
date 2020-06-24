import http.client
import pandas as pd
import json
import os
import csv
from progressbar import ProgressBar
def pull_sale_date(data):
    pbar = ProgressBar()
    log_data = pd.DataFrame([])
    out_data = pd.DataFrame([])
    conn = http.client.HTTPSConnection("api.gateway.attomdata.com")
    headers = {
        'accept': "application/json",
        'apikey': "48ed381483aabf5758717c7aa023980f",}
    #print(data)
    for x,rows in data.iterrows():
        if rows["STREET"] != "":
            #print("in")
            street = str(rows["STREET"])
            street = street.replace(" ","_")
        if rows["CITY"] != "":
            city = str(rows["CITY"])
            city = city.replace(" ","_")
        out_address = str(rows["S_HSENO"]) + " " + street + " " + str(rows["S_SFX"]) + " " + city + " " + str(rows["STATE"])
        address = "/propertyapi/v1.0.0/saleshistory/detail?address1=" + str(rows["S_HSENO"]) + "%20" + street + "%20" + str(rows["S_SFX"]) + "&address2=" + city + "%2C%20" + str(rows["STATE"])
        try:
            conn.request("GET", address, headers=headers)
            res = conn.getresponse()
            out = res.read()
            date_out = out.decode("utf-8")
            #print(date_out)
            rest_out = json.loads(date_out)
            out_list = rest_out["property"]
            out_list_dat = out_list[0]
            print("in")
            out_sale_history = out_list_dat["salehistory"]
            #out_last_modified = out_list_dat["vintage"]
            out_transaction = out_sale_history[0]
            #out_modified = out_last_modified[0]
            sold_date = out_transaction['saleTransDate']
            #print(sold_date)
            #modified_date = out_modified["lastModified"]
            county = rows["CO3"]
            out_data = out_data.append(pd.DataFrame({"address":out_address,"County":county,"City":city,"Sold_date":sold_date,"Listed_date":rows["DATE"]},index=[0]), ignore_index=True)
            print("Hi")
        except:
            print("err")
            log_data = log_data.append(pd.DataFrame({"address":out_address,"City":city},index=[0]), ignore_index=True)
    #print(out_data)
    return out_data,log_data
def main():
    data = pd.read_csv("D:\Capstone Project\Dataset\DATE\APN\parcelquestlacounty2020taxdef.csv")
    out_data,log_data = pull_sale_date(data)
    out_data.to_csv("D:\Capstone Project\Dataset\DATE\APN modified\parcelquestlacounty2020taxdef_modified.csv")
    log_data.to_csv("D:\Capstone Project\Dataset\DATE\PARCEL QUEST GOLD log\parcelquestlacounty2020taxdef_log.csv")
main()