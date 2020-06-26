import http.client
import pandas as pd
import json
import os
import csv
from progressbar import ProgressBar

# Function to pull data from Attom
def pull_sale_date(data):
    pbar = ProgressBar()

    # Initializing the required dataframes
    log_data = pd.DataFrame([])
    out_data = pd.DataFrame([])

    # Establishing connection to the API
    conn = http.client.HTTPSConnection("api.gateway.attomdata.com")

    # Header required for the API to validate
    headers = {
        'accept': "application/json",
        'apikey': "48ed381483aabf5758717c7aa023980f",}

    # Looping on the dataframe containing the address of properties to fetch the sold dates from the API
    for x,rows in data.iterrows():

        # Replacing the spaces in the City,street with "_" because the API takes "_" not the spaces
        if rows["STREET"] != "":
            #print("in")
            street = str(rows["STREET"])
            street = street.replace(" ","_")
        if rows["CITY"] != "":
            city = str(rows["CITY"])
            city = city.replace(" ","_")

        # Concatenating the address variables to create a address column in the output and log file
        out_address = str(rows["S_HSENO"]) + " " + street + " " + str(rows["S_SFX"]) + " " + city + " " + str(rows["STATE"])

        # Concatenating the search string(address) for the API
        address = "/propertyapi/v1.0.0/saleshistory/detail?address1=" + str(rows["S_HSENO"]) + "%20" + street + "%20" + str(rows["S_SFX"]) + "&address2=" + city + "%2C%20" + str(rows["STATE"])

        # If the address is found in the API try block executes if address is not found the property is pushed to log file which is in except block
        try:

            # Connecting with the API
            conn.request("GET", address, headers=headers)

            # getting the response from API
            res = conn.getresponse()

            # Reading the output file from API
            out = res.read()

            # Decoding the output file utf-8 format
            date_out = out.decode("utf-8")

            # The output file is a nested list and dictionary, below code is to unwrap the list and dictionary
            rest_out = json.loads(date_out)

            # The drill down is Property->salehistory->saltransdate(sold date of property)
            out_list = rest_out["property"]
            out_list_dat = out_list[0]
            out_sale_history = out_list_dat["salehistory"]
            out_transaction = out_sale_history[0]
            sold_date = out_transaction['saleTransDate']

            # The county information is pushed into the output file (CO3 is the header name of county in the CSV (dataframe))
            county = rows["CO3"]

            # Pushing the resultant data into a new output dataframe
            out_data = out_data.append(pd.DataFrame({"address":out_address,"County":county,"City":city,"Sold_date":sold_date,"Listed_date":rows["DATE"]},index=[0]), ignore_index=True)

        except:
            # If the address is not found the property is pushed into log file
            log_data = log_data.append(pd.DataFrame({"address":out_address,"City":city},index=[0]), ignore_index=True)

    # Returning the resultant output dataframe containing the sold dates of property and the log file
    return out_data,log_data

# Main function
def main():
    # Reading the input data file i.e. list of properties
    data = pd.read_csv("D:\Capstone Project\Dataset\DATE\APN\parcelquestlacounty2020taxdef.csv")

    # calling pull_sale_data function to get the resultant and log files
    out_data,log_data = pull_sale_date(data)

    # Exporting the resultant and and log files
    out_data.to_csv("D:\Capstone Project\Dataset\DATE\APN modified\parcelquestlacounty2020taxdef_modified.csv")
    log_data.to_csv("D:\Capstone Project\Dataset\DATE\PARCEL QUEST GOLD log\parcelquestlacounty2020taxdef_log.csv")
main()