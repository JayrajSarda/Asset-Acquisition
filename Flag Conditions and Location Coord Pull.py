import mysql.connector
import pandas as pd
import requests
from tqdm import tqdm
import json
import time
from datetime import date

# initializing required variables
property_list = []
resultant_list = []
final_list = []
vacancy_list = []
tax_del_list = []
token = "b142105a208be4"
loader = pd.DataFrame()


def main():

    #take the input filter
    df = pd.read_csv('titaniumapril2020.csv')


    #connect to MySQL database
    connection = mysql.connector.connect(
        host = 'localhost',
        database = 'realestate',
        user = 'root'
    )
    cursor = connection.cursor()

    #vacancy data query
    cursor.execute('SELECT * FROM vacancy')
    vacancy = cursor.fetchall()
    vacancy_df = pd.DataFrame(vacancy, columns = cursor.column_names)
    vacancy_list = vacancy_df.to_dict('records')


    #tax delinquent data query
    cursor.execute('SELECT * FROM tax_deliquency')
    tax_del = cursor.fetchall()
    tax_del_df = pd.DataFrame(tax_del, columns = cursor.column_names)
    tax_del_list = tax_del_df.to_dict('records')



    #federal mail list
    #-- code goes here


    #---------------------------------

    #deceased owner database
    #-- code goes here



    #---------------------------------

    #initial constant filter conditions
    #filtered_df = initial_filter(df)


    #default flag condition set
    property_list = default_flags(df)



    # Red flag check
    resultant_list = flag_check(property_list, vacancy_list, tax_del_list)


    # get the final list
    final_list = final_filter(resultant_list)


    # get the flag count
    output = flag_count(final_list)


    #pull latitude and longitude of the properties from
    # locationIQ
    loader = loc_cord_pull(output, token)
    






# Initial constant conditions to filter the input files
def initial_filter(df):
    filtered_df = df[(df["type"] == "SFR") & (df["est_equity"] > 70) &
                 (df["est_value"] >= 300000) & (df["est_value"] <= 800000) &
                 (df["listed_for_sale"] == 0)]
    return filtered_df


# Adding default flag conditions
def default_flags(filtered_df):
    property_dict = filtered_df.to_dict('records')

    for dic in property_dict:
        dic['tax_deliquent'] = False
        dic['mail_undeliv'] = False
        dic['vacant'] = False
        dic['deceased'] = False

    return property_dict


# Module to check the red flag conditions
def flag_check(property_list, vacancy_list, tax_del_list):
    # get apn numbers from vacancy list and tax delinquent list
    vac_apn = []
    tax_apn = []

    for vac in vacancy_list:
        vac_apn.append(vac["apn"])

    for tax in tax_del_list:
        tax_apn.append(tax["apn_d"])

    for props in property_list:
        if props["apn"] in vac_apn:
            props["vacant"] = True
        if props["apn"] in tax_apn:
            props["tax_deliquent"] = True

    return property_list


# Filter out the red flag properties
def final_filter(resultant_list):
    result = []
    for p in resultant_list:
        if(p['mail_undeliv'] == True or p['tax_deliquent'] == True or
           p['vacant'] == True or p['deceased'] == True):
            result.append(p)

    return result

# get the count of flags for each property
def flag_count(final_list):
    count = 0
    for b in final_list:
        if b["vacant"] == True and b["tax_deliquent"]:
           count = 2
        elif b["tax_deliquent"] == True or b["vacant"]:
            count = 1
        else:
            count = 0
        b["flag_count"] = count
        count = 0

    return final_list


def loc_cord_pull(output, token):
    data = pd.DataFrame(output)
    YOUR_PRIVATE_TOKEN = token
    lat = []
    long = []
    for x,rows in tqdm(data.iterrows()):
        state = str(rows['state'])
        # Concadinating the address fields of the data frame to input into locationIQ API
        address = rows['address']+","+rows['city']+","+state+","+str(rows['zip'])
        SEARCH_STRING = address
        # Calling the LocationIQ API and pusing the token(Access key) and SearchString(Address)
        resp = requests.get(f'https://us1.locationiq.com/v1/search.php?key={YOUR_PRIVATE_TOKEN}&q={SEARCH_STRING}&format=json')
        # Pushing the contents of the resultant data into a variable "x"
        x = resp.content
        loaded_json = json.loads(x)
        # The resultant file is a list of json, so I am getting the required json file
        out = loaded_json[0]
        # Appending the latitude and longitude data into lists(lat,long)
        lat.append(out['lat'])
        long.append(out['lon'])
        # The API allows 2 records per second so I used sleep function to make sure it is followed
        time.sleep(1)

    data['lat']=lat
    data['lon']=long
    return data

def get_today_date():
    


main()
