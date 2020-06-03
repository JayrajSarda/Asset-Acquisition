import mysql.connector
import pandas as pd

# initializing required variables
property_list = []


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
    filtered_df = initial_filter(df)

    #default flag condition set
    property_list = default_flags(filtered_df)


    # Vacancy flag




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




main()
