import pandas as pd
def filter_vacant_dat():
    # Reading the Source CSV files
    dat_main = pd.read_csv(r'D:\Capstone Project\Dataset\Shrunken_Data.csv')
    dat_vacant = pd.read_csv(r"D:\Capstone Project\Dataset\VC VACANT MAY 2020.csv")
    #print(dat_vacant.head())
    # Creating a new column in dataframes which has the concadinated address in it
    dat_vacant["address_full"] = dat_vacant["adress"].astype(str) + ' ' + dat_vacant["city"].astype(str) + ' ' + dat_vacant["state"].astype(str) + '  ' + dat_vacant["zip"].astype(str)
    dat_main["address_full"] = dat_main["adress"].astype(str) + ' ' + dat_main["city"].astype(str) + ' ' + dat_main["state"].astype(str) + '  ' + dat_main["zip"].astype(str)
    vacant = []
    vacant_list = []
    # Creating a list of vacant address to compare with the data set for filtering
    vacant_list = list(dat_vacant["address_full"])
    # Looping on the actual data set to filter the vacant addresses
    for x,rows in dat_main.iterrows():
        z = rows["address_full"]
        # A Check to find if the address is in the vacant address list
        if z in vacant_list:
            # If yes, appending True to a list called vacant
            vacant.append("True")
        else:
            # If No, appending False to a list called vacant
            vacant.append("False")
    # Converting list to series so that it fits into the data frame
    vacant_data = pd.Series(vacant)
    # Adding the series to the dataframe as a new column
    dat_main.insert(len(dat_main.columns),"Vacant",vacant_data)
    # Dropping the rows that has False in the "Vacant" column to get the filtered list
    sort_vacant_dat = dat_main[dat_main["Vacant"] == "True"]
    sort_vacant_dat.to_csv (r'D:\Capstone Project\Dataset\dat_vacency3.csv', index = False, header=True)
    #dat_vacant.to_csv (r'D:\Capstone Project\Dataset\dat_vacency123.csv', index = False, header=True)
    #dat_main.to_csv (r'D:\Capstone Project\Dataset\dat_vacency143.csv', index = False, header=True)
filter_vacant_dat()