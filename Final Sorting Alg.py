import pandas as pd

# pre-defined variables, these variable will handle the conversions
# and store the intermediate datatypes while the program runs
df = pd.DataFrame()
property_list = []
ceonditions_checked = []
filtered_df = pd.DataFrame()
result_list = []
result_df = pd.DataFrame()

def main():

    #open the source file
    df = pd.read_csv('titaniumapril2020.csv')

    # filter through constant conditions
    filtered_df = filter_constant(df)

    # convert the dataframe to list of dictionaries and add default red flags
    property_list = default_flags(filtered_df)

    # check red flag conditions
    conditions_checked = red_flags_check(property_list)

    # get properties with red flags
    result_list = red_flag_properties(conditions_checked)

    # get the file output
    result_out(result_list)

# -----------------------------------------------------------------------------
# This function will take the dataframe and shrink the data through
# constant conditions given to us in pdf
def filter_constant(df):
    filtered_df = df[(df["type"] == "SFR") & (df["est_equity"] > 70) &
                 (df["est_value"] >= 300000) & (df["est_value"] <= 800000) &
                 (df["listed_for_sale"] == 0)]
    return filtered_df

#------------------------------------------------------------------------------

# This function will set the default red flag conditions to False
# By doing this, the program ensures the flag conditions are added
# when the program checks for conditions
def default_flags(filtered_df):
    property_dict = filtered_df.to_dict('records')

    for dic in property_dict:
        dic['tax_deliquent'] = False
        dic['mail_undeliv'] = False
        dic['vacant'] = False
        dic['deceased'] = False

    return property_dict

#------------------------------------------------------------------------------
# This function will check the red flag conditions and alter the default
# value of flags if conditions are met (waiting for other 3 conditons
def red_flags_check(property_list):
    for dic in property_list:
        #mail not delivered
        if "PO BOX" in dic['mail_address']:
            dic['mail_undeliv'] = True
        #tax deliquent
        # --

        #vacant
        # --

        #deceased
        # --
    return property_list


#------------------------------------------------------------------------------
# This function will extract the properties with read flags
# into a list
def red_flag_properties(conditions_checked):
    result = []
    for dic in conditions_checked:
        if(dic['mail_undeliv'] == True or dic['tax_deliquent'] == True or
           dic['vacant'] == True or dic['deceased'] == True):
            result.append(dic)

    return result


#------------------------------------------------------------------------------
# This function will give a file output containing properties
# with red flags
def result_out(result_list):
    result_df = pd.DataFrame(result_list)
    result_df.to_csv('output.csv', index = False)


#------------------------------------------------------------------------------
main()
