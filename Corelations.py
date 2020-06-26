import mysql.connector
import pandas as pd

# Function to get the correlation of real estate dataset
def Correlation_data():

  # Data base connection
  mydb = mysql.connector.connect(
    host="161.35.113.255",
    user="sachinvk",
    password="sachin11",
    database="realestate"
  )

  # SQL query to get the required data set
  df = pd.read_sql_query("SELECT * FROM tax_deliquency",mydb)

  # Corr function to find the corellation of the data set
  correlation = df.corr(method ='pearson')

  # Exporting the file to CSV
  correlation.to_csv("D:\Capstone Project\Correlations.csv")

Correlation_data()

