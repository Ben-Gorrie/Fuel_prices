# Databricks notebook source
# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# MAGIC sudo apt-get update
# MAGIC sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17

# COMMAND ----------

#import required libraries
import os
import zipfile
import xml.etree.ElementTree as et
import pandas as pd
from datetime import datetime
import sys
sys.stdout.fileno = lambda: False
import urllib
import pyodbc

# COMMAND ----------

schema = "FuelPrices"
table_name = f"{schema}.Daily"
## Creates schema for Data Table on dbricks delta lake ##
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# COMMAND ----------

## Connects to Database
def connect_to_db():
    username = 'GENERIC_SQL_USER'
    password = dbutils.secrets.get(scope="azurekv-scope",key='sqlPwd')
    server_name = 'ekiodp-dev-sql-01'
    db_name = 'ekiodp-dev-sql-01-db-01'
    driver = '{ODBC Driver 17 for SQL Server}'

    #méthode pyodbc
    cnxn = pyodbc.connect(f"""Driver={driver};
                          Server=tcp:{server_name}.database.windows.net,1433;
                          Database={db_name};
                          Uid={username};Pwd={password}; ;
                          Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30; """)
    return cnxn

# COMMAND ----------

connection = connect_to_db()
cursor = connection.cursor()

trigger_date_time = datetime.now().replace(microsecond=0)
source_id = 4

# COMMAND ----------

#Download the .zip file to the current working directory
def download_file(url : str, date : str) -> None:
    urllib.request.urlretrieve(url + date, "/tmp/PrixCarburants_annuel_" + date + ".zip")

# COMMAND ----------

#Create directory where files will be
def create_directory(path : str) -> None:
    if os.path.exists(path):
        pass
    else:
        os.mkdir(path)

# COMMAND ----------

#Unzip file into new directory   
def unzip_file(path_to_zip_file : str, path_to_directory : str) -> None:
    with zipfile.ZipFile(path_to_zip_file, "r") as zip_ref:
        zip_ref.extractall(path_to_directory)

# COMMAND ----------

#Chain the above functions to extract the .zip file into a directory
def ready_file(url : str, date : str, path_to_zip_file : str, path_to_directory : str) -> None:
    download_file(url, date)
    create_directory(path_to_directory)
    unzip_file(path_to_zip_file, path_to_directory)

# COMMAND ----------

def ready_all_files(url : str, start_date : int, end_date : int) -> None:
    new_dir = "/tmp/Fuel_prices_data"
    create_directory(new_dir)
    for year in range(start_date, end_date + 1):
        path_to_zip_file = "/tmp/PrixCarburants_annuel_" + str(year) + ".zip"
        ready_file(url, str(year), path_to_zip_file, new_dir)

# COMMAND ----------

#Parse xml for a given file (@ param year) and return a list with all relevant information
def parse_xml(path : str) -> list:
    """
    Parse XML file to get all data regarding price of fuel in a pandas DataFrame
    
    :param path: relative or absolute path to file
    :return: pandas DataFrame with data about price of fuel for a given day
    """
    mytree = et.parse("/tmp/Fuel_prices_data/PrixCarburants_annuel_" + path + ".xml")
    myroot = mytree.getroot()
    data = []
    for node in myroot:
        attribs = node.findall("prix")
        for blob in attribs:
            data.append(blob.attrib)
    return data

# COMMAND ----------

#Add each list together using parse_xml()
def get_data_range(start_year : int, end_year : int) -> pd.DataFrame:
    data = []
    for year in range(start_year, end_year + 1):
        data += parse_xml(str(year))
        print("\nDone with " + str(year))
    df = pd.DataFrame(data)
    print("Done converting to df")
    return df

# COMMAND ----------

def clean_df(df : pd.DataFrame) -> pd.DataFrame:
    """
    Process data as described below
    :param df: pandas DataFrame containing data to be processed
    :return: pandas DataFrame with processed data
    """
    #Remove any rows with NaN
    df.dropna(inplace = True)
    print("Removed NaNs")

    #Remove redundant id column
    df.drop("id", axis=1, inplace=True)
    print("Removed redundant id")

    #Convert "valeur" column to float
    df["valeur"] = df["valeur"].astype("float64")
    print("Converted 'valeur' to float")

    #Convert time to datetime format
    df["maj"] = pd.to_datetime(df["maj"])
    print("Converted time to datetime")

    #Divide fuel values by 1000 if taken before 2022
    df.loc[df["maj"].dt.year < 2022, "valeur"] = df["valeur"] / 1000
    print("Divided fuel prices by 1000 as needed")

    #Remove time from datetime
    df.loc[:, "maj"] = df["maj"].dt.date
    print("Removed time")

    #Group by fuel type and date, and take mean of fuel
    df = df.groupby(["nom", "maj"]).mean().round(decimals = 3)
    print("Grouped and taken mean")

    return df

# COMMAND ----------

def get_fuel_data(url : str, start_year : str, end_year : str) -> pd.DataFrame:
    """
    Given the url to fuel data with the date, scrape the data from the xml file, save it and return a pd.DataFrame which details the average price of each type of fuel for that given date
    
    :param url: "Designed for https://donnees.roulez-eco.fr/opendata/jour/"
    :param start_year: Year we want data from (min = 2007)
    :param end_year: Year we want data to (max = current year) 
    """
    ready_all_files(url, start_year, end_year)

    df = get_data_range(int(start_year), int(end_year))

    df = clean_df(df)
    
    return df.reset_index()

# COMMAND ----------

def scrapper_main(start_year : str, end_year : str):
    """
    Get data about fuel prices in France between the start_year and the end_year (inclusive both sides) and add it to ODE database
    :param start_year: Year we want data from (min = 2007)
    :param end_year: Year we want data to (max = current year) 
    """
    start_date_time = datetime.now().replace(microsecond=0)
    cursor.execute("INSERT INTO dbo.ExtractionResults (source_id, trigger_date_time, start_date_time, end_date_time, status, nb_rows, error_message)values(?,?,?,?,?,?,?)",source_id, trigger_date_time, start_date_time, datetime.fromtimestamp(0), "WIP", 0, None)
    connection.commit()
    
    try:
        df = get_fuel_data("https://donnees.roulez-eco.fr/opendata/annee/", start_year, end_year)
        
        df["source_id"] = source_id
        df["trigger_date_time"] = trigger_date_time
        df["execution_date_time"] = start_date_time
        print("Got data")
        df.rename(columns = {"nom" : "fuel_type", "maj" : "date", "valeur" : "average_price"}, inplace = True)
        
        r"""for index, row in df.iterrows():
            cursor.execute("INSERT INTO FuelPrices.Daily (source_id, trigger_date_time, execution_date_time, fuel_type, date, average_price) values (?,?,?,?,?,?)", row.source_id, row.trigger_date_time, row.start_date_time, row.nom, row.maj, row.valeur)
            connection.commit()
            print("Added one row")"""
        
        spark_df = spark.createDataFrame(df)
        spark_df.write.format("delta").mode("append").option("path",f"dbfs:/mnt/ekiodp/rfd/{schema}/{table_name}/").saveAsTable(table_name)
        
        cursor.execute("UPDATE dbo.ExtractionResults SET status=?, end_date_time=?, nb_rows=?, error_message=? WHERE start_date_time=?", "OK", datetime.now(), df.shape[0], None, start_date_time)
        connection.commit()
        cursor.close()
    except Exception as ex:
        print(f"Error: {ex}")
        cursor.execute(f"UPDATE dbo.ExtractionResults SET status=?, end_date_time=?,nb_rows=?,error_message=? WHERE start_date_time=?","FAILED",datetime.now(),0,f"{ex}",start_date_time)
        connection.commit()
        cursor.close()

# COMMAND ----------

scrapper_main(2019, 2022)
