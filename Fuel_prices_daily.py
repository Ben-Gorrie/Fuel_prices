# Databricks notebook source
# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# MAGIC sudo apt-get update
# MAGIC sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17

# COMMAND ----------

#import libraries
import os
import zipfile
import xml.etree.ElementTree as et
import pandas as pd
from datetime import datetime, date, timedelta
import sys
sys.stdout.fileno = lambda: False
import urllib
import pyodbc
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

# COMMAND ----------

schema = "FuelPrices"
table_name = f"{schema}.Daily"

#Creates schema for Data Table on dbricks delta lake
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

engine = create_engine("mssql+pyodbc://", poolclass=StaticPool, creator=lambda: connection)

trigger_date_time = datetime.now().replace(microsecond=0)
source_id = 4


#Parameters for main function
year = (date.today() - timedelta(days = 1)).strftime("%Y")
month = (date.today() - timedelta(days = 1)).strftime("%m")
day = (date.today() - timedelta(days = 1)).strftime("%d")

# COMMAND ----------

#Download the .zip file to the current working directory
def download_file(url : str, date : str) -> None:
    urllib.request.urlretrieve(url + date, "/tmp/PrixCarburants_quotidien_" + date + ".zip")

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

def get_date_range() -> list:
    #Read existing database and get the latest date
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    current_df = spark.table(f"{table_name}")

    df = current_df.select("*").toPandas()

    df.sort_values(by='date', ascending = False, inplace=True)

    latest_date = df.iloc[0].date

    dates_to_run = []
    if date.today() - timedelta(days = 29) >= latest_date:
        for i in range(1, 30):
            dates_to_run.append(date.today()-timedelta(days = 30 - i))
    else:
        while latest_date < date.today():
            dates_to_run.append(latest_date)
            latest_date += timedelta(days = 1)
    return dates_to_run

# COMMAND ----------

def parse_xml(path : str) -> pd.DataFrame:
    """
    Parse XML file to get all data regarding price of fuel in a pandas DataFrame
    
    :param path: relative or absolute path to file
    :return: pandas DataFrame with data about price of fuel for a given day
    """
    mytree = et.parse(path)
    myroot = mytree.getroot()
    data = []
    for node in myroot:
        attribs = node.findall("prix")
        for blob in attribs:
            data.append(blob.attrib)
    return pd.DataFrame(data)

# COMMAND ----------

def clean_df(df : pd.DataFrame, date_format : datetime.date) -> pd.DataFrame:
    """
    Process data as described below
    :param df: pandas DataFrame containing data to be processed
    :param date_format: datetime.date of the day we are currently looking at (e.g: 2022-06-30)
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

    #Remove any extraneous data that is not required
    df = df[df["maj"].dt.date == date_format]

    #Divide fuel values by 1000 if taken before 2022
    df.loc[df["maj"].dt.year < 2022, "valeur"] = df["valeur"] / 1000
    print("Divided fuel prices by 1000 as needed")

    #Remove time from datetime
    df.loc[:, "maj"] = df["maj"].dt.date
    print("Removed time")

    #Group by fuel type and date, and take mean of fuel. Then round to 2 dp
    df = df.groupby(["nom", "maj"]).mean().round(decimals = 3)
    print("Grouped and taken mean")

    return df

# COMMAND ----------

def get_fuel_data(url : str, year : str, month : str, day : str) -> pd.DataFrame:
    """
    Given the url to fuel data with the date, scrape the data from the xml file, save it and return a pd.DataFrame which details the average price of each type of fuel for that given date
    
    :param url: "Designed for https://donnees.roulez-eco.fr/opendata/jour/"
    :param year, month, day: Self-explanatory, but keep in mind the data has be over the last 30 days
    """
    if int(day) < 10:
        day = "0" + day
    if int(month) < 10:
        month = "0" + month
    date_wanted = year + month + day
    date_format = datetime.strptime(year + "/" + month + "/" + day, "%Y/%m/%d").date()
    
    new_dir = "/tmp/PrixCarburants_quotidien_" + date_wanted
    path_to_zip_file = "/tmp/PrixCarburants_quotidien_" + date_wanted + ".zip"
    path_to_file = new_dir + "/PrixCarburants_quotidien_" + date_wanted + ".xml"

    ready_file(url, date_wanted, path_to_zip_file, new_dir)

    df = parse_xml(path_to_file)

    df = clean_df(df, date_format)
    
    return df.reset_index()

# COMMAND ----------

def get_fuel_data_over_range(dates_to_run : list) -> pd.DataFrame:
    """
    Get data for fuel prices over the dates_to_run
    :param dates_to_run: list of either last 29 days or number of days since this scraper has been run
    :return: dataframe which contains all the data
    """
    dfs = []
    for date in dates_to_run:
        dfs.append(get_fuel_data("https://donnees.roulez-eco.fr/opendata/jour/", str(date.year), str(date.month), str(date.day)))
    df = pd.concat(dfs, axis = 0)
    return df

# COMMAND ----------

def scrapper_main():
    """
    Get average fuel prices for specific day in France and add data to the ODE database
    """
    start_date_time = datetime.now().replace(microsecond=0)
    cursor.execute("INSERT INTO dbo.ExtractionResultsNew (source_id, trigger_date_time, start_date_time, end_date_time, status, nb_rows, error_message, error_type)values(?,?,?,?,?,?,?,?)",source_id, trigger_date_time, start_date_time, datetime.fromtimestamp(0), "WIP", 0, None, None)
    connection.commit()
    
    try:
        #df = get_fuel_data("https://donnees.roulez-eco.fr/opendata/jour/", year, month, day)
        dates_to_run = get_date_range()
        df = get_fuel_data_over_range(dates_to_run)
        df["source_id"] = source_id
        df["trigger_date_time"] = trigger_date_time
        df["execution_date_time"] = start_date_time
        print("Got data")
        df.rename(columns = {"nom" : "fuel_type", "maj" : "date", "valeur" : "average_price"}, inplace = True)
        
        #df.to_sql('Test', con=engine, schema = 'FuelPrices', if_exists='append',method=None, index = False) # you can look up for the relevant chunksize integer if you're interested but don't spend too much time on this as we won't be using sql tables for long !
        
        spark_df = spark.createDataFrame(df)
        spark_df.write.format("delta").mode("append").option("path",f"dbfs:/mnt/ekiodp/rfd/{schema}/{table_name}/").saveAsTable(table_name)
        
        cursor.execute("UPDATE dbo.ExtractionResultsNew SET status=?, end_date_time=?, nb_rows=?, error_message=?, error_type=? WHERE start_date_time=?", "OK", datetime.now(), df.shape[0], None, None, start_date_time)
        connection.commit()
        cursor.close()
    except Exception as ex:
        print(f"Error: {ex}")
        cursor.execute(f"UPDATE dbo.ExtractionResultsNew SET status=?, end_date_time=?,nb_rows=?,error_message=?, error_type=? WHERE start_date_time=?","FAILED",datetime.now(),0,f"{ex}",f"{type(ex)}",start_date_time)
        connection.commit()
        cursor.close()

# COMMAND ----------

scrapper_main()
