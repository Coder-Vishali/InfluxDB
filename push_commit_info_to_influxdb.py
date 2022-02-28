import xml.etree.ElementTree as ET
import csv
import pandas as pd
import datetime as dt
from influxdb_client import InfluxDBClient, Point, WritePrecision
import time
import os
from datetime import date,datetime,timedelta

# Start the timer
start = time.time()

# Customization of SVN repo & credentials
svn_repo = "https://xyz/trunk/test"
user = "vishali"
root_path = r'D:\test_scripts'
f = open(root_path + r'\Credentials.txt', "r")
password = f.read()

# For the first time we have to use this command (Pushing all commits made so far into the InfluxDB bucket)
# Generating SVN log file
#os.system("svn log --xml -v > D:/test_scripts/svn.log https://xyz/trunk/test --username {user} --password {password}")

# Customization of InfluxDB
bucket_name = "SVN_Data"
org_name = "BITS"
write_sample_svn_data_token = "GZpzaMq-3PVxv_yO3II3GGcgjm4VYC_MZU1Ff2F0DZMRVhEvi8fkP-mhCFRpvmapikw=="
client_url = 'http://<hostname or ip address>:8086'

# Start the timer
start = time.time()

# To get svn commits information for each upcoming days and add it into the bucket
today = datetime.today().strftime("%Y-%m-%d")
tomorrow = (datetime.today() + timedelta(1)).strftime("%Y-%m-%d")
os.system(f"svn log --xml -v > D:/test_scripts/svn.log {svn_repo} -r {{{today}}}:{{{tomorrow}}} --username {user} --password {password}")
print("Generating SVN Log File")

# Reading the SVN log file
tree = ET.parse(root_path + r'\svn.log')
root = tree.getroot()
Date_field = []
print("Log File Parsed")

# Extracting only Date field
for tags in root.findall(".//date"):
    Date_field.append(tags.text)
    
dict = {'Date': Date_field}
df = pd.DataFrame(dict) 
df.to_csv(root_path + r'\results.csv')

# Performing operations to extract commits counts per day
df = pd.read_csv (root_path + r'\results.csv', encoding="utf-8")
df["Date"] = pd.to_datetime(df["Date"])
df['date_minus_time'] = df["Date"].apply( lambda df : dt.datetime(year=df.year, month=df.month, day=df.day))
Daily_commit = df.groupby('date_minus_time')
df = df.groupby('date_minus_time')
df = Daily_commit.count().reset_index()
df = df.drop(["Unnamed: 0"], axis=1)
df.to_csv(root_path + r'\results.csv', header=["Date", "Count"], index=False)

# Setting up the proxies
http_proxy  = ""
https_proxy = ""

proxyDict = { 
              "http"  : http_proxy, 
              "https" : https_proxy
            }

# Define InfluxDB Client
client = InfluxDBClient(url=client_url, proxies = proxyDict, token = write_sample_svn_data_token)

# File Path of CSV
file_path = root_path + r'\results.csv'

csvReader = pd.read_csv(file_path)
print(csvReader.shape)
print(csvReader.columns)
write_api = client.write_api()

from datetime import datetime
import pytz

# Writing into the bucket
for row_index, row in csvReader.iterrows() : 
    my_date = pd.to_datetime(row[0])
    my_time = datetime.min.time()
    my_datetime = datetime.combine(my_date, my_time)
    timezone = pytz.timezone("GMT")
    with_timezone = timezone.localize(my_datetime)
    json_body = [
        {
            "time": with_timezone,
            "measurement": "Commits_per_day",
            "tags": {
                        "_Tag": "XYZ_Test"
                    },
            "fields": {
                        "Count": row[1]
                        },
            "value": row[1]
        }
    ]
    write_api.write(bucket=bucket_name, org=org_name, record=json_body)
print("Completed writing data into InfluxDB bucket")

# Close
write_api.close()  
client.close()

# script execution Stats
end = time.time()
Time_Elapsed = round(end - start, 3)
print("Script Executed in " + str(Time_Elapsed) + " seconds")
