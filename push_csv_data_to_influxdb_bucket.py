import xml.etree.ElementTree as ET
import csv
import pandas as pd
import datetime as dt
from influxdb_client import InfluxDBClient, Point, WritePrecision
import time
import os
from datetime import date,datetime,timedelta
from time import strptime, mktime

# Start the timer
start = time.time()

# Customization of root path
root_path = r'C:\XYZ'

# Customization of InfluxDB
bucket_name = "Test"
org_name = "BITS"
token = "NuF8PbO8nO-aqUVeKK14ThNsJUuDFpqRMhyUk3yhyosauVB1z-o23ivES1XCDBOUnlOqOhdPSkEQ=="
client_url = 'http://<hostname or ip address>:8086'
# Start the timer
start = time.time()

# Setting up the proxies
http_proxy  = ""
https_proxy = ""

proxyDict = { 
              "http"  : http_proxy, 
              "https" : https_proxy
            }

# Define InfluxDB Client
client = InfluxDBClient(url=client_url, proxies = proxyDict, token = token)

# File Path of CSV
file_path = root_path + r'\data.csv'

csvReader = pd.read_csv(file_path)
print(csvReader.shape)
print(csvReader.columns)
write_api = client.write_api()

from datetime import datetime
import pytz

# Writing into the bucket
for row_index, row in csvReader.iterrows() : 
    timeStruct = strptime(row[0],"%j:%H:%M:%S")
    YEAR = 2000
    DAY_OF_YEAR = timeStruct.tm_yday
    time_string = str(timeStruct.tm_hour) +":" + str(timeStruct.tm_min)+":" + str(timeStruct.tm_sec)
    d = dt.date(YEAR, 1, 1) + dt.timedelta(DAY_OF_YEAR - 1)
    my_time = dt.datetime.strptime(time_string,"%H:%M:%S").time()
    my_datetime = datetime.combine(d, my_time)
    timezone = pytz.timezone("GMT")
    with_timezone = timezone.localize(my_datetime)
    json_body = [
        {
            "time": with_timezone,
            "measurement": "Test_data",
            "tags": {
                        "_Tag": "Name"
                    },
            "fields": {
                        "Name": row[1]
                        },
            "value": row[1]
        }
    ]
    write_api.write(bucket=bucket_name, org=org_name, record=json_body)
    json_body = [
        {
            "time": with_timezone,
            "measurement": "Test_data",
            "tags": {
                        "_Tag": "Age"
                    },
            "fields": {
                        "Age": row[2]
                        },
            "value": row[2]
        }
    ]
    write_api.write(bucket=bucket_name, org=org_name, record=json_body)
    json_body = [
        {
            "time": with_timezone,
            "measurement": "Test_data",
            "tags": {
                        "_Tag": "DOB"
                    },
            "fields": {
                        "DOB": row[3]
                        },
            "value": row[3]
        }
    ]
    write_api.write(bucket=bucket_name, org=org_name, record=json_body)
    json_body = [
        {
            "time": with_timezone,
            "measurement": "Test_data",
            "tags": {
                        "_Tag": "Score"
                    },
            "fields": {
                        "Score": row[4]
                        },
            "value": row[4]
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
