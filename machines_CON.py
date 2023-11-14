from datetime import datetime
import pandas as pd

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

import os
from kafka import KafkaConsumer
import json
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import time

topic_name = "Machine"

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='Machine-group'
)

# You can generate a Token from the "Tokens Tab" in the UI
token = "mk4P1Xg1n_yklBdBQMoti2A8xBLsVTAbabuBRFUlLJLqqwGItDStISRZEe6WuDOGusyTvSxeLfpuhW2xMAi1SQ=="
org = "Filtrona"
bucket = "Machine_Kafka"
for message in consumer:

    mvalue = json.loads(message.value.decode('utf-8'))
    mkey = message.key
    mpart = message.partition
    moffset = message.offset
    

    client = InfluxDBClient(url="http://localhost:8086", token=token)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    print(message)
    
    point = Point("Machine")\
            .tag("ID", row["ID"])\
            .field("Cutspeed", row["Cutspeed"])\
            .field("Filterspeed", row["Filterspeed"])\
            .field("Cutcounter", row["Cutcounter"])\
            .field("Filtercounter", row["Filtercounter"])\
            .field("Waste", row["Waste"])\
            .field("Wateratio", row["Wateratio"])\
            .field("Traycount", row["Traycount"])\
            .field("Last_Trayfill", row["Last_Trayfill"])\
            .field("Trayfill", row["Trayfill"])\
            .field("TowSign", row["TowSign"])\
            .field("Operator", row["Operator"])\
            .field("isLoggedOn", row["isLoggedOn"])\
            .field("MO", row["MO"])\
            .field("Item", row["Item"])\
            .field("TargetSpeed", row["TargetSpeed"])\
            .field("targetWaste", row["targetWaste"])\
            .field("targetFill", row["targetFill"])\
            .field("rodsPerShift", row["rodsPerShift"])\
            .field("expectable", row["expectable"])\
            .field("isConnected", row["isConnected"])\
            .time(index, WritePrecision.S)
            
            

    write_api.write(bucket, org, point)

    print(point)

    print('Writing was successful to Kafka topic');

