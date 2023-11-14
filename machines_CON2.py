from kafka import KafkaConsumer
import json
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timedelta  # Import timedelta for time subtraction
import os

# Kafka topic name should match the producer code
topic_name = "Machines"

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='Machine-group2'
)

# InfluxDB connection details
token = "y5JnLjRdpqXB42woz0A9ghOjGxz_GtawVVXeVQSTeFl5QbZ8AKyBziSL40otBEgXURS-hHyptX4tzxiaCcw4Vg=="
org = "Filtrona" 
bucket = "Filtrona_Machine"

# Mapping of Hungarian to English months
hungarian_to_english_month = {
    'jan.': '01',
    'febr.': '02',
    'márc.': '03',
    'ápr.': '04',
    'máj.': '05',
    'jún.': '06',
    'júl.': '07',
    'aug.': '08',
    'szept.': '09',
    'okt.': '10',
    'nov.': '11',
    'dec.': '12',
}

for message in consumer:
    try:
        mvalue = json.loads(message.value.decode('utf-8'))
        mkey = message.key
        moffset = message.offset

        # Extract relevant fields from the message
        ID = mvalue["ID"]
        Date_str = mvalue["Date"]

        # Replace Hungarian month abbreviations with numerical representations
        for hungarian, numeric in hungarian_to_english_month.items():
            Date_str = Date_str.replace(hungarian, numeric)

        # Define the custom date format for parsing
        date_format = "%Y-%m %d. %H:%M:%S"

        # Parse the date string using the custom format
        Date = datetime.strptime(Date_str, date_format)

        # Subtract two hours from the date
        Date -= timedelta(hours=1) #In summer -2, Winter -1

        # Format the date as ISO8601 format (e.g., "2023-09-28T08:00:00.000Z")
        iso_date_str = Date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        # Initialize InfluxDB client and write API
        client = InfluxDBClient(url="http://localhost:8086", token=token, org=org)
        write_api = client.write_api(write_options=SYNCHRONOUS)

        # Create an InfluxDB point with the extracted fields
        data_point = Point("Machines") \
            .tag("ID", ID) \
            .field("Address", mvalue["Address"]) \
            .field("Cutspeed", int(mvalue["Cutspeed"])) \
            .field("Filterspeed", int(mvalue["Filterspeed"])) \
            .field("Cutcounter", int(mvalue["Cutcounter"])) \
            .field("Filtercounter", int(mvalue["Filtercounter"])) \
            .field("Waste", int(mvalue["Waste"])) \
            .field("Wasteratio", float(mvalue["Wasteratio"])) \
            .field("Traycount", int(mvalue["Traycount"])) \
            .field("Last_Trayfill", int(mvalue["Last_Trayfill"])) \
            .field("Trayfill", int(mvalue["Trayfill"])) \
            .field("TowSign", int(mvalue["TowSign"])) \
            .field("Operator", mvalue["Operator"]) \
            .field("isLoggedOn", bool(mvalue["isLoggedOn"])) \
            .field("MO", mvalue["MO"]) \
            .field("Item", mvalue["Item"]) \
            .field("TargetSpeed", int(mvalue["TargetSpeed"])) \
            .field("targetWaste", float(mvalue["targetWaste"])) \
            .field("targetFill", int(mvalue["targetFill"])) \
            .field("rodsPerShift", int(mvalue["rodsPerShift"])) \
            .field("expectable", int(mvalue["expectable"])) \
            .field("isConnected", bool(mvalue["isConnected"])) \
            .time(iso_date_str)  # Use the ISO8601 formatted date

        # Write the point to InfluxDB
        write_api.write(bucket=bucket, record=data_point)
        print(data_point)

        print(f"Data written to InfluxDB for ID {ID} at {iso_date_str}")
    except Exception as e:
        print(f"Error processing message: {str(e)}")
