import json
from csv import DictReader
import time
import schedule
import requests
import pandas as pd
from kafka import KafkaProducer

# Set up for Kafka Producer
bootstrap_servers = ['localhost:9092']
topicname = "Machines"
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Create an empty DataFrame with the specified columns
columns = [
    'ID', 'Date', 'Address', 'Cutspeed', 'Filterspeed', 'Cutcounter',
    'Filtercounter', 'Waste', 'Wasteratio', 'Traycount', 'Last_Trayfill',
    'Trayfill', 'TowSign', 'Operator', 'isLoggedOn', 'MO', 'Item',
    'TargetSpeed', 'targetWaste', 'targetFill', 'rodsPerShift', 'expectable', 'isConnected'
]
df = pd.DataFrame(columns=columns)

def fetch_and_store_data():
    global df  # Declare df as a global variable

    # Specify the URL of the online CSV file
    csv_url = 'http://10.9.161.25/dataList.csv'

    # Use requests to download the CSV data
    response = requests.get(csv_url)

    if response.status_code == 200:
        # Parse the CSV data
        csv_data = response.text
        csv_lines = csv_data.split('\n')
        csv_dict_reader = DictReader(csv_lines)

        rows_to_append = []  # Store rows to append

        for row in csv_dict_reader:
            # Map CSV column names to DataFrame column names
            mapped_row = {
                'ID': row.get('ID'),
                'Date': row.get('Date'),  # Convert Timestamp to ISO 8601 string
                'Address': row.get('Address'),
                'Cutspeed': row.get('Cutspeed'),
                'Filterspeed': row.get('Filterspeed'),
                'Cutcounter': row.get('Cutcounter'),
                'Filtercounter': row.get('Filtercounter'),
                'Waste': row.get('Waste'),
                'Wasteratio': row.get('Wasteratio'),
                'Traycount': row.get('Traycount'),
                'Last_Trayfill': row.get('Last_Trayfill'),
                'Trayfill': row.get('Trayfill'),
                'TowSign': row.get('TowSign'),
                'Operator': row.get('Operator'),
                'isLoggedOn': row.get('isLoggedOn'),
                'MO': row.get('MO'),
                'Item': row.get('Item'),
                'TargetSpeed': row.get('TargetSpeed'),
                'targetWaste': row.get('targetWaste'),
                'targetFill': row.get('targetFill'),
                'rodsPerShift': row.get('rodsPerShift'),
                'expectable': row.get('expectable'),
                'isConnected': row.get('isConnected')
            }

            rows_to_append.append(mapped_row)

        # Convert the list of rows to a DataFrame and append it
        if rows_to_append:
            df = pd.concat([df, pd.DataFrame(rows_to_append)], ignore_index=True)

        # Now you have the data in a DataFrame (df) with the current timestamp in ISO 8601 format in the 'Date' column
        # You can send the data to Kafka from the DataFrame like this:
        for index, row in df.iterrows():
            producer.send(topicname, json.dumps(row.to_dict()).encode('utf-8'))
            print("Data sent to Kafka from DataFrame:", row.to_dict())
    else:
        print(f"Failed to fetch data from {csv_url}")

# Schedule the data fetching and sending
schedule.every(15).seconds.do(fetch_and_store_data)

while True:
    schedule.run_pending()
    time.sleep(12)
    

