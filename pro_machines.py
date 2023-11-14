import json
import time
import schedule
import requests
import pandas as pd
from kafka import KafkaProducer
from queue import Queue
from io import StringIO

# Set up for Kafka Producer
bootstrap_servers = ['localhost:9092']
topic_name = "Machines"
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Initialize an empty DataFrame with specified columns
columns = ['ID', 'Date', 'Address', 'Cutspeed', 'Filterspeed', 'Cutcounter', 'Filtercounter', 'Waste', 'Wasteratio', 'Traycount', 'Last_Trayfill', 'Trayfill', 'TowSign', 'Operator', 'isLoggedOn', 'MO', 'Item', 'TargetSpeed', 'targetWaste', 'targetFill', 'rodsPerShift', 'expectable', 'isConnected']
df = pd.DataFrame(columns=columns)

# In-memory buffer
data_buffer = Queue(maxsize=100)  # Adjust the size based on your requirements

def fetch_and_store_data():
    global df, data_buffer  # Declare df and data_buffer as global variables

    # Specify the URL of the online CSV file
    csv_url = 'http://10.9.161.25/dataList.csv'

    # Use requests to download the CSV data
    response = requests.get(csv_url)

    if response.status_code == 200:
        # Parse the CSV data and append it to the DataFrame
        df = pd.concat([df, pd.read_csv(StringIO(response.text))], ignore_index=True)

        # Add each row to the in-memory buffer
        for _, row in df.iterrows():
            data_buffer.put(json.dumps(row.to_dict()).encode('utf-8'))
       
        # Empty the DataFrame after adding data to the buffer
        df = pd.DataFrame(columns=columns)
    else:
        print(f"Failed to fetch data from {csv_url}")

# Schedule the data fetching and sending
schedule.every(15).seconds.do(fetch_and_store_data)

while True:
    schedule.run_pending()
    time.sleep(12)

    # Producer code: Send data from the buffer to Kafka
    while not data_buffer.empty():
        data = data_buffer.get()
        producer.send(topic_name, data)
        print (data)
        print("Data sent to Kafka from in-memory buffer")