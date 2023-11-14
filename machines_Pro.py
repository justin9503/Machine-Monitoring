from kafka import KafkaConsumer, KafkaProducer
import json
from json import loads
from csv import DictReader
import time
import schedule
import requests

# Set up for Kafka Producer
bootstrap_servers = ['localhost:9092']
topicname = "Machines"
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

def fetch_and_store_data():
    # Specify the URL of the online CSV file
    csv_url = 'http://10.9.161.25/dataList.csv'

    # Use requests to download the CSV data
    response = requests.get(csv_url)

    if response.status_code == 200:
        # Parse the CSV data
        csv_data = response.text
        csv_lines = csv_data.split('\n')
        csv_dict_reader = DictReader(csv_lines)

        index = 0

        for row in csv_dict_reader:
            producer.send(topicname, json.dumps(row).encode('utf-8'))
            print("Data sent successfully", row)
            index += 1

            if (index % 20) == 0:
                time.sleep(5)
    else:
        print(f"Failed to fetch data from {csv_url}")

# Schedule the data fetching and sending
schedule.every(5).seconds.do(fetch_and_store_data)

while True:
    schedule.run_pending()
    time.sleep(4)
