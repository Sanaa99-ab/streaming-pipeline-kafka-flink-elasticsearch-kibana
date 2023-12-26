from kafka import KafkaProducer
import requests
import json
import time
from rich.console import Console

# Kafka Producer configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'cryptocurrency_data'

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Function to fetch data from API with pagination
def fetch_data_with_pagination():
    page = 1
    while True:
        api_url = 'https://api.coingecko.com/api/v3/coins/markets'
        params = {
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': 250,  # Maximum allowed per page
            'page': page
        }

        response = requests.get(api_url, params=params)

        if response.status_code == 200:
            data = response.json()
            if not data:  # No more data available
                break

            yield data
            page += 1
        else:
            print(f"Failed to fetch data: {response.status_code}")
            break

# Function to continuously fetch data and send to Kafka
def continuous_fetch_and_produce():
    console = Console()
    count = 0
    for data in fetch_data_with_pagination():
        if data is not None:
            for item in data:
                try:
                    producer.send(topic_name, value=item)
                    count += 1
                except Exception as e:
                    print(f"Failed to send data for {item['name']} to Kafka topic '{topic_name}': {str(e)}")

                # Display JSON data with a delay
                json_str = json.dumps(item, indent=2)
                console.print(json_str)
                time.sleep(0.5)  # Adjust the delay duration as needed

            producer.flush()  # Flush to ensure messages are sent immediately

        else:
            print("Failed to fetch data")

        # Pause for 5 seconds before fetching the next page
        time.sleep(5)

    print(f"Total cryptocurrencies fetched: {count}")

if __name__ == '__main__':
    continuous_fetch_and_produce()