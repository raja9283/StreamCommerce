import psycopg2
import time
import datetime
import json
import random
from google.cloud import pubsub_v1
import os
from dotenv import load_dotenv

# Get the current working directory and move one folder up
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=env_path)

input_pubsub_subscription = os.getenv("trending_info_input_pubsub_subscription")
# PostgreSQL connection parameters
DB_HOST = os.getenv('DB_HOST')  # e.g., 'localhost'
DB_PORT = os.getenv('DB_PORT')  # e.g., '5432'
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Google Cloud Pub/Sub parameters
PROJECT_ID = os.getenv('project_id')
TOPIC_ID = os.getenv('click_stream_topic')

# Initialize Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# Function to serialize the row dictionary values
def serialize_row(row_dict):
    """Convert row values to JSON serializable format."""
    for key, value in row_dict.items():
        if isinstance(value, (datetime.datetime, datetime.date)):
            row_dict[key] = value.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(value, datetime.time):
            row_dict[key] = value.strftime('%H:%M:%S')
    return row_dict

# Function to connect to the PostgreSQL database
def get_postgres_connection():
    """Create and return a PostgreSQL database connection."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

# Function to stream historical data from PostgreSQL and send to Pub/Sub
def stream_historical_data():
    # Initial reference time
    last_processed_event_time = "2024-10-11 00:00:00"  # Adjust as needed
    BATCH_SIZE = 1000  # Define your batch size
    print("Inside the function.")

    # Connect to PostgreSQL database
    conn = get_postgres_connection()
    cursor = conn.cursor()

    while True:
        # Define the query to fetch records with event_name 'BOOKING' based on last_processed_event_time
        query = f"""
            SELECT *
            FROM click_stream_ecom  -- Replace 'your_table_name' with your actual table name
            WHERE event_time > '{last_processed_event_time}' AND event_name in ('BOOKING','ADD_TO_CART','ITEM_DETAIL')
            ORDER BY event_time
            LIMIT {BATCH_SIZE}
        """

        # Execute the query
        cursor.execute(query)
        new_records = cursor.fetchall()

        # Get column names for creating a dictionary from rows
        column_names = [desc[0] for desc in cursor.description]

        # Convert the result into a list of dictionaries with serialized datetime values
        rows_to_send = []
        for row in new_records:
            # Convert each row into a dictionary with column names as keys
            row_dict = dict(zip(column_names, row))

            # Serialize the dictionary to ensure all fields are JSON serializable
            serialized_row = serialize_row(row_dict)
            rows_to_send.append(serialized_row)

        # Check if there are records to process
        if not rows_to_send:
            print("No new records found.")
            break  # Exit loop if no new records are found

        # Control the delay to insert the record exactly at its event time
        for row in rows_to_send:
            # Convert row to a dictionary for easier handling
            event_time_str = row['event_time']
            event_time = time.mktime(time.strptime(event_time_str, '%Y-%m-%d %H:%M:%S'))

            # Print publishing information
#             print(f"Publishing record with event time: {event_time_str} to Pub/Sub")

            # Convert the row dictionary to a JSON string and send to Pub/Sub
            message_data = json.dumps(row).encode('utf-8')
#             print(message_data)
            event = json.loads(message_data.decode('utf-8'))
            print((event['customer_id'],event['event_name'],event['product_id'], event['quantity']))
            # Publish the message to Pub/Sub
            publisher.publish(topic_path, message_data)

            # Sleep for a random interval between 1 and 30 seconds before sending the next record
            sleep_time = random.randint(1, 10)
#             print(f"Sleeping for {sleep_time} seconds before the next message...")
            time.sleep(sleep_time)

    # Close the cursor and connection after processing
    cursor.close()
    conn.close()

# Start streaming the historical data
stream_historical_data()
