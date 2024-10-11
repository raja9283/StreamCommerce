import apache_beam as beam
import json
from apache_beam.transforms.window import SlidingWindows
from apache_beam.options.pipeline_options import PipelineOptions
import psycopg2
import datetime
from apache_beam.io.gcp.internal.clients import bigquery
import os
from dotenv import load_dotenv

# Get the current working directory and move one folder up
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
load_dotenv(dotenv_path=env_path)

DB_HOST = os.getenv('DB_HOST')  # e.g., 'localhost'
DB_PORT = os.getenv('DB_PORT')  # e.g., '5432'
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

input_pubsub_subscription = os.getenv('event_metrics_input_pubsub_subscription')
output_bigquery_table = bigquery.TableReference(
    projectId=os.getenv('project_id'),
    datasetId=os.getenv('dataset_id'),
    tableId=os.getenv('enriched_user_events'))

# Define the unified BigQuery schema
unified_schema = "customer_id:INTEGER,session_id:STRING,event_name:STRING,event_time:STRING,event_id:STRING,traffic_source:STRING,event_metadata:STRING,payment_status:STRING,search_keywords:STRING,booking_id:STRING,product_id:FLOAT,quantity:FLOAT,item_price:FLOAT,price_usd:FLOAT,first_name:STRING,last_name:STRING,username:STRING,email:STRING,gender:STRING,birthdate:STRING,device_type:STRING,device_id:STRING,device_version:STRING,home_location_lat:FLOAT,home_location_long:FLOAT,home_location:STRING,home_country:STRING,first_join_date:STRING,product_gender:STRING,masterCategory:STRING,subCategory:STRING,articleType:STRING,baseColour:STRING,season:STRING,product_year:INTEGER,usage:STRING,productDisplayName:STRING"

def serialize_row(row_dict):
    """Convert row values to JSON serializable format."""
    for key, value in row_dict.items():
        if isinstance(value, (datetime.datetime, datetime.date)):
            row_dict[key] = value.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(value, datetime.time):
            row_dict[key] = value.strftime('%H:%M:%S')
    return row_dict


def get_customer_data():
    """Fetch customer data from PostgreSQL and format it as a list of dictionaries."""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM customer")
    new_records = cursor.fetchall()

    # Get column names for creating a dictionary from rows
    column_names = [desc[0] for desc in cursor.description]

    # Convert the result into a list of dictionaries with serialized datetime values
    rows_to_send = []
    for row in new_records:
        row_dict = dict(zip(column_names, row))
        serialized_row = serialize_row(row_dict)
        rows_to_send.append(serialized_row)

    cursor.close()
    conn.close()
    return rows_to_send

def get_product_data():
    """Fetch customer data from PostgreSQL and format it as a list of dictionaries."""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()
    cursor.execute("SELECT id product_id,gender product_gender,mastercategory,subcategory,articletype,basecolour,season,year product_year,usage,productdisplayname FROM product")
    new_records = cursor.fetchall()

    # Get column names for creating a dictionary from rows
    column_names = [desc[0] for desc in cursor.description]

    # Convert the result into a list of dictionaries with serialized datetime values
    rows_to_send = []
    for row in new_records:
        row_dict = dict(zip(column_names, row))
        serialized_row = serialize_row(row_dict)
        rows_to_send.append(serialized_row)

    cursor.close()
    conn.close()
    return rows_to_send


class EnrichWithCustomerData(beam.DoFn):
    """Enrich events with customer data using a side input dictionary."""
    def process(self, element, customer_map):
        """Enrich each event with customer information."""
        # `element` is now a tuple: (customer_id, event)
        _, event = element  # Extract the event dictionary from the tuple

        # Get customer ID from the event
        customer_id = event.get('customer_id')

        # Lookup customer details using the side input dictionary
        customer_info = customer_map.get(customer_id, {})

        # Add customer info to the event
        enriched_event = event.copy()
        enriched_event.update(customer_info)
        yield enriched_event

class EnrichWithProductData(beam.DoFn):
    """Enrich events with product data using a side input dictionary."""
    def process(self, element, product_map):
        """Enrich each event with product information."""
        # Extract product_id from the event
        product_id = element.get('product_id')

        # Lookup product details using the side input dictionary
        product_info = product_map.get(product_id, {})

        # Add product info to the event
        enriched_event = element.copy()
        enriched_event.update(product_info)
        yield enriched_event

class ParseEvent(beam.DoFn):
    def process(self, element):
        try:
            
            event = json.loads(element.decode('utf-8'))
            print((event['customer_id'],event['event_name'],event['product_id'], event['quantity']))
            yield event
        except json.JSONDecodeError as e:
            print(f"Failed to parse event: {element} - Error: {e}")


class FormatEvent(beam.DoFn):
    """Formats streaming event records into a key-value format."""
    def process(self, element):
        customer_id = element['customer_id']
        yield (customer_id, element)  # Output as a tuple (customer_id, event_data)



def transform_to_unified_schema(element):
    """Transform the element into the unified schema format."""
    # Fill missing customer fields with None
    unified_record = {
        'customer_id': element.get('customer_id', None),
        'session_id': element.get('session_id', None),
        'event_name': element.get('event_name', None),
        'event_time': element.get('event_time', None),
        'event_id': element.get('event_id', None),
        'traffic_source': element.get('traffic_source', None),
        'event_metadata': element.get('event_metadata', None),
        'payment_status': element.get('payment_status', None),
        'search_keywords': element.get('search_keywords', None),
        'booking_id': element.get('booking_id', None),
        'product_id': element.get('product_id', None),
        'quantity': element.get('quantity', None),
        'item_price': element.get('item_price', None),
        'price_usd': element.get('price_usd', None),
        'first_name': element.get('first_name', None),
        'last_name': element.get('last_name', None),
        'username': element.get('username', None),
        'email': element.get('email', None),
        'gender': element.get('gender', None),
        'birthdate': element.get('birthdate', None),
        'device_type': element.get('device_type', None),
        'device_id': element.get('device_id', None),
        'device_version': element.get('device_version', None),
        'home_location_lat': element.get('home_location_lat', None),
        'home_location_long': element.get('home_location_long', None),
        'home_location': element.get('home_location', None),
        'home_country': element.get('home_country', None),
        'first_join_date': element.get('first_join_date', None),
        'product_gender': element.get('product_gender', None),  # Updated to use the renamed gender
        'masterCategory': element.get('mastercategory', None),
        'subCategory': element.get('subcategory', None),
        'articleType': element.get('articletype', None),
        'baseColour': element.get('basecolour', None),
        'season': element.get('season', None),
        'product_year': element.get('product_year', None),  # Assuming you've renamed year to product_year
        'usage': element.get('usage', None),
        'productDisplayName': element.get('productdisplayname', None)
    }
    return unified_record

# Pipeline options and parameters
options = PipelineOptions(streaming=True, runner='DirectRunner')

# Step 1: Fetch static customer data
customer_data = get_customer_data()
product_data = get_product_data()

# Step 2: Set up the Beam pipeline
with beam.Pipeline(options=options) as p:
    # Step 3: Create a PCollection for streaming events from Pub/Sub
    events = (p
              | 'ReadData' >> beam.io.ReadFromPubSub(subscription=input_pubsub_subscription)
              | 'Parse Events' >> beam.ParDo(ParseEvent())
              | 'Format Events by Customer ID' >> beam.ParDo(FormatEvent()))

    # Step 4: Convert the static customer data into a PCollection of (customer_id, customer_info) tuples
    customer_pcoll = (p
                      | 'Create Customer PCollection' >> beam.Create(customer_data)
                      | 'Format Customer Data' >> beam.Map(lambda row: (row['customer_id'], row)))

    # Step 5: Convert the formatted PCol lection into a side input dictionary
    customer_side_input = beam.pvalue.AsDict(customer_pcoll)
    
    # Step 4: Convert the static customer data into a PCollection of (customer_id, customer_info) tuples
    product_pcoll = (p
                      | 'Create Product PCollection' >> beam.Create(product_data)
                      | 'Format Product Data' >> beam.Map(lambda row: (row['product_id'], row)))

    # Step 5: Convert the formatted PCol lection into a side input dictionary
    product_side_input = beam.pvalue.AsDict(product_pcoll)
    
    # Step 6: Enrich the events using the customer side input
    enriched_events = (events
                       | 'Enrich with Customer Data' >> beam.ParDo(EnrichWithCustomerData(), customer_side_input))

    enriched_events_2 = (enriched_events
                       | 'Enrich with Product Data' >> beam.ParDo(EnrichWithProductData(), product_side_input))

    # Transform the data into the unified schema
    transformed_events = enriched_events_2 | 'Transform to Unified Schema' >> beam.Map(transform_to_unified_schema)

    # Write the transformed events to a single BigQuery table
    transformed_events | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
        output_bigquery_table,
        schema=unified_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )
