import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import Sessions, SlidingWindows
import json
import datetime
import os
from dotenv import load_dotenv

# Get the current working directory and move one folder up
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
load_dotenv(dotenv_path=env_path)

input_pubsub_subscription = os.getenv('cart_abandonment_input_pubsub_subscription')
output_pubsub_topic = os.getenv('cart_abandonment_output_pubsub_topic')

class ExtractUserEvents(beam.DoFn):
    def process(self, element):
        event = json.loads(element.decode('utf-8'))
        event['event_time'] = datetime.datetime.strptime(event['event_time'], '%Y-%m-%d %H:%M:%S')
        if event['event_name'] in ['ADD_TO_CART', 'BOOKING']:
            yield event

def format_output(element):
    json_data = json.dumps(element)
    return json_data.encode('utf-8')

# Function to identify cart abandonment by checking the count of distinct events
def detect_cart_abandonment(events):
    """Detects cart abandonment if only ADD_TO_CART is present and not followed by a BOOKING."""
    # Get distinct event names for the given `session_id` and `product_id`
    distinct_events = {event['event_name'] for event in events}

    # If only ADD_TO_CART is present, it's an abandoned cart
    if distinct_events == {'ADD_TO_CART'}:
        abandoned_event = events[0]
        yield {
            'session_id': abandoned_event['session_id'],
            'product_id': abandoned_event['product_id'],
            'abandoned_time': abandoned_event['event_time'].strftime('%Y-%m-%d %H:%M:%S'),
            'item_price': abandoned_event['price_usd'],
            'quantity': abandoned_event['quantity'],
            'lost_sale': abandoned_event['price_usd'] * abandoned_event['quantity']
        }

# Custom CombineFn to accumulate lost sales and abandoned carts
class CartAbandonmentAccumulator(beam.CombineFn):
    def create_accumulator(self):
        return {
            'abandoned_count': 0,
            'total_lost_sales': 0.0
        }

    def add_input(self, accumulator, input):
        accumulator['abandoned_count'] += 1
        accumulator['total_lost_sales'] += input['lost_sale']
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for accumulator in accumulators:
            merged['abandoned_count'] += accumulator['abandoned_count']
            merged['total_lost_sales'] += accumulator['total_lost_sales']
        return merged

    def extract_output(self, accumulator):
        return {
            'abandoned_count': accumulator['abandoned_count'],
            'total_lost_sales': round(accumulator['total_lost_sales'],2),
            'timestamp': datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        }

# Pipeline options and parameters

options = PipelineOptions(streaming=True, runner="DirectRunner")

with beam.Pipeline(options=options) as p:
    output = (
        p
        | "Read Data" >> beam.io.ReadFromPubSub(subscription=input_pubsub_subscription)
        | "Format Input Data" >> beam.ParDo(ExtractUserEvents())
        | 'Group by Session and Product' >> beam.Map(lambda x: ((x['session_id'], x['product_id']), x))
        | "Sliding Window of 5 Minutes" >> beam.WindowInto(Sessions(60 * 5))
        | "Group By Session_id And Product_id" >> beam.GroupByKey()
        | "Detect Cart Abandonment" >> beam.FlatMap(lambda kv: detect_cart_abandonment(kv[1]))
        # Sliding window of 1 hour with a 10-second period
        | "Sliding Window of 1 Hour" >> beam.WindowInto(SlidingWindows(60*60, 10))
        # Sum the lost sales and abandoned carts
        | "Sum Lost Sales and Abandoned Carts" >> beam.CombineGlobally(CartAbandonmentAccumulator()).without_defaults()
        | 'Format the output' >> beam.Map(format_output)
        | 'Write to Pub/Sub' >> beam.io.WriteToPubSub(output_pubsub_topic)
    )
