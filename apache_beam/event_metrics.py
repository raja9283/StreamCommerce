import apache_beam as beam
from apache_beam.transforms.window import SlidingWindows
from apache_beam.options.pipeline_options import PipelineOptions
from collections import defaultdict
import json
import os
from dotenv import load_dotenv

# Get the current working directory and move one folder up
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
load_dotenv(dotenv_path=env_path)

input_pubsub_subscription = os.getenv('event_metrics_input_pubsub_subscription')
output_pubsub_topic = os.getenv('event_metrics_output_pubsub_topic')

class ExtractAndFilterUserEvents(beam.DoFn):
    def process(self, element):
        event = json.loads(element.decode('utf-8'))
        session_id = event["session_id"]  # Assuming session ID is included in the JSON
        event_type = event["event_name"]  # e.g., page_view, add_to_cart, purchase
        yield (session_id, event_type)

def format_output(element):
    json_data = json.dumps(element)
    return json_data.encode('utf-8')

class MetricsAccumulator(beam.CombineFn):
    def create_accumulator(self):
        return {
            'distinct_sessions': set(),
            'page_view_count': 0,
            'add_to_cart_count': 0,
            'purchase_count': 0,
        }

    def add_input(self, accumulator, input):
        session_id, event_type = input
        
        accumulator['distinct_sessions'].add(session_id)  # Count every session
        if event_type == 'ITEM_DETAIL':
            accumulator['page_view_count'] += 1
        elif event_type == 'ADD_TO_CART':
            accumulator['add_to_cart_count'] += 1
        elif event_type == 'BOOKING':
            accumulator['purchase_count'] += 1
            
        return accumulator

    def merge_accumulators(self, accumulators):
        merged_accumulator = self.create_accumulator()
        for accumulator in accumulators:
            merged_accumulator['distinct_sessions'].update(accumulator['distinct_sessions'])
            merged_accumulator['page_view_count'] += accumulator['page_view_count']
            merged_accumulator['add_to_cart_count'] += accumulator['add_to_cart_count']
            merged_accumulator['purchase_count'] += accumulator['purchase_count']
        return merged_accumulator

    def extract_output(self, accumulator):
        total_sessions = len(accumulator['distinct_sessions'])
        return {
            'session_count': total_sessions,
            'page_view_count': accumulator['page_view_count'],
            'add_to_cart_count': accumulator['add_to_cart_count'],
            'purchase_count': accumulator['purchase_count'],
            'page_view_percentage': round((accumulator['page_view_count'] / total_sessions) * 100, 2) if total_sessions > 0 else 0,
            'add_to_cart_percentage': round((accumulator['add_to_cart_count'] / total_sessions) * 100, 2) if total_sessions > 0 else 0,
            'purchase_percentage': round((accumulator['purchase_count'] / total_sessions) * 100, 2) if total_sessions > 0 else 0,
        }

# Pipeline options and parameters
options = PipelineOptions(streaming=True, runner='DirectRunner')

with beam.Pipeline(options=options) as p:
    metrics = (
        p
        | 'ReadData' >> beam.io.ReadFromPubSub(subscription=input_pubsub_subscription)
        | 'ExtractAndFilterEvents' >> beam.ParDo(ExtractAndFilterUserEvents())
        | 'WindowIntoSliding' >> beam.WindowInto(SlidingWindows(60*60, 10))  # 1 day window with 10 second interval
        | 'CombineMetrics' >> beam.CombineGlobally(MetricsAccumulator()).without_defaults()
        | 'FormatTopProducts' >> beam.Map(lambda x: x)
        | 'PrintTopProducts' >> beam.Map(format_output)
        | 'Write to Pub/Sub' >> beam.io.WriteToPubSub(output_pubsub_topic)  # Uncomment to write to Pub/Sub
    )
