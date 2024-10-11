import apache_beam as beam
from apache_beam.transforms.window import SlidingWindows
from apache_beam.options.pipeline_options import PipelineOptions
from collections import defaultdict
import json
import os
from dotenv import load_dotenv

# Get the current working directory and move one folder up
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
load_dotenv(dotenv_path=env_path)

input_pubsub_subscription = os.getenv("trending_info_input_pubsub_subscription")
output_pubsub_topic = os.getenv("trending_info_output_pubsub_topic")


class ExtractAndFilterProductSale(beam.DoFn):

    def process(self, element):
        event = json.loads(element.decode("utf-8"))
        product_id = event["product_id"]
        quantity = event["quantity"]
        price = event["price_usd"]  # Assuming price is included in the JSON
        event_type = event[
            "event_name"
        ]  # Assuming the event type is included in the JSON
        if event_type in ("BOOKING", "ADD_TO_CART"):
            yield (
                event_type,
                product_id,
                quantity,
                price,
            )  # Yield event type, product_id, quantity, and price


def format_output(element):
    json_data = json.dumps(element)
    return json_data.encode("utf-8")


class Top10Fn(beam.CombineFn):
    def create_accumulator(self):
        return {
            "total_quantity": 0,
            "total_amount": 0.0,
            "products": defaultdict(
                lambda: {"quantity": 0, "price": 0}
            ),  # Store quantity and price
        }

    def add_input(self, accumulator, input):
        event_type, product_id, quantity, price = input
        accumulator["total_quantity"] += quantity
        accumulator["total_amount"] += quantity * price  # Sum the total sales amount
        accumulator["products"][(event_type, product_id)]["quantity"] += quantity
        accumulator["products"][(event_type, product_id)]["price"] += (
            price * quantity
        )  # Store the latest price
        return accumulator

    def merge_accumulators(self, accumulators):
        merged_accumulator = self.create_accumulator()
        for accumulator in accumulators:
            merged_accumulator["total_quantity"] += accumulator["total_quantity"]
            merged_accumulator["total_amount"] += accumulator["total_amount"]
            for (event_type, product_id), data in accumulator["products"].items():
                merged_accumulator["products"][(event_type, product_id)][
                    "quantity"
                ] += data["quantity"]
                merged_accumulator["products"][(event_type, product_id)][
                    "price"
                ] += data[
                    "price"
                ]  # Keep the latest price
        return merged_accumulator

    def extract_output(self, accumulator):
        result = defaultdict(
            lambda: {"top10": [], "total_quantity": 0, "total_amount": 0.0}
        )

        # Fill result with top 10 products and total quantities and amounts
        for (event_type, product_id), data in accumulator["products"].items():
            quantity = data["quantity"]
            price = data["price"]
            result[event_type]["total_quantity"] += quantity
            result[event_type]["total_amount"] += (
                quantity * price
            )  # Calculate total amount
            result[event_type]["top10"].append(
                {
                    "product_id": product_id,
                    "quantity": quantity,
                    "amount": price * quantity,  # Include price in the top 10 result
                }
            )

        # Sort and take the top 10 for each event type
        for event_type in result:
            result[event_type]["top10"] = sorted(
                result[event_type]["top10"],
                key=lambda item: (item["amount"], item["quantity"]),
                reverse=True,
            )[:10]

        return dict(result)


# Pipeline options and parameters
options = PipelineOptions(streaming=True, runner="DirectRunner")


with beam.Pipeline(options=options) as p:
    top_sold_products = (
        p
        | "ReadData" >> beam.io.ReadFromPubSub(subscription=input_pubsub_subscription)
        | "ExtractAndFilterSales"
        >> beam.ParDo(ExtractAndFilterProductSale())  # Pass min_quantity to ParDo
        | "WindowIntoSliding" >> beam.WindowInto(SlidingWindows(60 * 60, 10))
        | "SumSales"
        >> beam.CombineGlobally(
            Top10Fn()
        ).without_defaults()  # Use custom CombineFn to get top 10
        | "FormatTopProducts" >> beam.Map(lambda x: x)  # Already formatted in Top10Fn
        | "PrintTopProducts" >> beam.Map(format_output)
        | "Write to Pub/Sub" >> beam.io.WriteToPubSub(output_pubsub_topic)
    )
