import json
import os
from collections import Counter
from datetime import datetime, timedelta, timezone

import tldextract

import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.connectors.kafka import operators as kop
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.operators.windowing import SystemClock, TumblingWindower

KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')
KAFKA_TOPIC = 'browser_history'
WINDOW_LENGTH_SECONDS = int(os.getenv('WINDOW_LENGTH_SECONDS', 10))


def get_domain(message):
    """Deserializes the message value and extracts the domain"""
    try:
        # Kafka message value is bytes, needs decoding
        url_data = json.loads(message.value.decode('utf-8'))
        url = url_data.get('url')
        if not url: return None
        
        return tldextract.extract(url).top_domain_under_public_suffix
    except (json.JSONDecodeError, AttributeError):
        return None


def format_output(key_window_list):
    """Counts domains in a window and formats the top 5 for printing"""
    key, (window_id, domains) = key_window_list
    if not domains:
        return f"--- No data in window {window_id} ---"

    counts = Counter(domains)
    top_five = counts.most_common(5)

    output_str = f"\nTop 5 domains for window {window_id} ({WINDOW_LENGTH_SECONDS}s)\n"
    for domain, count in top_five:
        output_str += f"{domain}: {count} visits\n"
    output_str += "--------------------------------------"
    return output_str


flow = Dataflow("processor")

kafka_in = kop.input("kafka_in", flow, brokers=[KAFKA_SERVER], topics=[KAFKA_TOPIC])

# windowing configuration
clock = SystemClock()
align_to = datetime(2025, 1, 1, tzinfo=timezone.utc)
windower = TumblingWindower(
    length=timedelta(seconds=WINDOW_LENGTH_SECONDS), align_to=align_to
)

# op.filter_map is more efficient than a separate map and filter
valid_domains = op.filter_map("get_domain", kafka_in.oks, get_domain)

# I assigne all items the same key so they end up in the same window
keyed_domains = op.key_on("key_on_all", valid_domains, lambda _domain: "ALL")

# then collect all domains within each 10-second window
windowed_stream = win.collect_window("collect", keyed_domains, clock, windower)

# format the results for printing
# the .down attribute of the window output is the stream of results
formatted_stream = op.map("format_output", windowed_stream.down, format_output)

op.output("stdout", formatted_stream, StdOutSink())