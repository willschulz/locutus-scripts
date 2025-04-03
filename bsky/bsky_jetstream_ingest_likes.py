import websocket
import json
import redis

from datetime import datetime
import pytz

# Save script start time in Pacific Time Zone
pacific = pytz.timezone("America/Los_Angeles")
start_time = datetime.now(pacific).strftime("%Y-%m-%d %H:%M:%S %Z")
with open("ingest_start_time.txt", "w") as f:
    f.write(start_time + "\n")


def on_message(wsapp, message):
    # Parse the message to see if it's a commit or not
    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        return  # skip malformed

    # Actually, in this scenario, we can push *all* messages to Redis
    # and filter in the consumer. But we could also filter here.
    r.rpush("bsky_like_queue", message)

def run_ws():
    wsapp = websocket.WebSocketApp(
        "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.like",
        on_message=on_message
    )
    wsapp.run_forever()

if __name__ == "__main__":
    # Connect to Redis
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    run_ws()
