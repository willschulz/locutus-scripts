import websocket
import json
import redis

def on_message(wsapp, message):
    # Parse the message to see if it's a commit or not
    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        return  # skip malformed

    # Actually, in this scenario, we can push *all* messages to Redis
    # and filter in the consumer. But we could also filter here.
    r.rpush("bsky_post_queue", message)

def run_ws():
    wsapp = websocket.WebSocketApp(
        "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
        on_message=on_message
    )
    wsapp.run_forever()

if __name__ == "__main__":
    # Connect to Redis
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    run_ws()
