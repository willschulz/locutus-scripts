import websocket

def on_message(wsapp, message):
    print(message)

wsapp = websocket.WebSocketApp("wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post", on_message=on_message)
wsapp.run_forever()