import asyncio
import time
import logging

from gf_mqtt_client.mqtt_client import MQTTClient
from gf_mqtt_client.message_handler import RequestHandlerBase
from gf_mqtt_client.topic_manager import TopicManager


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Public MQTT broker details
# Replace with your actual broker details if needed.
PUBLIC_BROKER = "broker.emqx.io"
PUBLIC_PORT = 1883
DEVICE_TAG = "2D_XX_0_9999"

# The following function creates a client, which listens for requests and responds with a mock response.
# It simulates a device that can handle requests for a specific path, in this case, "mock".
# It repeatedly sends requests to itself every 2 seconds, simulating a request-response cycle.

def create_response(request_payload: dict) -> dict:
    """Generate a mock response based on the request payload."""
    path = request_payload["header"]["path"]
    assert path == "mock"

    return {
        "header": {
            "response_code": 205,
            "path": path,
            "request_id": request_payload["header"]["request_id"],
            "correlation_id": request_payload["header"].get("correlation_id"),
        },
        "body": [0, 1, 2, 3, 4],
        "timestamp": str(int(time.time() * 1000)),
    }

# Request handler that processes incoming requests and sends a response.
async def request_handler(client: MQTTClient, topic: str, payload: dict) -> dict:
    response = create_response(payload)
    # print in red text
    print(f"\033[91m[{DEVICE_TAG}] Responding to {payload['header']['request_id']}\033[0m")
    response_topic = TopicManager().build_response_topic(
        request_topic=topic
    )
    await client.publish(response_topic, response)
    return response

# Create an MQTT client that listens for requests and responds accordingly.
async def create_mqtt_client():
    client = MQTTClient(
        broker=PUBLIC_BROKER, port=PUBLIC_PORT, timeout=5, identifier=DEVICE_TAG
    )
    await client.add_message_handler(
        RequestHandlerBase(process=request_handler, propagate=False)
    )
    await client.connect()
    return client


async def main():
    client = await create_mqtt_client()
    try:
        print(f"Connected to MQTT broker as {DEVICE_TAG}. Listening for requests...")
        while True:
            await client.request(target_device_tag=DEVICE_TAG, subsystem="example", path="mock")
            await asyncio.sleep(2)  
    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
