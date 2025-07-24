import asyncio
import time
import logging

from gf_mqtt_client import SyncMQTTClient, ResponseHandlerBase, MQTTBrokerConfig


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Public MQTT broker details
# Replace with your actual broker details if needed.
BROKER_CONFIG = MQTTBrokerConfig(
    hostname="broker.emqx.io",
    port=1883,
    username="user",
    password=""
)

REQUESTOR_TAG = "2D_XX_0_9998"
TARGET_DEVICE_TAG = "2D_XX_0_9999"


# Request handler that processes incoming requests and sends a response.
async def response_handler(client: SyncMQTTClient, topic: str, payload: dict) -> dict:
    logging.info(f"Recevied response for request ID {payload['header']['request_id']} on topic {topic}")
    return payload

# Create an MQTT client that listens for requests and responds accordingly.
def create_mqtt_client():
    client = SyncMQTTClient(
        broker=BROKER_CONFIG.hostname, port=BROKER_CONFIG.port, timeout=5, identifier=REQUESTOR_TAG, username=BROKER_CONFIG.username, password=BROKER_CONFIG.password
    )
    client.add_message_handler(
        ResponseHandlerBase(process=response_handler, propagate=False)
    )
    client.connect()
    return client


def main():
    client = create_mqtt_client()
    try:
        logging.info(f"Connected to MQTT broker as {REQUESTOR_TAG}. Listening for requests...")
        while True:
            response = client.request(target_device_tag=TARGET_DEVICE_TAG, subsystem="axuv", path="gains", method="GET", timeout=5)
            logging.info(f"Received response: {response}")
            time.sleep(2)  
    except KeyboardInterrupt:
        logging.info("Exiting...")
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
