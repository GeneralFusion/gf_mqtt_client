import time
import logging

from gf_mqtt_client import SyncMQTTClient, ResponseHandlerBase, MQTTBrokerConfig, ResponseException, set_compatible_event_loop_policy


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BROKER_CONFIG = MQTTBrokerConfig(
    hostname="broker.emqx.io",
    port=1883,
    username="user",
    password=""
)

REQUESTOR_TAG = "2D_XX_0_9998"
TARGET_DEVICE_TAG = "2D_XX_0_9999"

# This ensures compatibility with asyncio and aiomqtt on Windows on modern Python versions.
set_compatible_event_loop_policy()

# Request handler that processes incoming requests and sends a response.
async def response_handler(client: SyncMQTTClient, topic: str, payload: dict) -> dict:

    logging.info(f"{client.identifier} Received response for request ID {payload['header']['request_id']} on topic {topic}")

    # Add any additional processing or debugging here if needed

    return payload

# Create an MQTT client that sends requests and listens for responses.
def create_mqtt_client():
    client = SyncMQTTClient(
        broker=BROKER_CONFIG.hostname, port=BROKER_CONFIG.port, timeout=5
    )
    # Add the custom response handler for debugging responses
    client.add_message_handler(
        ResponseHandlerBase(process=response_handler, propagate=False)
    )
    return client.connect()


def main():
    client = create_mqtt_client()
    try:
        logging.info(f"Connected to MQTT broker as {REQUESTOR_TAG}. Listening for requests...")
        while True:
            try:
                response = client.request(target_device_tag=TARGET_DEVICE_TAG, subsystem="axuv", path="gains", method="GET", timeout=1)
                logging.info(f"Received response: {response}")
                time.sleep(2)  
            except ResponseException as e:
                logging.error(f"Protocol exception occurred: {e}")
    except KeyboardInterrupt:
        logging.info("Exiting...")
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
