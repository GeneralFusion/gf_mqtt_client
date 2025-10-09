# MQTT CoAP Module Documentation

This module provides a Python-based implementation for MQTT communication inspired by the CoAP protocol, designed for asynchronous operations and potential microcontroller compatibility (e.g., Arduino). It includes a flexible payload structure to support general messaging and a get-request interaction model.

## Python Compatibility
This library has been tested on:
  - 3.11.9
  - 3.12.8
  - 3.13.5

It is currently not compatible with Python <3.11 or >3.13

## Asyncio Compatibility

Starting in Python 3.8, Windows switched its default loop policy to `WindowsProactorEventLoopPolicy`, which is incompatible with some third-party libraries (such as `paho-mqtt`, which depends on add_reader support). This library includes a module to manage asyncio compatibility - [asyncio_compatibility.py](https://github.com/generalmattza/gf_mqtt_client/blob/main/src/gf_mqtt_client/asyncio_compatibility.py), which allows developers to detect, warn about, or automatically correct such incompatibilities by setting the safer `WindowsSelectorEventLoopPolicy` when needed.

To enable compatibility mode, set environment variable `ASYNCIO_COMPATIBILITY_MODE=True` and call `configure_asyncio_compatibility()` early in your application. This will ensure that the correct loop policy is used when needed. Compatibility mode can prevent subtle runtime errors caused by mismatched event loop behavior. 

When an MQTTClient object is initialized, `ensure_compatible_event_loop_policy()` is run, which will generate a warning if compatibility mode is not set. To disable this warning, set environment variable `SUPPRESS_ASYNCIO_WARNINGS=True`

## Payload Structure

The module defines three types of payloads: **General Payload**, **Request Payload**, and **Response Payload**. Each payload is validated using Pydantic models to ensure data integrity and consistency.

### General Payload

The General Payload is used for standard publish-subscribe messaging with customizable content.

* **Structure**:

  * `body` (int | float | str | array): Contains the data being transmitted. The keys and values depend on the information being passed (e.g., `{"state": "ONLINE", "last_updated": "1745534869619"}`).
  * `timestamp` (int64 | str): Epoch time in milliseconds, represented as a 64-bit signed integer or a string to support 32-bit microcontrollers (e.g., `"1745534869619"`).

* **Example**:

  ```json
  {
    "body": {
      "state": "ONLINE",
      "last_updated": "1745534869619"
    },
    "timestamp": "1745534869619"
  }
  ```

### Request Payload

The Request Payload is used to initiate a request (e.g., GET) to a specific device, mimicking CoAP's request-response paradigm.

* **Structure**:

  * `header` (object):

    * `method` (int | str): Specifies the request method. Valid values are `1` or `"GET"`, `2` or `"POST"`, `3` or `"PUT"`, `4` or `"DELETE"`.
    * `path` (str): The resource path (e.g., `"gains"`).
    * `request_id` (str): A 128-bit / 32-character hexadecimal string (e.g., `"16fd2706-8baf-433b-82eb-8c7fada847da"`).
    * `token` (Optional: str): An authentication token (e.g., `"123456789=="`).
    * `correlation_id` (Optional: str): A 128-bit / 32-character hexadecimal string for batching messages (e.g., `"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"`) or absent/null.
  * `body` (Optional: int | float | str | array | JSON): The payload body, used with the selected method (e.g., `[0, 1, 2, 3, 4]` or `{"data": [0, 1, 2]}`).
  * `timestamp` (int64 | str): Epoch time in milliseconds (e.g., `"1745534869619"`).

* **Example**:

  ```json
  {
    "header": {
      "method": 1,
      "path": "gains",
      "request_id": "16fd2706-8baf-433b-82eb-8c7fada847da",
      "token": "123456789==",
      "correlation_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    },
    "body": [0, 1, 2, 3, 4],
    "timestamp": "1745534869619"
  }
  ```

### Response Payload

The Response Payload is sent in response to a Request Payload, providing the result of the requested action.

* **Structure**:

  * `header` (object):

    * `response_code` (int): The response code from the server, based on CoAP codes multiplied by 100 (e.g., `205` for Content).
    * `path` (str): The resource path (e.g., `"gains"`), matching the request.
    * `request_id` (str): Copied from the request payload (e.g., `"16fd2706-8baf-433b-82eb-8c7fada847da"`).
    * `correlation_id` (Optional: str): Copied from the request payload if present (e.g., `"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"`) or absent/null.
  * `body` (int | float | str | array): The response data (e.g., `[0, 1, 2, 3, 4]`).
  * `timestamp` (int64 | str): Epoch time in milliseconds (e.g., `"1745534869619"`).

* **Example**:

  ```json
  {
    "header": {
      "response_code": 205,
      "path": "gains",
      "request_id": "16fd2706-8baf-433b-82eb-8c7fada847da",
      "correlation_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    },
    "body": [0, 1, 2, 3, 4],
    "timestamp": "1745534869619"
  }
  ```

## Response Codes

The `ResponseCode` enum defines CoAP‑style status codes multiplied by 100 (per RFC 7252):

| Name                         | Code | Description                          |
| ---------------------------- | ---- | ------------------------------------ |
| CREATED                      | 201  | Resource successfully created        |
| DELETED                      | 202  | Resource successfully deleted        |
| VALID                        | 203  | Valid request processed (no content) |
| CHANGED                      | 204  | Resource successfully updated        |
| CONTENT                      | 205  | Representation of resource           |
| BAD\_REQUEST                 | 400  | Bad request                          |
| UNAUTHORIZED                 | 401  | Unauthorized                         |
| BAD\_OPTION                  | 402  | Bad option                           |
| FORBIDDEN                    | 403  | Forbidden                            |
| NOT\_FOUND                   | 404  | Resource not found                   |
| METHOD\_NOT\_ALLOWED         | 405  | Method not allowed                   |
| NOT\_ACCEPTABLE              | 406  | Not acceptable                       |
| PRECONDITION\_FAILED         | 412  | Precondition failed                  |
| REQUEST\_ENTITY\_TOO\_LARGE  | 413  | Request entity too large             |
| UNSUPPORTED\_CONTENT\_FORMAT | 415  | Unsupported content format           |
| INTERNAL\_SERVER\_ERROR      | 500  | Internal server error                |
| NOT\_IMPLEMENTED             | 501  | Not implemented                      |
| BAD\_GATEWAY                 | 502  | Bad gateway                          |
| SERVICE\_UNAVAILABLE         | 503  | Service unavailable                  |
| GATEWAY\_TIMEOUT             | 504  | Gateway timeout                      |
| PROXYING\_NOT\_SUPPORTED     | 505  | Proxying not supported               |

## Exceptions

The client library uses a hierarchy of exceptions to signal errors based on response codes:

* `ResponseException` (base class for all response errors)
* `BadRequestResponse` (`400 Bad Request`)
* `UnauthorizedResponse` (`401 Unauthorized`)
* `NotFoundResponse` (`404 Not Found`)
* `MethodNotAllowedResponse` (`405 Method Not Allowed`)
* `InternalServerErrorResponse` (`500 Internal Server Error`)
* `GatewayTimeoutResponse` (`504 Gateway Timeout`)

Each exception carries attributes:

* `response_code`: numeric status
* `path`: the requested URI or topic
* `detail`: any error message or payload
* `source`: origin of the error (e.g., client or broker)
* `target`: intended request recipient

## Customizing Behavior with Handlers

Both the async and sync clients let you plug in custom logic before and after requests via **RequestHandlers** and **ResponseHandlers**.

### Adding Request Handlers (Async)

Before sending a request or when receiving an incoming message, you can intercept and modify the payload or implement side‑effects by registering a `RequestHandlerBase`:

```python
from gf_mqtt_client.message_handler import RequestHandlerBase

async def my_request_interceptor(client, topic: str, payload: dict) -> dict:
    # e.g. add a timestamp header or log the outgoing request
    payload["header"]["timing"] = int(time.time() * 1000)
    return payload

await mqtt_client.add_message_handler(
    RequestHandlerBase(
        process=my_request_interceptor,
        propagate=True  # True to continue to other handlers
    )
)
```

Use `propagate=False` to stop further handlers from running after yours.

### Adding Response Handlers (Async)

To handle responses centrally—log codes, raise on errors, or transform the body—use a `ResponseHandlerBase`:

```python
from gf_mqtt_client.message_handler import ResponseHandlerBase
from gf_mqtt_client.exceptions import NotFoundResponse

async def my_response_handler(client, topic: str, payload: dict) -> dict:
    code = payload["header"]["response_code"]
    if code == ResponseCode.NOT_FOUND.value:
        # raise a typed exception
        raise NotFoundResponse(path=payload["header"]["path"], detail="Resource missing")
    # otherwise pass it along
    return payload

await mqtt_client.add_message_handler(
    ResponseHandlerBase(
        process=my_response_handler,
        propagate=False,       # do not continue to default handler
        raise_exceptions=True   # exceptions bubble up to request() caller
    )
)
```

### Customizing the Sync Wrapper

The sync client exposes the same handler API (blocking under the hood). Just pass your async handlers into `add_message_handler`, and they’ll run in the background loop:

```python
client = SyncMQTTClient(...)
# Handlers are added in the same way:
client.add_message_handler(ResponseHandlerBase(process=my_response_handler, propagate=False))
client.connect()
```

### Handler Ordering and Propagation

* **Order matters**: handlers run in the sequence you register them.
* **propagate=True**: after your handler returns, subsequent ones still run.
* **propagate=False**: stops processing further handlers.
* **raise\_exceptions (ResponseHandlerBase only)**: when True, any exception in your handler will be thrown back to the caller of `request()` or `publish()`.

## Validation

* Payloads are validated using Pydantic models to ensure:

  * `request_id` and `correlation_id` are 32-character hexadecimal strings.
  * `method` matches the defined enum values (1-4 or "GET"/"POST"/"PUT"/"DELETE").
  * `response_code` matches the defined CoAP-based codes.
  * `timestamp` is a valid integer or string representation of an integer.
* The `PayloadHandler` class automatically determines the payload type (General, Request, or Response) based on the presence of `method` or `response_code` in the header.

## Usage

### Asynchronous Client (asyncio)

Below is an example of how to use the **async** client to send and respond to requests:

```python
import asyncio
import time
import logging

from gf_mqtt_client.mqtt_client import MQTTClient
from gf_mqtt_client.message_handler import RequestHandlerBase
from gf_mqtt_client.topic_manager import TopicManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

PUBLIC_BROKER = "broker.emqx.io"
PUBLIC_PORT = 1883
DEVICE_TAG = "2D_XX_0_9999"

# Generate a mock response for incoming requests
async def request_handler(client: MQTTClient, topic: str, payload: dict) -> dict:
    response = {
        "header": {
            "response_code": 205,
            "path": payload["header"]["path"],
            "request_id": payload["header"]["request_id"],
            "correlation_id": payload["header"].get("correlation_id"),
        },
        "body": [0, 1, 2, 3, 4],
        "timestamp": str(int(time.time() * 1000)),
    }
    print(f"[Responder] Responding to {payload['header']['request_id']}")
    response_topic = TopicManager().build_response_topic(request_topic=topic)
    await client.publish(response_topic, response)
    return response

async def main():
    client = MQTTClient(
        broker=PUBLIC_BROKER,
        port=PUBLIC_PORT,
        timeout=5,
        identifier=DEVICE_TAG
    )
    # Register request handler
    await client.add_message_handler(RequestHandlerBase(process=request_handler, propagate=False))
    await client.connect()

    try:
        print(f"Connected as {DEVICE_TAG}, sending GET to itself every 2s...")
        while True:
            resp = await client.request(target_device_tag=DEVICE_TAG, subsystem="example", path="mock")
            print(f"Received: {resp}")
            await asyncio.sleep(2)
    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
```

---

### Synchronous Client (blocking)

The sync wrapper runs the async client in a background thread, exposing blocking methods:

```python
# main_sync.py
import time
import logging

from gf_mqtt_client import SyncMQTTClient, ResponseHandlerBase, MQTTBrokerConfig, ResponseException

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BROKER_CONFIG = MQTTBrokerConfig(
    hostname="broker.emqx.io",
    port=1883,
    username="user",
    password=""
)

REQUESTOR_TAG = "2D_XX_0_9998"
TARGET_DEVICE_TAG = "2D_XX_0_9999"

# Example response handler for debugging
async def response_handler(client, topic: str, payload: dict) -> dict:
    logging.info(f"{client.identifier} got response {payload['header']['request_id']} on {topic}")
    return payload

if __name__ == "__main__":
    client = SyncMQTTClient(
        broker=BROKER_CONFIG.hostname,
        port=BROKER_CONFIG.port,
        timeout=5,
        identifier=REQUESTOR_TAG,
        username=BROKER_CONFIG.username,
        password=BROKER_CONFIG.password
    )
    # Attach handler
    client.add_message_handler(ResponseHandlerBase(process=response_handler, propagate=False))
    client.connect()

    try:
        logging.info(f"Connected as {REQUESTOR_TAG}, polling every 2s...")
        while True:
            try:
                resp = client.request(target_device_tag=TARGET_DEVICE_TAG, subsystem="axuv", path="gains")
                logging.info(f"Received: {resp}")
            except ResponseException as e:
                logging.error(f"Protocol error: {e}")
            time.sleep(2)
    except KeyboardInterrupt:
        logging.info("Exiting...")
    finally:
        client.disconnect()
```

---

## Validation

* Use the `PayloadHandler` class to create and validate payloads:

  * `create_general_payload(body, timestamp)`
  * `create_request_payload(method, path, request_id, body=None, token=None, correlation_id=None)`
  * `create_response_payload(response_code, path, request_id, body, correlation_id=None)`
  * `validate_payload(payload)` to check payload structure
  * `parse_payload(json_string)` to parse and validate JSON strings

---

For more details, refer to the docstrings and Pydantic models in each module.
