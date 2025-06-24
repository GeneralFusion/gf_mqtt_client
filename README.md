# MQTT CoAP Module Documentation

This module provides a Python-based implementation for MQTT communication inspired by the CoAP protocol, designed for asynchronous operations and potential microcontroller compatibility (e.g., Arduino). It includes a flexible payload structure to support general messaging and a get-request interaction model.

## Payload Structure

The module defines three types of payloads: **General Payload**, **Request Payload**, and **Response Payload**. Each payload is validated using Pydantic models to ensure data integrity and consistency.

### General Payload
The General Payload is used for standard publish-subscribe messaging with customizable content.

- **Structure**:
  - `body` (int | float | str | array): Contains the data being transmitted. The keys and values depend on the information being passed (e.g., `{"state": "ONLINE", "last_updated": "1745534869619"}`).
  - `timestamp` (int64 | str): Epoch time in milliseconds, represented as a 64-bit signed integer or a string to support 32-bit microcontrollers (e.g., `"1745534869619"`).

- **Example**:
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

- **Structure**:
  - `header` (object):
    - `method` (int | str): Specifies the request method. Valid values are `1` or `"GET"`, `2` or `"POST"`, `3` or `"PUT"`, `4` or `"DELETE"`.
    - `path` (str): The resource path (e.g., `"gains"`).
    - `request_id` (str): A 128-bit / 32-character hexadecimal string (e.g., `"16fd2706-8baf-433b-82eb-8c7fada847da"`).
    - `token` (Optional: str): An authentication token (e.g., `"123456789=="`).
    - `correlation_id` (Optional: str): A 128-bit / 32-character hexadecimal string for batching messages (e.g., `"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"`) or absent/null.
  - `body` (Optional: int | float | str | array | JSON): The payload body, used with the selected method (e.g., `[0, 1, 2, 3, 4]` or `{"data": [0, 1, 2]}`).
  - `timestamp` (int64 | str): Epoch time in milliseconds (e.g., `"1745534869619"`).

- **Example**:
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

- **Structure**:
  - `header` (object):
    - `response_code` (int): The response code from the server, based on CoAP codes multiplied by 100 (e.g., `205` for Content).
    - `path` (str): The resource path (e.g., `"gains"`), matching the request.
    - `request_id` (str): Copied from the request payload (e.g., `"16fd2706-8baf-433b-82eb-8c7fada847da"`).
    - `correlation_id` (Optional: str): Copied from the request payload if present (e.g., `"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"`) or absent/null.
  - `body` (int | float | str | array): The response data (e.g., `[0, 1, 2, 3, 4]`).
  - `timestamp` (int64 | str): Epoch time in milliseconds (e.g., `"1745534869619"`).

- **Example**:
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

## Validation
- Payloads are validated using Pydantic models to ensure:
  - `request_id` and `correlation_id` are 32-character hexadecimal strings.
  - `method` matches the defined enum values (1-4 or "GET"/"POST"/"PUT"/"DELETE").
  - `response_code` matches the defined CoAP-based codes.
  - `timestamp` is a valid integer or string representation of an integer.
- The `PayloadHandler` class automatically determines the payload type (General, Request, or Response) based on the presence of `method` or `response_code` in the header.

## Usage
- Use the `PayloadHandler` class to create and validate payloads:
  - `create_general_payload(body, timestamp)`
  - `create_request_payload(method, path, request_id, body=None, token=None, correlation_id=None)`
  - `create_response_payload(response_code, path, request_id, body, correlation_id=None)`
  - `validate_payload(payload)` to check payload structure
  - `parse_payload(json_string)` to parse and validate JSON strings
```