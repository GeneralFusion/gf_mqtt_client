import pytest
import asyncio
from src.mqtt_client import MQTTClient

# Ensure this topic works with your broker setup
TEST_TOPIC_PREFIX = "gf_ext_v1/fusion_hall/m3v3/axuv/gain:1:2:a"
TEST_TOPIC = f"{TEST_TOPIC_PREFIX}/2D_AD_0_0001"

@pytest.mark.asyncio
async def test_connect():
    client = MQTTClient(broker="lm26consys.gf.local", port=1893, timeout=5)
    client.username_pw_set("user", "goodlinerCompressi0n!")  # Set your credentials if needed
    await client.connect()
    assert client.is_connected, "Client failed to connect"
    await client.disconnect()

@pytest.mark.asyncio
async def test_request_success(mqtt_responder):
    client = MQTTClient("broker.emqx.io", timeout=3)
    await client.connect()

    response = await client.request(TEST_TOPIC)

    assert response is not None, "Expected a valid response, got None"
    assert "header" in response, "Missing 'header' in response"
    assert "response_code" in response["header"], "Missing 'response_code' in header"
    assert response["header"]["response_code"] == 205, "Unexpected response code"

    await client.disconnect()

@pytest.mark.asyncio
async def test_request_timeout(mqtt_responder):
    client = MQTTClient("broker.emqx.io", timeout=1)
    await client.connect()

    dead_topic = "nonexistent/topic/with/no/publisher"
    response = await client.request(dead_topic)

    assert response is None, "Expected no response due to timeout"
    await client.disconnect()

@pytest.mark.asyncio
async def test_concurrent_requests(mqtt_responder):
    client = MQTTClient("broker.emqx.io", timeout=10)
    await client.connect()

    async def send(index):
        resp = await client.request(TEST_TOPIC)
        assert resp is not None, f"Request {index} got no response"
        assert "header" in resp and "response_code" in resp["header"], f"Invalid response {index}"
        print(f"[{index}] Response: {resp['header']['response_code']}")

    await asyncio.gather(*[send(i) for i in range(5)])

    await client.disconnect()


@pytest.mark.asyncio
async def test_concurrent_requests_stress(mqtt_responder):
    client = MQTTClient("broker.emqx.io", timeout=100)
    await client.connect()

    async def send(index):
        resp = await client.request(TEST_TOPIC)
        assert resp is not None, f"Request {index} got no response"
        assert "header" in resp and "response_code" in resp["header"], f"Invalid response {index}"
        print(f"[{index}] Response: {resp['header']['response_code']}")

    # Stress with 50 concurrent requests
    await asyncio.gather(*[send(i) for i in range(10)])

    await client.disconnect()
