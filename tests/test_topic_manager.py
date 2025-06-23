import pytest
from src.topic_manager import TopicManager

def test_build_request_topic():
    tm = TopicManager(namespace="gf_int_v1")
    topic = tm.build_request_topic("2D_AD_0_0001", "axuv", "abc123")
    assert topic == "gf_int_v1/axuv/request/2D_AD_0_0001/abc123"

def test_build_response_topic():
    tm = TopicManager(namespace="gf_int_v1")
    request_topic = "gf_int_v1/axuv/request/2D_AD_0_0001/abc123"
    response_topic = tm.build_response_topic(request_topic)
    assert response_topic == "gf_int_v1/axuv/response/2D_AD_0_0001/abc123"
    
def test_build_response_topic_invalid_format():
    tm = TopicManager(namespace="gf_int_v1")
    with pytest.raises(IndexError):
        tm.build_response_topic("invalid/topic/format")