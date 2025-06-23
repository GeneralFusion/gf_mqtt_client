import pytest
from src.topic_manager import TopicManager

def test_construct_topic():
    manager = TopicManager()
    topic = manager.construct_topic("fusion_hall", "m3v3", "axuv", "gain:1:2:a", "2D_AD_0_0001")
    assert topic == "gf_ext_v1/fusion_hall/m3v3/axuv/gain:1:2:a/2D_AD_0_0001"

def test_validate_topic():
    manager = TopicManager()
    assert manager.validate_topic("gf_ext_v1/fusion_hall/m3v3/axuv/gain:1:2:a/2D_AD_0_0001") == True
    assert manager.validate_topic("invalid/topic") == False
