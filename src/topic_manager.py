class TopicManager:
    def __init__(self):
        self.base_topic = "gf_ext_v1"

    def construct_topic(self, area: str, system: str, subsystem: str, variable: str, device_id: str) -> str:
        return f"{self.base_topic}/{area}/{system}/{subsystem}/{variable}/{device_id}"

    def validate_topic(self, topic: str) -> bool:
        parts = topic.split("/")
        return (len(parts) == 6 and parts[0] == self.base_topic and ":" in parts[4])

    def extract_variable(self, topic: str) -> str:
        if self.validate_topic(topic):
            return topic.split("/")[4]
        raise ValueError("Invalid topic format")
