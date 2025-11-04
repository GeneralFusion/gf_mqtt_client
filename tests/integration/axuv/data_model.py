from pydantic import BaseModel, Field, model_validator, ValidationError
from typing import List, Dict

class MetricProperties(BaseModel):
    source_name: str = Field(..., description="Name of the data source, e.g. 'AXUV_chan_1'")
    channel_id: int = Field(..., ge=0, description="Channel index (0–3)")
    ch_gain: int = Field(4, description="Channel gain")
    ch_range: int = Field(5, description="Channel range")
    ch_offset: int = Field(0, description="Offset applied to channel data")
    wave_byte_order: int = Field(0, description="Byte order for waveform")
    time_zero: int = Field(0, description="Reference time offset")
    time_range: int = Field(0, description="Time span of acquisition")
    acq_srate: int = Field(0, description="Acquisition sample rate")
    acq_bit_res: int = Field(0, description="Acquisition bit resolution")
    acq_total_samples: int = Field(0, description="Total number of acquired samples")
    checksum: int = Field(0, description="Checksum of the data")

class Metric(BaseModel):
    name: str = Field(..., description="Device identifier")
    timestamp: int = Field(..., description="Epoch seconds when this channel was captured")
    properties: MetricProperties
    data: str = Field(..., description="Hex-encoded sample data (4 hex chars per sample)")

class DataPayload(BaseModel):
    timestamp: int = Field(..., description="Epoch seconds for the overall payload generation time")
    metrics: List[Metric]

    @model_validator(mode='before')
    def check_metrics_length(cls, values: Dict) -> Dict:
        metrics = values.get('metrics')
        if metrics is None:
            raise ValueError("'metrics' field is required")
        if len(metrics) != 4:
            raise ValueError(f"Expected 4 metrics, got {len(metrics)}")
        return values

# Example payload and validation
if __name__ == '__main__':
    sample_payload = {
        "timestamp": 10210201210,
        "metrics": [
            {
                "name": "AXUVTE99",
                "timestamp": 10210201210,
                "properties": {"source_name": "AXUV_chan_1", "channel_id": 0},
                "data": "ABCDABCDABCD"
            },
            {
                "name": "AXUVTE99",
                "timestamp": 10210201210,
                "properties": {"source_name": "AXUV_chan_2", "channel_id": 1},
                "data": "ABCDABCDABCD"
            },
            {
                "name": "AXUVTE99",
                "timestamp": 10210201210,
                "properties": {"source_name": "AXUV_chan_3", "channel_id": 2},
                "data": "ABCDABCDABCD"
            },
            {
                "name": "AXUVTE99",
                "timestamp": 10210201210,
                "properties": {"source_name": "AXUV_chan_4", "channel_id": 3},
                "data": "ABCDABCDABCD"
            }
        ]
    }
    try:
        payload = DataPayload(**sample_payload)
        print("Valid payload:", payload.json(indent=2))
    except ValidationError as e:
        print("Validation failed:", e.json())