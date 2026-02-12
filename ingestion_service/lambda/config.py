"""
Ingestion Layer 설정

bounding box, S3 경로 등 Lambda 실행에 필요한 설정을 관리한다.
환경변수로 오버라이드 가능하며, 도로 추가 시 BBOXES에 항목만 추가하면 된다.
"""

import os

# S3 설정
BUCKET = os.environ.get("S3_BUCKET")
PREFIX = os.environ.get("S3_PREFIX", "raw-sensor-data")

# 대상 도로 bounding box
# 도로 추가 시 여기에 항목 추가
BBOXES = {
    "863": {
        "min_lon": 127.52,
        "max_lon": 127.78,
        "min_lat": 34.50,
        "max_lat": 35.07,
    },
}

# 데이터 유효 범위 (대한민국)
VALID_RANGE = {
    "lon": (124.0, 132.0),
    "lat": (33.0, 43.0),
    "spd": (0.0, 300.0),
    "acc": (-100.0, 100.0),
    "gyr": (-2000.0, 2000.0),
}