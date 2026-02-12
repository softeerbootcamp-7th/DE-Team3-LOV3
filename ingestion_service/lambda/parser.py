"""
차량 센서 JSON 파싱 모듈

원본 JSON의 중첩 구조를 flat row 단위로 변환한다.
1 trip JSON → N개의 flat row dict 리스트를 반환한다.
"""

from config import VALID_RANGE


def parse_trip(raw: dict) -> tuple[str, str, str, list[dict]]:
    """trip JSON을 메타데이터와 parsed rows로 분리한다.

    Args:
        raw: 차량 단말기에서 전송된 원본 JSON dict.

    Returns:
        (trip_id, vehicle_id, date, parsed_rows) 튜플.
        date는 start_time에서 추출한 YYYY-MM-DD 문자열.
    """
    trip_id = raw["trip_id"]
    vehicle_id = raw["vin"]
    date = raw["start_time"][:10]  # "2025-02-11T08:30:00+09:00" → "2025-02-11"

    rows = []
    for record in raw.get("records", []):
        parsed = _parse_record(record, trip_id, vehicle_id)
        if parsed is not None:
            rows.append(parsed)

    return trip_id, vehicle_id, date, rows


def _parse_record(record: dict, trip_id: str, vehicle_id: str) -> dict | None:
    """단일 record를 flat row로 변환한다.

    필수 필드가 누락되거나 유효 범위를 벗어나면 None을 반환한다.

    Args:
        record: records[] 내 단일 측정값.
        trip_id: trip 고유 ID.
        vehicle_id: 차량 식별자.

    Returns:
        파싱된 row dict. 유효하지 않으면 None.
    """
    try:
        acc = record["acc"]
        gyr = record["gyr"]
        gps = record["gps"]

        row = {
            "timestamp": record["ts"],
            "trip_id": trip_id,
            "vehicle_id": vehicle_id,
            "accel_x": float(acc[0]),
            "accel_y": float(acc[1]),
            "accel_z": float(acc[2]),
            "gyro_x": float(gyr[0]),
            "gyro_y": float(gyr[1]),
            "gyro_z": float(gyr[2]),
            "velocity": float(record.get("spd", 0)),
            "lon": float(gps["lng"]),
            "lat": float(gps["lat"]),
            "hdop": float(gps.get("hdop", 99.0)),
            "satellites": int(gps.get("sat", 0)),
        }

        if not _is_valid(row):
            return None

        return row

    except (KeyError, IndexError, TypeError, ValueError):
        return None


def _is_valid(row: dict) -> bool:
    """row의 값이 유효 범위 안에 있는지 확인한다."""
    lon_min, lon_max = VALID_RANGE["lon"]
    lat_min, lat_max = VALID_RANGE["lat"]
    spd_min, spd_max = VALID_RANGE["spd"]
    acc_min, acc_max = VALID_RANGE["acc"]
    gyr_min, gyr_max = VALID_RANGE["gyr"]

    if not (lon_min <= row["lon"] <= lon_max):
        return False
    if not (lat_min <= row["lat"] <= lat_max):
        return False
    if not (spd_min <= row["velocity"] <= spd_max):
        return False

    for axis in ("accel_x", "accel_y", "accel_z"):
        if not (acc_min <= row[axis] <= acc_max):
            return False

    for axis in ("gyro_x", "gyro_y", "gyro_z"):
        if not (gyr_min <= row[axis] <= gyr_max):
            return False

    return True
