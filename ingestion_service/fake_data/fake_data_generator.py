"""
더미 센서 데이터 생성 및 S3 적재 (producer.py 기반)

producer.py의 generate_trip 로직을 재사용하되,
Kinesis 대신 S3에 parquet으로 저장한다.

Lambda와 동일한 파이프라인:
  generate_trip → parser → filter → writer (S3)

옵션 1 적용: records 수 증가 (100 → 1,000)로 빠른 배치 처리
  한 trip: 80-120 KB
  필요: 50,000 trips
  시간: ~14시간 (하루 안에 5GB 적재)
"""

import sys
import json
import time
import csv
import math
import random
import logging
from pathlib import Path
from datetime import datetime, timedelta

# producer.py의 로직 임포트 (같은 디렉토리에 있다고 가정)
sys.path.insert(0, str(Path(__file__).parent.parent / "producer"))

# Lambda 모듈 임포트 (파싱 + 필터링 + S3 저장)
sys.path.insert(0, str(Path(__file__).parent.parent / "lambda"))
from parser import parse_trip
from filter import filter_by_bbox
from writer import write_to_s3

# producer 설정 (일부 복사)
SAMPLING_INTERVAL_MS = 100
DEVICE_PROFILES = {
    "OBD2": {"acc_noise": 0.3, "gyr_noise": 0.08, "gps_noise": 0.00008},
    "DTG": {"acc_noise": 0.5, "gyr_noise": 0.12, "gps_noise": 0.00012},
}

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")


def load_road_network(csv_path: str) -> dict:
    """도로망 CSV에서 세그먼트 좌표 로드 (producer.py 복사)"""
    links = {}
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            lid = row["link_id"]
            if lid not in links:
                links[lid] = []
            links[lid].append({
                "s_id": row["s_id"],
                "start_lon": float(row["start_lon"]),
                "start_lat": float(row["start_lat"]),
                "end_lon": float(row["end_lon"]),
                "end_lat": float(row["end_lat"]),
            })
    logger.info(f"도로망 로드: {len(links)}개 링크, {sum(len(v) for v in links.values())}개 세그먼트")
    return links


def pick_route(links: dict, min_segments: int = 10, max_segments: int = 100) -> list[dict]:
    """도로망에서 랜덤 경로 선택 (producer.py 복사)"""
    target = int(random.lognormvariate(math.log(30), 0.5))
    target = max(min_segments, min(target, max_segments))

    link_ids = list(links.keys())
    start_link = random.choice(link_ids)
    route = []
    route.extend(links[start_link])

    visited = {start_link}
    current_end = (links[start_link][-1]["end_lon"], links[start_link][-1]["end_lat"])

    while len(route) < target:
        best_link = None
        best_dist = float("inf")
        for lid, segs in links.items():
            if lid in visited:
                continue
            start = (segs[0]["start_lon"], segs[0]["start_lat"])
            dist = (current_end[0] - start[0]) ** 2 + (current_end[1] - start[1]) ** 2
            if dist < best_dist:
                best_dist = dist
                best_link = lid

        if best_link is None or best_dist > 0.001:
            break
        visited.add(best_link)
        route.extend(links[best_link])
        current_end = (links[best_link][-1]["end_lon"], links[best_link][-1]["end_lat"])

    if len(route) < min_segments:
        for lid in random.sample(link_ids, min(5, len(link_ids))):
            if lid not in visited:
                route.extend(links[lid])
                if len(route) >= min_segments:
                    break

    return route[:target]


def generate_speed_profile(num_points: int) -> list[float]:
    """현실적인 속도 프로파일 생성 (producer.py 복사)"""
    speeds = []
    current_speed = 0.0
    target_speed = random.uniform(50, 80)
    state = "accelerating"

    for i in range(num_points):
        if state == "accelerating":
            current_speed += random.uniform(1.0, 3.0)
            if current_speed >= target_speed:
                current_speed = target_speed
                state = "cruising"
        elif state == "cruising":
            current_speed += random.gauss(0, 1.5)
            current_speed = max(30, min(current_speed, 100))
            if random.random() < 0.03:
                state = "decelerating"
                target_speed = random.uniform(0, 30)
        elif state == "decelerating":
            current_speed -= random.uniform(2.0, 5.0)
            if current_speed <= target_speed:
                current_speed = max(0, target_speed)
                state = "stopped" if current_speed < 5 else "accelerating"
                target_speed = random.uniform(50, 80)
        elif state == "stopped":
            current_speed = 0
            if random.random() < 0.15:
                state = "accelerating"
                target_speed = random.uniform(50, 80)

        speeds.append(round(max(0, current_speed), 1))

    return speeds


def generate_pothole_impact(intensity: float = 1.0) -> list[dict]:
    """포트홀 충격 패턴 생성 (producer.py 복사)"""
    pattern = []
    main_impact = random.uniform(5, 15) * intensity
    pattern.append({"acc_z": main_impact, "gyr": random.uniform(1, 4) * intensity})
    pattern.append({"acc_z": main_impact * 0.6, "gyr": random.uniform(0.5, 2) * intensity})
    for j in range(random.randint(2, 3)):
        decay = 0.3 ** (j + 1)
        pattern.append({
            "acc_z": main_impact * decay,
            "gyr": random.uniform(0.2, 1) * intensity * decay,
        })
    return pattern


def generate_trip(
    vehicle_id: str,
    route: list[dict],
    points_per_seg: int = 10,
) -> dict:
    """
    현실적인 주행 데이터 생성 (producer.py 기반)

    Args:
        vehicle_id: 차량 ID
        route: 세그먼트 리스트
        points_per_seg: 세그먼트당 센서 포인트 (기본 10 = 100 → 1,000으로 증가)

    Returns:
        trip JSON dict (Lambda 입력 형식과 호환)
    """
    device_type = random.choice(list(DEVICE_PROFILES.keys()))
    profile = DEVICE_PROFILES[device_type]

    total_points = len(route) * points_per_seg
    speeds = generate_speed_profile(total_points)

    records = []
    ts_base = int(time.time() * 1000)
    ts_offset = random.randint(0, 3600) * 1000
    ts = ts_base - ts_offset

    trip_id = f"TRIP-{time.strftime('%Y%m%d')}-{vehicle_id}-{int(time.time()) % 10000:04d}"

    impact_queue = []
    point_idx = 0

    for seg in route:
        for j in range(points_per_seg):
            if point_idx >= total_points:
                break

            t = j / points_per_seg
            lon = seg["start_lon"] + (seg["end_lon"] - seg["start_lon"]) * t
            lat = seg["start_lat"] + (seg["end_lat"] - seg["start_lat"]) * t

            lon += random.gauss(0, profile["gps_noise"])
            lat += random.gauss(0, profile["gps_noise"])

            speed = speeds[point_idx]
            speed_factor = speed / 60.0

            acc_x = random.gauss(0, profile["acc_noise"] * speed_factor)
            acc_y = random.gauss(0, profile["acc_noise"] * speed_factor)
            acc_z = 9.81 + random.gauss(0, profile["acc_noise"] * 0.5)
            gyr_x = random.gauss(0, profile["gyr_noise"] * speed_factor)
            gyr_y = random.gauss(0, profile["gyr_noise"] * speed_factor)
            gyr_z = random.gauss(0, profile["gyr_noise"] * speed_factor)

            if impact_queue:
                impact = impact_queue.pop(0)
                acc_z += impact["acc_z"]
                gyr_x += impact["gyr"]
                gyr_y += impact["gyr"] * random.uniform(0.3, 0.7)

            if j == 0:
                if random.random() < 0.05 and speed > 10:  # 5% 포트홀 확률
                    intensity = random.uniform(0.5, 2.0)
                    impact_queue.extend(generate_pothole_impact(intensity))

            if random.random() < 0.005 and speed > 20:
                acc_z += random.uniform(3, 8)
                gyr_x += random.uniform(0.5, 1.5)

            hdop = round(random.uniform(0.8, 3.0), 1)
            satellites = random.randint(6, 12)

            records.append({
                "ts": ts + (point_idx * SAMPLING_INTERVAL_MS),
                "acc": [round(acc_x, 4), round(acc_y, 4), round(acc_z, 4)],
                "gyr": [round(gyr_x, 4), round(gyr_y, 4), round(gyr_z, 4)],
                "spd": speed,
                "gps": {
                    "lng": round(lon, 6),
                    "lat": round(lat, 6),
                    "hdop": hdop,
                    "sat": satellites,
                },
            })

            point_idx += 1

    duration_ms = len(records) * SAMPLING_INTERVAL_MS
    start_dt = time.strftime("%Y-%m-%dT%H:%M:%S+09:00", time.localtime(ts / 1000))
    end_dt = time.strftime("%Y-%m-%dT%H:%M:%S+09:00", time.localtime((ts + duration_ms) / 1000))

    return {
        "trip_id": trip_id,
        "vin": vehicle_id,
        "device_type": device_type,
        "start_time": start_dt,
        "end_time": end_dt,
        "records": records,
    }


def process_and_upload(trip_json: dict) -> dict:
    """trip JSON → 파싱 → 필터링 → S3 저장 (Lambda 파이프라인)"""
    try:
        # 1. 파싱
        trip_id, vehicle_id, date, rows = parse_trip(trip_json)

        if not rows:
            return {"status": "parse_failed", "trip_id": trip_id}

        # 2. 필터링 (도로 범위)
        filtered_rows = filter_by_bbox(rows)

        if not filtered_rows:
            return {"status": "filtered_out", "trip_id": trip_id}

        # 3. S3 저장
        s3_key = write_to_s3(filtered_rows, date, trip_id)

        return {
            "status": "success",
            "trip_id": trip_id,
            "vehicle_id": vehicle_id,
            "date": date,
            "rows_uploaded": len(filtered_rows),
            "s3_key": s3_key,
        }

    except Exception as e:
        logger.error(f"Error processing trip: {e}")
        return {"status": "error", "error": str(e)}


def batch_generate_and_upload(num_trips: int, links: dict):
    """배치로 더미 데이터 생성 및 S3 업로드"""
    logger.info(f"생성 시작: {num_trips}개 trip (옵션 1: points_per_seg=10, ~80-120KB/trip)")

    success_count = 0
    skip_count = 0
    error_count = 0

    start_time = time.time()

    for i in range(num_trips):
        vehicle_id = f"v{(i % 100):03d}"
        route = pick_route(links)

        # 옵션 1: points_per_seg=10 (기본 3-8에서 10으로 증가)
        trip_json = generate_trip(vehicle_id, route, points_per_seg=10)

        result = process_and_upload(trip_json)

        if result["status"] == "success":
            success_count += 1
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta_sec = (num_trips - i - 1) / rate if rate > 0 else 0
            eta_hours = eta_sec / 3600

            logger.info(
                f"[{i+1:5d}/{num_trips}] ✓ {result['trip_id']}: "
                f"{result['rows_uploaded']} rows → {result['s3_key'].split('/')[-1]} "
                f"| 속도: {rate:.1f} trips/sec | ETA: {eta_hours:.1f}h"
            )
        else:
            if result["status"] == "filtered_out":
                skip_count += 1
            else:
                error_count += 1
                logger.warning(f"[{i+1:5d}] {result['status']}: {result.get('error', '')}")

    elapsed = time.time() - start_time
    logger.info(
        f"\n완료!\n"
        f"  성공: {success_count}/{num_trips}\n"
        f"  스킵: {skip_count}\n"
        f"  오류: {error_count}\n"
        f"  소요시간: {elapsed/3600:.1f}시간 ({elapsed/60:.0f}분)"
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="더미 데이터 배치 생성 (옵션 1: 빠른 처리)")
    parser.add_argument("--trips", type=int, default=100, help="생성할 trip 수")
    parser.add_argument(
        "--road-network",
        type=str,
        default=str(Path(__file__).parent.parent / "producer" / "road_network_863.csv"),
        help="도로망 CSV 경로",
    )
    parser.add_argument("--test", action="store_true", help="테스트 모드 (데이터 생성만, S3 저장 X)")

    args = parser.parse_args()

    links = load_road_network(args.road_network)

    if args.test:
        # 테스트 모드: 데이터 생성만 확인
        logger.info("=== 테스트 모드: 데이터 생성 확인 ===")
        vehicle_id = f"v001"
        route = pick_route(links)
        trip_json = generate_trip(vehicle_id, route, points_per_seg=10)

        logger.info(f"Trip ID: {trip_json['trip_id']}")
        logger.info(f"Vehicle ID: {trip_json['vin']}")
        logger.info(f"Start Time: {trip_json['start_time']}")
        logger.info(f"Records 수: {len(trip_json['records'])}")
        logger.info(f"Trip JSON 크기: {len(json.dumps(trip_json)) / 1024:.2f} KB")

        # 첫 번째 record 샘플
        if trip_json['records']:
            logger.info(f"\n첫 번째 record 샘플:")
            logger.info(json.dumps(trip_json['records'][0], indent=2))

        logger.info("\n✓ 데이터 생성 성공!")

    else:
        # 정상 모드: S3 업로드
        batch_generate_and_upload(args.trips, links)
