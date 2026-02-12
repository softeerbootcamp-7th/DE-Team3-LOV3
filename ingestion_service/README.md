# Ingestion Service (Layer 1)

차량 센서 데이터를 실시간으로 수집하여 대상 도로(지방도 863호선) 데이터만 필터링 후 S3에 적재한다.

## 아키텍처

```
차량 단말기 → Kinesis Data Streams → Lambda → S3 (Parquet)
```

- **Kinesis Data Streams**: 차량 데이터 수신 버퍼. 프로듀서와 컨슈머의 속도 차이를 흡수한다.
- **Lambda**: Kinesis 이벤트 소스 매핑으로 트리거. 파싱 → 필터링 → parquet 변환 → S3 비동기 적재.
- **S3**: Detection Layer(Layer 2)의 입력 저장소.

## 데이터 흐름

```
차량 JSON (1 trip = 1 JSON)
  │
  ▼ Kinesis (n개의 메세지 당 배치로 Lambda 호출)
  │
  ▼ Lambda
  │  1. 파싱: 중첩 JSON → flat row (trip_id, vehicle_id 매핑)
  │  2. 유효성 검증: 좌표 / 속도 / 가속도 범위 체크
  │  3. 필터링: 863호선 bounding box 밖 데이터 드롭
  │  4. 적재: parquet 변환 → trip_id별 S3 비동기 적재
  │
  ▼ S3 (Parquet)
     s3://{bucket_name}/raw-sensor-data/dt=YYYY-MM-DD/{trip_id}.parquet
```

## S3 적재 구조

```
s3://{bucket_name}/raw-sensor-data/
  └── dt=2025-02-12/
      ├── TRIP-20250212-v001-0001.parquet
      ├── TRIP-20250212-v002-0032.parquet
      └── ...
```

- `dt=` 날짜 파티션으로 Spark가 특정 날짜만 읽을 수 있다.
- trip_id는 파일명 + 파일 내 컬럼 양쪽에 존재한다.

## Parquet 스키마

| 컬럼 | 타입 | 설명 |
|------|------|------|
| timestamp | INT64 | 측정 시각 (epoch ms) |
| trip_id | STRING | 주행 고유 ID |
| vehicle_id | STRING | 차량 식별자 |
| accel_x | DOUBLE | X축 가속도 (m/s²) |
| accel_y | DOUBLE | Y축 가속도 |
| accel_z | DOUBLE | Z축 가속도 |
| gyro_x | DOUBLE | X축 자이로 (°/s) |
| gyro_y | DOUBLE | Y축 자이로 |
| gyro_z | DOUBLE | Z축 자이로 |
| velocity | DOUBLE | 주행 속도 (km/h) |
| lon | DOUBLE | 경도 |
| lat | DOUBLE | 위도 |
| hdop | DOUBLE | GPS 수평 정밀도 (기본값 99.0) |
| satellites | INT32 | 수신 위성 수 (기본값 0) |

## 폴더 구조

```
ingestion-service/
├── lambda/
│   ├── lambda_function.py      # Lambda 핸들러 (파싱 + 필터링 + S3 비동기 적재)
│   ├── config.py               # bounding box, S3 경로, 유효 범위 설정
│   ├── parser.py               # JSON → flat row 파싱 + 유효성 검증
│   ├── filter.py               # 도로 bounding box 필터링
│   ├── writer.py               # parquet 변환 + S3 비동기 적재
│   ├── Dockerfile              # Lambda Container Image 빌드
│   └── requirements.txt
├── producer/
│   ├── producer.py             # 테스트용 더미 데이터 프로듀서
│   └── requirements.txt
```

## 배포

### 사전 조건
- AWS CLI 설치 + `aws configure` 완료
- Docker 설치
- IAM Role 생성 (Lambda용: Kinesis 읽기 + S3 쓰기 + CloudWatch 로그)

### Lambda 배포

```bash
cd lambda

# Docker 빌드 (⚠️ --provenance=false 필수)
docker buildx build --platform linux/amd64 --provenance=false -t pothole-ingestion .

# ECR push
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=ap-northeast-2

aws ecr get-login-password --region ${REGION} | \
  docker login --username AWS --password-stdin \
  ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com

docker tag pothole-ingestion:latest \
  ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/pothole-ingestion:latest

docker push \
  ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/pothole-ingestion:latest

# Lambda 생성 (CLI 권장, 콘솔에서 만들면 CMD 빠질 수 있음)
aws lambda create-function \
  --function-name pothole-ingestion \
  --package-type Image \
  --code ImageUri=${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/pothole-ingestion:latest \
  --role arn:aws:iam::${ACCOUNT_ID}:role/pothole-lambda-role \
  --timeout 300 \
  --memory-size 512 \
  --environment "Variables={S3_BUCKET={bucket_name},S3_PREFIX=raw-sensor-data}" \
  --architectures x86_64 \
  --region ${REGION}
```

### Kinesis + 트리거 연결

1. Kinesis Data Streams 생성: `pothole-sensor-raw` (온디맨드)
2. Lambda 트리거 추가: 소스 Kinesis → 배치 사이즈 10 → 시작 위치 LATEST

## 테스트

```bash
# 프로듀서로 더미 데이터 전송 (863호선 실제 좌표 기반)
cd producer
pip install boto3
S3_STREAM_NAME="your_stream_name"
python producer.py --vehicles 3 --pothole-rate 0.3 --noise
```

### 프로듀서 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--vehicles` | 1 | 동시 주행 차량 수 |
| `--interval` | 10 | 전송 간격 (초) |
| `--pothole-rate` | 0.1 | 랜덤 포트홀 발생 확률 (0~1) |
| `--pothole-segments` | - | 특정 세그먼트에 포트홀 지정 (쉼표 구분) |
| `--noise` | false | GPS 음영, 범위 밖 좌표 포함 |
| `--road-network` | ./road_network_863.csv | 도로망 CSV 경로 |

### 프로듀서가 시뮬레이션하는 환경

- 차량별 주행 시간/거리가 다름 (log-normal 분포)
- 가속 → 순항 → 감속 → 정차 → 재가속 속도 프로파일
- 포트홀 충격: 주 충격 + 여진 (감쇠 패턴), 심각도 랜덤
- 과속방지턱 (0.5% 확률, z축 급격한 충격)
- GPS 음영 (터널 등, hdop 급증 + 위성 수 감소)
- OBD2 / DTG 단말기별 센서 노이즈 차이
- 범위 밖 좌표 (다른 도로에서 863호선 진입 시뮬레이션)

## 설정

대상 도로 추가: `lambda/config.py`의 `BBOXES`에 항목 추가

```python
BBOXES = {
    "863": {
        "min_lon": 127.52,
        "max_lon": 127.78,
        "min_lat": 34.50,
        "max_lat": 35.07,
    },
    # 도로 추가 시 여기에
}
```
