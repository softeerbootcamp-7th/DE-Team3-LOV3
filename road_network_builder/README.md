# Road Network Builder

도로 네트워크 데이터를 일정 간격의 세그먼트로 분할하여
포트홀 탐지 파이프라인에서 사용할 수 있는 형태로 변환 후 저장하는 모듈입니다.

## 원본 데이터 준비

1. [ITS 국가교통정보센터](https://www.its.go.kr/nodelink/nodelinkRef)에서
   **전국 표준노드링크** 데이터를 다운로드합니다
2. 다운받은 shapefile을 `data/` 폴더에 넣습니다.

## 사용법

### 1. 설치
```bash
pip install -r requirements.txt
```

### 2. 설정
```bash
cp config.yaml.example config.yaml
```

`config.yaml`을 열어 자신의 환경에 맞게 수정합니다.

| 항목 | 설명 | 기본값 |
|------|------|--------|
| `source.path` | shapefile 경로 | `data/MOCT_LINK.shp` |
| `filter.column` | 필터링 컬럼명 | `ROAD_NO` |
| `filter.value` | 필터링 값 (노선번호) | `863` |
| `segment.length_meters` | 세그먼트 길이 (m) | `50` |
| `output.path` | 출력 parquet 경로 | `output/road_segments.parquet` |

### 3. 실행
```bash
python build_segments.py
```

### 4. 결과

`output/road_segments.csv`이 생성됩니다.

| 컬럼 | 설명 |
|------|------|
| `s_id` | 세그먼트 ID (`{링크ID}_{번호}`) |
| `link_id` | 원본 링크 ID |
| `start_lon`, `start_lat` | 세그먼트 시작점 (WGS84) |
| `end_lon`, `end_lat` | 세그먼트 끝점 (WGS84) |

이 파일을 detection-service의 S3 road network 경로에 업로드하면 됩니다.

## 다른 도로에 적용하기

`config.yaml`의 `filter` 설정만 바꾸면 어떤 도로든 적용 가능합니다.
```yaml
# 예: 국도 1호선
filter:
  column: "ROAD_NO"
  value: "1"

# 예: 도로 등급으로 필터링
filter:
  column: "ROAD_RANK"
  value: "107"        # 지방도
```

## 처리 흐름
```
원본 shapefile (EPSG:5179/5186)
  → 노선번호 필터링
  → 미터 좌표계(EPSG:5186)로 변환
  → 50m 단위로 LineString 분할
  → WGS84(EPSG:4326)로 변환
  → start/end 좌표 추출
  → csv 저장
```
