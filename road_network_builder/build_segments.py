"""
Road Network Segment Builder

ITS 표준 노드링크 shapefile을 읽어 지정된 도로를 필터링하고,
일정 간격(기본 50m)으로 세그먼트를 분할하여 csv로 출력합니다.

사용법:
    python build_segments.py                  # config.yaml 사용
    python build_segments.py custom.yaml      # 커스텀 설정 파일 사용

출력 스키마:
    s_id       - 세그먼트 ID ({링크ID}_{번호})
    link_id    - 원본 링크 ID
    start_lon  - 세그먼트 시작점 경도 (WGS84)
    start_lat  - 세그먼트 시작점 위도 (WGS84)
    end_lon    - 세그먼트 끝점 경도 (WGS84)
    end_lat    - 세그먼트 끝점 위도 (WGS84)
"""

import geopandas as gpd
from shapely.ops import substring
import yaml
import os
import sys


def load_config(path="config.yaml"):
    """설정 파일(YAML)을 로드한다.

    Args:
        path: config 파일 경로. 기본값은 "config.yaml".

    Returns:
        dict: 파싱된 설정 딕셔너리.
    """
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_and_filter(source_config, filter_config):
    """원본 shapefile을 로드하고 지정된 조건으로 필터링한다.

    Args:
        source_config: source 설정 (path, encoding).
        filter_config: filter 설정 (column, value).

    Returns:
        GeoDataFrame: 필터링된 도로 링크 데이터.

    Raises:
        SystemExit: 필터링 결과가 0건일 경우.
    """
    links = gpd.read_file(
        source_config["path"],
        encoding=source_config.get("encoding", "utf-8")
    )
    filtered = links[
        links[filter_config["column"]].astype(str) == str(filter_config["value"])
    ].copy()

    print(f"필터링 결과 링크 수: {len(filtered)}")

    if len(filtered) == 0:
        print("필터링 결과가 0건입니다. config.yaml의 filter 설정을 확인하세요.")
        sys.exit(1)

    return filtered


def split_segments(gdf, segment_length, source_crs):
    """도로 링크를 일정 간격으로 분할하여 세그먼트를 생성한다.

    각 링크의 LineString을 미터 좌표계에서 segment_length 간격으로 잘라
    개별 세그먼트로 만든다. 마지막 세그먼트는 segment_length보다 짧을 수 있다.

    Args:
        gdf: 필터링된 도로 링크 GeoDataFrame.
        segment_length: 세그먼트 길이 (미터).
        source_crs: 미터 단위 좌표계 (예: "EPSG:5186").

    Returns:
        GeoDataFrame: 분할된 세그먼트 (source_crs 좌표계).
    """
    gdf_m = gdf.to_crs(source_crs)
    segments = []

    for _, link in gdf_m.iterrows():
        line = link.geometry
        total_length = line.length
        link_id = link["LINK_ID"]
        offset = 0
        seg_idx = 0

        while offset < total_length:
            end = min(offset + segment_length, total_length)
            sub_line = substring(line, offset, end)

            segments.append({
                "s_id": f"{link_id}_{seg_idx:04d}",
                "link_id": link_id,
                "geometry": sub_line,
            })

            offset = end
            seg_idx += 1

    return gpd.GeoDataFrame(segments, crs=source_crs)


def extract_coordinates(gdf, output_crs):
    """세그먼트 GeoDataFrame을 WGS84로 변환하고 시작/끝 좌표를 추출한다.

    Args:
        gdf: 세그먼트 GeoDataFrame (미터 좌표계).
        output_crs: 출력 좌표계 (예: "EPSG:4326").

    Returns:
        DataFrame: s_id, link_id, start_lon/lat, end_lon/lat 컬럼.
    """
    gdf = gdf.to_crs(output_crs)

    gdf["start_lon"] = gdf.geometry.apply(lambda g: round(g.coords[0][0], 6))
    gdf["start_lat"] = gdf.geometry.apply(lambda g: round(g.coords[0][1], 6))
    gdf["end_lon"] = gdf.geometry.apply(lambda g: round(g.coords[-1][0], 6))
    gdf["end_lat"] = gdf.geometry.apply(lambda g: round(g.coords[-1][1], 6))

    return gdf[["s_id", "link_id", "start_lon", "start_lat", "end_lon", "end_lat"]]


def save(df, output_path):
    """결과를 csv
     파일로 저장한다.

    Args:
        df: 저장할 DataFrame.
        output_path: 출력 파일 경로.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)


def build(config):
    """전체 빌드 파이프라인을 실행한다.

    shapefile 로드 → 필터링 → 세그먼트 분할 → 좌표 추출 → 저장.

    Args:
        config: load_config()로 로드된 설정 딕셔너리.
    """
    seg_cfg = config["segment"]
    out_cfg = config["output"]

    # 로드 및 필터링
    filtered = load_and_filter(config["source"], config["filter"])

    # 세그먼트 분할
    segments = split_segments(filtered, seg_cfg["length_meters"], seg_cfg["source_crs"])

    # 좌표 추출
    result = extract_coordinates(segments, seg_cfg["output_crs"])

    # 저장
    save(result, out_cfg["path"])

    print(f"총 세그먼트 수: {len(result)}")
    print(result.head(10).to_string())
    print(f"저장 완료: {out_cfg['path']}")


if __name__ == "__main__":
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    config = load_config(config_path)
    build(config)