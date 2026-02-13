"""
Stage 1: Anomaly Detection - 비즈니스 로직

원시 센서 데이터를 읽어 컨텍스트 필터링 → 트립별 Z-score 정규화 → 가중 합으로 임팩트 스코어를 계산하고,
임계값을 초과한 이벤트만 추려 parquet로 저장하는 일 배치 단계입니다.

엔트리 포인트는 `run_job(spark, config, input_base_path, output_base_path, batch_date)` 이며,
`config_local.yaml` / `config_prod.yaml`의 설정에 따라 입출력 경로와 Spark 튜닝 옵션이 결정됩니다.
"""

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, LongType,
)
from pyspark.sql.utils import AnalysisException
from typing import Dict, Any
from datetime import date, timedelta
import os
import logging

logger = logging.getLogger(__name__)

STAGE1_OUTPUT_COLUMNS = [
    "timestamp", "trip_id", "vehicle_id",
    "accel_x", "accel_y", "accel_z",
    "gyro_x", "gyro_y", "gyro_z",
    "velocity", "lon", "lat", "hdop", "satellites",
    "nor_accel_z", "nor_gyro_y", "impact_score",
]


def get_input_schema() -> StructType:
    return StructType([
        StructField("timestamp", LongType(), False),
        StructField("trip_id", StringType(), False),
        StructField("vehicle_id", StringType(), False),
        StructField("accel_x", DoubleType(), False),
        StructField("accel_y", DoubleType(), False),
        StructField("accel_z", DoubleType(), False),
        StructField("gyro_x", DoubleType(), False),
        StructField("gyro_y", DoubleType(), False),
        StructField("gyro_z", DoubleType(), False),
        StructField("velocity", DoubleType(), False),
        StructField("lon", DoubleType(), False),
        StructField("lat", DoubleType(), False),
        StructField("hdop", DoubleType(), False),
        StructField("satellites", IntegerType(), False),
    ])


def _batch_date(batch_date_str: str = None) -> str:
    if batch_date_str and batch_date_str.strip():
        return batch_date_str.strip()
    env_date = os.getenv("BATCH_DATE", "").strip()
    if env_date:
        return env_date
    return (date.today() - timedelta(days=1)).isoformat()


class AnomalyDetectionPipeline:
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.config_broadcast = spark.sparkContext.broadcast(config)
        self._validate_config()

    def _validate_config(self) -> None:
        """설정 유효성 검증 — 파이프라인 시작 전 KeyError 방지"""
        cfg = self.config_broadcast.value
        required_keys = {
            "context_filtering": ["velocity_threshold", "hdop_threshold", "min_satellites"],
            "impact_score": ["weights", "threshold"],
        }
        for section, keys in required_keys.items():
            if section not in cfg:
                raise ValueError(f"필수 설정 섹션 누락: {section}")
            for key in keys:
                if key not in cfg[section]:
                    raise ValueError(f"필수 설정 키 누락: {section}.{key}")
        if "accel_z" not in cfg["impact_score"].get("weights", {}):
            raise ValueError("필수 가중치 누락: impact_score.weights.accel_z")
        if "gyro_y" not in cfg["impact_score"].get("weights", {}):
            raise ValueError("필수 가중치 누락: impact_score.weights.gyro_y")

    def run(self, input_df: DataFrame) -> DataFrame:
        filtered_df = self._context_filtering(input_df)
        # 빈 DataFrame 감지 및 경고
        if filtered_df.rdd.isEmpty():
            logger.warning("컨텍스트 필터링 후 데이터 없음 — 이상 징후 없이 빈 결과 반환")
        normalized_df = self._z_score_normalization(filtered_df)
        result_df = self._calculate_impact_score(normalized_df)
        # 최종 빈 DataFrame 감지
        if result_df.rdd.isEmpty():
            logger.warning("임팩트 스코어 필터링 후 데이터 없음")
        return result_df

    def _context_filtering(self, df: DataFrame) -> DataFrame:
        cfg = self.config_broadcast.value.get("context_filtering", {})
        v_th = cfg.get("velocity_threshold")
        h_th = cfg.get("hdop_threshold")
        s_min = cfg.get("min_satellites")

        if v_th is None or h_th is None or s_min is None:
            raise ValueError(
                "context_filtering 설정 키 누락: velocity_threshold, hdop_threshold, min_satellites"
            )

        return df.filter(
            (F.col("velocity") > v_th) &
            (F.col("hdop") <= h_th) &
            (F.col("satellites") >= s_min)
        )

    def _z_score_normalization(self, df: DataFrame) -> DataFrame:
        """Window 셔플을 1회로 줄이기 위해 withColumn 체이닝 사용"""
        trip_window = Window.partitionBy("trip_id")
        mean_accel_z = F.avg(F.col("accel_z")).over(trip_window)
        std_accel_z = F.stddev(F.col("accel_z")).over(trip_window)
        mean_gyro_y = F.avg(F.col("gyro_y")).over(trip_window)
        std_gyro_y = F.stddev(F.col("gyro_y")).over(trip_window)

        df = df.withColumn(
            "nor_accel_z",
            F.when(std_accel_z.isNull() | (std_accel_z == 0), F.lit(0.0)).otherwise(
                (F.col("accel_z") - mean_accel_z) / std_accel_z
            ),
        ).withColumn(
            "nor_gyro_y",
            F.when(std_gyro_y.isNull() | (std_gyro_y == 0), F.lit(0.0)).otherwise(
                (F.col("gyro_y") - mean_gyro_y) / std_gyro_y
            ),
        )
        return df

    def _calculate_impact_score(self, df: DataFrame) -> DataFrame:
        cfg = self.config_broadcast.value.get("impact_score", {})
        weights = cfg.get("weights", {})
        w1 = weights.get("accel_z")
        w2 = weights.get("gyro_y")
        threshold = cfg.get("threshold")

        if w1 is None or w2 is None or threshold is None:
            raise ValueError(
                "impact_score 설정 키 누락: weights.accel_z, weights.gyro_y, threshold"
            )

        scored = df.withColumn(
            "impact_score",
            (F.abs(F.col("nor_accel_z")) * F.lit(w1)) + (F.abs(F.col("nor_gyro_y")) * F.lit(w2)),
        )
        return scored.filter(F.col("impact_score") > threshold)


def run_job(
    spark: SparkSession,
    config: Dict[str, Any],
    input_base_path: str,
    output_base_path: str,
    batch_date: str = None,
) -> None:
    """
    Stage 1 일 배치 실행: 입력 읽기 → 파이프라인 → 출력 저장

    개선 사항:
    - 입력 경로 존재 여부 체크 및 예외 처리
    - Window 셔플을 1회로 감소
    - coalesce 대신 repartition으로 균등 분배 후 적용
    - cache 효율성 개선 (write 전에만 cache, 최종 count는 제거)
    - 빈 DataFrame 감시
    """
    batch_dt = _batch_date(batch_date)
    input_base_path = input_base_path.rstrip("/")
    output_base_path = output_base_path.rstrip("/")
    partition_input = f"{input_base_path}/dt={batch_dt}"
    partition_output = f"{output_base_path}/dt={batch_dt}"

    logger.info("배치 날짜: %s", batch_dt)
    logger.info("입력: %s", partition_input)
    logger.info("출력: %s", partition_output)

    # 1. 입력 경로 존재 여부 체크
    try:
        input_df = spark.read.schema(get_input_schema()).parquet(partition_input)
    except AnalysisException as e:
        logger.error("입력 경로 읽기 실패 (파일 없음?): %s — 배치 스킵", partition_input)
        raise RuntimeError(f"입력 데이터 없음: {partition_input}") from e

    # 2. config 유효성 검사
    try:
        pipeline = AnomalyDetectionPipeline(spark, config)
    except ValueError as e:
        logger.error("파이프라인 설정 오류: %s", str(e))
        raise

    # 3. 파이프라인 실행
    result_df = pipeline.run(input_df)
    result_df = result_df.select(STAGE1_OUTPUT_COLUMNS)

    # 4. 출력 파티션 수 최적화
    # (로컬: 파일 개수 줄이기 / 프로드: 균등 분배 후 파티션 감소로 데이터 스큐 완화)
    spark_config = config.get("spark", {})
    target_partitions = spark_config.get("coalesce_partitions", 16)
    current_partitions = result_df.rdd.getNumPartitions()

    if current_partitions > target_partitions:
        # repartition: 셔플을 통해 데이터 균등 분배
        result_df = result_df.repartition(target_partitions)
        # coalesce: 파티션을 줄이되 셔플 없이 인접 파티션 병합
        result_df = result_df.coalesce(target_partitions)
        logger.info("출력 파티션 조정: %d → %d", current_partitions, target_partitions)

    # 5. write 전에만 cache (write와 count 모두 활용)
    result_df = result_df.cache()
    result_df.write.mode("overwrite").option("compression", "snappy").parquet(partition_output)
    logger.info("출력 저장 완료: %s", partition_output)

    # 6. 최종 통계 (write 후 cache에서 읽음)
    final_count = result_df.count()
    result_df.unpersist()

    # 7. 로그: 빈 결과 경고
    if final_count == 0:
        logger.warning("최종 결과가 비어 있음 — 이상 징후 없이 출력됨")
    else:
        logger.info("완료: 출력 %d 레코드 저장", final_count)


if __name__ == "__main__":
    import argparse
    import sys

    # stage1 디렉터리를 sys.path에 추가 (connection.py import용)
    _stage1_dir = os.path.dirname(os.path.abspath(__file__))
    if _stage1_dir not in sys.path:
        sys.path.insert(0, _stage1_dir)

    from connection import load_config, get_spark_session

    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

    parser = argparse.ArgumentParser(description="Stage 1: Anomaly Detection")
    parser.add_argument("--env", required=True, choices=["local", "prod"])
    parser.add_argument("--batch-date", default=None, help="YYYY-MM-DD")
    args = parser.parse_args()

    # config 파일은 stage1 안에 있음 (config_local.yaml, config_prod.yaml)
    config_path = os.path.join(_stage1_dir, f"config_{args.env}.yaml")
    if not os.path.isfile(config_path):
        logger.error("설정 파일 없음: %s", config_path)
        sys.exit(1)

    config = load_config(config_path)
    storage = config.get("storage", {})
    input_base = storage.get("input_base_path", "./data/raw-sensor-data")
    output_base = storage.get("output_base_path", "./data/stage1_anomaly_detected")

    spark = get_spark_session(config)
    try:
        run_job(spark, config, input_base, output_base, args.batch_date)
    finally:
        spark.stop()
