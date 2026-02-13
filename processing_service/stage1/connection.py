"""
Stage 1: 설정 및 Spark 연결 관리 모듈

이 모듈은 Stage 1 Anomaly Detection 파이프라인의 초기화를 담당합니다.
YAML 설정 파일 로드 및 Spark 세션 생성, Adaptive Query Execution 최적화를 제공합니다.

Module Functions:
    load_config: YAML 설정 파일을 로컬 경로 또는 S3에서 로드하고 파싱
    get_spark_session: Spark 설정을 기반으로 SparkSession 인스턴스 생성

Example:
    >>> config = load_config("config_prod.yaml")
    >>> spark = get_spark_session(config)
    >>> input_df = spark.read.parquet("s3://bucket/data")

Supported Config Formats:
    - Local: /path/to/config.yaml
    - S3: s3://bucket-name/path/to/config.yaml

Error Handling:
    - 설정 파일 로드 실패 시 경고 로그 기록 후 빈 dict 반환
    - KeyError는 상위 호출자가 처리 (pipeline 유효성 검증)
"""

import os
import yaml
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def load_config(config_path: str) -> Dict[str, Any]:
    """
    YAML 설정 파일 로드 (로컬 또는 s3://)
    """
    path = config_path.strip()
    try:
        if path.startswith("s3://"):
            from urllib.parse import urlparse
            import boto3
            parsed = urlparse(path)
            bucket, key = parsed.netloc, parsed.path.lstrip("/")
            s3 = boto3.client("s3")
            obj = s3.get_object(Bucket=bucket, Key=key)
            config = yaml.safe_load(obj["Body"].read().decode("utf-8"))
        else:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
        logger.info("설정 로드: %s", config_path)
        return config or {}
    except Exception as e:
        logger.warning("설정 로드 실패 (%s): %s", config_path, e)
        return {}


def get_spark_session(config: Dict[str, Any]) -> SparkSession:
    """
    config의 spark 설정으로 SparkSession 생성
    """
    spark_cfg = config.get("spark", {})
    app_name = spark_cfg.get("app_name", "Stage1-AnomalyDetection")
    master = spark_cfg.get("master")

    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
        logger.info("Spark Master: %s", master)
    else:
        logger.info("Spark Master: 미설정 (클러스터 모드)")

    builder = (
        builder
        .config("spark.sql.adaptive.enabled", spark_cfg.get("adaptive_enabled", True))
        .config("spark.sql.adaptive.coalescePartitions.enabled", spark_cfg.get("adaptive_coalesce_enabled", True))
        .config("spark.sql.adaptive.skewJoin.enabled", spark_cfg.get("skew_join_enabled", True))
        .config("spark.sql.files.openCostInBytes", f"{spark_cfg.get('min_partition_size_mb', 128) * 1024 * 1024}")
        .config("spark.sql.files.maxPartitionBytes", f"{spark_cfg.get('partition_size_mb', 128) * 1024 * 1024}")
    )
    shuffle = spark_cfg.get("shuffle_partitions")
    if shuffle is not None and shuffle != "auto":
        builder = builder.config("spark.sql.shuffle.partitions", shuffle)

    spark = builder.getOrCreate()
    logger.info("SparkSession 생성 완료")
    return spark
