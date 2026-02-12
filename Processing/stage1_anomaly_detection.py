from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType
)
import yaml
import os
import logging
from typing import Dict, Any
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_input_schema() -> StructType:
    """
    명시적 입력 스키마 정의 (Schema Enforcement)
    inferSchema 사용을 피하여 로드 속도 최적화
    """
    return StructType([
        StructField("timestamp", TimestampType(), False),
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


def load_config(config_path: str = "config/config.yaml") -> Dict[str, Any]:
    """
    외부 YAML 설정 파일 로드 (Dynamic Config)
    
    Args:
        config_path: YAML 설정 파일 경로
        
    Returns:
        설정 딕셔너리
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        logger.info(f"설정 파일 로드 완료: {config_path}")
        return config
    except FileNotFoundError:
        logger.warning(f"설정 파일을 찾을 수 없습니다: {config_path}. 기본값 사용.")
        # 기본 설정 반환
        return {
            'context_filtering': {
                'velocity_threshold': 5.0,
                'hdop_threshold': 5.0,
                'min_satellites': 4
            },
            'impact_score': {
                'weights': {'accel_z': 0.7, 'gyro_y': 0.3},
                'threshold': 3.0
            },
            'spark': {
                'coalesce_partitions': 8,
                'target_file_size_mb': 256
            }
        }


class AnomalyDetectionPipeline:
    """
    Stage 1: 이상 탐지 파이프라인
    
    처리 단계:
    1. Context Filtering: 정상 주행 상태 필터링
    2. Z-Score 정규화: 필요한 센서 데이터만 정규화 (accel_z, gyro_y)
    3. Impact Score 계산: 충격 점수 산출 (가중치 기반)
    
    최적화:
    - Broadcast Variables로 설정값 공유
    - Vectorized Operations로 연산 최적화
    - 필요한 컬럼만 정규화하여 성능 향상 (repartition 제거)
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        
        # Broadcast Variable로 설정값 공유 (모든 워커에 한 번만 복사)
        self.config_broadcast = spark.sparkContext.broadcast(config)
        logger.info("설정값이 Broadcast Variable로 등록되었습니다.")
        
    def run(self, input_df: DataFrame) -> DataFrame:
        """
        전체 이상 탐지 파이프라인 실행
        
        Args:
            input_df: 원시 센서 데이터 DataFrame
            
        Returns:
            이상 탐지가 완료된 DataFrame (Output Schema 포함)
        """
        logger.info("=" * 60)
        logger.info("Stage 1: Anomaly Detection 시작")
        logger.info("=" * 60)
        
        # Step 1: Context Filtering
        filtered_df = self._context_filtering(input_df)
        
        # Step 2: Z-Score 정규화 (StandardScaler 사용)
        normalized_df = self._z_score_normalization(filtered_df)
        
        # Step 3: Impact Score 계산 및 임계값 필터링
        scored_df = self._calculate_impact_score(normalized_df)
        
        logger.info("Stage 1: Anomaly Detection 완료")
        return scored_df
    
    def _context_filtering(self, df: DataFrame) -> DataFrame:
        """
        FR-PL1-001: Context Filtering (Step 1)
        
        정상 주행 상태 데이터만 필터링:
        - 조건: velocity <= 5.0 (정지 상태) OR hdop > 5.0 OR satellites < 4 (GPS 불량)
        - 위 조건에 해당하는 데이터는 제외
        - 필터링 조건은 YAML 설정 파일에서 로드
        
        Args:
            df: 입력 DataFrame
            
        Returns:
            필터링된 DataFrame
        """
        logger.info("-" * 60)
        logger.info("Step 1: Context Filtering 시작")
        logger.info("-" * 60)
        
        # Broadcast Variable에서 설정값 로드
        config = self.config_broadcast.value
        velocity_threshold = config['context_filtering']['velocity_threshold']
        hdop_threshold = config['context_filtering']['hdop_threshold']
        min_satellites = config['context_filtering']['min_satellites']
        
        initial_count = df.count()
        logger.info(f"필터링 전 레코드 수: {initial_count:,}")
        
        # 필터링 조건 적용 (정상 주행 상태만 유지)
        filtered_df = df.filter(
            (F.col("velocity") > velocity_threshold) &      # 차량이 정지 상태가 아님
            (F.col("hdop") <= hdop_threshold) &             # GPS 정확도가 충분함
            (F.col("satellites") >= min_satellites)         # 충분한 위성 신호
        )
        
        filtered_count = filtered_df.count()
        filtered_ratio = (initial_count - filtered_count) / initial_count * 100
        
        logger.info(f"필터링 후 레코드 수: {filtered_count:,}")
        logger.info(f"필터링된 비율: {filtered_ratio:.2f}%")
        logger.info(f"필터링 조건:")
        logger.info(f"  - velocity > {velocity_threshold} km/h")
        logger.info(f"  - hdop <= {hdop_threshold}")
        logger.info(f"  - satellites >= {min_satellites}")
        logger.info("Context Filtering 완료")
        
        return filtered_df
    
    def _z_score_normalization(self, df: DataFrame) -> DataFrame:
        """
        FR-PL1-002: Z-Score 정규화 (Step 2)
        
        센서 데이터의 Z-Score 계산:
        - 정규화 대상: nor_accel_z, nor_gyro_y (Impact Score 계산에 필요한 컬럼만)
        - 정규화 방법: z_score = (x - mean) / std
        - trip_id 기준으로 평균/분산 적용 (Window Function 사용)
        - 불필요한 센서 정규화 스킵으로 성능 최적화
        - 결과 컬럼: nor_accel_z, nor_gyro_y
        
        최적화:
        - Window Function으로 Join/Drop 제거
        - 필요한 컬럼만 정규화하여 메모리 및 계산 최소화
        
        Args:
            df: 필터링된 DataFrame
            
        Returns:
            정규화된 컬럼이 추가된 DataFrame
        """
        logger.info("-" * 60)
        logger.info("Step 2: Z-Score 정규화 시작")
        logger.info("-" * 60)
        
        # Impact Score 계산에 필요한 센서 컬럼만 정규화
        required_columns = ['accel_z', 'gyro_y']
        
        # trip_id 기준 Window 생성
        trip_window = Window.partitionBy("trip_id")
        
        # Z-Score 계산: (x - mean) / std (Window Function 사용, Join 불필요)
        logger.info("Z-Score 계산 중 (Window Function으로 직접 계산)...")
        normalized_df = df
        for col in required_columns:
            mean_col = F.avg(col).over(trip_window)
            std_col = F.stddev(col).over(trip_window)
            
            normalized_df = normalized_df.withColumn(
                f"nor_{col}",
                F.when(
                    std_col.isNull() | (std_col == 0),
                    0.0  # std가 0이면 정규화 불가, 0으로 설정
                ).otherwise(
                    (F.col(col) - mean_col) / std_col
                )
            )
        
        # 샘플 데이터 로깅
        logger.info("정규화 샘플 데이터:")
        normalized_df.select(
            "vehicle_id", "timestamp",
            "accel_z", "nor_accel_z",
            "gyro_y", "nor_gyro_y"
        ).show(5, truncate=False)
        
        logger.info("Z-Score 정규화 완료")
        return normalized_df
    
    def _calculate_impact_score(self, df: DataFrame) -> DataFrame:
        """
        FR-PL1-003: Impact Score 계산 (Step 3)
        
        정규화된 센서값을 기반으로 충격 점수 계산:
        - 가중치 기반 계산: impact_score = (|nor_accel_z| * w1) + (|nor_gyro_y| * w2)
        - w1 = 0.7 (수직 가속도 가중치), w2 = 0.3 (피치 자이로 가중치)
        - 가중치는 Broadcast Variable에서 로드
        - 임계값 이상 데이터만 다음 Stage로 전달 (Stage 2 부하 감소)
        - Vectorized Operation으로 성능 최적화
        
        Args:
            df: 정규화된 DataFrame
            
        Returns:
            Impact Score가 계산되고 임계값 필터링된 DataFrame
        """
        logger.info("-" * 60)
        logger.info("Step 3: Impact Score 계산 시작")
        logger.info("-" * 60)
        
        # Broadcast Variable에서 설정값 로드
        config = self.config_broadcast.value
        w1 = config['impact_score']['weights']['accel_z']
        w2 = config['impact_score']['weights']['gyro_y']
        threshold = config['impact_score']['threshold']
        
        logger.info(f"가중치 설정: w1(accel_z)={w1}, w2(gyro_y)={w2}")
        logger.info(f"임계값 설정: threshold={threshold}")
        
        # Impact Score 계산 (Vectorized Operation)
        logger.info("Impact Score 계산 중 (Vectorized Operations)...")
        scored_df = df.withColumn(
            "impact_score",
            (F.abs(F.col("nor_accel_z")) * F.lit(w1)) + 
            (F.abs(F.col("nor_gyro_y")) * F.lit(w2))
        )
        
        # 임계값 필터링 (Stage 2의 부하를 줄임)
        logger.info(f"임계값 필터링 적용: impact_score > {threshold}")
        
        initial_count = scored_df.count()
        filtered_df = scored_df.filter(F.col("impact_score") > threshold)
        filtered_count = filtered_df.count()
        
        if initial_count > 0:
            detection_ratio = filtered_count / initial_count * 100
        else:
            detection_ratio = 0.0
        
        logger.info(f"필터링 전 레코드 수: {initial_count:,}")
        logger.info(f"임계값 이상 레코드 수: {filtered_count:,}")
        logger.info(f"이상 탐지 비율: {detection_ratio:.2f}%")
        
        if filtered_count > 0:
            # Impact Score 통계
            logger.info("Impact Score 통계:")
            filtered_df.select("impact_score").describe().show()
            
            # 상위 이상 데이터 샘플
            sample_size = self.config.get('logging', {}).get('log_sample_size', 10)
            logger.info(f"상위 이상 탐지 샘플 (Impact Score 기준, 상위 {sample_size}개):")
            filtered_df.select(
                "vehicle_id", "timestamp", "lat", "lon",
                "accel_z", "nor_accel_z", "gyro_y", "nor_gyro_y", "impact_score"
            ).orderBy(F.desc("impact_score")).show(sample_size, truncate=False)
        else:
            logger.warning("임계값을 초과하는 데이터가 없습니다.")
        
        logger.info("Impact Score 계산 완료")
        return filtered_df


def calculate_optimal_partitions(data_size_gb: float, partition_size_mb: int = 128) -> int:
    """
    적정 파티션 수 계산
    
    Args:
        data_size_gb: 전체 데이터 크기 (GB)
        partition_size_mb: 목표 파티션 크기 (MB, 기본값: 128MB)
        
    Returns:
        최적 파티션 수
    """
    data_size_mb = data_size_gb * 1024
    optimal_partitions = int(data_size_mb / partition_size_mb)
    return max(optimal_partitions, 1)  # 최소 1개


def main(input_path: str = None, output_path: str = None, config_path: str = "config/config.yaml"):
    """
    메인 실행 함수
    
    Args:
        input_path: 입력 데이터 경로 (기본값: config에서 로드)
        output_path: 출력 데이터 경로 (기본값: config에서 로드)
        config_path: 설정 파일 경로
    """
    # 설정 파일 로드
    config = load_config(config_path)
    
    # Spark 설정
    spark_config = config.get('spark', {})
    
    # Spark 세션 생성 (최적화 설정 적용)
    spark_builder = SparkSession.builder \
        .appName("Stage1-AnomalyDetection") \
        .config("spark.sql.adaptive.enabled", spark_config.get('adaptive_enabled', True)) \
        .config("spark.sql.adaptive.coalescePartitions.enabled", spark_config.get('adaptive_coalesce_enabled', True)) \
        .config("spark.sql.adaptive.skewJoin.enabled", spark_config.get('skew_join_enabled', True)) \
        .config("spark.sql.files.openCostInBytes", f"{spark_config.get('min_partition_size_mb', 128) * 1024 * 1024}") \
        .config("spark.sql.files.maxPartitionBytes", f"{spark_config.get('partition_size_mb', 128) * 1024 * 1024}")
    
    # shuffle_partitions 설정 (auto가 아닌 경우)
    if spark_config.get('shuffle_partitions', 'auto') != 'auto':
        spark_builder = spark_builder.config("spark.sql.shuffle.partitions", spark_config['shuffle_partitions'])
    
    spark = spark_builder.getOrCreate()
    
    try:
        logger.info("=" * 80)
        logger.info("Stage 1: Anomaly Detection Pipeline")
        logger.info("=" * 80)
        
        # 입력 경로 설정
        if input_path is None:
            input_path = os.getenv('S3_RAW_PATH', 'data/raw_sensor_data')
        
        logger.info(f"입력 데이터 경로: {input_path}")
        
        # 명시적 스키마로 데이터 로드 (Schema Enforcement)
        logger.info("데이터 로드 중 (Schema Enforcement 적용)...")
        input_schema = get_input_schema()
        
        # Parquet 형식 로드 (날짜 파티션 포함 시 Predicate Pushdown 활용)
        input_df = spark.read \
            .schema(input_schema) \
            .parquet(input_path)
        
        initial_count = input_df.count()
        logger.info(f"로드된 레코드 수: {initial_count:,}")
        logger.info("입력 스키마:")
        input_df.printSchema()
        
        # 이상 탐지 파이프라인 실행
        pipeline = AnomalyDetectionPipeline(spark, config)
        result_df = pipeline.run(input_df)
        
        # 출력 경로 설정
        if output_path is None:
            output_path = os.getenv('S3_STAGE1_OUTPUT_PATH', 'data/stage1_anomaly_detected')
        
        logger.info(f"결과 저장 경로: {output_path}")
        
        # Coalesce를 통한 Small Files 방지
        coalesce_partitions = spark_config.get('coalesce_partitions', 8)
        logger.info(f"파티션 병합 (Coalesce): {coalesce_partitions}개 파티션으로 병합")
        result_df = result_df.coalesce(coalesce_partitions)
        
        # 결과 저장 (날짜 파티셔닝으로 Predicate Pushdown 활용)
        logger.info("결과 저장 중...")
        
        # dt 컬럼 추가 (날짜 파티셔닝용)
        result_df = result_df.withColumn("dt", F.to_date(F.col("timestamp")))
        
        result_df.write \
            .mode("overwrite") \
            .partitionBy("dt") \
            .option("compression", "snappy") \
            .parquet(output_path)
        
        final_count = result_df.count()
        
        logger.info("=" * 80)
        logger.info("Stage 1 파이프라인 완료")
        logger.info("=" * 80)
        logger.info(f"입력 레코드 수: {initial_count:,}")
        logger.info(f"출력 레코드 수: {final_count:,}")
        logger.info(f"필터링 비율: {(1 - final_count/initial_count)*100:.2f}%")
        logger.info(f"출력 경로: {output_path}")
        logger.info(f"파티셔닝: dt (날짜별)")
        logger.info(f"압축 형식: Snappy")
        
        # S3 Lifecycle Policy 안내
        ttl_days = config.get('storage', {}).get('ttl_days', 14)
        logger.info(f"주의: S3 Lifecycle Policy를 통해 {ttl_days}일 후 자동 삭제 설정 권장")
        
    except Exception as e:
        logger.error(f"파이프라인 실행 중 오류 발생: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()
        logger.info("Spark 세션 종료")


if __name__ == "__main__":
    import sys
    
    # CLI 인자 파싱
    input_path = sys.argv[1] if len(sys.argv) > 1 else None
    output_path = sys.argv[2] if len(sys.argv) > 2 else None
    config_path = sys.argv[3] if len(sys.argv) > 3 else "config/config.yaml"
    
    main(input_path, output_path, config_path)
