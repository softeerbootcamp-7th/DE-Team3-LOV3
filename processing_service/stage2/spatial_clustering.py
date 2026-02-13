import yaml
import pymysql
import pandas as pd
import logging
import traceback
from scipy.spatial import cKDTree
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType


class PotholeSegmentProcessor:
    """
    S3 센서 데이터를 도로 세그먼트에 매핑하고, 단순 발생 횟수를 집계하여
    EC2 MySQL에 저장하는 최적화된 프로세서.

    Note:
        Input 데이터는 이미 필터링된(충격이 있는) 데이터이므로,
        별도의 Score 계산 없이 행 개수(Count)만 집계합니다.
    """

    def __init__(self, config_path="config.yaml"):
        """설정 로드 및 Spark 세션 초기화."""
        # 1. Config 로드
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
        except FileNotFoundError:
            logging.critical(f"Config file not found at: {config_path}")
            raise
        except yaml.YAMLError as e:
            logging.critical(f"Error parsing config file: {config_path}, Error: {e}")
            raise

        self.spark = SparkSession.builder \
            .appName(self.config['app']['name']) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.autoBroadcastJoinThreshold",
                    self.config['processing']['join_broadcast_limit'] * 1024 * 1024) \
            .getOrCreate()

        self.logger = self.spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)

    def load_data(self):
        """S3 데이터 로드."""
        self.logger.info("Loading Data from S3...")
        self.sensor_df = self.spark.read.parquet(self.config['input']['sensor_data_path'])
        self.road_df = self.spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(self.config['input']['road_network_path'])

    def _prepare_spatial_index(self):
        """KDTree 생성 및 Broadcast (Spatial Join 준비)."""
        # Pandas로 변환 (도로 데이터가 크기에 따라 아키텍쳐 재설정)
        road_pdf = self.road_df.select("s_id", "start_lon", "start_lat", "end_lon", "end_lat").toPandas()

        # 중심점 계산
        road_pdf['mid_lon'] = (road_pdf['start_lon'] + road_pdf['end_lon']) / 2
        road_pdf['mid_lat'] = (road_pdf['start_lat'] + road_pdf['end_lat']) / 2

        # KDTree 생성
        coords = road_pdf[['mid_lon', 'mid_lat']].values
        tree = cKDTree(coords)

        # Broadcast
        self.bc_tree = self.spark.sparkContext.broadcast(tree)
        self.bc_road_ids = self.spark.sparkContext.broadcast(road_pdf['s_id'].values)

    def map_points_to_segments(self):
        """GPS 좌표를 가장 가까운 세그먼트 ID(s_id)에 매핑."""
        self._prepare_spatial_index()

        bc_tree = self.bc_tree
        bc_road_ids = self.bc_road_ids

        def find_nearest_segment(lon, lat):
            if lon is None or lat is None: return None
            # 가장 가까운 1개 점 탐색
            dist, idx = bc_tree.value.query([lon, lat], k=1)
            return str(bc_road_ids.value[idx])

        find_segment_udf = F.udf(find_nearest_segment, StringType())

        self.mapped_df = self.sensor_df.withColumn(
            "s_id",
            find_segment_udf(F.col("lon"), F.col("lat"))
        )

    def aggregate_metrics(self):
        """
        오직 세그먼트별 '발생 횟수(Count)'만 집계합니다.
        """
        # 'date' 컬럼에 코드 실행 시점의 현재 날짜를 추가
        df_with_date = self.mapped_df.withColumn("date", F.current_date())

        # 단순 카운트 및 좌표 추출
        # avg, max 등의 연산이 없어 셔플링 데이터 크기가 최소화됨
        self.result_df = df_with_date.groupBy("s_id", "date").agg(
            F.count("*").alias("impact_count"),  # 단순히 몇 건 발생했는지 카운트
            F.first("lon").alias("centroid_lon"),  # 대표 좌표 (시각화용)
            F.first("lat").alias("centroid_lat")
        )

    def save_to_rdb_upsert(self):
        """MySQL에 Bulk Upsert 수행."""
        db_conf = self.config['rdb']

        def write_partition(iterator):
            conn = None
            cursor = None
            try:
                conn = pymysql.connect(
                    host=db_conf['host'], port=db_conf['port'],
                    user=db_conf['user'], password=db_conf['password'],
                    database=db_conf['database'], charset='utf8mb4'
                )
                cursor = conn.cursor()

                # 쿼리에서도 score 관련 컬럼 제거됨
                sql = f"""
                    INSERT INTO {db_conf['table']} (s_id, centroid_lon, centroid_lat, date, impact_count)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        impact_count = impact_count + VALUES(impact_count),
                        centroid_lon = VALUES(centroid_lon),
                        centroid_lat = VALUES(centroid_lat)
                """

                batch_data = []
                for row in iterator:
                    batch_data.append((
                        str(row.s_id),
                        float(row.centroid_lon),
                        float(row.centroid_lat),
                        row.date,
                        int(row.impact_count)
                    ))

                    if len(batch_data) >= db_conf['batch_size']:
                        cursor.executemany(sql, batch_data)
                        conn.commit()
                        batch_data = []

                if batch_data:
                    cursor.executemany(sql, batch_data)
                    conn.commit()

            except Exception as e:
                # DB 쓰기 오류 발생 시, 상세한 에러 로그와 스택 트레이스를 기록
                import traceback
                print(f"Error writing to DB in partition. Error: {e}\n{traceback.format_exc()}")
                raise e
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()

        self.logger.info("Saving counts to EC2 MySQL...")
        self.result_df.foreachPartition(write_partition)

    def run(self):
        self.logger.info("Starting Light-weight Job")
        self.load_data()
        self.map_points_to_segments()
        self.aggregate_metrics()

        self.logger.info("--- Aggregated Results Preview (Top 20) ---")
        #self.result_df.show() # 테스트용 print
        #self.result_df.coalesce(1).write.option("header", "true").mode("overwrite").csv("data/output/pothole_results") # 테스트용 csv 저장
        self.logger.info(f"Total aggregated segments count: {self.result_df.count()}")

        self.save_to_rdb_upsert() # 테스트를 위해 DB 저장 비활성화
        self.spark.stop()


if __name__ == "__main__":
    processor = PotholeSegmentProcessor()
    processor.run()