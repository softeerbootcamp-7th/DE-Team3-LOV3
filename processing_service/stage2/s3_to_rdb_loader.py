import yaml
import pymysql
import logging
import traceback
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class S3ToRDBLoader:
    """
    S3에 저장된 집계 데이터(Parquet)를 읽어 MySQL RDB에 Upsert 방식으로 적재하는 로더.
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
            .appName("S3ToRDBLoader") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()

        self.logger = self.spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)

    def load_from_s3(self):
        """S3에서 집계된 Parquet 데이터 로드."""
        s3_path = self.config['output']['s3_path']
        self.logger.info(f"Loading aggregated data from S3: {s3_path}")
        
        try:
            self.agg_df = self.spark.read.parquet(s3_path)
            # 파티션 컬럼(date)이 문자열로 인식될 수 있으므로 필요시 변환, 여기서는 스키마 자동 추론 의존
        except Exception as e:
            self.logger.error(f"Failed to load data from S3: {e}")
            raise e

    def save_to_rdb_upsert(self):
        """MySQL에 Bulk Upsert 수행."""
        if not hasattr(self, 'agg_df'):
            self.logger.warning("No data loaded. Skipping DB write.")
            return

        db_conf = self.config['rdb']
        
        # 파티션(Task)별로 DB 연결 및 쓰기 수행
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

                # SQL 쿼리 (Upsert)
                # s_id와 date가 복합키(Unique Key)라고 가정하거나 s_id가 PK인 경우에 맞게 조정 필요
                # 여기서는 기존 로직대로 구현
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
                        row.date, # Date type or String
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
                # Worker 노드에서의 출력을 위해 print 사용 (Executor stdout)
                import traceback
                print(f"Error writing to DB in partition. Error: {e}\n{traceback.format_exc()}")
                raise e
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()

        self.logger.info("Saving counts to EC2 MySQL...")
        
        # 빈 데이터프레임 체크
        if self.agg_df.rdd.isEmpty():
             self.logger.info("Dataframe is empty. Nothing to write.")
             return

        self.agg_df.foreachPartition(write_partition)
        self.logger.info("Finished saving to MySQL.")

    def run(self):
        self.logger.info("Starting S3 to RDB Loader Job")
        self.load_from_s3()
        
        self.logger.info(f"Loaded records count: {self.agg_df.count()}")
        
        self.save_to_rdb_upsert()
        self.spark.stop()


if __name__ == "__main__":
    loader = S3ToRDBLoader()
    loader.run()
