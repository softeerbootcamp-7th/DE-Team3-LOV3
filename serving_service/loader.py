"""
Layer 3: Serving - S3 Parquet → PostgreSQL Loader

Stage 2 Spark 잡이 생성한 parquet 파일을 S3에서 읽어
PostgreSQL pothole_segments 테이블에 적재하는 모듈.

Airflow 사용 예시:
    from datetime import datetime
    from airflow.operators.python import PythonOperator
    from serving_service.loader import load_stage2_results, PotholeLoader

    # Task 1: 데이터 적재
    def load_data(**context):
        ds = context["ds"]  # YYYY-MM-DD
        s3_path = f"s3://bucket/stage2/pothole_segments/dt={ds}/"
        load_stage2_results(s3_path, ds, config_path="serving_service/config.yaml")

    load_task = PythonOperator(
        task_id="load_result",
        python_callable=load_data,
        provide_context=True,
        dag=my_dag,
    )

    # Task 2: MV 갱신
    def refresh_views(**context):
        loader = PotholeLoader("serving_service/config.yaml")
        try:
            loader.refresh_materialized_views()
        finally:
            loader.close()

    refresh_task = PythonOperator(
        task_id="refresh_materialized_views",
        python_callable=refresh_views,
        dag=my_dag,
    )

    load_task >> refresh_task
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import yaml


class PotholeLoader:
    """S3 parquet → PostgreSQL 적재 엔진"""

    def __init__(self, config_path="config.yaml"):
        """설정 로드 및 DB 연결 초기화"""
        self.logger = self._setup_logger()
        self.config = self._load_config(config_path)
        self.db_engine = self._create_db_connection()

    def _load_config(self, config_path):
        """YAML 설정 파일 로드 (환경 변수 치환)"""
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                # 환경 변수 치환 (${VAR_NAME} → 환경 변수 값)
                config_content = os.path.expandvars(f.read())
                config = yaml.safe_load(config_content)
            self.logger.info(f"Config loaded from {config_path}")
            return config
        except FileNotFoundError:
            self.logger.error(f"Config file not found: {config_path}")
            sys.exit(1)
        except yaml.YAMLError as e:
            self.logger.error(f"YAML parse error: {e}")
            sys.exit(1)

    def _setup_logger(self):
        """로깅 설정"""
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "[%(asctime)s] %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    def _create_db_connection(self):
        """PostgreSQL 연결 생성 (SQLAlchemy)"""
        try:
            pg_config = self.config["postgres"]
            connection_string = (
                f"postgresql://{pg_config['user']}:{pg_config['password']}"
                f"@{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
            )
            engine = create_engine(connection_string, echo=False)

            # 연결 테스트
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            self.logger.info("PostgreSQL connection established")
            return engine
        except Exception as e:
            self.logger.error(f"DB connection failed: {e}")
            raise

    def load_from_s3(self, s3_path, execution_date):
        """
        S3에서 Stage 2 결과 parquet 읽기 및 적재

        Args:
            s3_path: S3 parquet 파일 경로 (s3://bucket/path/)
            execution_date: Airflow 실행 날짜 (YYYY-MM-DD)

        Note:
            AWS credentials는 IAM Role 또는 환경 변수에서 자동 로드됨
        """
        try:
            self.logger.info(f"Loading parquet from S3: {s3_path}")

            # S3 경로 파싱
            s3_uri = s3_path.rstrip('/')
            if s3_uri.startswith('s3://'):
                s3_uri = s3_uri[5:]
            bucket, key = s3_uri.split('/', 1)

            # boto3로 S3 접근 (IAM Role 또는 환경 변수에서 credentials 자동 로드)
            s3_client = boto3.client('s3')

            # S3에서 parquet 파일 목록 가져오기
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)

            if 'Contents' not in response:
                raise FileNotFoundError(f"No files found in S3: {s3_path}")

            # parquet 파일 찾기
            parquet_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]

            if not parquet_files:
                raise FileNotFoundError(f"No parquet files found in S3: {s3_path}")

            # 모든 parquet 파일을 읽어서 하나의 DataFrame으로 병합
            df_list = []
            for parquet_key in parquet_files:
                self.logger.info(f"Reading parquet file: s3://{bucket}/{parquet_key}")
                obj = s3_client.get_object(Bucket=bucket, Key=parquet_key)
                df_part = pd.read_parquet(obj['Body'])
                df_list.append(df_part)

            if not df_list:
                raise IOError(f"Failed to read parquet files from: {s3_path}")

            df = pd.concat(df_list, ignore_index=True)

            self.logger.info(f"Loaded {len(df)} records from S3 ({len(parquet_files)} files)")

            # date 컬럼 추가
            df["date"] = execution_date

            # 데이터 검증
            self._validate_dataframe(df)

            # PostgreSQL 적재
            self._insert_to_postgresql(df)

            self.logger.info("Data successfully loaded to PostgreSQL")
            return len(df)

        except Exception as e:
            self.logger.error(f"S3 load failed: {e}")
            raise

    def _validate_dataframe(self, df):
        """DataFrame 스키마 및 데이터 검증"""
        required_columns = {"s_id", "centroid_lon", "centroid_lat", "impact_count", "date"}

        if not required_columns.issubset(df.columns):
            missing = required_columns - set(df.columns)
            raise ValueError(f"Missing columns: {missing}")

        # NULL 체크
        null_counts = df.isnull().sum()
        if null_counts.sum() > 0:
            self.logger.warning(f"Null values detected:\n{null_counts[null_counts > 0]}")

        # 데이터 타입 변환
        df["s_id"] = df["s_id"].astype(str)
        df["centroid_lon"] = pd.to_numeric(df["centroid_lon"], errors="coerce")
        df["centroid_lat"] = pd.to_numeric(df["centroid_lat"], errors="coerce")
        df["impact_count"] = df["impact_count"].astype(int)
        df["date"] = pd.to_datetime(df["date"]).dt.date

        self.logger.info("DataFrame validation passed")

    def _insert_to_postgresql(self, df):
        """PostgreSQL에 데이터 삽입"""
        try:
            batch_size = self.config["postgres"].get("batch_size", 1000)

            df.to_sql(
                "pothole_segments",
                self.db_engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=batch_size
            )

            self.logger.info(f"Inserted {len(df)} rows (batch_size: {batch_size})")

        except Exception as e:
            self.logger.error(f"Insert failed: {e}")
            raise

    def refresh_materialized_views(self):
        """Materialized View 갱신 (동시 갱신 활성화)"""
        try:
            views = [
                "mv_pothole_context",
                "mv_daily_summary",
                "mv_repair_priority",
                "mv_segment_trend"
            ]

            self.logger.info("Refreshing materialized views...")

            with self.db_engine.connect() as conn:
                for view in views:
                    try:
                        # CONCURRENTLY 옵션으로 읽기 블로킹 최소화
                        refresh_sql = f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view};"
                        conn.execute(text(refresh_sql))
                        self.logger.info(f"Refreshed {view}")
                    except Exception as e:
                        self.logger.warning(f"Failed to refresh {view}: {e}")

                conn.commit()

            self.logger.info("All materialized views refreshed")

        except Exception as e:
            self.logger.error(f"MV refresh failed: {e}")
            raise

    def close(self):
        """DB 연결 종료"""
        if self.db_engine:
            self.db_engine.dispose()
            self.logger.info("Database connection closed")


def load_stage2_results(s3_path, execution_date, config_path="config.yaml"):
    """
    Airflow DAG에서 호출하는 메인 함수

    Args:
        s3_path: S3 parquet 경로
        execution_date: 실행 날짜 (YYYY-MM-DD)
        config_path: 설정 파일 경로
    """
    loader = PotholeLoader(config_path)
    try:
        loader.load_from_s3(s3_path, execution_date)
        loader.refresh_materialized_views()
    finally:
        loader.close()


if __name__ == "__main__":
    # 로컬 테스트용
    import argparse

    parser = argparse.ArgumentParser(description="Load Stage 2 results to PostgreSQL")
    parser.add_argument("--s3-path", required=True, help="S3 parquet path")
    parser.add_argument("--date", required=True, help="Execution date (YYYY-MM-DD)")
    parser.add_argument("--config", default="config.yaml", help="Config file path")

    args = parser.parse_args()
    load_stage2_results(args.s3_path, args.date, args.config)
