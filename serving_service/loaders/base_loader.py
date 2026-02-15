"""
Base Loader - 공통 로직 (DB 연결, 로깅, 설정 로드)

데이터 적재 시 필요한 공통 기능을 제공하는 기본 클래스.
"""

import logging
import os
import sys
from pathlib import Path

from sqlalchemy import create_engine, text
import yaml


class BaseLoader:
    """데이터 적재의 공통 로직을 제공하는 기본 클래스"""

    def __init__(self, config_path="config.yaml"):
        """설정 로드 및 DB 연결 초기화"""
        self.logger = self._setup_logger()
        self.config = self._load_config(config_path)
        self.db_engine = self._create_db_connection()

    def _setup_logger(self):
        """로깅 설정"""
        logger = logging.getLogger(self.__class__.__name__)
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

    def execute_query(self, query: str):
        """SQL 쿼리 실행"""
        try:
            with self.db_engine.connect() as conn:
                result = conn.execute(text(query))
                conn.commit()
                return result
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            raise

    def fetch_query(self, query: str):
        """SQL 쿼리 조회 (SELECT)"""
        try:
            with self.db_engine.connect() as conn:
                result = conn.execute(text(query))
                return result.fetchall()
        except Exception as e:
            self.logger.error(f"Query fetch failed: {e}")
            raise

    def close(self):
        """DB 연결 종료"""
        if self.db_engine:
            self.db_engine.dispose()
            self.logger.info("Database connection closed")