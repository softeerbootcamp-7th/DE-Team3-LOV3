"""
Loaders - 데이터 적재 모듈

기본 클래스와 구체적 구현을 제공.
"""

from .base_loader import BaseLoader
from .pothole_loader import PotholeLoader, load_stage2_results

__all__ = ["BaseLoader", "PotholeLoader", "load_stage2_results"]