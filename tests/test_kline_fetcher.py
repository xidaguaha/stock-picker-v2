# -*- coding: utf-8 -*-
"""
K线获取器单元测试
=================
覆盖: 降级链、字段标准化、异常回退
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "共用模块"))

from kline_fetcher import KlineFetcher


class TestKlineFetcherInit:
    """初始化测试"""

    def test_default_cache_dir(self):
        """默认缓存目录"""
        kf = KlineFetcher()
        assert kf.cache_dir is not None
        assert kf.cache_dir.exists()

    def test_custom_cache_dir(self):
        """自定义缓存目录"""
        custom = Path(__file__).parent / "_test_kline_cache"
        kf = KlineFetcher(cache_dir=custom)
        assert kf.cache_dir == custom
        assert kf.cache_dir.exists()
        # 清理
        import shutil
        shutil.rmtree(custom, ignore_errors=True)


class TestFieldStandardization:
    """字段标准化测试"""

    def test_required_columns(self):
        """标准化输出必须包含的字段"""
        kf = KlineFetcher()
        # 构造一个模拟的原始DataFrame
        raw = pd.DataFrame({
            "date": ["2026-01-01", "2026-01-02"],
            "open": [10.0, 11.0],
            "high": [12.0, 13.0],
            "low": [9.0, 10.0],
            "close": [11.0, 12.0],
            "volume": [10000, 20000],
            "amount": [110000, 240000],
        })
        std = kf._standardize(raw)
        required = ["日期", "开盘", "最高", "最低", "收盘", "成交量", "成交额"]
        for col in required:
            assert col in std.columns, f"缺少字段: {col}"

    def test_empty_dataframe(self):
        """空DataFrame标准化"""
        kf = KlineFetcher()
        raw = pd.DataFrame()
        std = kf._standardize(raw)
        assert std.empty


class TestCredentialsLoading:
    """凭证加载测试"""

    def test_load_credentials_returns_dict(self):
        """凭证加载返回字典"""
        from kline_fetcher import _load_credentials
        creds = _load_credentials()
        assert isinstance(creds, dict)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
