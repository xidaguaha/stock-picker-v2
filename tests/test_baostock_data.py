# -*- coding: utf-8 -*-
"""
Baostock 数据接口单元测试
=========================
覆盖: 缓存读写、便捷函数、异常回退
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "共用模块"))

from baostock_data import BaostockData


class TestCache:
    """缓存层测试"""

    def setup_method(self):
        self.bd = BaostockData(cache_dir=Path(__file__).parent / "_test_cache")
        self.bd.cache_dir.mkdir(parents=True, exist_ok=True)

    def teardown_method(self):
        # 清理测试缓存
        import shutil
        if self.bd.cache_dir.exists():
            shutil.rmtree(self.bd.cache_dir, ignore_errors=True)

    def test_save_and_load_cache(self):
        """缓存保存和读取"""
        df = pd.DataFrame({
            "代码": ["000001", "600000"],
            "peTTM": [10.5, 8.2],
            "pbMRQ": [1.2, 0.9],
        })
        self.bd._save_cache("valuation", df)
        loaded = self.bd._load_cache("valuation")
        assert loaded is not None
        assert len(loaded) == 2
        assert list(loaded["代码"]) == ["000001", "600000"]

    def test_cache_miss(self):
        """缓存未命中返回 None"""
        loaded = self.bd._load_cache("nonexistent")
        assert loaded is None

    def test_cache_persistence(self):
        """缓存跨实例持久化"""
        df = pd.DataFrame({"代码": ["000001"], "行业": ["银行"]})
        self.bd._save_cache("industry", df)

        bd2 = BaostockData(cache_dir=self.bd.cache_dir)
        loaded = bd2._load_cache("industry")
        assert loaded is not None
        assert loaded.iloc[0]["行业"] == "银行"


class TestConvenienceMethods:
    """便捷函数测试（mock 数据）"""

    def test_get_pb_for_codes_empty(self):
        """空输入返回空字典"""
        bd = BaostockData()
        result = bd.get_pb_for_codes([])
        assert result == {}

    def test_get_industry_map_empty(self):
        """空输入返回空字典"""
        bd = BaostockData()
        result = bd.get_industry_map([])
        assert result == {}

    def test_get_roe_for_codes_empty(self):
        """空输入返回空字典"""
        bd = BaostockData()
        result = bd.get_roe_for_codes([])
        assert result == {}

    def test_get_cap_for_codes_empty(self):
        """空输入返回空字典"""
        bd = BaostockData()
        result = bd.get_cap_for_codes([])
        assert result == {}


class TestLogin:
    """登录管理测试"""

    def test_ensure_login_without_baostock(self):
        """未安装 baostock 时返回 False"""
        bd = BaostockData()
        # 如果 baostock 未安装，_ensure_login 应该返回 False
        try:
            import baostock as bs
            result = bd._ensure_login()
            assert isinstance(result, bool)
        except ImportError:
            # 未安装时直接返回 False
            assert bd._ensure_login() is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
