# -*- coding: utf-8 -*-
"""
回测引擎单元测试
================
覆盖: 涨停阈值、交易成本、停牌跳过、幸存者偏差估算
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import pytest

# 将项目根目录和共用模块目录加入路径
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "共用模块"))

from backtest_engine import (
    _get_limit_up_threshold,
    _calc_transaction_cost,
    _count_trading_days,
    _estimate_survivorship_bias,
)


class TestLimitUpThreshold:
    """涨停阈值动态判断测试"""

    def test_main_board(self):
        """主板 10% 涨停阈值"""
        assert _get_limit_up_threshold("600519") == pytest.approx(0.098)
        assert _get_limit_up_threshold("000001") == pytest.approx(0.098)
        assert _get_limit_up_threshold("601318") == pytest.approx(0.098)

    def test_kc_board(self):
        """科创板 20% 涨停阈值"""
        assert _get_limit_up_threshold("688981") == pytest.approx(0.198)
        assert _get_limit_up_threshold("688008") == pytest.approx(0.198)

    def test_cy_board(self):
        """创业板 20% 涨停阈值"""
        assert _get_limit_up_threshold("300750") == pytest.approx(0.198)
        assert _get_limit_up_threshold("301000") == pytest.approx(0.198)

    def test_bse_board(self):
        """北交所 30% 涨停阈值"""
        assert _get_limit_up_threshold("830000") == pytest.approx(0.298)
        assert _get_limit_up_threshold("430000") == pytest.approx(0.298)

    def test_string_input(self):
        """输入前导零和字符串兼容性"""
        assert _get_limit_up_threshold("000001") == pytest.approx(0.098)
        assert _get_limit_up_threshold("688981") == pytest.approx(0.198)


class TestTransactionCost:
    """交易成本计算测试"""

    def test_normal_stock(self):
        """普通股往返交易成本"""
        cost = _calc_transaction_cost(buy_price=100.0, sell_price=105.0, is_limit_up=False)
        # 买入: 佣金0.025% + 过户费0.001% + 滑点0.1% = 0.126%
        # 卖出: 佣金0.025% + 印花税0.05% + 过户费0.001% + 滑点0.1% = 0.176%
        # 总成本 = 0.00126 + 0.00176 * (105/100) = 0.00126 + 0.001848 = 0.003108
        assert cost > 0.003
        assert cost < 0.004

    def test_limit_up_stock(self):
        """涨停股买入滑点更大"""
        normal = _calc_transaction_cost(100.0, 105.0, is_limit_up=False)
        limit = _calc_transaction_cost(100.0, 105.0, is_limit_up=True)
        assert limit > normal
        # 涨停股买入滑点 0.5%  vs 普通股 0.1%
        assert limit - normal == pytest.approx(0.004, abs=0.001)

    def test_zero_buy_price(self):
        """买入价为0时的鲁棒性"""
        cost = _calc_transaction_cost(0.0, 105.0, is_limit_up=False)
        assert cost == pytest.approx(0.0, abs=0.001) or cost == float('inf')


class TestCountTradingDays:
    """停牌跳过测试"""

    def _make_kline(self, rows):
        """辅助: 构造K线DataFrame"""
        return pd.DataFrame(rows)

    def test_no_suspension(self):
        """无停牌，正常计数"""
        df = self._make_kline([
            {"日期": pd.Timestamp("2026-01-01"), "开盘": 10, "收盘": 11, "最高": 12, "最低": 9, "成交量": 10000},
            {"日期": pd.Timestamp("2026-01-02"), "开盘": 11, "收盘": 12, "最高": 13, "最低": 10, "成交量": 10000},
            {"日期": pd.Timestamp("2026-01-03"), "开盘": 12, "收盘": 13, "最高": 14, "最低": 11, "成交量": 10000},
        ])
        idx = _count_trading_days(df, 0, 2)
        assert idx == 1  # 从0开始，数2个交易日，到索引1

    def test_skip_suspension(self):
        """跳过停牌日"""
        df = self._make_kline([
            {"日期": pd.Timestamp("2026-01-01"), "开盘": 10, "收盘": 11, "最高": 12, "最低": 9, "成交量": 10000},
            {"日期": pd.Timestamp("2026-01-02"), "开盘": 11, "收盘": 11, "最高": 11, "最低": 11, "成交量": 0},  # 停牌
            {"日期": pd.Timestamp("2026-01-03"), "开盘": 11, "收盘": 12, "最高": 13, "最低": 10, "成交量": 10000},
        ])
        idx = _count_trading_days(df, 0, 2)
        # 第0天交易，第1天停牌跳过，第2天交易 -> 索引2
        assert idx == 2

    def test_all_suspension(self):
        """全部停牌"""
        df = self._make_kline([
            {"日期": pd.Timestamp("2026-01-01"), "开盘": 10, "收盘": 10, "最高": 10, "最低": 10, "成交量": 0},
            {"日期": pd.Timestamp("2026-01-02"), "开盘": 10, "收盘": 10, "最高": 10, "最低": 10, "成交量": 0},
        ])
        idx = _count_trading_days(df, 0, 1)
        assert idx is None


class TestSurvivorshipBias:
    """幸存者偏差估算测试"""

    def test_stage_worsening(self):
        """随持仓天数恶化"""
        bias = _estimate_survivorship_bias("600000", pd.Timestamp("2026-01-01"), [1, 3, 5, 10, 20], 100.0)
        assert bias[1] == pytest.approx(-0.05)
        assert bias[3] == pytest.approx(-0.15)
        assert bias[5] == pytest.approx(-0.25)
        assert bias[10] == pytest.approx(-0.40)
        assert bias[20] == pytest.approx(-0.55)

    def test_monotonicity(self):
        """跌幅随天数单调递增（更负）"""
        bias = _estimate_survivorship_bias("600000", pd.Timestamp("2026-01-01"), [1, 5, 20], 100.0)
        assert bias[1] > bias[5] > bias[20]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
