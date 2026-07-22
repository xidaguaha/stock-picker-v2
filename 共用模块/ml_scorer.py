#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
LightGBM 机器学习打分模块
====================================================
用历史因子得分训练LightGBM模型，替代手工加权平均。

核心思路：
  - 用当天的因子得分 → 预测当天的涨跌幅排名
  - 训练数据：历史N天的因子得分 + 涨跌幅
  - 预测：对新一天的因子得分预测涨跌幅排名 → 作为综合得分
  - 回退：训练数据不足时使用手工加权平均

数据来源：data/factor_scores/factors_*.parquet
"""

import json
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

def log(msg, level="INFO"):
    """简易日志函数（防止异常路径调用未定义的log导致NameError崩溃）"""
    from datetime import datetime as _dt
    print(f"[{_dt.now().strftime('%H:%M:%S')}] [{level}] {msg}")


try:
    import lightgbm as lgb
    HAS_LGB = True
except ImportError:
    HAS_LGB = False

_this_dir = Path(__file__).parent
# 统一指向项目根目录（数据目录在项目根）
BASE_DIR = _this_dir.parent
FACTOR_SCORES_DIR = BASE_DIR / "data" / "factor_scores"
CACHE_DIR = BASE_DIR / "cache"
MODEL_PATH = CACHE_DIR / "lgb_model.pkl"
MODEL_META_PATH = CACHE_DIR / "lgb_model_meta.json"

# 因子列（与FACTOR_WEIGHTS的key对应）
FACTOR_COLS = [
    "动量_涨跌幅", "动量_5日涨幅",
    "趋势_均线位置", "趋势_RSI强度",
    "量能_换手率", "量能_量比",
    "估值_PE反向", "估值_PB反向",
    "规模_小市值", "技术_MACD金叉",
    "概念_热度", "景气_行业",
    "情绪_市场", "情绪_板块", "LLM_信号",
]

# 最少需要多少天的训练数据（截面预测：1天1853只股票已足够）
MIN_TRAIN_DAYS = 1


def _load_all_factor_scores(max_days=30):
    """加载历史因子得分数据"""
    if not FACTOR_SCORES_DIR.exists():
        return None

    files = sorted(FACTOR_SCORES_DIR.glob("factors_*.parquet"))[-max_days:]
    if not files:
        return None

    dfs = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            if "代码" in df.columns and "涨跌幅" in df.columns:
                dfs.append(df)
        except Exception as e:
            log(f"操作异常: {e}", "WARN")

    if not dfs:
        return None

    return pd.concat(dfs, ignore_index=True)


def _prepare_training_data(df_all):
    """
    准备训练数据：因子得分 → 涨跌幅排名（0-1）
    用当天的因子预测当天的涨跌幅排名
    """
    available_cols = [c for c in FACTOR_COLS if c in df_all.columns]
    if not available_cols:
        return None, None, None

    # 按快照日期分组，每天内部做排名
    results_X = []
    results_y = []

    for date_str, group in df_all.groupby("快照日期"):
        if len(group) < 50:
            continue

        # 因子得分
        X = group[available_cols].fillna(0.5).values

        # 目标：涨跌幅的排名百分位（0-1），越高越好
        returns = group["涨跌幅"].values
        ranks = pd.Series(returns).rank(pct=True).values

        results_X.append(X)
        results_y.append(ranks)

    if not results_X:
        return None, None, None

    X = np.vstack(results_X)
    y = np.concatenate(results_y)

    return X, y, available_cols


def train_model(min_days=MIN_TRAIN_DAYS):
    """训练LightGBM模型"""
    if not HAS_LGB:
        return False, "lightgbm未安装"

    df_all = _load_all_factor_scores()
    if df_all is None:
        return False, "无因子得分数据"

    # 统计有多少天的数据
    n_days = df_all["快照日期"].nunique()
    if n_days < min_days:
        return False, f"训练数据不足（{n_days}天 < {min_days}天）"

    X, y, feature_names = _prepare_training_data(df_all)
    if X is None:
        return False, "数据准备失败"

    # 训练LightGBM
    params = {
        "objective": "regression",
        "metric": "mse",
        "num_leaves": 31,
        "learning_rate": 0.05,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "verbose": -1,
        "n_estimators": 100,
        "min_child_samples": 20,
    }

    model = lgb.LGBMRegressor(**params)
    model.fit(X, y)

    # 保存模型（pickle格式，避免LightGBM中文路径问题）
    import pickle
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        MODEL_PATH.unlink(missing_ok=True)
    except Exception as e:
        log(f"操作异常: {e}", "WARN")
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(model, f)

    meta = {
        "trained_at": datetime.now().isoformat(),
        "train_days": n_days,
        "train_samples": len(X),
        "features": feature_names,
        "feature_importance": dict(zip(feature_names, [round(float(v), 4) for v in model.feature_importances_])),
    }
    MODEL_META_PATH.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    return True, f"训练完成: {n_days}天数据, {len(X)}条样本, {len(feature_names)}个因子"


def load_model():
    """加载已训练的模型"""
    if not HAS_LGB:
        return None
    if not MODEL_PATH.exists():
        return None
    try:
        import pickle
        with open(MODEL_PATH, "rb") as f:
            model = pickle.load(f)
        meta = json.loads(MODEL_META_PATH.read_text(encoding="utf-8")) if MODEL_META_PATH.exists() else {}
        return model, meta
    except Exception as e:
        log(f"操作异常: {e}", "WARN")
        return None


def predict_scores(df, model=None, meta=None):
    """
    用LightGBM模型对股票打分。

    Args:
        df: 包含因子列的DataFrame
        model: 已加载的LightGBM模型（可选，自动加载）
        meta: 模型元数据（可选）

    Returns:
        df: 新增 '综合得分_ML' 列（0-100分），排序后返回
    """
    if model is None:
        loaded = load_model()
        if loaded is None:
            return df, False
        model, meta = loaded

    features = meta.get("features", FACTOR_COLS)
    available = [f for f in features if f in df.columns]
    if len(available) < 3:
        return df, False

    X = df[available].fillna(0.5).values
    try:
        preds = model.predict(X)
    except Exception:
        return df, False

    # 模型预测的是涨跌幅排名百分位(0-1)，直接乘100转为百分制
    # 不做min-max拉伸，让分值反映模型预测的绝对排名而非强制拉到100
    scores = np.clip(preds * 100, 0, 100)

    df["综合得分_ML"] = scores
    return df, True


def get_model_info():
    """获取模型信息"""
    meta = load_model()
    if meta is None:
        return {"状态": "无模型", "建议": "运行 train_model() 训练"}
    _, m = meta
    return {
        "状态": "已训练",
        "训练时间": m.get("trained_at", "未知"),
        "训练天数": m.get("train_days", 0),
        "训练样本": m.get("train_samples", 0),
        "特征重要性": m.get("feature_importance", {}),
    }


if __name__ == "__main__":
    print("=" * 50)
    print("  LightGBM 打分模块")
    print("=" * 50)

    success, msg = train_model()
    print(f"训练: {msg}")

    if success:
        info = get_model_info()
        print(f"\n模型信息:")
        for k, v in info.items():
            if k == "特征重要性":
                print(f"  {k}:")
                for feat, imp in sorted(v.items(), key=lambda x: -x[1]):
                    print(f"    {feat}: {imp}")
            else:
                print(f"  {k}: {v}")
