#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""配置加载器 — 共用模块"""
import json
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
CONFIG_FILE = BASE_DIR / "config.json"

DEFAULT_CFG = {
    "top_n": 3,
    "enable_feishu": False,
    "feishu_webhook": "",
    "ai_analysis": {"enabled": True},
}


def load_config() -> dict:
    """加载 config.json，缺失项填默认值"""
    if not CONFIG_FILE.exists():
        return dict(DEFAULT_CFG)
    try:
        cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8-sig"))
        for k, v in DEFAULT_CFG.items():
            if k not in cfg:
                cfg[k] = v
        return cfg
    except Exception as e:
        print(f"[配置] 读取失败: {e}，使用默认配置")
        return dict(DEFAULT_CFG)
