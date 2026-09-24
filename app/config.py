"""业务常量与运行配置。"""
import os

# 每炮固定通道数范围
MIN_CHANNELS = 2
MAX_CHANNELS = 8

# 炮次号范围
MIN_SHOT = 1
MAX_SHOT = 256

# 暂存窗口：只接受水位之后 WINDOW_SIZE 炮以内的片段
WINDOW_SIZE = 32

# 标识长度上限
MAX_RUN_ID_LEN = 64
MAX_OP_ID_LEN = 128
MAX_CHANNEL_LEN = 64


def database_url() -> str:
    return os.environ.get(
        "DATABASE_URL",
        "postgresql://pulse:pulse@localhost:5432/pulsedb",
    )
