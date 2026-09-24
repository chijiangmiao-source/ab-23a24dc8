"""请求/响应模型与输入校验。"""
from pydantic import BaseModel, Field, field_validator, model_validator

from .config import (
    MAX_CHANNEL_LEN,
    MAX_CHANNELS,
    MAX_OP_ID_LEN,
    MAX_RUN_ID_LEN,
    MAX_SHOT,
    MIN_CHANNELS,
    MIN_SHOT,
)


class FragmentIn(BaseModel):
    op_id: str = Field(min_length=1, max_length=MAX_OP_ID_LEN)
    channels: list[str] = Field(min_length=MIN_CHANNELS, max_length=MAX_CHANNELS)
    channel: str = Field(min_length=1, max_length=MAX_CHANNEL_LEN)
    shot: int = Field(ge=MIN_SHOT, le=MAX_SHOT)
    samples: list[int] = Field(min_length=1)

    @field_validator("channels")
    @classmethod
    def _check_channels(cls, value: list[str]) -> list[str]:
        if any(not c.strip() for c in value):
            raise ValueError("channel ids must be non-empty")
        if len(set(value)) != len(value):
            raise ValueError("channels must be unique")
        return value

    @field_validator("op_id")
    @classmethod
    def _check_op_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("op_id must be non-empty")
        return value

    @model_validator(mode="after")
    def _check_channel_member(self) -> "FragmentIn":
        if self.channel not in self.channels:
            raise ValueError("channel must be one of the run's fixed channels")
        return self


class BufferedShot(BaseModel):
    shot: int
    channels: list[str]
    missing_channels: list[str]
    complete: bool


class RunStatus(BaseModel):
    run_id: str
    channels: list[str]
    # 下一个等待齐备的炮次
    watermark: int
    # 已连续释放、对下游可见的炮次（不含任何越过缺口的炮次）
    released_shots: list[int]
    # 已提交过片段的全部炮次
    submitted_shots: list[int]
    # 水位之后的暂存摘要
    buffered: list[BufferedShot]
