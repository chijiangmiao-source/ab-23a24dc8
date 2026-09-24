"""Pydantic request/response shapes."""
from __future__ import annotations

import hashlib
import json
from typing import Any

from pydantic import BaseModel, Field, model_validator

SHOT_MIN = 1
SHOT_MAX = 256
CHANNEL_MIN = 2
CHANNEL_MAX = 8


class FragmentIn(BaseModel):
    """One fragment submitted by a digitizer.

    ``channels`` is the fixed channel set of the run (2..8 distinct ids);
    ``channel`` identifies which of them this fragment belongs to.
    """

    operation_id: str = Field(min_length=1, max_length=200)
    channels: list[str] = Field(min_length=CHANNEL_MIN, max_length=CHANNEL_MAX)
    channel: str = Field(min_length=1, max_length=200)
    shot: int = Field(ge=SHOT_MIN, le=SHOT_MAX)
    samples: list[int]

    @model_validator(mode="after")
    def _validate_shape(self) -> "FragmentIn":
        if any(not c for c in self.channels):
            raise ValueError("channel ids must be non-empty strings")
        if len(set(self.channels)) != len(self.channels):
            raise ValueError("channels must not contain duplicates")
        if self.channel not in self.channels:
            raise ValueError("channel must be one of the run channels")
        return self

    def canonical(self) -> dict[str, Any]:
        """Logical content of the request, independent of channel ordering."""
        return {
            "channels": sorted(set(self.channels)),
            "channel": self.channel,
            "shot": self.shot,
            "samples": list(self.samples),
        }

    def content_hash(self) -> str:
        body = json.dumps(self.canonical(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(body.encode("utf-8")).hexdigest()
