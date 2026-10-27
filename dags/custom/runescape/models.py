from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class TrackedUser:
    name: str
    id: uuid.UUID

    def to_dict(self) -> dict:
        data = asdict(self)
        data["id"] = str(self.id)
        return data

    @classmethod
    def from_dict(cls, data: dict) -> TrackedUser:
        data["id"] = uuid.UUID(data["id"])
        return cls(**data)
