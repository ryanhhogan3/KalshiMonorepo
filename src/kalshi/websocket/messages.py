# Kalshi WS frames
# TODO: raw dicts → dataclasses

"""
Pydantic models for Kalshi WebSocket messages.
Always return normalized envelopes with original raw data preserved.
"""
from pydantic import BaseModel, Field, ValidationError
from typing import Any, Dict, Optional
import logging

class MessageEnvelope(BaseModel):
    type: str = Field(default="unknown")
    data: Optional[Dict[str, Any]] = None
    raw: Any = None
    error: Optional[str] = None

    @classmethod
    def from_raw(cls, raw: Any) -> "MessageEnvelope":
        try:
            # TODO: Normalize based on Kalshi schema
            if isinstance(raw, dict) and "type" in raw:
                return cls(type=raw["type"], data=raw, raw=raw)
            return cls(type="unknown", data=None, raw=raw)
        except Exception as e:
            logging.error(f"MessageEnvelope normalization error: {e}")
            return cls(type="error", raw=raw, error=str(e))

# Example usage:
# envelope = MessageEnvelope.from_raw(ws_message)
