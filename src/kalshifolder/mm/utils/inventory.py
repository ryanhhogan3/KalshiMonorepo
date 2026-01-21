"""Inventory helpers for consistent sign conventions and exposure math."""
from __future__ import annotations

from typing import Union

Number = Union[int, float]

DEFAULT_CONVENTION = 'BUY_YES_POSITIVE'


def normalize_inventory(raw_pos: Union[Number, str, None], convention: str) -> float:
    """Convert a raw position into canonical BUY_YES_POSITIVE space."""
    if raw_pos is None:
        value = 0.0
    else:
        try:
            value = float(raw_pos)
        except (TypeError, ValueError):
            value = 0.0
    conv = (convention or DEFAULT_CONVENTION).upper()
    if conv == 'BUY_YES_NEGATIVE':
        return -value
    return value


def exposure_delta(size: Union[Number, str, None], api_side: str, action: str) -> float:
    """Return YES exposure delta (contracts) if an order fully fills."""
    try:
        qty = float(size or 0.0)
    except (TypeError, ValueError):
        qty = 0.0
    api = (api_side or 'yes').strip().lower()
    act = (action or 'buy').strip().lower()
    if api == 'yes':
        return qty if act == 'buy' else -qty
    # api == 'no'
    return -qty if act == 'buy' else qty
