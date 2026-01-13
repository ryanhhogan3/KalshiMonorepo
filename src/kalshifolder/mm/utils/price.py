"""
Price utility functions for binary market-making.
"""


def complement_100(price_cents: int) -> int:
    """
    Maps YES price <-> NO price via binary complement.
    
    In a binary market: yes_price + no_price = 100¢
    
    Examples:
        complement_100(52) -> 48  (YES bid at 52¢ → NO bid at 48¢)
        complement_100(31) -> 69  (YES ask at 31¢ → NO ask at 69¢)
    
    Args:
        price_cents: Integer 1-99
    
    Returns:
        Integer 1-99 (clamped to valid range)
    """
    return max(1, min(99, 100 - int(price_cents)))
