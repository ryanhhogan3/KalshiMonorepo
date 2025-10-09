# Registry for strategies
STRATEGY_REGISTRY = {}

def register_strategy(name, cls):
    STRATEGY_REGISTRY[name] = cls
