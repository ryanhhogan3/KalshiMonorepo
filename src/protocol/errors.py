# Common protocol exceptions
class StaleBook(Exception):
    pass

class SequenceGap(Exception):
    pass

class RiskBlocked(Exception):
    pass
