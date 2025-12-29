from kalshifolder.mm.state.store import EngineStateStore
from kalshifolder.mm.state.models import EngineState


def test_store_creates_market():
    es = EngineState(instance_id='i', version='v')
    store = EngineStateStore(es)
    m = store.get_market('T1')
    assert m.ticker == 'T1'
