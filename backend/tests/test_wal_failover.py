import pytest
from src.services.wal_engine import failover_recover, recover
from scripts.seed_demo import RECORDS
INIT={'sample_A':100,'sample_B':50}

def test_replay_after_confirmed_and_undo_uncommitted_tail():
    out=failover_recover(INIT,RECORDS,9,10,9)
    assert out['recovered']=={'sample_A':60,'sample_B':50}
    assert out['invariant_passed'] is True
    assert out['losers']==['T2'] and out['undo_count']==2
    assert out['newly_replayed_count']==1

def test_promotion_replays_received_but_unreplayed_logs():
    out=failover_recover(INIT,RECORDS,9,11,9)
    assert out['recovered']=={'sample_A':60,'sample_B':35}
    assert out['replay_lag']==2
    assert out['writes_opened_after_replay'] is True
    assert [t['replayed_during_promotion'] for t in out['trace']].count(True)==1

def test_confirmed_data_completeness_checked_against_received():
    short=failover_recover(INIT,RECORDS,9,7,7)
    assert short['missing_confirmed_transactions']==['T3']
    assert short['confirmed_data_preserved'] is False
    full=failover_recover(INIT,RECORDS,9,9,9)
    assert full['missing_confirmed_transactions']==[] and full['confirmed_data_preserved'] is True

def test_positions_validated():
    with pytest.raises(ValueError): failover_recover(INIT,RECORDS,9,9,10)
    with pytest.raises(ValueError): failover_recover(INIT,RECORDS,8,9,9)
    with pytest.raises(ValueError): failover_recover(INIT,RECORDS,99,99,99)

def test_legacy_recovery_unchanged():
    out=recover(INIT,RECORDS,10)
    assert out['recovered']=={'sample_A':60,'sample_B':50}
    assert out['invariant_passed'] is True
