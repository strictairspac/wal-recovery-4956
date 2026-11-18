import concurrent.futures
import pytest

def auth(client,role='operator'):
    token=client.post('/auth/login',json={'email':f'{role}@haili.local','password':'Recovery2026!'}).json()['access_token']
    return {'Authorization':'Bearer '+token}

def log_set_id(client,headers):
    return client.get('/log-sets',headers=headers).json()[0]['id']

def make(client,headers,name,confirmed=9,received=9,replayed=9):
    r=client.post('/failover-clusters',headers=headers,json={'name':name,'log_set_id':log_set_id(client,headers),
        'primary_name':'primary-a','standby_name':'standby-b','confirmed_commit_lsn':confirmed,
        'standby_received_lsn':received,'standby_replayed_lsn':replayed})
    assert r.status_code==201,r.text
    return r.json()

def test_blocked_promotion_explains_data_loss(client):
    h=auth(client); c=make(client,h,'接收落后集群',9,7,7)
    assert c['role_state']=='blocked' and c['writes_open'] is False and c['gap']==2
    r=client.post(f"/failover-clusters/{c['id']}/promote",headers=h,json={'request_key':'blocked-1'})
    assert r.status_code==409
    detail=r.json()['detail']
    assert detail['reason_code']=='insufficient_received'
    risk=detail['attempt']['risk']
    assert [x['tx'] for x in risk['lost_commits']]==['T3']
    assert risk['keys_affected']==['sample_A']
    assert '丢失' in detail['message']
    assert client.get(f"/failover-clusters/{c['id']}",headers=h).json()['role_state']=='blocked'

def test_catch_up_resumes_original_task_and_reopens_writes(client):
    h=auth(client); c=make(client,h,'补齐集群',9,7,7)
    partial=client.post(f"/failover-clusters/{c['id']}/catch-up",headers=h,json={'target_received_lsn':8})
    assert partial.status_code==200 and partial.json()['role_state']=='blocked' and partial.json()['writes_open'] is False
    blocked=client.post(f"/failover-clusters/{c['id']}/writes",headers=h,json={'node':'primary-a','term':0,'key':'sample_A','value':1})
    assert blocked.status_code==409 and blocked.json()['detail']['reason_code']=='writes_closed'
    full=client.post(f"/failover-clusters/{c['id']}/catch-up",headers=h,json={'target_received_lsn':9,'target_replayed_lsn':9})
    assert full.status_code==200
    body=full.json()
    assert body['task_state']=='resumed' and body['writes_open'] is True and body['resumed_after_catch_up'] is True

def test_promotion_replays_first_then_opens_writes(client):
    h=auth(client); c=make(client,h,'重放落后集群',9,11,9)
    assert c['can_promote'] is True and c['replay_lag']==2
    r=client.post(f"/failover-clusters/{c['id']}/promote",headers=h,json={'request_key':'replay-1'})
    assert r.status_code==200,r.text
    body=r.json()
    assert body['new_term']==1 and body['active_primary']=='standby-b'
    assert body['ledger']=={'sample_A':60,'sample_B':35}
    assert body['writes_opened_after_replay'] is True and body['writes_opened_lsn']==11
    assert body['invariant_passed'] is True

def test_uncommitted_tail_is_undone_before_write_open(client):
    h=auth(client); c=make(client,h,'未提交尾部集群',9,10,9)
    body=client.post(f"/failover-clusters/{c['id']}/promote",headers=h,json={'request_key':'tail-1'}).json()
    assert body['losers']==['T2'] and body['undo_count']==2
    assert body['ledger']=={'sample_A':60,'sample_B':50}

def test_concurrent_promotion_yields_single_term(client):
    h=auth(client); c=make(client,h,'并发升主集群')
    def fire(i):
        return client.post(f"/failover-clusters/{c['id']}/promote",headers=h,json={'request_key':f'c{i}'})
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        results=list(pool.map(fire,range(6)))
    winners=[r for r in results if r.status_code==200]
    assert len(winners)==1
    assert {r.json()['new_term'] for r in winners}=={1}
    detail=client.get(f"/failover-clusters/{c['id']}",headers=h).json()
    assert detail['current_term']==1
    assert [a['status'] for a in detail['attempts']].count('promoted')==1

def test_retry_does_not_promote_twice(client):
    h=auth(client); c=make(client,h,'重试集群')
    first=client.post(f"/failover-clusters/{c['id']}/promote",headers=h,json={'request_key':'same'})
    retry=client.post(f"/failover-clusters/{c['id']}/promote",headers=h,json={'request_key':'same'})
    other=client.post(f"/failover-clusters/{c['id']}/promote",headers=h,json={'request_key':'other'})
    assert first.status_code==200 and retry.status_code==200
    assert retry.json()['new_term']==1 and retry.json()['idempotent_retry'] is True
    assert other.status_code==409 and other.json()['detail']['reason_code']=='already_promoted'
    detail=client.get(f"/failover-clusters/{c['id']}",headers=h).json()
    assert detail['current_term']==1
    assert len([a for a in detail['attempts'] if a['request_key']=='same'])==1

def test_stale_primary_regression_write_rejected(client):
    h=auth(client); c=make(client,h,'旧主回归集群')
    client.post(f"/failover-clusters/{c['id']}/promote",headers=h,json={'request_key':'p1'})
    stale=client.post(f"/failover-clusters/{c['id']}/writes",headers=h,json={'node':'primary-a','term':1,'key':'sample_A','value':1})
    assert stale.status_code==409 and stale.json()['detail']['reason_code']=='stale_primary'
    assert stale.json()['detail']['write']['accepted'] is False
    wrong=client.post(f"/failover-clusters/{c['id']}/writes",headers=h,json={'node':'standby-b','term':0,'key':'sample_A','value':1})
    assert wrong.status_code==409 and wrong.json()['detail']['reason_code']=='term_mismatch'
    ok=client.post(f"/failover-clusters/{c['id']}/writes",headers=h,json={'node':'standby-b','term':1,'key':'sample_A','value':61})
    assert ok.status_code==201 and ok.json()['ledger']['sample_A']==61

def test_role_and_ledger_are_committed_together(client):
    h=auth(client); c=make(client,h,'账本一致性集群')
    body=client.post(f"/failover-clusters/{c['id']}/promote",headers=h,json={'request_key':'p1'}).json()
    detail=client.get(f"/failover-clusters/{c['id']}",headers=h).json()
    assert detail['role_state']=='promoted' and detail['ledger'] is not None
    assert detail['ledger_sha256']==body['result_sha256']

def test_positions_and_roles_are_guarded(client):
    h=auth(client); c=make(client,h,'守卫集群',9,7,7)
    assert client.patch(f"/failover-clusters/{c['id']}/positions",headers=h,json={'standby_replayed_lsn':10}).status_code==422
    assert client.patch(f"/failover-clusters/{c['id']}/positions",headers=h,json={'confirmed_commit_lsn':8}).status_code==422
    assert client.patch(f"/failover-clusters/{c['id']}/positions",headers=h,json={'standby_received_lsn':5}).status_code==422
    viewer=auth(client,'viewer')
    assert client.post(f"/failover-clusters/{c['id']}/promote",headers=viewer,json={'request_key':'v1'}).status_code==403
    assert client.patch(f"/failover-clusters/{c['id']}/positions",headers=viewer,json={'standby_received_lsn':9}).status_code==403
    promoted=make(client,h,'升主后锁定集群')
    client.post(f"/failover-clusters/{promoted['id']}/promote",headers=h,json={'request_key':'p1'})
    assert client.patch(f"/failover-clusters/{promoted['id']}/positions",headers=h,json={'standby_received_lsn':9}).status_code==409
    assert client.post(f"/failover-clusters/{promoted['id']}/catch-up",headers=h,json={'target_received_lsn':9}).status_code==409

def test_legacy_recovery_flow_and_export_kept(client):
    h=auth(client)
    case=client.post('/crash-cases',headers=h,json={'name':'旧场景','log_set_id':log_set_id(client,h),'crash_after':10})
    assert case.status_code==201
    run=client.post('/recoveries',headers=h,json={'crash_case_id':case.json()['id'],'label':'旧功能'})
    assert run.status_code==202
    detail=client.get(f"/recoveries/{run.json()['id']}",headers=h)
    assert detail.status_code==200
    export=client.get(f"/recoveries/{run.json()['id']}/export",headers=h)
    assert export.status_code==200 and 'attachment' in export.headers['content-disposition']
    c=make(client,h,'导出集群')
    client.post(f"/failover-clusters/{c['id']}/promote",headers=h,json={'request_key':'p1'})
    ex=client.get(f"/failover-clusters/{c['id']}/export",headers=h)
    payload=ex.json()
    assert ex.status_code==200 and payload['current_term']==1
    assert payload['attempts'] and payload['ledger']

def test_promoted_role_cannot_exist_without_ledger(db):
    from sqlalchemy import text
    from sqlalchemy.exc import IntegrityError
    import pytest as _pytest
    with db() as session, _pytest.raises(IntegrityError):
        session.execute(text("INSERT INTO failover_clusters (name,log_set_id,primary_name,standby_name,confirmed_commit_lsn,"
            "standby_received_lsn,standby_replayed_lsn,role_state,task_state,active_primary,current_term,writes_open,created_at) "
            "VALUES ('bad',1,'p','s',0,0,0,'promoted','promoted','s',1,1,'2026-01-01 00:00:00')"))
        session.commit()
