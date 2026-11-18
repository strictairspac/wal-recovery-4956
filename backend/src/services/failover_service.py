"""Primary/standby failover drill: positions, gated promotion, recovery ledger."""
import hashlib, json, uuid
from datetime import datetime, timezone
from sqlalchemy import select, update, func
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException
from src.models.entities import FailoverCluster, FailoverAttempt, FailoverWrite, LogSet
from src.services.wal_engine import state_at, failover_recover
from src.utils.serialize import record

PROMOTED='promoted'; BLOCKED='blocked'; STABLE='stable'

def _now(): return datetime.now(timezone.utc).replace(tzinfo=None)
def _sha(value): return hashlib.sha256(json.dumps(value,sort_keys=True,ensure_ascii=False).encode()).hexdigest()

def _records(db, cluster):
    logs=db.get(LogSet, cluster.log_set_id)
    if logs is None: raise HTTPException(409,'集群关联的日志集已不存在')
    return logs.initial, logs.records, logs.name

def _check_positions(records, confirmed, received, replayed):
    n=len(records)
    for label,value in (('已确认提交位置',confirmed),('备库接收位置',received),('备库重放位置',replayed)):
        if not isinstance(value,int) or isinstance(value,bool) or not 0<=value<=n:
            raise HTTPException(422,f'{label}须在0—{n}之间')
    if replayed>received: raise HTTPException(422,'备库重放位置不得领先接收位置')
    if confirmed and records[confirmed-1].get('op')!='commit':
        raise HTTPException(422,'已确认提交位置必须落在commit日志上')

def loss_risk(initial, records, confirmed, received, replayed=0):
    """Quantify confirmed data the standby would lose if promoted now."""
    gap=max(0,confirmed-received)
    if gap==0:
        return {'gap':0,'replay_lag':max(0,received-replayed),'lost_commits':[],'lost_transaction_count':0,'keys_affected':[],
                'confirmed_state':state_at(initial,records,confirmed),
                'standby_state':state_at(initial,records,received),
                'severity':'none','summary':'备库接收位置已覆盖全部已确认提交位置，升主不丢失已确认数据。'}
    lost=[];lost_txs=[]
    for lsn,r in enumerate(records[received:confirmed], received+1):
        if r.get('op')=='commit' and r.get('tx') not in lost_txs:
            lost_txs.append(r.get('tx'));lost.append({'lsn':lsn,'tx':r.get('tx')})
    keys=sorted({r['key'] for r in records[:confirmed] if r.get('op')=='write' and r.get('tx') in lost_txs})
    tx_text=('（'+'、'.join(lost_txs)+'）') if lost_txs else ''
    return {'gap':gap,'replay_lag':max(0,received-replayed),'lost_commits':lost,'lost_transaction_count':len(lost),
            'keys_affected':keys,'confirmed_state':state_at(initial,records,confirmed),
            'standby_state':state_at(initial,records,received),'severity':'high',
            'summary':f'备库仅接收至LSN {received}，已确认提交位置为LSN {confirmed}，缺少{gap}条已确认日志；若此时升主将丢失{len(lost)}个已提交事务{tx_text}，涉及账本键：{"、".join(keys) or "无"}。'}

def _diag(db, cluster):
    initial,records,_=_records(db,cluster)
    gap=max(0,cluster.confirmed_commit_lsn-cluster.standby_received_lsn)
    replay_lag=max(0,cluster.standby_received_lsn-cluster.standby_replayed_lsn)
    if cluster.role_state==PROMOTED:
        can,code,reason=False,'already_promoted',f'集群已升主，当前任期{cluster.current_term}，主库为{cluster.active_primary}；旧主不得再次升主。'
    elif gap>0:
        can,code,reason=False,'insufficient_received',loss_risk(initial,records,cluster.confirmed_commit_lsn,cluster.standby_received_lsn,cluster.standby_replayed_lsn)['summary']
    else:
        can,code,reason=True,None,'备库已覆盖已确认提交位置，可以升主；升主将先重放已接收未重放日志，再撤销未提交尾部。'
    return {'gap':gap,'replay_lag':replay_lag,'can_promote':can,'block_reason_code':code,'block_reason':reason,
            'risk':None if gap==0 else loss_risk(initial,records,cluster.confirmed_commit_lsn,cluster.standby_received_lsn,cluster.standby_replayed_lsn),
            'confirmed_state':state_at(initial,records,cluster.confirmed_commit_lsn),
            'standby_visible_state':state_at(initial,records,cluster.standby_received_lsn),
            'standby_applied_state':state_at(initial,records,cluster.standby_replayed_lsn)}

def cluster_payload(db, cluster):
    out=record(cluster);out.update(_diag(db,cluster))
    return out

def attempt_payload(attempt):
    out=record(attempt)
    out['positions']={'confirmed_commit_lsn':attempt.confirmed_commit_lsn,'standby_received_lsn':attempt.standby_received_lsn,
                      'standby_replayed_lsn':attempt.standby_replayed_lsn}
    return out

def write_payload(write): return record(write)

def _attempt_error(payload, code, message, status=409):
    raise HTTPException(status,{'message':message,'reason_code':code,'attempt':payload})

def list_clusters(db):
    return [cluster_payload(db,c) for c in db.scalars(select(FailoverCluster).order_by(FailoverCluster.id))]

def get_cluster(db,ident):
    obj=db.get(FailoverCluster,ident)
    if obj is None: raise HTTPException(404,'主备集群不存在')
    return obj

def cluster_detail(db,ident):
    cluster=get_cluster(db,ident)
    out=cluster_payload(db,cluster)
    attempts=list(db.scalars(select(FailoverAttempt).where(FailoverAttempt.cluster_id==ident).order_by(FailoverAttempt.id.desc()).limit(30)))
    writes=list(db.scalars(select(FailoverWrite).where(FailoverWrite.cluster_id==ident).order_by(FailoverWrite.id.desc()).limit(50)))
    out['attempts']=[attempt_payload(a) for a in attempts]
    out['writes']=[write_payload(w) for w in writes]
    return out

def _ledger_or_initial(initial, records, confirmed):
    return state_at(initial,records,confirmed)

def create_cluster(db,data):
    logs=db.get(LogSet,data.log_set_id)
    if logs is None: raise HTTPException(404,'日志集不存在')
    values=data.model_dump()
    _check_positions(logs.records,values['confirmed_commit_lsn'],values['standby_received_lsn'],values['standby_replayed_lsn'])
    if values['primary_name']==values['standby_name']: raise HTTPException(422,'主库与备库名称必须不同')
    ledger=_ledger_or_initial(logs.initial,logs.records,values['confirmed_commit_lsn'])
    gap=max(0,values['confirmed_commit_lsn']-values['standby_received_lsn'])
    state=STABLE if gap==0 else BLOCKED
    obj=FailoverCluster(**values,role_state=state,task_state='original' if gap==0 else BLOCKED,
                         active_primary=values['primary_name'],current_term=0,writes_open=gap==0,
                         ledger=ledger,ledger_sha256=_sha(ledger))
    db.add(obj);db.flush();return cluster_payload(db,obj)

def _accepted_write_count(db, cluster_id):
    return db.scalar(select(func.count()).select_from(FailoverWrite).where(FailoverWrite.cluster_id==cluster_id,FailoverWrite.accepted.is_(True)))

def configure_positions(db,ident,data):
    cluster=db.scalar(select(FailoverCluster).where(FailoverCluster.id==ident).with_for_update())
    if cluster is None: raise HTTPException(404,'主备集群不存在')
    if cluster.role_state==PROMOTED:
        raise HTTPException(409,{'message':f'集群已在任期{cluster.current_term}升主，旧主只读，不再接受位置配置。','reason_code':'already_promoted'})
    initial,records,_=_records(db,cluster)
    confirmed=data.confirmed_commit_lsn if data.confirmed_commit_lsn is not None else cluster.confirmed_commit_lsn
    received=data.standby_received_lsn if data.standby_received_lsn is not None else cluster.standby_received_lsn
    replayed=data.standby_replayed_lsn if data.standby_replayed_lsn is not None else cluster.standby_replayed_lsn
    _check_positions(records,confirmed,received,replayed)
    previous_blocked=cluster.task_state==BLOCKED or cluster.role_state==BLOCKED
    cluster.confirmed_commit_lsn=confirmed;cluster.standby_received_lsn=received;cluster.standby_replayed_lsn=replayed
    gap=max(0,confirmed-received)
    resumed=False
    if gap>0:
        cluster.role_state=BLOCKED;cluster.task_state=BLOCKED;cluster.writes_open=False
    else:
        cluster.role_state=STABLE;cluster.writes_open=True;cluster.writes_open_lsn=received
        if previous_blocked:
            cluster.task_state='resumed';cluster.resumed_at=_now();resumed=True
    if _accepted_write_count(db,cluster.id)==0:
        cluster.ledger=_ledger_or_initial(initial,records,confirmed);cluster.ledger_sha256=_sha(cluster.ledger)
    db.flush()
    out=cluster_payload(db,cluster);out['resumed_after_catch_up']=resumed
    return out

def catch_up(db,ident,data,actor):
    cluster=db.scalar(select(FailoverCluster).where(FailoverCluster.id==ident).with_for_update())
    if cluster is None: raise HTTPException(404,'主备集群不存在')
    if cluster.role_state==PROMOTED:
        raise HTTPException(409,{'message':f'集群已在任期{cluster.current_term}升主，备库已完成接管；恢复账本只读。','reason_code':'already_promoted'})
    initial,records,_=_records(db,cluster)
    confirmed=cluster.confirmed_commit_lsn
    received=data.target_received_lsn if data.target_received_lsn is not None else cluster.standby_received_lsn
    replayed=data.target_replayed_lsn if data.target_replayed_lsn is not None else cluster.standby_replayed_lsn
    if received<cluster.standby_received_lsn or replayed<cluster.standby_replayed_lsn:
        raise HTTPException(422,'补齐位置只能向前推进，不能回退')
    _check_positions(records,confirmed,received,replayed)
    cluster.standby_received_lsn=received;cluster.standby_replayed_lsn=replayed
    gap=max(0,confirmed-received)
    resumed=False
    if gap==0:
        was_blocked=cluster.task_state in (BLOCKED,) or cluster.role_state==BLOCKED
        cluster.role_state=STABLE
        cluster.task_state='resumed' if was_blocked else cluster.task_state
        cluster.writes_open=True;cluster.writes_open_lsn=received
        if was_blocked: cluster.resumed_at=_now();resumed=True
        if _accepted_write_count(db,cluster.id)==0:
            cluster.ledger=_ledger_or_initial(initial,records,confirmed);cluster.ledger_sha256=_sha(cluster.ledger)
        code, status='catch_up_resumed','resumed'
        reason=f'备库接收推进至LSN {received}、重放至LSN {replayed}，已覆盖确认提交位置LSN {confirmed}；原任务恢复运行，写入重新开放。'
    else:
        risk=loss_risk(initial,records,confirmed,received,replayed)
        cluster.role_state=BLOCKED;cluster.task_state=BLOCKED;cluster.writes_open=False
        code,status='insufficient_received','blocked';reason=risk['summary']
    attempt=FailoverAttempt(cluster_id=cluster.id,request_key='catchup-'+uuid.uuid4().hex[:12],status=status,
        reason_code=code,reason=reason,risk=loss_risk(initial,records,confirmed,received,replayed) if gap else None,
        confirmed_commit_lsn=confirmed,standby_received_lsn=received,standby_replayed_lsn=replayed,
        gap=gap,replay_lag=max(0,received-replayed),active_primary=cluster.active_primary,actor=actor)
    db.add(attempt);db.flush()
    out=cluster_payload(db,cluster);out['attempt']=attempt_payload(attempt);out['resumed_after_catch_up']=resumed
    return out

def _find_request(db,cluster_id,request_key):
    return db.scalar(select(FailoverAttempt).where(FailoverAttempt.cluster_id==cluster_id,FailoverAttempt.request_key==request_key).order_by(FailoverAttempt.id.desc()))

def _promotion_success(db, attempt):
    out=attempt_payload(attempt);out['message']=f'备库已升主，任期{attempt.new_term}；重放已接收日志并撤销未提交尾部后才开放写入。'
    return out

def _reject_after_promotion(db, ident, request_key, actor):
    """Another request won promotion: record a rejection, never a second term."""
    db.expire_all()
    cluster=db.get(FailoverCluster,ident)
    existing=_find_request(db,ident,request_key)
    message=f'集群已在任期{cluster.current_term}升主，主库为{cluster.active_primary}；本次请求不会产生新任期。'
    if existing is None:
        attempt=FailoverAttempt(cluster_id=ident,request_key=request_key,status='rejected',
            reason_code='already_promoted',reason=message,
            confirmed_commit_lsn=cluster.confirmed_commit_lsn,standby_received_lsn=cluster.standby_received_lsn,
            standby_replayed_lsn=cluster.standby_replayed_lsn,
            gap=max(0,cluster.confirmed_commit_lsn-cluster.standby_received_lsn),
            replay_lag=max(0,cluster.standby_received_lsn-cluster.standby_replayed_lsn),
            new_term=None,active_primary=cluster.active_primary,actor=actor)
        db.add(attempt)
    else:
        attempt=existing;attempt.status='rejected';attempt.reason_code='already_promoted';attempt.reason=message
        attempt.new_term=None;attempt.updated_at=_now();attempt.actor=actor
    db.commit()
    _attempt_error(attempt_payload(attempt),'already_promoted',message)

def promote(db,ident,data,actor):
    """Gated, idempotent promotion. Only one new term can ever be created."""
    cluster=db.scalar(select(FailoverCluster).where(FailoverCluster.id==ident).with_for_update())
    if cluster is None: raise HTTPException(404,'主备集群不存在')
    initial,records,_=_records(db,cluster)
    existing=_find_request(db,cluster.id,data.request_key)
    if cluster.role_state==PROMOTED:
        if existing and existing.status=='promoted':
            out=_promotion_success(db,existing);out['idempotent_retry']=True;return out
        message=f'集群已在任期{cluster.current_term}升主，主库为{cluster.active_primary}；本次请求不会产生新任期。'
        if existing is None:
            attempt=FailoverAttempt(cluster_id=cluster.id,request_key=data.request_key,status='rejected',
                reason_code='already_promoted',reason=message,
                confirmed_commit_lsn=cluster.confirmed_commit_lsn,standby_received_lsn=cluster.standby_received_lsn,
                standby_replayed_lsn=cluster.standby_replayed_lsn,gap=max(0,cluster.confirmed_commit_lsn-cluster.standby_received_lsn),
                replay_lag=max(0,cluster.standby_received_lsn-cluster.standby_replayed_lsn),
                new_term=None,active_primary=cluster.active_primary,actor=actor)
            db.add(attempt);db.flush()
        else:
            attempt=existing;attempt.status='rejected';attempt.reason_code='already_promoted';attempt.reason=message
            attempt.updated_at=_now();attempt.actor=actor;db.flush()
        db.commit()
        _attempt_error(attempt_payload(attempt),'already_promoted',attempt.reason)
    gap=max(0,cluster.confirmed_commit_lsn-cluster.standby_received_lsn)
    if gap>0:
        risk=loss_risk(initial,records,cluster.confirmed_commit_lsn,cluster.standby_received_lsn,cluster.standby_replayed_lsn)
        if existing and existing.status=='blocked':
            attempt=existing
            attempt.confirmed_commit_lsn=cluster.confirmed_commit_lsn;attempt.standby_received_lsn=cluster.standby_received_lsn
            attempt.standby_replayed_lsn=cluster.standby_replayed_lsn;attempt.gap=gap
            attempt.replay_lag=max(0,cluster.standby_received_lsn-cluster.standby_replayed_lsn)
            attempt.reason=risk['summary'];attempt.risk=risk;attempt.updated_at=_now();attempt.actor=actor
        else:
            attempt=FailoverAttempt(cluster_id=cluster.id,request_key=data.request_key,status='blocked',
                reason_code='insufficient_received',reason=risk['summary'],risk=risk,
                confirmed_commit_lsn=cluster.confirmed_commit_lsn,standby_received_lsn=cluster.standby_received_lsn,
                standby_replayed_lsn=cluster.standby_replayed_lsn,gap=gap,
                replay_lag=max(0,cluster.standby_received_lsn-cluster.standby_replayed_lsn),
                new_term=None,active_primary=cluster.active_primary,actor=actor)
            db.add(attempt)
        cluster.role_state=BLOCKED;cluster.task_state=BLOCKED;cluster.writes_open=False
        db.flush();db.commit()
        _attempt_error(attempt_payload(attempt),'insufficient_received',risk['summary'])
    # Ready: replay received logs, undo the uncommitted tail, then open writes.
    outcome=failover_recover(initial,records,cluster.confirmed_commit_lsn,cluster.standby_received_lsn,cluster.standby_replayed_lsn)
    term=cluster.current_term+1
    if existing and existing.status!='promoted':
        attempt=existing
        attempt.status='promoted';attempt.reason_code='promoted'
        attempt.reason=f'备库接收至LSN {cluster.standby_received_lsn}、重放至LSN {cluster.standby_replayed_lsn}，先重放{outcome["newly_replayed_count"]}条已接收未重放日志，再撤销{outcome["undo_count"]}条未提交写入后开放写入。'
        attempt.risk=None;attempt.new_term=term;attempt.active_primary=cluster.standby_name
        attempt.ledger=outcome['recovered'];attempt.ledger_sha256=outcome['result_sha256']
        attempt.redo_count=outcome['redo_count'];attempt.undo_count=outcome['undo_count'];attempt.losers=outcome['losers']
        attempt.trace=outcome['trace'];attempt.checkpoint_lsn=outcome['checkpoint_lsn']
        attempt.writes_opened_lsn=cluster.standby_received_lsn;attempt.writes_opened_after_replay=True
        attempt.input_sha256=outcome['input_sha256'];attempt.result_sha256=outcome['result_sha256']
        attempt.confirmed_commit_lsn=cluster.confirmed_commit_lsn;attempt.standby_received_lsn=cluster.standby_received_lsn
        attempt.standby_replayed_lsn=cluster.standby_replayed_lsn;attempt.gap=0
        attempt.replay_lag=max(0,cluster.standby_received_lsn-cluster.standby_replayed_lsn)
        attempt.updated_at=_now();attempt.actor=actor
        db.flush()
    else:
        attempt=FailoverAttempt(cluster_id=cluster.id,request_key=data.request_key,status='promoted',
            reason_code='promoted',reason='备库已升主，先重放已接收未重放日志，再撤销未提交尾部后开放写入。',
            confirmed_commit_lsn=cluster.confirmed_commit_lsn,standby_received_lsn=cluster.standby_received_lsn,
            standby_replayed_lsn=cluster.standby_replayed_lsn,gap=0,
            replay_lag=max(0,cluster.standby_received_lsn-cluster.standby_replayed_lsn),
            new_term=term,active_primary=cluster.standby_name,ledger=outcome['recovered'],
            ledger_sha256=outcome['result_sha256'],redo_count=outcome['redo_count'],undo_count=outcome['undo_count'],
            losers=outcome['losers'],trace=outcome['trace'],checkpoint_lsn=outcome['checkpoint_lsn'],
            writes_opened_lsn=cluster.standby_received_lsn,writes_opened_after_replay=True,
            input_sha256=outcome['input_sha256'],result_sha256=outcome['result_sha256'],actor=actor)
        try:
            with db.begin_nested():
                db.add(attempt);db.flush()
        except IntegrityError:
            # A concurrent request already created this term; converge on its result.
            db.rollback()
            winner=db.scalar(select(FailoverAttempt).where(FailoverAttempt.cluster_id==ident,FailoverAttempt.new_term==term))
            if winner is None: raise
            if winner.request_key==data.request_key:
                out=_promotion_success(db,winner);out['idempotent_retry']=True;return out
            _reject_after_promotion(db,ident,data.request_key,actor)
    # Compare-and-set so a stale concurrent request cannot move the term twice.
    changed=db.execute(update(FailoverCluster).where(FailoverCluster.id==cluster.id,FailoverCluster.role_state!=PROMOTED,
                            FailoverCluster.current_term==cluster.current_term)
        .values(current_term=term,role_state=PROMOTED,task_state='promoted',active_primary=cluster.standby_name,
                writes_open=True,writes_open_lsn=cluster.standby_received_lsn,ledger=outcome['recovered'],
                ledger_sha256=outcome['result_sha256'],promotion_attempt_id=attempt.id,promoted_at=_now())).rowcount
    if not changed:
        db.rollback()
        winner=db.scalar(select(FailoverAttempt).where(FailoverAttempt.cluster_id==ident,FailoverAttempt.new_term==term))
        if winner is not None and winner.request_key==data.request_key:
            out=_promotion_success(db,winner);out['idempotent_retry']=True;return out
        _reject_after_promotion(db,ident,data.request_key,actor)
    db.commit();db.refresh(cluster);db.refresh(attempt)
    out=_promotion_success(db,attempt)
    out['promoted']=True;out['ledger']=outcome['recovered'];out['oracle']=outcome['oracle']
    out['invariant_passed']=outcome['invariant_passed'];out['writes_opened_lsn']=cluster.writes_open_lsn
    out['writes_opened_after_replay']=True
    return out

def record_write(db,ident,data,actor):
    cluster=db.scalar(select(FailoverCluster).where(FailoverCluster.id==ident).with_for_update())
    if cluster is None: raise HTTPException(404,'主备集群不存在')
    ledger=dict(cluster.ledger or {})
    accepted=False;code=None;reason=None
    if data.key not in ledger:
        code,reason=('unknown_key',f'账本不存在键 {data.key}；写入被拒绝。')
    elif cluster.role_state==PROMOTED:
        if data.node!=cluster.active_primary:
            code,reason=('stale_primary',f'节点 {data.node} 不是当前主库（{cluster.active_primary}）。旧主回归写入被拒绝，任期停留在{cluster.current_term}。')
        elif data.term!=cluster.current_term:
            code,reason=('term_mismatch',f'节点 {data.node} 提交的任期{data.term}与当前任期{cluster.current_term}不符，写入被拒绝。')
        else: accepted=True
    else:
        if data.node==cluster.standby_name:
            code,reason=('standby_readonly',f'节点 {data.node} 是备库，备库只读；写入只能提交到主库 {cluster.active_primary}。')
        elif data.node!=cluster.active_primary:
            code,reason=('not_primary',f'节点 {data.node} 不是当前主库（{cluster.active_primary}）。')
        elif not cluster.writes_open:
            code,reason=('writes_closed','任务已阻止或恢复未完成，写入保持关闭；先补齐并恢复原任务。')
        elif data.term!=cluster.current_term:
            code,reason=('term_mismatch',f'节点 {data.node} 提交的任期{data.term}与当前任期{cluster.current_term}不符，写入被拒绝。')
        else: accepted=True
    if accepted:
        ledger[data.key]=data.value
        cluster.ledger=ledger;cluster.ledger_sha256=_sha(ledger)
    write=FailoverWrite(cluster_id=cluster.id,node=data.node,term=data.term,key=data.key,value=data.value,
        accepted=accepted,reason_code=code or 'accepted',reason=reason or f'写入已接受；账本 {data.key}={data.value}，当前任期{cluster.current_term}。',
        ledger_after=ledger if accepted else dict(cluster.ledger or {}),actor=actor)
    db.add(write);db.flush();db.commit();db.refresh(write)
    out=write_payload(write);out['ledger']=cluster.ledger;out['ledger_sha256']=cluster.ledger_sha256
    if not accepted:
        raise HTTPException(409,{'message':reason,'reason_code':code,'write':out})
    return out

def export_cluster(db,ident):
    detail=cluster_detail(db,ident)
    detail['exported_at']=_now().isoformat()+'Z'
    detail['boundary']='主备切换教学模型导出：包含位置、阻止原因、重放/撤销轨迹与恢复账本。'
    return detail
