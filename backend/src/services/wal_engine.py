"""Quiescent-checkpoint WAL teaching model with strict write locks and redo/undo."""
import copy,hashlib,json,re

def validate(initial,records):
    if not initial or len(initial)>30 or not records or len(records)>200: raise ValueError('需1—30个键及1—200条日志')
    for key,value in initial.items():
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,50}',key) or isinstance(value,bool) or not isinstance(value,int) or abs(value)>1000000000:
            raise ValueError('初始键为字母数字下划线短横线，值为绝对值不超过10亿的整数')
    state=copy.deepcopy(initial); active={};used=set();locks={}
    for lsn,r in enumerate(records,1):
        op=r.get('op');tx=r.get('tx')
        if op=='checkpoint':
            if active:raise ValueError(f'LSN {lsn}: 仅支持无活动事务的静止检查点')
            continue
        if not tx:raise ValueError(f'LSN {lsn}: 需要事务标识')
        if op=='begin':
            if tx in used:raise ValueError(f'LSN {lsn}: 事务标识不得重复')
            active[tx]=[];used.add(tx)
        elif op=='write':
            if tx not in active:raise ValueError(f'LSN {lsn}: 写入事务尚未begin')
            key=r.get('key');value=r.get('value')
            if key not in initial:raise ValueError(f'LSN {lsn}: 写入键必须存在于初始账本')
            if isinstance(value,bool) or not isinstance(value,int) or abs(value)>1000000000:raise ValueError(f'LSN {lsn}: 数值超限')
            if key in locks and locks[key]!=tx:raise ValueError(f'LSN {lsn}: 违反严格写锁，活动事务持有同键')
            locks[key]=tx;active[tx].append((key,state[key]));state[key]=value
        elif op in ('commit','abort'):
            if tx not in active:raise ValueError(f'LSN {lsn}: 事务未开启或已结束')
            if op=='abort':
                for key,before in reversed(active[tx]):state[key]=before
            del active[tx]
            locks={k:v for k,v in locks.items() if v!=tx}
        else:raise ValueError(f'LSN {lsn}: 未知操作')
    return True

def _apply_prefix(initial,records,upto):
    """Forward history pass: redo writes, honour aborts, remember checkpoint pages."""
    state=copy.deepcopy(initial);active={};winners=[];aborted=[];checkpoint=0;durable=copy.deepcopy(initial);trace=[];writes=[]
    for lsn,r in enumerate(records[:upto],1):
        op=r['op'];tx=r.get('tx')
        if op=='begin':active[tx]=[]
        elif op=='write':
            row={'lsn':lsn,'tx':tx,'key':r['key'],'before':state[r['key']],'after':r['value']}
            active[tx].append(row);writes.append(row);state[r['key']]=r['value']
            trace.append(dict(row,phase='redo'))
        elif op=='abort':
            for row in reversed(active[tx]):
                state[row['key']]=row['before'];trace.append(dict(row,phase='redo_abort_undo'))
            del active[tx];aborted.append(tx)
        elif op=='commit':winners.append(tx);del active[tx]
        elif op=='checkpoint':
            checkpoint=lsn;durable=copy.deepcopy(state);trace=[];writes=[];winners=[];aborted=[]
    return {'state':state,'active':active,'winners':winners,'aborted':aborted,'checkpoint':checkpoint,
            'durable':durable,'trace':trace,'writes':writes}

def _undo_rows(active):
    return sorted([w for tx in active for w in active[tx]],key=lambda w:w['lsn'],reverse=True)

def _state_at(initial,records,upto):
    """Visible ledger at a log position: redo prefix, undo transactions unfinished there."""
    part=_apply_prefix(initial,records,upto);state=copy.deepcopy(part['state'])
    for row in _undo_rows(part['active']):state[row['key']]=row['before']
    return state

def recover(initial,records,crash_after):
    validate(initial,records)
    if not isinstance(crash_after,int) or not 0<=crash_after<=len(records):raise ValueError('崩溃点超出日志范围')
    part=_apply_prefix(initial,records,crash_after)
    state=part['state'];durable=part['durable'];checkpoint=part['checkpoint'];winners=part['winners'];aborted=part['aborted'];trace=part['trace'];writes=part['writes']
    # Simulated crash durable pages are exactly the last quiescent checkpoint.
    # Recovery repeats history after that point, then undoes all unfinished writes.
    before_undo=copy.deepcopy(state);losers=sorted(part['active'])
    undo=_undo_rows(part['active'])
    for row in undo:
        state[row['key']]=row['before'];trace.append(dict(row,phase='undo_loser'))
    # Independent winner-only oracle from durable checkpoint excludes aborted/unfinished transactions.
    oracle=copy.deepcopy(durable)
    for row in writes:
        if row['tx'] in winners:oracle[row['key']]=row['after']
    checksum=hashlib.sha256(json.dumps(state,sort_keys=True).encode()).hexdigest()
    source=json.dumps({'initial':initial,'records':records,'crash_after':crash_after},sort_keys=True)
    return {'simulation':True,'model_version':'strict-wal-quiescent-v1','crash_after':crash_after,'checkpoint_lsn':checkpoint,
            'durable_checkpoint':durable,'after_redo':before_undo,'recovered':state,'oracle':oracle,'invariant_passed':state==oracle,
            'committed':winners,'aborted':aborted,'losers':losers,'redo_count':len([t for t in trace if t['phase']=='redo']),
            'undo_count':len(undo),'trace':trace,'result_sha256':checksum,'input_sha256':hashlib.sha256(source.encode()).hexdigest(),
            'boundary':'Python WAL教学模型；严格写锁、静止检查点、整数键值、日志前缀持久。没有注入真实PostgreSQL崩溃，未执行数据库物理恢复。'}

def failover_recover(initial,records,confirmed_commit_lsn,standby_received_lsn,standby_replayed_lsn):
    """Standby takeover: replay received-but-not-replayed logs, then undo losers.

    The standby may already have applied a prefix (standby_replayed_lsn), but a
    takeover must consume the whole received prefix before writes reopen. Any
    transaction still active at standby_received_lsn is an uncommitted tail and
    is undone in LSN-descending order.
    """
    validate(initial,records)
    n=len(records)
    for label,value in (('已确认提交位置',confirmed_commit_lsn),('备库接收位置',standby_received_lsn),('备库重放位置',standby_replayed_lsn)):
        if not isinstance(value,int) or isinstance(value,bool) or not 0<=value<=n:raise ValueError(f'{label}超出日志范围')
    if standby_replayed_lsn>standby_received_lsn:raise ValueError('备库重放位置不得领先接收位置')
    if confirmed_commit_lsn and records[confirmed_commit_lsn-1].get('op')!='commit':
        raise ValueError('已确认提交位置必须落在commit日志上')

    pre=_apply_prefix(initial,records,standby_replayed_lsn)
    full=_apply_prefix(initial,records,standby_received_lsn)
    state=copy.deepcopy(full['state']);before_undo=copy.deepcopy(state)
    losers=sorted(full['active'])
    undo=_undo_rows(full['active'])
    trace=[]
    for row in full['trace']:
        item=dict(row);item['replayed_during_promotion']=row['lsn']>standby_replayed_lsn;trace.append(item)
    for row in undo:
        state[row['key']]=row['before']
        trace.append(dict(row,phase='undo_loser',replayed_during_promotion=row['lsn']>standby_replayed_lsn))

    oracle=copy.deepcopy(full['durable'])
    for row in full['writes']:
        if row['tx'] in full['winners']:oracle[row['key']]=row['after']

    # Every commit at or before the confirmed position must exist on the standby.
    committed=[]
    for lsn,r in enumerate(records[:standby_received_lsn],1):
        if r.get('op')=='commit':committed.append(r.get('tx'))
    confirmed_committed=[r.get('tx') for r in records[:confirmed_commit_lsn] if r.get('op')=='commit']
    missing=[tx for tx in confirmed_committed if tx not in committed]
    confirmed_state=_state_at(initial,records,confirmed_commit_lsn)
    result_sha256=hashlib.sha256(json.dumps({'recovered':state,'trace':trace,'losers':losers},sort_keys=True).encode()).hexdigest()
    source=json.dumps({'initial':initial,'records':records,'confirmed_commit_lsn':confirmed_commit_lsn,
                       'standby_received_lsn':standby_received_lsn,'standby_replayed_lsn':standby_replayed_lsn},sort_keys=True)
    return {'simulation':True,'model_version':'strict-wal-failover-v1',
            'confirmed_commit_lsn':confirmed_commit_lsn,'standby_received_lsn':standby_received_lsn,
            'standby_replayed_lsn':standby_replayed_lsn,'replay_lag':standby_received_lsn-standby_replayed_lsn,
            'checkpoint_lsn':full['checkpoint'],'durable_checkpoint':full['durable'],
            'standby_applied_before_replay':pre['state'],'before_replay_visible':_state_at(initial,records,standby_replayed_lsn),
            'after_redo':before_undo,'recovered':state,'oracle':oracle,'invariant_passed':state==oracle,
            'confirmed_state':confirmed_state,'committed':full['winners'],'committed_all':committed,
            'confirmed_committed_transactions':confirmed_committed,'missing_confirmed_transactions':missing,
            'confirmed_data_preserved':not missing,'aborted':full['aborted'],'losers':losers,
            'redo_count':len([t for t in trace if t['phase']=='redo']),
            'newly_replayed_count':len([t for t in trace if t['phase']=='redo' and t['replayed_during_promotion']]),
            'undo_count':len(undo),'trace':trace,'writes_opened_after_replay':True,
            'result_sha256':result_sha256,'input_sha256':hashlib.sha256(source.encode()).hexdigest(),
            'boundary':'Python主备切换教学模型；只演算日志位置、重放与撤销，不复制真实数据页，不连接真实备库。'}

state_at=_state_at
