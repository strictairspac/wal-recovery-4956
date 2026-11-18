from datetime import datetime, timezone
from sqlalchemy import String, Integer, JSON, DateTime, ForeignKey, Boolean, CheckConstraint, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from src.config.database import Base

def now(): return datetime.now(timezone.utc).replace(tzinfo=None)
class User(Base):
    __tablename__='users'
    id: Mapped[int]=mapped_column(primary_key=True)
    email: Mapped[str]=mapped_column(String(120),unique=True)
    role: Mapped[str]=mapped_column(String(20))
    password_hash: Mapped[str]=mapped_column(String(250))
class LogSet(Base):
    __tablename__='log_sets'
    id: Mapped[int]=mapped_column(primary_key=True)
    name: Mapped[str]=mapped_column(String(100),unique=True)
    initial: Mapped[dict]=mapped_column(JSON)
    records: Mapped[list]=mapped_column(JSON)
    description: Mapped[str]=mapped_column(String(500))
class CrashCase(Base):
    __tablename__='crash_cases'
    id: Mapped[int]=mapped_column(primary_key=True)
    log_set_id: Mapped[int]=mapped_column(ForeignKey('log_sets.id'))
    name: Mapped[str]=mapped_column(String(100),unique=True)
    crash_after: Mapped[int]=mapped_column(Integer)
class Recovery(Base):
    __tablename__='recoveries'
    id: Mapped[int]=mapped_column(primary_key=True)
    crash_case_id: Mapped[int]=mapped_column(ForeignKey('crash_cases.id'))
    label: Mapped[str]=mapped_column(String(120))
    status: Mapped[str]=mapped_column(String(20),default='queued')
    snapshot: Mapped[dict]=mapped_column(JSON)
    result: Mapped[dict | None]=mapped_column(JSON,nullable=True)
    created_at: Mapped[datetime]=mapped_column(DateTime,default=now)
class Audit(Base):
    __tablename__='audits'
    id: Mapped[int]=mapped_column(primary_key=True)
    actor: Mapped[str]=mapped_column(String(120))
    action: Mapped[str]=mapped_column(String(120))
    created_at: Mapped[datetime]=mapped_column(DateTime,default=now)

class FailoverCluster(Base):
    """Primary/standby drill cluster: positions, term and the recovery ledger."""
    __tablename__='failover_clusters'
    __table_args__=(
        CheckConstraint("role_state <> 'promoted' OR ledger IS NOT NULL",name='ck_failover_promoted_ledger'),
        CheckConstraint("current_term >= 0",name='ck_failover_term_nonnegative'),
    )
    id: Mapped[int]=mapped_column(primary_key=True)
    name: Mapped[str]=mapped_column(String(100),unique=True)
    log_set_id: Mapped[int]=mapped_column(ForeignKey('log_sets.id'))
    primary_name: Mapped[str]=mapped_column(String(60))
    standby_name: Mapped[str]=mapped_column(String(60))
    confirmed_commit_lsn: Mapped[int]=mapped_column(Integer,default=0)
    standby_received_lsn: Mapped[int]=mapped_column(Integer,default=0)
    standby_replayed_lsn: Mapped[int]=mapped_column(Integer,default=0)
    role_state: Mapped[str]=mapped_column(String(20),default='stable')
    task_state: Mapped[str]=mapped_column(String(20),default='original')
    active_primary: Mapped[str]=mapped_column(String(60))
    current_term: Mapped[int]=mapped_column(Integer,default=0)
    writes_open: Mapped[bool]=mapped_column(Boolean,default=True)
    writes_open_lsn: Mapped[int | None]=mapped_column(Integer,nullable=True)
    ledger: Mapped[dict | None]=mapped_column(JSON,nullable=True)
    ledger_sha256: Mapped[str | None]=mapped_column(String(64),nullable=True)
    promotion_attempt_id: Mapped[int | None]=mapped_column(Integer,nullable=True)
    promoted_at: Mapped[datetime | None]=mapped_column(DateTime,nullable=True)
    resumed_at: Mapped[datetime | None]=mapped_column(DateTime,nullable=True)
    created_at: Mapped[datetime]=mapped_column(DateTime,default=now)

class FailoverAttempt(Base):
    """Every promote request is an idempotent attempt, including blocked ones."""
    __tablename__='failover_attempts'
    __table_args__=(
        UniqueConstraint('cluster_id','request_key',name='uq_failover_attempt_request'),
        UniqueConstraint('cluster_id','new_term',name='uq_failover_attempt_term'),
    )
    id: Mapped[int]=mapped_column(primary_key=True)
    cluster_id: Mapped[int]=mapped_column(ForeignKey('failover_clusters.id'))
    request_key: Mapped[str]=mapped_column(String(80))
    status: Mapped[str]=mapped_column(String(20))
    reason_code: Mapped[str]=mapped_column(String(60))
    reason: Mapped[str]=mapped_column(String(500))
    risk: Mapped[dict | None]=mapped_column(JSON,nullable=True)
    confirmed_commit_lsn: Mapped[int]=mapped_column(Integer)
    standby_received_lsn: Mapped[int]=mapped_column(Integer)
    standby_replayed_lsn: Mapped[int]=mapped_column(Integer)
    gap: Mapped[int]=mapped_column(Integer,default=0)
    replay_lag: Mapped[int]=mapped_column(Integer,default=0)
    new_term: Mapped[int | None]=mapped_column(Integer,nullable=True)
    active_primary: Mapped[str | None]=mapped_column(String(60),nullable=True)
    ledger: Mapped[dict | None]=mapped_column(JSON,nullable=True)
    ledger_sha256: Mapped[str | None]=mapped_column(String(64),nullable=True)
    redo_count: Mapped[int]=mapped_column(Integer,default=0)
    undo_count: Mapped[int]=mapped_column(Integer,default=0)
    losers: Mapped[list | None]=mapped_column(JSON,nullable=True)
    trace: Mapped[list | None]=mapped_column(JSON,nullable=True)
    checkpoint_lsn: Mapped[int]=mapped_column(Integer,default=0)
    writes_opened_lsn: Mapped[int | None]=mapped_column(Integer,nullable=True)
    writes_opened_after_replay: Mapped[bool]=mapped_column(Boolean,default=False)
    input_sha256: Mapped[str | None]=mapped_column(String(64),nullable=True)
    result_sha256: Mapped[str | None]=mapped_column(String(64),nullable=True)
    actor: Mapped[str]=mapped_column(String(120))
    created_at: Mapped[datetime]=mapped_column(DateTime,default=now)
    updated_at: Mapped[datetime]=mapped_column(DateTime,default=now,onupdate=now)

class FailoverWrite(Base):
    """Accepted or rejected application write, kept for stale-primary evidence."""
    __tablename__='failover_writes'
    id: Mapped[int]=mapped_column(primary_key=True)
    cluster_id: Mapped[int]=mapped_column(ForeignKey('failover_clusters.id'))
    node: Mapped[str]=mapped_column(String(60))
    term: Mapped[int]=mapped_column(Integer)
    key: Mapped[str]=mapped_column(String(50))
    value: Mapped[int]=mapped_column(Integer)
    accepted: Mapped[bool]=mapped_column(Boolean,default=False)
    reason_code: Mapped[str]=mapped_column(String(60))
    reason: Mapped[str]=mapped_column(String(500))
    ledger_after: Mapped[dict | None]=mapped_column(JSON,nullable=True)
    actor: Mapped[str]=mapped_column(String(120))
    created_at: Mapped[datetime]=mapped_column(DateTime,default=now)
