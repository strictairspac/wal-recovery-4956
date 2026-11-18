from pydantic import BaseModel, Field
class FailoverClusterInput(BaseModel):
    name: str=Field(min_length=2,max_length=100)
    log_set_id: int=Field(gt=0)
    primary_name: str=Field(min_length=1,max_length=60,pattern=r'^[A-Za-z0-9_.-]+$')
    standby_name: str=Field(min_length=1,max_length=60,pattern=r'^[A-Za-z0-9_.-]+$')
    confirmed_commit_lsn: int=Field(ge=0,le=200)
    standby_received_lsn: int=Field(ge=0,le=200)
    standby_replayed_lsn: int=Field(ge=0,le=200)
class PositionsInput(BaseModel):
    confirmed_commit_lsn: int | None=Field(default=None,ge=0,le=200)
    standby_received_lsn: int | None=Field(default=None,ge=0,le=200)
    standby_replayed_lsn: int | None=Field(default=None,ge=0,le=200)
class CatchUpInput(BaseModel):
    target_received_lsn: int | None=Field(default=None,ge=0,le=200)
    target_replayed_lsn: int | None=Field(default=None,ge=0,le=200)
class PromoteInput(BaseModel):
    request_key: str=Field(min_length=2,max_length=80,pattern=r'^[A-Za-z0-9_-]+$')
class FailoverWriteInput(BaseModel):
    node: str=Field(min_length=1,max_length=60,pattern=r'^[A-Za-z0-9_.-]+$')
    term: int=Field(ge=0,le=1000000)
    key: str=Field(min_length=1,max_length=50,pattern=r'^[A-Za-z0-9_-]+$')
    value: int=Field(ge=-1000000000,le=1000000000)
