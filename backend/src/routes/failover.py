import json
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from src.config.database import get_db
from src.middlewares.auth import current_user, writable
from src.schemas.failover import FailoverClusterInput, PositionsInput, CatchUpInput, PromoteInput, FailoverWriteInput
from src.services import failover_service as service
from src.controllers.actions import audit
router=APIRouter()
@router.get('/failover-clusters')
def listing(db=Depends(get_db),user=Depends(current_user)):return service.list_clusters(db)
@router.post('/failover-clusters',status_code=201)
def create(data:FailoverClusterInput,db=Depends(get_db),user=Depends(writable)):
    out=service.create_cluster(db,data);audit(db,user,f'failover:create:{out["id"]}');db.commit();return out
@router.get('/failover-clusters/{ident}')
def detail(ident:int,db=Depends(get_db),user=Depends(current_user)):return service.cluster_detail(db,ident)
@router.patch('/failover-clusters/{ident}/positions')
def positions(ident:int,data:PositionsInput,db=Depends(get_db),user=Depends(writable)):
    out=service.configure_positions(db,ident,data);audit(db,user,f'failover:positions:{ident}');db.commit();return out
@router.post('/failover-clusters/{ident}/catch-up')
def catch_up(ident:int,data:CatchUpInput,db=Depends(get_db),user=Depends(writable)):
    try: out=service.catch_up(db,ident,data,user.email)
    except HTTPException:
        audit(db,user,f'failover:catch-up:{ident}:rejected');db.commit();raise
    audit(db,user,f'failover:catch-up:{ident}:{out["attempt"]["status"]}');db.commit();return out
@router.post('/failover-clusters/{ident}/promote')
def promote(ident:int,data:PromoteInput,db=Depends(get_db),user=Depends(writable)):
    try: out=service.promote(db,ident,data,user.email)
    except HTTPException:
        audit(db,user,f'failover:promote:{ident}:{data.request_key}:rejected');db.commit();raise
    audit(db,user,f'failover:promote:{ident}:{data.request_key}:{out.get("new_term")}');db.commit();return out
@router.post('/failover-clusters/{ident}/writes',status_code=201)
def write(ident:int,data:FailoverWriteInput,db=Depends(get_db),user=Depends(writable)):
    try: out=service.record_write(db,ident,data,user.email)
    except HTTPException:
        audit(db,user,f'failover:write:{ident}:{data.node}:rejected');db.commit();raise
    audit(db,user,f'failover:write:{ident}:{data.node}:accepted');db.commit();return out
@router.get('/failover-clusters/{ident}/export')
def export(ident:int,db=Depends(get_db),user=Depends(current_user)):
    payload=service.export_cluster(db,ident)
    return Response(json.dumps(payload,ensure_ascii=False,indent=2),media_type='application/json',
                    headers={'Content-Disposition':f'attachment; filename="failover-{ident}.json"'})
