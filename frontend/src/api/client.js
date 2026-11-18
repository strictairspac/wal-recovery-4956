import {session} from '../stores/session.js';
export async function api(path, options={}) {
 const response=await fetch('/api'+path,{...options,headers:{'Content-Type':'application/json','Authorization':'Bearer '+session.token,...options.headers}});
 if(!response.ok){const data=await response.json().catch(()=>({}));if(response.status===401 && path!='/auth/login'){session.clear();location.hash='#login';}
  const detail=data.detail;
  const error=new Error(typeof detail==='string'?detail:(detail&&detail.message)||JSON.stringify(detail||'请求失败'));
  if(detail&&typeof detail==='object'){error.code=detail.reason_code||detail.code;error.payload=detail;}
  error.status=response.status;throw error;}
 return response.json();
}
export async function download(id){const r=await fetch('/api/recoveries/'+id+'/export',{headers:{Authorization:'Bearer '+session.token}});if(!r.ok)throw new Error('导出失败');const u=URL.createObjectURL(await r.blob());const a=document.createElement('a');a.href=u;a.download='recovery-'+id+'.json';a.click();setTimeout(()=>URL.revokeObjectURL(u),1000);}
export async function downloadFailover(id){const r=await fetch('/api/failover-clusters/'+id+'/export',{headers:{Authorization:'Bearer '+session.token}});if(!r.ok)throw new Error('导出失败');const u=URL.createObjectURL(await r.blob());const a=document.createElement('a');a.href=u;a.download='failover-'+id+'.json';a.click();setTimeout(()=>URL.revokeObjectURL(u),1000);}
