import {api} from './client.js';
export const logSetsAPI={list:()=>api('/log-sets'),create:data=>api('/log-sets',{method:'POST',body:JSON.stringify(data)})};
export const crashCasesAPI={list:()=>api('/crash-cases'),create:data=>api('/crash-cases',{method:'POST',body:JSON.stringify(data)})};
export const recoveriesAPI={list:()=>api('/recoveries'),get:id=>api('/recoveries/'+id),create:data=>api('/recoveries',{method:'POST',body:JSON.stringify(data)})};
export const failoverAPI={
 list:()=>api('/failover-clusters'),
 get:id=>api('/failover-clusters/'+id),
 create:data=>api('/failover-clusters',{method:'POST',body:JSON.stringify(data)}),
 positions:(id,data)=>api('/failover-clusters/'+id+'/positions',{method:'PATCH',body:JSON.stringify(data)}),
 catchUp:(id,data)=>api('/failover-clusters/'+id+'/catch-up',{method:'POST',body:JSON.stringify(data)}),
 promote:(id,data)=>api('/failover-clusters/'+id+'/promote',{method:'POST',body:JSON.stringify(data)}),
 write:(id,data)=>api('/failover-clusters/'+id+'/writes',{method:'POST',body:JSON.stringify(data)}),
};
