/** @typedef {{id:number,name:string,initial:object,records:object[],description:string}} LogSet */
/** @typedef {{id:number,name:string,log_set_id:number,crash_after:number}} CrashCase */
/** @typedef {{id:number,crash_case_id:number,label:string,status:string,snapshot:object,result:object|null}} Recovery */
/** @typedef {{id:number,name:string,log_set_id:number,primary_name:string,standby_name:string,confirmed_commit_lsn:number,standby_received_lsn:number,standby_replayed_lsn:number,role_state:string,task_state:string,current_term:number,ledger:object|null}} FailoverCluster */
export {};
