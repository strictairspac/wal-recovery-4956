import {logSetsAPI,crashCasesAPI,recoveriesAPI,failoverAPI} from '../api/entities.js';
export const store={logs:[],cases:[],runs:[],clusters:[],async refresh(){[this.logs,this.cases,this.runs,this.clusters]=await Promise.all([logSetsAPI.list(),crashCasesAPI.list(),recoveriesAPI.list(),failoverAPI.list()]);}};
