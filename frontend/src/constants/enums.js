export const ROLES = {admin:'管理员',operator:'操作员',viewer:'观察员'};
export const DRILL_STATUSES = {queued:'等待执行',running:'执行中',completed:'已完成',failed:'失败'};
export const FAILOVER_ROLE_STATES = {stable:'主备稳定',blocked:'升主阻止',promoted:'已升主'};
export const FAILOVER_TASK_STATES = {original:'原任务运行',blocked:'原任务暂停',resumed:'补齐后恢复',promoted:'备库接管'};
export const FAILOVER_ATTEMPT_STATUSES = {blocked:'已阻止',resumed:'补齐恢复',promoted:'升主成功',rejected:'拒绝重复升主'};
export const WRITE_REASONS = {accepted:'已接受',stale_primary:'旧主迟到写入',term_mismatch:'任期不符',standby_readonly:'备库只读',unknown_key:'未知键',writes_closed:'写入未开放'};
