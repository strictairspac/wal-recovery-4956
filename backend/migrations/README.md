# 初始化结构
SQLAlchemy metadata.create_all首次建users、log_sets、crash_cases、recoveries、audits、failover_clusters、failover_attempts、failover_writes。log_sets.name、crash_cases.name、failover_clusters.name唯一，关联使用外键，入队冻结日志和崩溃点快照。

## 主备切换表
- failover_clusters：主备节点名、已确认提交位置/备库接收位置/备库重放位置、角色（stable/blocked/promoted）、原任务状态、active_primary、current_term、writes_open、恢复账本ledger及SHA256。`ck_failover_promoted_ledger`约束保证role_state=promoted时ledger非空，角色与账本同一事务落库。
- failover_attempts：每次升主/补齐请求一行，(cluster_id,request_key)唯一保证同键重试幂等，(cluster_id,new_term)唯一保证并发升主只产生一个任期；记录阻止原因、风险明细、重做/撤销轨迹与摘要。
- failover_writes：接受或拒绝的应用写入，保留旧主迟到写入与任期不符的证据。

暂无在线schema升级器；已有库需重建或手工补建上述表。
