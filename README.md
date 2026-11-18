# 海砾 · 采样账本 WAL 恢复演练
海砾计量数据保障组（8人）用可重放日志替代恢复演练时手工拼接的SQL片段和账本截图。

## 场景与模块
上游为事务日志样例，下游为值班复核人。原流程人工标记提交、表格算恢复余额、截图写结论；痛点是崩溃点不清、撤销顺序容易错、恢复证据缺失。模块为事务日志集 / 故障注入点 / 恢复队列 / 恢复证据 / 主备切换演练，实体LogSet/CrashCase/Recovery/FailoverCluster贯穿全栈。主库断电后，单份日志无法判断备库能否安全接管，因此新增主备切换演练。

**Python WAL教学模型，不注入真实PostgreSQL故障，不执行数据库物理恢复。** PostgreSQL真实保存业务输入、冻结快照、任务和结果，Redis实际用于worker互斥，后台Python实际演算恢复历史。

## 启动
```bash
cp .env.example .env
docker compose up -d --build
# http://localhost:18246
# http://localhost:19246/docs
docker compose exec -T backend python -m scripts.seed_demo
docker compose down --remove-orphans
```
SEED_DEMO=true时启动自动执行种子；自然键查重，不覆盖用户修改。初始化失败显式阻止启动，不隐藏结构错误。删除数据需显式down -v。

|账号|密码|权限|典型动作|
|---|---|---|---|
|admin@haili.local|Recovery2026!|读写/导出/审计|查询审计记录|
|operator@haili.local|Recovery2026!|新增日志/场景/运行/导出|设置断电点10|
|viewer@haili.local|Recovery2026!|只读/导出|核对恢复账本|

## 一天的复核剧本
09:00操作员登录；09:05查看海砾采样账本-晚班（A100、B50）；09:10复制日志集保存shift-20261117；09:15在第10条日志后注入断电点；09:20提交恢复；09:25核对检查点LSN4、已提交T3、未提交T2两条写入逆序撤销，最终A60/B50；09:30下载JSON；09:40观察员核对且写操作403；10:00管理员查看/api/audits。

断电接管剧本：11:00主库断电，打开“05 主备切换演练”，登记已确认提交位置9、备库接收7、备库重放7，页面立即显示升主阻止与丢失事务T3；11:05在切换台把接收补齐到9、重放补齐到9，原任务恢复、写入重开；11:10升主，系统先重放已接收未重放日志、再撤销未提交尾部，然后才开放写入，任期变为1；11:15用旧主节点名提交一次写入，被拒绝并记入“旧主迟到写入”；11:20导出切换账本核对阻止原因、重做/撤销轨迹与恢复账本。重放落后（接收11、重放9）不会阻止升主，但升主时会先补重放；同一请求标识重试不重复升主，并发升主只产生一个任期。

## 算法、格式和失败态
日志为JSON数组，操作为begin/write/commit/abort/checkpoint；write包含tx/key/value，其他事务操作包含tx，checkpoint不带tx。键必须在初始整数账本存在。事务ID不可复用；严格写锁直到commit/abort释放，同键重叠活动事务拒绝；检查点仅在无活动事务时允许。最多200条日志/30键/整数绝对值不超过10亿。

主备切换：集群绑定日志集，记录已确认提交位置（必须是commit的LSN）、备库接收位置与备库重放位置（不得领先接收位置）。已确认提交位置超过接收位置时升主被阻止，返回reason_code=insufficient_received，列出丢失的已提交事务、涉及键与两侧账本，写入保持关闭；补齐后原任务恢复为resumed并重开写入。升主时先重放接收但未重放的日志，再按LSN倒序撤销未提交尾部，之后才开放写入；角色改为promoted与恢复账本在同一事务提交，数据库约束禁止promoted缺少ledger。每次升主请求带request_key：同键重试幂等返回原任期，其他请求返回already_promoted，并发升主通过行锁+任期唯一约束+条件更新只产生一个任期。升主后旧主回归写入返回stale_primary，任期不符返回term_mismatch，拒绝记录留档；promoted集群的位置配置与补齐全被拒绝。主备模型只演算日志位置、重放和撤销，不复制真实数据页。

crash_after=N表示前N条日志已持久化。最近静止检查点保存全部数据页，检查点后数据页假定均未写回（no-force）。恢复重放日志写入和已abort的撤销，再按LSN倒序撤销未结束事务。独立oracle从检查点账本仅应用已提交事务写入，结果须相同。输出重做/撤销轨迹、事务集合、各阶段账本、SHA256和模型版本。空输入、未知键、事务顺序非法、检查点有活动事务、写锁冲突、崩溃点超限均拒绝。模型不涉及页LSN、CLR、模糊检查点或真实磁盘故障；不是完整ARIES实现。

## 工程与开发
前端原生HTML/CSS/JS+Nginx1.27；后端Python3.11+FastAPI+SQLAlchemy+PyJWT；PostgreSQL16和Redis7；5服务均healthcheck。持久任务队列在PG，单worker通过Redis锁互斥。20条待执行配额，审计记录写入操作，JWT8小时、PBKDF2随机盐密码。后端路由不含/api，Nginx/api/去前缀代理。

目录frontend/src/{api,stores,types,components/common,hooks,pages,router,utils,constants}与backend/src/{routes,controllers,services,models,schemas,middlewares,utils,constants,config}。本地开发安装backend/requirements.txt并设DATABASE_URL/REDIS_URL/JWT_SECRET，再运行uvicorn src.main:app；前端需代理/api。migrations记录初始化结构，暂无在线升级器。

环境COMPOSE_PROJECT_NAME控制命名，FRONTEND_PORT/BACKEND_PORT/DB_PORT为18246/19246/17746，DB_PASSWORD控制演示数据库密码，JWT_SECRET签名密钥，SEED_DEMO控制种子（含两个主备演练集群：接收落后、重放落后）。命名卷db-data/redis-data/artifacts；共享artifacts预留，导出由冻结快照即时生成。Google Fonts需外网，业务离线可运行。默认凭据仅本地演示。新增接口：GET/POST /api/failover-clusters，GET /api/failover-clusters/{id}，PATCH …/positions，POST …/catch-up、…/promote、…/writes，GET …/export；升主与写入请求需操作员或管理员，观察员可查看与导出。自动化验证：`python -m pytest backend/tests -q`（SQLite临时库，覆盖接收落后、重放落后、未提交尾部、并发升主、旧主迟到写入与旧功能回归）。

共享枚举roles与任务states定义于backend/src/constants/enums.py、frontend/src/constants/enums.js，状态使用models/entities.py/services/recovery_service.py/worker.py/components/common/status.js；日志操作类型在schemas/log_set.py和pages/logs.js中声明/说明。MIT许可。
