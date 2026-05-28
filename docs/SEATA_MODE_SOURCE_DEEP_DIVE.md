# Seata 各事务模式源码级实现深度剖析

> 本文在 [AT](./SEATA_MODE_AT.md)、[TCC](./SEATA_MODE_TCC.md)、[XA](./SEATA_MODE_XA.md)、[Saga](./SEATA_MODE_SAGA.md) 基础上，从 **调用栈、关键方法、分支条件、SPI 路由** 四个维度做源码级补充。  
> 包名默认：`org.apache.seata`（`compatible` 模块提供 `io.seata` 兼容层，逻辑一致）。

---

## 目录

1. [四种模式的 TC 路由入口](#1-四种模式的-tc-路由入口)
2. [RM 公共分支注册 RPC 链](#2-rm-公共分支注册-rpc-链)
3. [AT 模式源码剖析](#3-at-模式源码剖析)
4. [TCC 模式源码剖析](#4-tcc-模式源码剖析)
5. [XA 模式源码剖析](#5-xa-模式源码剖析)
6. [Saga 模式源码剖析](#6-saga-模式源码剖析)
7. [二阶段调度：DefaultCore 源码](#7-二阶段调度defaultcore-源码)
8. [模式对比总表](#8-模式对比总表)

---

## 1. 四种模式的 TC 路由入口

### 1.1 AbstractCore 的 SPI 加载

`DefaultCore` 构造时加载所有 `AbstractCore` 实现：

```java
// server/coordinator/DefaultCore.java
List<AbstractCore> allCore = EnhancedServiceLoader.loadAll(
    AbstractCore.class, new Class[] {RemotingServer.class}, new Object[] {remotingServer});
for (AbstractCore core : allCore) {
    CORE_MAP.put(core.getHandleBranchType(), core);
}
```

`META-INF/services/org.apache.seata.server.coordinator.AbstractCore` 注册：

- `ATCore` → `BranchType.AT`
- `TccCore` → `BranchType.TCC`
- `SagaCore` → `BranchType.SAGA`
- `XACore` → `BranchType.XA`
- `SagaAnnotationCore` → `BranchType.SAGA_ANNOTATION`

全局提交时按 **每个 BranchSession 的 branchType** 调用 `getCore(type).branchCommit()`。

### 1.2 分支注册统一模板（TC）

所有模式的 RM 最终都进入：

```java
// server/coordinator/AbstractCore.java — branchRegister()
GlobalSession globalSession = assertGlobalSessionNotNull(xid, false);
return SessionHolder.lockAndExecute(globalSession, () -> {
    globalSessionStatusCheck(globalSession);
    BranchSession branchSession = SessionHelper.newBranchByGlobal(
        globalSession, branchType, resourceId, applicationData, lockKeys, clientId);
    branchSessionLock(globalSession, branchSession);  // AT 才有实质加锁
    globalSession.addBranch(branchSession);
    return branchSession.getBranchId();
});
```

**模式差异**集中在 `branchSessionLock()` 的实现：

| Core | branchSessionLock |
|------|-------------------|
| `ATCore` | `branchSession.lock(autoCommit, skipCheckLock)` |
| `TccCore` / `XACore` / `SagaCore` | 默认空实现或仅校验 |

---

## 2. RM 公共分支注册 RPC 链

```text
AbstractResourceManager.branchRegister()
  → 构造 BranchRegisterRequest(xid, resourceId, lockKey, branchType, applicationData)
  → RmNettyRemotingClient.sendSyncRequest(request)
  → TC ServerOnRequestProcessor
  → DefaultCoordinator.onRequest(BranchRegisterRequest)
  → AbstractTCInboundHandler.handle()
  → DefaultCore.branchRegister()
```

`AbstractResourceManager`（`rm/AbstractResourceManager.java`）在注册失败时根据 `BranchRegisterResponse.transactionExceptionCode` 抛 `RmTransactionException`（如 `LockKeyConflict`）。

---

## 3. AT 模式源码剖析

### 3.1 何时进入 AT 拦截逻辑

`ExecuteTemplate.execute()` 入口判断：

```java
// rm-datasource/exec/ExecuteTemplate.java
if (!RootContext.requireGlobalLock() && BranchType.AT != RootContext.getBranchType()) {
    return statementCallback.execute(statementProxy.getTargetStatement(), args);
}
```

即：**无全局锁需求** 且 **分支类型不是 AT** 时，SQL 直通，不生成 undo。

随后 `SQLVisitorFactory.get(sql, dbType)` 解析 SQL，按 `SQLType` 分派 Executor（INSERT → `MySQLInsertExecutor` 等，由 dbType SPI 扩展）。

### 3.2 prepareUndoLog 与 lockKey

`BaseTransactionalExecutor.prepareUndoLog()`：

1. `buildLockKey(tableRecords)` — 格式 `table:pk,pk;table2:...`
2. `context.appendLockKey(lockKey)`
3. `context.appendUndoItem(new SQLUndoLog(sqlType, tableName, beforeImage, afterImage))`

`AbstractDMLBaseExecutor.executeAutoCommitFalse()` 固定顺序：

```text
beforeImage() → statementCallback.execute() → afterImage() → prepareUndoLog()
```

**UPDATE** 的 `beforeImage()` 典型实现：根据 WHERE 构造 `SELECT ... FOR UPDATE`，在**同连接**上查询（`buildTableRecords`），既拿锁键又拿前镜像。

### 3.3 ConnectionProxy 提交链（核心）

```247:281:rm-datasource/src/main/java/org/apache/seata/rm/datasource/ConnectionProxy.java
    private void processGlobalTransactionCommit() throws SQLException {
        try {
            register();
        } catch (TransactionException e) {
            recognizeLockKeyConflictException(e, context.buildLockKeys());
        }
        try {
            UndoLogManagerFactory.getUndoLogManager(this.getDbType()).flushUndoLogs(this);
            targetConnection.commit();
        } catch (Throwable ex) {
            ...
            report(false);
            throw new SQLException(ex);
        }
        if (IS_REPORT_SUCCESS_ENABLE) {
            report(true);
        }
        context.reset();
    }

    private void register() throws TransactionException {
        if (!context.hasUndoLog() || !context.hasLockKey()) {
            return;
        }
        Long branchId = DefaultResourceManager.get()
                .branchRegister(BranchType.AT, getDataSourceProxy().getResourceId(),
                    null, context.getXid(), context.getApplicationData(), context.buildLockKeys());
        context.setBranchId(branchId);
    }
```

**要点**：

- 先 **RPC 注册分支 + TC 加锁**，再写 undo、再 commit；若 commit 失败会 `report(false)`。
- 纯读或未改数据：`hasUndoLog()` 为 false 则 **不注册分支**（无 DML undo）。

`setAutoCommit(true)` 在全局事务中会先 `doCommit()`（JDBC 规范），从而触发上述流程。

### 3.4 flushUndoLogs 编码

`AbstractUndoLogManager.flushUndoLogs()`：

```text
BranchUndoLog ← ConnectionContext.getUndoItems()
undoLogContent = UndoLogParser.encode(branchUndoLog)
可选 CompressorFactory.compress
INSERT INTO undo_log (xid, branch_id, context, rollback_info, ...)
```

`context` 列存 serializer 名、compressor 类型等，供回滚时解码。

### 3.5 回滚 undo 循环

```364:378:rm-datasource/src/main/java/org/apache/seata/rm/datasource/undo/AbstractUndoLogManager.java
                        List<SQLUndoLog> sqlUndoLogs = branchUndoLog.getSqlUndoLogs();
                        if (sqlUndoLogs.size() > 1) {
                            Collections.reverse(sqlUndoLogs);
                        }
                        for (SQLUndoLog sqlUndoLog : sqlUndoLogs) {
                            ...
                            AbstractUndoExecutor undoExecutor =
                                    UndoExecutorFactory.getUndoExecutor(dataSourceProxy.getDbType(), sqlUndoLog);
                            undoExecutor.executeOn(connectionProxy);
                        }
```

每条 `SQLUndoLog` 经 `dataValidation` 比对当前行与 afterImage，防止脏回滚。

**无 undo 行**（一阶段未落库）：`insertUndoLogWithGlobalFinished()` 写防御状态（#489）。

### 3.6 AT 二阶段：AsyncWorker

`DataSourceManager.branchCommit()` → `AsyncWorker.branchCommit()`：

- 同步返回 `PhaseTwo_Committed`
- 后台 `doBranchCommit()` 按 resourceId 分组 `batchDeleteUndoLog`

TC 侧 `DefaultCore.commit()` 若 `globalSession.canBeCommittedAsync()` → `asyncCommit()`，定时任务里 `doGlobalCommit(session, true)` **跳过** `canBeCommittedAsync()` 的分支。

### 3.7 ATCore 锁与 applicationData

```java
// server/transaction/at/ATCore.java — branchSessionLock()
Map<String, Object> data = objectMapper.readValue(applicationData, HashMap.class);
// AUTO_COMMIT, SKIP_CHECK_LOCK
if (!branchSession.lock(autoCommit, skipCheckLock)) {
    throw new BranchTransactionException(LockKeyConflict, ...);
}
```

`ConnectionContext.getApplicationData()` 在一阶段把 `GlobalLockConfig`、autocommit 变更写入 JSON 传给 TC。

---

## 4. TCC 模式源码剖析

### 4.1 拦截入口

```java
// tcc/.../TccActionInterceptorHandler.java
if (!RootContext.inGlobalTransaction() || RootContext.inSagaBranch()) {
    return invocation.proceed();  // 非全局事务或 Saga 子分支内不拦
}
RootContext.bindBranchType(BranchType.TCC);
return actionInterceptorHandler.proceed(method, arguments, xid, param, callback);
```

### 4.2 Try 前注册（ActionInterceptorHandler）

```67:131:integration-tx-api/src/main/java/org/apache/seata/integration/tx/api/interceptor/ActionInterceptorHandler.java
    public Object proceed(...) throws Throwable {
        ...
        String branchId = doTxActionLogStore(method, arguments, businessActionParam, actionContext);
        actionContext.setBranchId(branchId);
        try {
            BusinessActionContextUtil.setContext(actionContext);
            doBeforeTccPrepare(...);
            if (businessActionParam.getUseCommonFence()) {
                return DefaultCommonFenceHandler.get()
                    .prepareFence(xid, Long.valueOf(branchId), actionName, targetCallback);
            } else {
                return targetCallback.execute();  // 真正的 Try 方法
            }
        } finally {
            doAfterTccPrepare(...);
            BusinessActionContextUtil.reportContext(actionContext);
            ...
        }
    }
```

`doTxActionLogStore()` 核心：

```java
Map<String, Object> applicationContext = Collections.singletonMap(Constants.TX_ACTION_CONTEXT, context);
String applicationContextStr = JsonUtil.toJSONString(applicationContext);
Long branchId = DefaultResourceManager.get().branchRegister(
    businessActionParam.getBranchType(), actionName, null, xid, applicationContextStr, null);
```

**lockKeys 恒为 null**；TC 不对 TCC 走 AT 行锁。

`context` 内含：`PREPARE_METHOD`、`COMMIT_METHOD`、`ROLLBACK_METHOD`、`ACTION_NAME`、业务 `@BusinessActionContextParameter` 字段。

### 4.3 二阶段反射调用

`TCCResourceManager.branchCommit()`：

```java
TCCResource tccResource = getTCCResource(resourceId);
BusinessActionContext context = BusinessActionContextUtil.getBusinessActionContext(
    xid, branchId, resourceId, applicationData);
// useCommonFence → DefaultCommonFenceHandler.commitFence()
Object[] commitArgs = getTwoPhaseMethodParams(tccResource.getPhaseTwoCommitKeys(), context, commitMethod);
Object result = commitMethod.invoke(targetTCCBean, commitArgs);
return parseBranchStatus(result);  // boolean / TwoPhaseResult → BranchStatus
```

`branchRollback()` 对称调用 `rollbackMethod`。

### 4.4 TccCore

仅覆盖 `branchDelete()` → `super.branchRollback()`（删除分支记录 = 执行 Cancel）。

---

## 5. XA 模式源码剖析

### 5.1 分支起点：setAutoCommit(false)

```170:217:rm-datasource/src/main/java/org/apache/seata/rm/datasource/xa/ConnectionProxyXA.java
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        ...
        } else {  // autoCommit == false
            ...
            branchId = DefaultResourceManager.get()
                .branchRegister(BranchType.XA, resource.getResourceId(), null, xid, null, null);
            this.xaBranchXid = XAXidBuilder.build(xid, branchId);
            keepIfNecessary();
            start();  // xaResource.start(xaBranchXid, TMNOFLAGS)
            this.xaActive = true;
        }
    }
```

与 AT **对比**：注册发生在 **开启本地事务时**，而非 commit；**无 lockKeys、无 undo**。

### 5.2 一阶段结束：end + prepare

连接 `close()` / commit 路径调用 `end(TMSUCCESS)` → `xaResource.prepare(xaBranchXid)`：

- 成功 → `reportStatusToTC(PhaseOne_Done)` 或 `PhaseOne_RDONLY`
- 失败 → `xa end(TMFAIL)` + `xaRollback` + `PhaseOne_Failed`

`shouldBeHeld` 为 true 时连接进入 `resource.hold(xaBranchXid, this)`，供二阶段复用物理连接。

### 5.3 二阶段 ResourceManagerXA

```text
finishBranch(committed, xid, branchId, resourceId)
  → getConnectionForXAFinish(xaBranchXid)  // 从 keeper 取 ConnectionProxyXA
  → xaCommit(xaBranchXid) / xaRollback(xaBranchXid)
  → 映射 XAException.XAER_NOTA → PhaseTwo_*_XAER_NOTA_Retryable
```

### 5.4 TC：XA 只读分支

`DefaultCore.doGlobalCommit()` 对 `PhaseOne_RDONLY` 且 `BranchType.XA` 的分支 **直接 removeBranch**，不再发 commit RPC（Oracle 等只读优化）。

---

## 6. Saga 模式源码剖析

### 6.1 状态机启动与 xid 绑定

`DbAndReportTcStateLogStore.recordStateMachineStarted()`（逻辑摘要）：

```text
若非子状态机：
  globalTransaction = sagaTransactionalTemplate.beginTransaction()
  machineInstance.setId(globalTransaction.getXid())
  RootContext.bind(xid); RootContext.bindBranchType(SAGA)
  INSERT state_machine_inst
```

**xid = 状态机实例 ID**，与 TC `GlobalSession` 一一对应。

### 6.2 每状态可选分支注册

`recordStateStarted()`：对正向 `ServiceTask` 调用 `branchRegister(SAGA, stateMachineName + "#" + stateName, ...)`，便于 TC 感知细粒度状态（可配置关闭）。

### 6.3 结束：globalReport 而非 commit

```216:239:saga/seata-saga-spring/src/main/java/org/apache/seata/saga/engine/store/db/DbAndReportTcStateLogStore.java
    protected void reportTransactionFinished(StateMachineInstance machineInstance, ProcessContext context) {
        ...
        if (ExecutionStatus.SU.equals(machineInstance.getStatus())
                && machineInstance.getCompensationStatus() == null) {
            globalStatus = GlobalStatus.Committed;
        } else if (ExecutionStatus.SU.equals(machineInstance.getCompensationStatus())) {
            globalStatus = GlobalStatus.Rollbacked;
        } else if (ExecutionStatus.FA.equals(machineInstance.getCompensationStatus()) ...) {
            globalStatus = GlobalStatus.RollbackRetrying;
        ...
        sagaTransactionalTemplate.reportTransaction(globalTransaction, globalStatus);
    }
```

`DefaultSagaTransactionalTemplate.reportTransaction()` → `tx.globalReport(globalStatus)` → `GlobalReportRequest`。

### 6.4 SagaCore 单 Channel 二阶段

```65:81:server/src/main/java/org/apache/seata/server/transaction/saga/SagaCore.java
    public BranchStatus branchCommitSend(...) {
        String sagaResourceId = getSagaResourceId(globalSession);  // appId#txGroup
        Channel sagaChannel = ChannelManager.getRmChannels().get(sagaResourceId);
        BranchCommitResponse response = (BranchCommitResponse) remotingServer.sendSyncRequest(sagaChannel, request);
        return response.getBranchStatus();
    }
```

`doGlobalCommit` 在 `DefaultCore` 里对 `globalSession.isSaga()` **整单委托** `SagaCore`，不按多 SQL 分支循环。

### 6.5 RM：forward / compensate

`SagaResourceManager.branchCommit()`：

```java
StateMachineEngine engine = StateMachineEngineHolder.getStateMachineEngine();
ExecutionStatus status = engine.forward(xid, null);
// 映射 ExecutionStatus → BranchStatus
```

`branchRollback()` → `engine.compensate(xid)`，驱动 statelang 中 `CompensateState` 链。

### 6.6 注解 Saga（SAGA_ANNOTATION）

`SagaAnnotationCore.branchCommit()` 直接返回 `PhaseTwo_Committed`；真实补偿在 `SagaAnnotationActionInterceptorHandler` + `ActionInterceptorHandler`（`BranchType.SAGA_ANNOTATION`），模型接近 **代码版 TCC**。

---

## 7. 二阶段调度：DefaultCore 源码

### 7.1 commit() 状态机

```java
// DefaultCore.commit()
if (globalSession.getStatus() == GlobalStatus.Begin) {
    globalSession.close();  // 禁止再注册分支
    if (globalSession.canBeCommittedAsync()) {
        globalSession.asyncCommit();
    } else {
        globalSession.changeGlobalStatus(GlobalStatus.Committing);
        shouldCommitNow = true;
    }
}
if (shouldCommit) {
    doGlobalCommit(globalSession, false);
}
```

### 7.2 doGlobalCommit 分支循环

```java
for (BranchSession branchSession : globalSession.getSortedBranches()) {
    if (!retrying && branchSession.canBeCommittedAsync()) continue;
    BranchStatus branchStatus = getCore(branchSession.getBranchType())
        .branchCommit(globalSession, branchSession);
    switch (branchStatus) {
        case PhaseTwo_Committed: removeBranch; break;
        case PhaseTwo_CommitFailed_Unretryable: endCommitFailed; return false;
        default: queueToRetryCommit(); return false;
    }
}
```

Saga 全局：`if (globalSession.isSaga()) success = getCore(SAGA).doGlobalCommit(...)`。

### 7.3 rollback 对称

`doGlobalRollback()` 对每个分支 `getCore(type).branchRollback()`，失败入 `RollbackRetrying` 队列，由 `DefaultCoordinator` 定时重试。

---

## 8. 模式对比总表

| 维度 | AT | TCC | XA | Saga |
|------|----|-----|-----|------|
| RM 模块 | rm-datasource | tcc + integration-tx-api | rm-datasource/xa | saga-* |
| 一阶段触发 | ConnectionProxy.commit | Try 方法前 branchRegister | setAutoCommit(false) | 状态机 start |
| TC 锁 | 有 | 无 | 无 | 无 |
| 持久化补偿 | undo_log | 业务表 + 可选 fence | XA prepare | 状态表 + 补偿服务 |
| 全局结束 API | commit/rollback | commit/rollback | commit/rollback | **globalReport** 为主 |
| TC Core | ATCore | TccCore | XACore | SagaCore |
| 二阶段 RM | 删 undo / 执行 undo | 反射 confirm/cancel | xaCommit/Rollback | forward/compensate |

---

## 延伸阅读

| 文档 | 内容 |
|------|------|
| [SEATA_MODE_AT.md](./SEATA_MODE_AT.md) | AT 产品设计、配置、运维 |
| [SEATA_MODE_TCC.md](./SEATA_MODE_TCC.md) | TCC 注解、Fence、悬挂 |
| [SEATA_MODE_XA.md](./SEATA_MODE_XA.md) | XA 连接持有、NOTA |
| [SEATA_MODE_SAGA.md](./SEATA_MODE_SAGA.md) | Statelang、状态存储 |
| [SEATA_RPC_COMMUNICATION.md](./SEATA_RPC_COMMUNICATION.md) | 报文字段 |

---

*基于当前仓库 `org.apache.seata` 源码整理。*
