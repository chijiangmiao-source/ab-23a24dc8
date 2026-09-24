# 脉冲采集片段归集服务 (FastAPI + PostgreSQL)

多台数字化仪乱序、重传同一炮次的采集片段时，本服务保证：

- 首个合法请求原子建立跑次批次，通道集合（2–8 个固定通道）此后不可改变；
- 只暂存已提交水位之后 32 炮内的片段；只有水位下一炮全部通道齐备时，
  **在同一数据库事务中**连续释放随后已齐备的炮次并推进水位，
  因此任何响应及进程重启后都观察不到半炮、重复释放或越过缺口的结果；
- 同操作标识同内容重试返回首次回执（持久化、重启后仍一致）；
  异参复用、同通道同炮次异值重传、越窗请求稳定拒绝，且失败不改变任何状态。

## API

### `PUT /api/runs/{run_id}/fragments`

```json
{
  "op_id": "op-0001",
  "channels": ["ch-a", "ch-b"],
  "channel": "ch-a",
  "shot": 3,
  "samples": [1024, -3, 77]
}
```

约束：`channels` 2–8 个且不重复，`channel` 必须属于其中，`shot` 1–256，
`samples` 为非空整数数组。成功回执示例：

```json
{
  "run_id": "run-1",
  "op_id": "op-0001",
  "shot": 3,
  "channel": "ch-a",
  "accepted": true,
  "duplicate": false,
  "watermark": 3,
  "released_shots": [3]
}
```

错误码（HTTP 409，`error.code`）：`CHANNEL_SET_MISMATCH`、`OP_ID_REUSED`、
`CONFLICTING_FRAGMENT`、`SHOT_OUT_OF_WINDOW`、`SHOT_ALREADY_RELEASED`；
跑次不存在的 `GET` 返回 404。

### `GET /api/runs/{run_id}`

返回固定通道、当前水位（下一个待齐备炮次）、已连续释放炮次、
已提交炮次列表，以及水位之后每炮的已到/缺失通道摘要。

### `GET /health`

数据库可用且建表完成才返回 200；就绪前业务请求返回 503。

## 启动

宿主机端口可通过环境变量配置：

```bash
HOST_HTTP_PORT=18000 HOST_DB_PORT=55432 docker compose up -d --build api
```

默认 HTTP 8000、数据库 5432。

## 验证（verify 服务）

verify 服务依次执行：构建检查（compileall）→ 代码测试（pytest）→
HTTP 冒烟（越序补洞、并发重试、稳定拒绝、经 Docker 重启 api 后恢复校验），
只运行一次，以退出码报告：

```bash
docker compose up --build --abort-on-container-exit --exit-code-from verify
echo $?   # 0 表示全部通过
```

本地直接跑测试：

```bash
pip install -r requirements.txt -r requirements-dev.txt
DATABASE_URL=postgresql://pulse:pulse@localhost:5432/pulsedb pytest
```

## 一致性设计要点

每个跑次的所有写事务先对 `runs` 行加 `FOR UPDATE` 行锁串行化；
片段插入、水位之后的连续齐备扫描、水位推进与回执写入全部在同一事务提交。
状态只存于 PostgreSQL（`runs` / `fragments` / `receipts` 三张表），
进程无内存暂存，重启即恢复。
