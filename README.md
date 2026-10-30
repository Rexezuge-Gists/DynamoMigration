# DynamoDB 数据迁移工具

`migrate.py` 将一张 DynamoDB 表的数据迁移到另一张表，采用**并行 Scan + BatchWriteItem**，边扫描边写入，适合跨账号、跨区域的一次性数据迁移。

> 目标表需要提前创建（例如由 CloudFormation 管理），并且**主键定义必须与源表一致**。脚本只读写数据，不修改表结构，不会造成 CloudFormation drift。

## 特性

- 并行 Scan：通过 `TotalSegments` / `Segment` 拆分扫描任务，多线程消费
- 流式迁移：按页读取、按批写入，内存占用与表大小无关
- 跨账号 / 跨区域：源端和目标端使用两套独立凭证与 Region
- 自动重试：使用 boto3 `adaptive` 重试模式，`BatchWriteItem` 的未处理项由 `batch_writer` 自动重试
- 幂等写入：`put_item` 覆盖同主键数据，失败或中断后可直接重跑
- 迁移前检查：校验表状态、主键结构，提前发现配置错误
- 优雅中断：`Ctrl+C` 后停止拉取新数据，保留已写入内容

## 工作方式

```text
SOURCE_* 凭证
    │
    ▼
DynamoDB Scan（Segment 0..N-1，逐页 LastEvaluatedKey）
    │
    ▼
batch_writer（默认 25 条/批）
    │
    ▼
DynamoDB BatchWriteItem（自动重试 UnprocessedItems）
    │
    ▼
TARGET_* 凭证
```

每个 segment 独立分页扫描；所有 segment 都完成后，脚本汇总处理条数。实际并发线程数最多为 64，同时排队的任务数量也有限制，因此 `--segments` 很大时不会耗尽内存或线程资源。

## 环境要求

- Python 3.9+（建议 3.11+）
- boto3（建议使用最新版，`>=1.26`）
- 目标表已创建，且主键（分区键/排序键）名称和类型与源表一致

安装依赖：

```bash
python3 -m pip install -U boto3
```

## 权限要求

| 账号 | 所需权限 | 资源 |
| --- | --- | --- |
| 源账号 | `dynamodb:Scan`、`dynamodb:DescribeTable` | 源表 ARN |
| 目标账号 | `dynamodb:BatchWriteItem`、`dynamodb:DescribeTable` | 目标表 ARN |

如果使用 `--skip-preflight` 跳过迁移前检查，则不需要 `dynamodb:DescribeTable`。

## 配置

### 凭证（必填）

源端和目标端分别从带前缀的环境变量读取凭证：

| 变量 | 必填 | 说明 |
| --- | --- | --- |
| `SOURCE_AWS_ACCESS_KEY_ID` | 是 | 源账号 Access Key |
| `SOURCE_AWS_SECRET_ACCESS_KEY` | 是 | 源账号 Secret Key |
| `SOURCE_AWS_SESSION_TOKEN` | 否 | 使用临时凭证（STS）时填写 |
| `TARGET_AWS_ACCESS_KEY_ID` | 是 | 目标账号 Access Key |
| `TARGET_AWS_SECRET_ACCESS_KEY` | 是 | 目标账号 Secret Key |
| `TARGET_AWS_SESSION_TOKEN` | 否 | 使用临时凭证（STS）时填写 |

凭证只来自上述变量，**不会使用 `AWS_PROFILE` 或实例角色自动发现**。

### 表与 Region（必填，环境变量或命令行二选一）

| 环境变量 | 对应参数 | 说明 |
| --- | --- | --- |
| `SOURCE_TABLE` | `--source-table` | 源表名 |
| `TARGET_TABLE` | `--target-table` | 目标表名 |
| `SOURCE_REGION` | `--source-region` | 源表 Region |
| `TARGET_REGION` | `--target-region` | 目标表 Region |
| `MIGRATE_SEGMENTS` | `--segments` | 并行扫描段数，默认 `8` |

命令行参数优先于环境变量。

### 全部参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--source-table` | `SOURCE_TABLE` | 源表名 |
| `--target-table` | `TARGET_TABLE` | 目标表名 |
| `--source-region` | `SOURCE_REGION` | 源表 Region |
| `--target-region` | `TARGET_REGION` | 目标表 Region |
| `--segments` | `MIGRATE_SEGMENTS` 或 `8` | 并行扫描段数，范围 `1..1000000`；实际线程数最多 64 |
| `--consistent-read` | 关闭 | 源表使用强一致性读，RCU 消耗约翻倍，通常不需要 |
| `--skip-preflight` | 关闭 | 跳过迁移前检查；仅在 `DescribeTable` 无权限等特殊场景使用 |

## 快速开始

### 1. 配置环境变量

```bash
export SOURCE_AWS_ACCESS_KEY_ID="..."
export SOURCE_AWS_SECRET_ACCESS_KEY="..."
# 临时凭证才需要：
# export SOURCE_AWS_SESSION_TOKEN="..."

export TARGET_AWS_ACCESS_KEY_ID="..."
export TARGET_AWS_SECRET_ACCESS_KEY="..."
# export TARGET_AWS_SESSION_TOKEN="..."

export SOURCE_TABLE="orders"
export TARGET_TABLE="orders-migrated"
export SOURCE_REGION="us-east-1"
export TARGET_REGION="eu-west-1"
```

### 2. 执行迁移

```bash
chmod +x migrate.py   # 首次运行时
./migrate.py
```

使用更多并行段数：

```bash
./migrate.py --segments 16
```

也可以完全通过命令行传参：

```bash
./migrate.py \
  --source-table orders --target-table orders-migrated \
  --source-region us-east-1 --target-region eu-west-1 \
  --segments 16
```

### 3. 示例输出

```text
=== 迁移前检查 ===
[SOURCE] orders status=ACTIVE items≈152340 billing=PAY_PER_REQUEST
[TARGET] orders-migrated status=ACTIVE items≈0 billing=PAY_PER_REQUEST

=== 开始迁移 segments=16 workers=16 ===
  segment   0 OK | done 1/16 items=9541 rate=1203/s elapsed=7.9s
  segment   1 OK | done 2/16 items=19012 rate=1188/s elapsed=16.0s
  ...
=== 结束：处理 152340 条，用时 126.4s (1205 items/s) ===
注意：目标表 ItemCount 是近似值且有延迟，请抽样比对关键项。
```

## 迁移前检查

默认执行以下检查，任一项失败都会以退出码 `2` 终止，避免扫描完整个源表后才发现无法写入：

1. 源表和目标表都存在，且 `TableStatus` 为 `ACTIVE`
2. 两张表的主键完全一致（分区键/排序键的名称和类型）
3. 目标表为预置容量模式时，打印 WCU 提示
4. 目标表已有数据时，提示同主键数据会被覆盖

如果账号没有 `dynamodb:DescribeTable` 权限，可使用 `--skip-preflight` 跳过检查，但这样就失去了主键一致性的提前保护。

## 退出码

| 退出码 | 含义 | 处理方式 |
| --- | --- | --- |
| `0` | 全部 segment 成功 | 抽样校验数据 |
| `1` | 至少一个 segment 失败 | 查看 stderr 中的 segment 错误；修复后重跑 |
| `2` | 参数、凭证、客户端或迁移前检查失败 | 根据错误提示修正配置 |
| `130` | 收到 `Ctrl+C` 中断 | 直接重跑，已写入的数据会保留 |

## 使用建议

- **先用小表试跑**：确认账号、Region、表名和权限配置正确。
- **目标表建议使用 on-demand**：预置容量模式下请确保 WCU 足够，否则会触发限流（脚本会重试，但速度会变慢）。
- **segments 从 8 开始**：通常 8–32 足够；继续增加并发不一定能线性提速，还可能给源表和目标表带来更大压力。
- **谨慎使用 `--consistent-read`**：它不会提供跨页快照一致性，只是把单次读取改为强一致性，RCU 消耗约翻倍。
- **迁移期间尽量停止源表写入**：见下方“限制”。

## 限制与注意事项

- **不是快照迁移**：DynamoDB Scan 不提供跨页一致性快照。迁移期间源表若有写入，目标表可能缺少新数据，或不同 segment 读到不同时间点的值。建议迁移期间停止写入，或在迁移后做一次增量/校验。
- **没有断点续传**：中断后重跑会从头扫描整个源表。写入是幂等的，但会产生额外的读取费用和时间。
- **不会删除目标表已有数据**：脚本只执行写入；目标表中主键不存在的旧数据会保留。
- **不复制表配置**：不创建或修改表、GSI/LSI、TTL、Streams、PITR 等，这些应由 IaC（如 CloudFormation）配置。
- **`ItemCount` 是近似值**：DynamoDB 约每 6 小时更新一次，不能作为迁移是否完整的唯一依据。
- **不做自动校验**：迁移结束后请抽样比对，或根据业务规则做精确核对。
- **boto3 配置会被继承**：如果 shell 中设置了 `AWS_ENDPOINT_URL`（例如指向 LocalStack 或代理），请求会发往该地址。连接真实 AWS 前请确认：
  ```bash
  unset AWS_ENDPOINT_URL
  ```

## 常见问题

| 现象 | 可能原因 | 处理 |
| --- | --- | --- |
| `缺少参数：...` | 表名或 Region 未配置 | 设置环境变量或传对应命令行参数 |
| `AccessDeniedException` | 权限不足，或源/目标凭证配反 | 检查两套凭证及 IAM 权限 |
| `ResourceNotFoundException` | 表名或 Region 错误 | 确认表存在且位于指定 Region |
| `ValidationException ... Missing the key` | 源表与目标表主键不一致 | 修正目标表主键；预检应已拦截 |
| `ProvisionedThroughputExceededException` | 目标表写入容量不足 | 提高 WCU 或改用 on-demand |
| `describe_table 失败` | 无 `DescribeTable` 权限或表不存在 | 补权限；确认表名和 Region；特殊情况下用 `--skip-preflight` |
| 运行很慢 | segments 太少或目标表限流 | 适当增加 `--segments`，提高目标表容量 |
| `Ctrl+C` 后退出码 130 | 正常中断 | 重跑即可，已写入数据不会丢 |

## 安全建议

- 不要把 Access Key 写入脚本、README 或提交到 Git；使用环境变量或密钥管理服务。
- 优先使用 STS 临时凭证，并填写 `*_AWS_SESSION_TOKEN`。
- 按最小权限原则只授予源表 `Scan`、目标表 `BatchWriteItem` 和必要的 `DescribeTable`。
- 迁移完成后及时撤销临时凭证。

## 参考

- [DynamoDB Parallel Scan](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan)
- [DynamoDB BatchWriteItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html)
- [boto3 DynamoDB](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html)
