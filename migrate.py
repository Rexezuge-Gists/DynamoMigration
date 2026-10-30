#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DynamoDB 数据迁移：并行 Scan + BatchWrite
- 从环境变量读取 SOURCE_/TARGET_ 两套凭证
- 目标表由 CloudFormation 创建，脚本只做数据面写入，不触发 drift
- 边扫描边写入，内存占用与表大小无关
- 支持跨账号 / 跨区域
"""

import argparse
import os
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

MAX_SEGMENTS = 1_000_000  # DynamoDB Scan.TotalSegments 的上限
MAX_WORKERS = 64          # 实际并发线程上限，避免 segments 过大时耗尽资源


# ---------- 凭证 / 客户端 ----------

def _env(name):
    v = os.environ.get(name)
    return v if v else None


def _env_int(name, default: int) -> int:
    raw = _env(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        print(f"环境变量 {name} 必须是整数，当前值：{raw!r}", file=sys.stderr)
        sys.exit(2)


def get_session(prefix: str) -> boto3.Session:
    ak = _env(f"{prefix}_AWS_ACCESS_KEY_ID")
    sk = _env(f"{prefix}_AWS_SECRET_ACCESS_KEY")
    st = _env(f"{prefix}_AWS_SESSION_TOKEN")
    if not ak or not sk:
        raise RuntimeError(
            f"缺少环境变量 {prefix}_AWS_ACCESS_KEY_ID / {prefix}_AWS_SECRET_ACCESS_KEY"
        )
    return boto3.Session(
        aws_access_key_id=ak,
        aws_secret_access_key=sk,
        aws_session_token=st,  # 为空时传 None 即可
    )


def build_table(session: boto3.Session, region: str, table_name: str):
    resource = session.resource(
        "dynamodb",
        region_name=region,
        config=Config(
            retries={"max_attempts": 20, "mode": "adaptive"},
            max_pool_connections=128,
        ),
    )
    return resource.Table(table_name)


# ---------- 迁移前检查 ----------

def _key_schema(table_desc) -> list:
    attr_types = {
        a["AttributeName"]: a["AttributeType"]
        for a in table_desc.get("AttributeDefinitions", [])
    }
    keys = []
    for key in table_desc.get("KeySchema", []):
        name = key["AttributeName"]
        keys.append((key["KeyType"], name, attr_types.get(name)))
    return sorted(keys)


def _format_key_schema(schema) -> str:
    return ", ".join(f"{kind}:{name}({typ})" for kind, name, typ in schema) or "<无>"


def preflight(src_tbl, dst_tbl) -> bool:
    print("=== 迁移前检查 ===")
    descs = {}
    for label, table in (("SOURCE", src_tbl), ("TARGET", dst_tbl)):
        try:
            desc = table.meta.client.describe_table(TableName=table.name)["Table"]
        except (ClientError, BotoCoreError) as e:
            print(f"[{label}] describe_table 失败: {e}", file=sys.stderr)
            return False
        descs[label] = desc
        billing = desc.get("BillingModeSummary", {}).get("BillingMode", "PROVISIONED")
        print(
            f"[{label}] {desc['TableName']} status={desc['TableStatus']} "
            f"items≈{desc.get('ItemCount')} billing={billing}"
        )

    ok = True
    for label, desc in descs.items():
        if desc.get("TableStatus") != "ACTIVE":
            print(
                f"[{label}] 表状态为 {desc.get('TableStatus')}，"
                f"必须为 ACTIVE 才能迁移",
                file=sys.stderr,
            )
            ok = False

    src_keys = _key_schema(descs["SOURCE"])
    dst_keys = _key_schema(descs["TARGET"])
    if src_keys != dst_keys:
        print("[ERROR] 源表与目标表主键不一致，无法直接迁移：", file=sys.stderr)
        print(f"  SOURCE: {_format_key_schema(src_keys)}", file=sys.stderr)
        print(f"  TARGET: {_format_key_schema(dst_keys)}", file=sys.stderr)
        ok = False

    target = descs["TARGET"]
    target_billing = target.get("BillingModeSummary", {}).get(
        "BillingMode", "PROVISIONED"
    )
    if target_billing != "PAY_PER_REQUEST":
        wcu = target.get("ProvisionedThroughput", {}).get("WriteCapacityUnits")
        print(
            f"[TARGET] 预置模式（{wcu} WCU）：写入过快会触发限流，"
            f"脚本会自动重试；必要时请临时提高容量或切换为 on-demand"
        )

    target_items = target.get("ItemCount") or 0
    if target_items > 0:
        print(
            f"[TARGET] 目标表已有约 {target_items} 条数据，"
            f"同主键的项会被源表数据覆盖"
        )

    if not ok:
        print("迁移前检查未通过，已停止。", file=sys.stderr)
    return ok


# ---------- 进度统计 ----------

class Progress:
    def __init__(self, total_segments: int):
        self._lock = threading.Lock()
        self._count = 0
        self._done = 0
        self._total = total_segments
        self._start = time.time()
        self.errors = []

    @property
    def count(self) -> int:
        with self._lock:
            return self._count

    def add(self, n: int):
        with self._lock:
            self._count += n

    def finish_segment(self, seg: int, ok: bool, err=None):
        with self._lock:
            self._done += 1
            if not ok:
                self.errors.append((seg, repr(err)))
            elapsed = max(time.time() - self._start, 1e-9)
            rate = self._count / elapsed
            done = self._done
        status = "OK" if ok else f"FAIL: {err!r}"
        print(
            f"  segment {seg:>3} {status} | "
            f"done {done}/{self._total} "
            f"items={self._count} rate={rate:.0f}/s elapsed={elapsed:.1f}s",
            flush=True,
        )


# ---------- 单个 segment 的迁移 ----------

def migrate_segment(source_table, target_table, seg, total_segments,
                    consistent_read, progress: Progress, stop: threading.Event):
    try:
        kwargs = {"TotalSegments": total_segments, "Segment": seg}
        if consistent_read:
            kwargs["ConsistentRead"] = True

        with target_table.batch_writer() as bw:
            resp = source_table.scan(**kwargs)
            while True:
                # Ctrl+C 后不再开始处理新的一页
                if stop.is_set():
                    return
                items = resp.get("Items", [])
                for it in items:
                    bw.put_item(Item=it)
                progress.add(len(items))

                lek = resp.get("LastEvaluatedKey")
                if not lek:
                    break
                kwargs["ExclusiveStartKey"] = lek
                resp = source_table.scan(**kwargs)

        progress.finish_segment(seg, True)
    except Exception as e:
        progress.finish_segment(seg, False, e)
        raise


# ---------- 主流程 ----------

def main():
    ap = argparse.ArgumentParser(description="DynamoDB Scan+BatchWrite 数据迁移")
    ap.add_argument("--source-table",   default=os.environ.get("SOURCE_TABLE"))
    ap.add_argument("--target-table",   default=os.environ.get("TARGET_TABLE"))
    ap.add_argument("--source-region",  default=os.environ.get("SOURCE_REGION"))
    ap.add_argument("--target-region",  default=os.environ.get("TARGET_REGION"))
    ap.add_argument(
        "--segments", type=int, default=None,
        help=(
            f"并行扫描段数（默认取 MIGRATE_SEGMENTS 或 8，"
            f"上限 {MAX_SEGMENTS}）"
        ),
    )
    ap.add_argument("--consistent-read", action="store_true",
                    help="使用强一致性读（成本翻倍，通常不需要）")
    ap.add_argument("--skip-preflight", action="store_true",
                    help="跳过迁移前检查（不推荐）")
    args = ap.parse_args()

    if args.segments is None:
        args.segments = _env_int("MIGRATE_SEGMENTS", 8)

    missing = [k for k, v in {
        "source_table":  args.source_table,
        "target_table":  args.target_table,
        "source_region": args.source_region,
        "target_region": args.target_region,
    }.items() if not v]
    if missing:
        print(f"缺少参数：{', '.join(missing)}", file=sys.stderr)
        sys.exit(2)
    if not 1 <= args.segments <= MAX_SEGMENTS:
        print(f"--segments 必须在 1 到 {MAX_SEGMENTS} 之间", file=sys.stderr)
        sys.exit(2)

    try:
        src_session = get_session("SOURCE")
        dst_session = get_session("TARGET")
    except RuntimeError as e:
        print(e, file=sys.stderr)
        sys.exit(2)

    try:
        src_tbl = build_table(src_session, args.source_region, args.source_table)
        dst_tbl = build_table(dst_session, args.target_region, args.target_table)
    except (BotoCoreError, ValueError) as e:
        print(f"创建 DynamoDB 客户端失败: {e}", file=sys.stderr)
        sys.exit(2)

    if args.skip_preflight:
        print("=== 已跳过迁移前检查 ===")
    elif not preflight(src_tbl, dst_tbl):
        sys.exit(2)

    workers = min(args.segments, MAX_WORKERS)
    print(f"\n=== 开始迁移 segments={args.segments} workers={workers} ===")
    progress = Progress(args.segments)
    stop = threading.Event()
    t0 = time.time()

    pool = ThreadPoolExecutor(max_workers=workers)
    futures = set()
    next_seg = 0

    def submit_one():
        nonlocal next_seg
        future = pool.submit(
            migrate_segment,
            src_tbl, dst_tbl,
            next_seg, args.segments,
            args.consistent_read, progress, stop,
        )
        futures.add(future)
        next_seg += 1

    # 最多同时排队 workers*2 个 segment，segments 很大时也不会撑爆内存
    pending_limit = workers * 2
    interrupted = False
    try:
        while next_seg < args.segments and len(futures) < pending_limit:
            submit_one()

        while futures:
            done, _ = wait(futures, return_when=FIRST_COMPLETED)
            for f in done:
                futures.discard(f)
                try:
                    f.result()
                except Exception:
                    pass  # 已记录到 progress.errors
                if next_seg < args.segments and not stop.is_set():
                    submit_one()
    except KeyboardInterrupt:
        interrupted = True
        stop.set()
        print("\n收到 Ctrl+C，正在停止（已写入的数据保留，可直接重跑）...",
              file=sys.stderr)
    finally:
        pool.shutdown(wait=True, cancel_futures=True)

    elapsed = time.time() - t0
    print(f"\n=== 结束：处理 {progress.count} 条，用时 {elapsed:.1f}s "
          f"({progress.count / max(elapsed, 1e-9):.0f} items/s) ===")

    if progress.errors:
        print("\n!!! 以下 segment 失败（put_item 幂等，修复后可直接重跑）:",
              file=sys.stderr)
        for seg, err in progress.errors:
            print(f"  segment {seg}: {err}", file=sys.stderr)

    if interrupted:
        print(
            f"\n迁移被中断：已处理 {progress.count} 条；"
            f"未完成的 segment 重跑即可（put 幂等）",
            file=sys.stderr,
        )
        sys.exit(130)

    if progress.errors:
        sys.exit(1)

    print("注意：目标表 ItemCount 是近似值且有延迟，请抽样比对关键项。")


if __name__ == "__main__":
    main()
