"""指标增量结果表测试：增量计算、连续版本推进、中断恢复、跨模块交付。"""
from __future__ import annotations

import unittest

from app.services.metric_increment import (
    IncrementWriteInterrupted,
    MetricIncrementService,
    metric_increment_service,
)
from app.store import store

METRIC_ID = 1  # 种子中的实时订单吞吐量 REAL-0001


def result_keys(metric_id: int = METRIC_ID) -> list[tuple[int, str]]:
    return [
        (int(r["source_version"]), str(r["window_start"]))
        for r in store.rows("metric_increment_results")
        if r.get("metric_id") == metric_id
    ]


class TestIncrementRefresh(unittest.TestCase):
    def setUp(self) -> None:
        # 每个用例使用独立服务实例与干净的产物表
        self.svc = MetricIncrementService()
        for table in (
            "metric_increment_results",
            "metric_increment_cursors",
            "metric_source_versions",
            "ttl_delivery_log",
        ):
            store.rows(table).clear()

    def test_refresh_only_processes_new_versions(self) -> None:
        """增量刷新只处理已消费版本之后的窗口，不重扫历史。"""
        self.svc.ingest_source_versions(METRIC_ID, count=3)
        first = self.svc.refresh_increment(METRIC_ID)
        self.assertTrue(first["ok"])
        self.assertEqual(first["consumed_before"], 0)
        self.assertEqual(first["consumed_after"], 3)
        self.assertEqual(len(first["written"]), 3)
        # 首次提交后结果表版本从 v0 推进到 v1
        self.assertEqual(first["table_version"], 1)

        # 没有新版本时再次刷新：不写入任何窗口，版本保持
        second = self.svc.refresh_increment(METRIC_ID)
        self.assertTrue(second["ok"])
        self.assertEqual(second["written"], [])
        self.assertEqual(second["consumed_after"], 3)
        self.assertEqual(second["table_version"], 1)

        # 再来两个新版本，只增量处理这两个
        self.svc.ingest_source_versions(METRIC_ID, count=2)
        third = self.svc.refresh_increment(METRIC_ID)
        self.assertEqual(third["consumed_before"], 3)
        self.assertEqual(third["consumed_after"], 5)
        self.assertEqual(len(third["written"]), 2)
        self.assertEqual(third["table_version"], 2)

        # 全表窗口唯一，无重复累计
        keys = result_keys()
        self.assertEqual(len(keys), len(set(keys)))
        self.assertEqual(len(keys), 5)

    def test_aggregation_is_deterministic(self) -> None:
        """同一窗口聚合值确定，可复算。"""
        self.svc.ingest_source_versions(METRIC_ID, count=1)
        self.svc.refresh_increment(METRIC_ID)
        row = next(r for r in store.rows("metric_increment_results") if r.get("metric_id") == METRIC_ID)
        source = store.rows("metric_source_versions")[0]
        # count 聚合值等于窗口观测记录数
        self.assertEqual(row["agg_value"], int(source["observed_records"]))


class TestInterruptAndRecovery(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = MetricIncrementService()
        for table in (
            "metric_increment_results",
            "metric_increment_cursors",
            "metric_source_versions",
            "ttl_delivery_log",
        ):
            store.rows(table).clear()

    def test_interrupt_keeps_cursor_and_recover_dedups(self) -> None:
        """写入中断后游标不前进；恢复补齐缺口且同一窗口不重复累计。"""
        self.svc.ingest_source_versions(METRIC_ID, count=4)

        # 写入 2 个窗口后模拟中断
        with self.assertRaises(IncrementWriteInterrupted) as ctx:
            self.svc.refresh_increment(METRIC_ID, fail_after=2)
        partial = ctx.exception.partial
        self.assertEqual(len(partial["written"]), 2)

        cursor = self.svc.get_cursor(METRIC_ID)["cursor"]
        self.assertEqual(cursor["consumed_version"], 0)  # 游标保持不动
        self.assertEqual(cursor["table_version"], 0)

        # 中断残留两行，状态为"新增"
        partial_rows = [
            r for r in store.rows("metric_increment_results")
            if r.get("metric_id") == METRIC_ID and r.get("row_state") == "新增"
        ]
        self.assertEqual(len(partial_rows), 2)

        # 恢复：前两个窗口跳过（去重），补齐后两个，游标连续推进到 4
        recovered = self.svc.recover_increment(METRIC_ID)
        self.assertTrue(recovered["ok"])
        self.assertEqual(len(recovered["skipped"]), 2)
        self.assertEqual(len(recovered["written"]), 2)
        self.assertEqual(recovered["consumed_after"], 4)

        keys = result_keys()
        self.assertEqual(len(keys), 4)
        self.assertEqual(len(set(keys)), 4)  # 无重复窗口
        # 恢复后所有行标记为已消费
        states = {r["row_state"] for r in store.rows("metric_increment_results") if r.get("metric_id") == METRIC_ID}
        self.assertEqual(states, {"已消费"})

    def test_recover_is_idempotent(self) -> None:
        """恢复后再次恢复/刷新不会产生重复行。"""
        self.svc.ingest_source_versions(METRIC_ID, count=3)
        try:
            self.svc.refresh_increment(METRIC_ID, fail_after=1)
        except IncrementWriteInterrupted:
            pass
        self.svc.recover_increment(METRIC_ID)
        again = self.svc.recover_increment(METRIC_ID)
        self.assertEqual(again["written"], [])
        self.assertEqual(len(result_keys()), 3)
        self.assertEqual(len(set(result_keys())), 3)


class TestVersionGap(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = MetricIncrementService()
        for table in (
            "metric_increment_results",
            "metric_increment_cursors",
            "metric_source_versions",
            "ttl_delivery_log",
        ):
            store.rows(table).clear()

    def test_gap_versions_are_reported_and_cursor_stops_before_gap(self) -> None:
        """版本断档时只推进连续区间，缺口版本显式告警。"""
        self.svc.ingest_source_versions(METRIC_ID, count=2)
        self.svc.refresh_increment(METRIC_ID)  # 消费 1,2
        # 手工制造断档：只补版本 4（缺少版本 3）
        self.svc.ingest_source_versions(METRIC_ID, count=1)
        gap_row = store.rows("metric_source_versions")[-1]
        gap_row["source_version"] = 4

        result = self.svc.refresh_increment(METRIC_ID)
        self.assertEqual(result["gap_versions"], [3])
        self.assertEqual(result["consumed_after"], 2)  # 不越过缺口
        self.assertEqual(result["written"], [])


class TestLifecycleDelivery(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = MetricIncrementService()
        for table in (
            "metric_increment_results",
            "metric_increment_cursors",
            "metric_source_versions",
            "ttl_delivery_log",
        ):
            store.rows(table).clear()

    def test_delivery_preserves_version_and_is_idempotent(self) -> None:
        """交付保留结果表当前版本，重复交付幂等。"""
        before = self.svc.deliver_to_lifecycle(METRIC_ID)
        self.assertFalse(before["ok"])  # 尚无增量版本

        self.svc.ingest_source_versions(METRIC_ID, count=2)
        self.svc.refresh_increment(METRIC_ID)
        first = self.svc.deliver_to_lifecycle(METRIC_ID)
        self.assertTrue(first["ok"])
        self.assertEqual(first["delivery"]["result_table_version"], 1)
        self.assertEqual(first["delivery"]["consumed_version"], 2)
        self.assertEqual(first["delivery"]["source_module"], "realtime_metrics")
        self.assertEqual(first["delivery"]["target_module"], "ttl_policy")

        # 同版本重复交付不新增记录
        second = self.svc.deliver_to_lifecycle(METRIC_ID)
        self.assertTrue(second["duplicated"])
        self.assertEqual(len(self.svc.list_deliveries()), 1)

        # 再刷新产生新版本后交付，保留新版本
        self.svc.ingest_source_versions(METRIC_ID, count=1)
        self.svc.refresh_increment(METRIC_ID)
        third = self.svc.deliver_to_lifecycle(METRIC_ID)
        self.assertFalse(third["duplicated"])
        self.assertEqual(third["delivery"]["result_table_version"], 2)

    def test_retention_check_uses_topic_retention(self) -> None:
        """生命周期保留期核对覆盖结果表涉及的主题。"""
        self.svc.ingest_source_versions(METRIC_ID, count=2)
        self.svc.refresh_increment(METRIC_ID)
        report = self.svc.retention_check(METRIC_ID)
        self.assertTrue(report["ok"])
        codes = [item["topic_code"] for item in report["items"]]
        self.assertIn("TOPI-0001", codes)
        topic_item = next(i for i in report["items"] if i["topic_code"] == "TOPI-0001")
        self.assertIn(topic_item["consumable"], (True, False, None))
        self.assertTrue(topic_item["reason"])


class TestSingletonShared(unittest.TestCase):
    def test_module_singleton_exists(self) -> None:
        self.assertIsInstance(metric_increment_service, MetricIncrementService)


if __name__ == "__main__":
    unittest.main()
