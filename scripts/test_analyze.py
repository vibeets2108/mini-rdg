import contextlib
import csv
import io
import json
import tempfile
import unittest
from pathlib import Path

from analyze import analyze, percentile, query_stats


class AnalysisTests(unittest.TestCase):
    def test_nearest_rank_and_empty_samples(self):
        self.assertIsNone(percentile([], 0.95))
        self.assertEqual(4, percentile([4, 1, 3, 2], 0.95))
        self.assertEqual(2, percentile([4, 1, 3, 2], 0.50))

    def test_failures_are_counted_but_not_silently_included_in_success_latency(self):
        rows = [
            {"status": "SUCCESS", "latency_ms": "10", "end_ms": "1000", "lag_ms": "2"},
            {"status": "TIMEOUT", "latency_ms": "2000", "end_ms": "3000", "lag_ms": "3"},
            {"status": "REJECTED", "latency_ms": "1", "end_ms": "1500", "lag_ms": "1"},
        ]
        result = query_stats(rows, 2)
        self.assertEqual(3, result["offered"])
        self.assertEqual(2, result["failures"])
        self.assertEqual(10, result["p95_ms"])
        self.assertAlmostEqual(1 / 3, result["completed_per_second_including_drain"])
        self.assertEqual(1, result["statuses"]["TIMEOUT"])
        self.assertTrue(result["p99_samples_below_1000"])

    def test_all_failed_cohort_has_no_latency_quantile(self):
        result = query_stats(
            [{"status": "TIMEOUT", "latency_ms": "1000", "end_ms": "2000", "lag_ms": "0"}], 1
        )
        self.assertIsNone(result["p99_ms"])
        self.assertEqual(0, result["success_pct"])

    def fixture(self, root):
        order = []
        for scheduler in ("FIFO", "ROUND_ROBIN"):
            for adaptive in (False, True):
                name = f"r0-qps1-{scheduler}-{adaptive}"
                order.append(name)
                directory = root / name
                directory.mkdir()
                (directory / "run.json").write_text(json.dumps({
                    "rate": 1, "seconds": 2,
                    "config": {"scheduler": scheduler, "adaptive": adaptive, "fixed": 20},
                }))
                (directory / "queries.csv").write_text(
                    "id,class,account,scheduled_ms,start_ms,end_ms,status,lag_ms,latency_ms,service_ms\n"
                    "0,small,s0,0,1,11,SUCCESS,1,11,10\n"
                    "1,large,l0,1000,1001,1021,SUCCESS,1,21,20\n"
                )
                (directory / "operations.csv").write_text(
                    "query,inflight_at_dispatch,limit_at_dispatch,queue_ms,driver_ms,slot_ms\n"
                    "0,1,20,1,2,2\n1,2,20,3,4,4\n"
                )
                (directory / "state.csv").write_text(
                    "pending,active_queries,limit\n0,1,20\n"
                )
        (root / "manifest.json").write_text(json.dumps({
            "status": "complete", "runOrder": order,
            "settings": {"repeats": 1, "rates": [1], "maxPending": 100, "maxQueries": 10},
            "adaptive": {"max": 64},
            "selectedFixedLimits": {"FIFO": 20, "ROUND_ROBIN": 20},
        }))
        return root / order[-1]

    def test_analysis_verifies_complete_paired_data_and_creates_reports(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.fixture(root)
            with contextlib.redirect_stdout(io.StringIO()):
                records = analyze(root)
            self.assertEqual(8, len(records))
            self.assertTrue((root / "latency.svg").exists())
            self.assertTrue((root / "report.md").exists())

    def test_unfair_arrivals_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            last = self.fixture(root)
            query_file = last / "queries.csv"
            query_file.write_text(query_file.read_text().replace(",s0,", ",s1,"))
            with self.assertRaisesRegex(ValueError, "unfair arrival"):
                analyze(root)

    def test_excess_dispatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            last = self.fixture(root)
            operations = last / "operations.csv"
            operations.write_text(operations.read_text().replace("0,1,20,", "0,21,20,"))
            with self.assertRaisesRegex(ValueError, "above the concurrency limit"):
                analyze(root)


if __name__ == "__main__":
    unittest.main()
