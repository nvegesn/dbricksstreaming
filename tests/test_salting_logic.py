"""
================================================================================
UNIT TEST: test_salting_logic.py
PURPOSE:
  Mathematically validates that Two-Stage Salting Aggregation produces
  results that are 100% identical to direct single-stage aggregation,
  proving that decomposing average into sum(amount)/sum(count) is exact
  and does not suffer from Simpson's paradox or weighted average error.
================================================================================
"""

import unittest
from collections import defaultdict


class TestTwoStageSaltingAggregation(unittest.TestCase):
    def setUp(self):
        # 10,000 orders at Times Square (severe skew) + 50 orders at rural store
        self.raw_orders = []
        
        # Times Square skewed orders
        for i in range(10000):
            self.raw_orders.append({
                "order_id": f"TS_{i}",
                "store_id": "STORE-NYC-7381",
                "amount": 5.0 + (i % 15) * 0.75,
                "wait_seconds": 120 + (i % 200)
            })
            
        # Rural store orders
        for i in range(50):
            self.raw_orders.append({
                "order_id": f"RURAL_{i}",
                "store_id": "STORE-RURAL-01",
                "amount": 4.5 + (i % 5) * 1.2,
                "wait_seconds": 60 + (i % 60)
            })

    def test_algebraic_equivalence(self):
        # 1. Benchmark: Direct single-stage aggregation
        direct_totals = defaultdict(lambda: {"sum_amt": 0.0, "sum_wait": 0.0, "count": 0})
        for o in self.raw_orders:
            st = o["store_id"]
            direct_totals[st]["sum_amt"] += o["amount"]
            direct_totals[st]["sum_wait"] += o["wait_seconds"]
            direct_totals[st]["count"] += 1

        direct_results = {}
        for st, data in direct_totals.items():
            direct_results[st] = {
                "count": data["count"],
                "aov": round(data["sum_amt"] / data["count"], 4),
                "avg_wait": round(data["sum_wait"] / data["count"], 4)
            }

        # 2. Two-Stage Salting Aggregation (Salt factor = 16)
        SALT_FACTOR = 16
        stage1_partials = defaultdict(lambda: {"p_amt": 0.0, "p_wait": 0.0, "p_count": 0})

        # Stage 1: Group by (store_id, salt)
        for o in self.raw_orders:
            salt = hash(o["order_id"]) % SALT_FACTOR
            key = (o["store_id"], salt)
            stage1_partials[key]["p_amt"] += o["amount"]
            stage1_partials[key]["p_wait"] += o["wait_seconds"]
            stage1_partials[key]["p_count"] += 1

        # Stage 2: Rollup by store_id without salt
        stage2_totals = defaultdict(lambda: {"sum_amt": 0.0, "sum_wait": 0.0, "count": 0})
        for (st, salt), pdata in stage1_partials.items():
            stage2_totals[st]["sum_amt"] += pdata["p_amt"]
            stage2_totals[st]["sum_wait"] += pdata["p_wait"]
            stage2_totals[st]["count"] += pdata["p_count"]

        salted_results = {}
        for st, data in stage2_totals.items():
            salted_results[st] = {
                "count": data["count"],
                "aov": round(data["sum_amt"] / data["count"], 4),
                "avg_wait": round(data["sum_wait"] / data["count"], 4)
            }

        # Validate exact mathematical parity
        for st in ["STORE-NYC-7381", "STORE-RURAL-01"]:
            self.assertEqual(direct_results[st]["count"], salted_results[st]["count"])
            self.assertAlmostEqual(direct_results[st]["aov"], salted_results[st]["aov"], places=4)
            self.assertAlmostEqual(direct_results[st]["avg_wait"], salted_results[st]["avg_wait"], places=4)


if __name__ == "__main__":
    unittest.main()
