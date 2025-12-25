from __future__ import annotations

import os
import json
import csv
from dataclasses import asdict
from typing import List, Dict, Any


class MetricsTracker:
    def __init__(self, run_dir: str):
        self.run_dir = run_dir
        os.makedirs(run_dir, exist_ok=True)
        self.equity_timeseries: List[Dict[str, Any]] = []
        self.fills: List[Dict[str, Any]] = []
        self.orders: List[Dict[str, Any]] = []
        self.counters = {'quote_count': 0, 'cancel_count': 0, 'modify_count': 0}

    def record_order(self, order):
        self.orders.append({
            'order_id': order.order_id,
            'side': order.side,
            'price': order.price,
            'size': order.size,
            'status': order.status,
            'created_ts_ms': order.created_ts_ms,
            'updated_ts_ms': order.updated_ts_ms,
        })

    def record_fill(self, f):
        self.fills.append({
            'order_id': f.order_id,
            'side': f.side,
            'price': f.price,
            'size': f.size,
            'ts_ms': f.ts_ms,
            'reason': f.reason,
        })

    def record_equity(self, ts_ms: int, inventory: float, cash: float, equity: float):
        self.equity_timeseries.append({'ts_ms': ts_ms, 'inventory': inventory, 'cash': cash, 'equity': equity})

    def incr(self, key: str, n: int = 1):
        self.counters[key] = self.counters.get(key, 0) + n

    def dump(self, stats: dict = None):
        # write summary.json and csvs
        summary = {
            'counters': self.counters,
            'n_equity_points': len(self.equity_timeseries),
            'n_fills': len(self.fills),
            'n_orders': len(self.orders),
        }
        # merge in run-level stats if provided (events, snapshots, deltas, etc.)
        if stats:
            for k, v in stats.items():
                # keep top-level numeric stats for easy consumption
                summary[k] = v
        with open(os.path.join(self.run_dir, 'summary.json'), 'w') as fh:
            json.dump(summary, fh, indent=2)

        # equity csv
        with open(os.path.join(self.run_dir, 'equity.csv'), 'w', newline='') as fh:
            w = csv.DictWriter(fh, fieldnames=['ts_ms', 'inventory', 'cash', 'equity'])
            w.writeheader()
            for r in self.equity_timeseries:
                w.writerow(r)

        # fills - always write header even if no fills
        fills_path = os.path.join(self.run_dir, 'fills.csv')
        fills_fields = ['order_id', 'side', 'price', 'size', 'ts_ms', 'reason']
        with open(fills_path, 'w', newline='') as fh:
            w = csv.DictWriter(fh, fieldnames=fills_fields)
            w.writeheader()
            for r in self.fills:
                w.writerow(r)

        # orders - always write header even if no orders
        orders_path = os.path.join(self.run_dir, 'orders.csv')
        orders_fields = ['order_id', 'side', 'price', 'size', 'status', 'created_ts_ms', 'updated_ts_ms']
        with open(orders_path, 'w', newline='') as fh:
            w = csv.DictWriter(fh, fieldnames=orders_fields)
            w.writeheader()
            for r in self.orders:
                w.writerow(r)
