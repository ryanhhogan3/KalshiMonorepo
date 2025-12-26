import re
from pathlib import Path
import pandas as pd
from datetime import datetime, timezone

from backtest.cli import main as backtest_main
from backtest.tests.fixtures.generate_fixture import write_sample_parquet


def safe_slug(s: str) -> str:
    # Windows-safe filename component
    return re.sub(r'[<>:"/\\|?*]', '-', s)


def test_e2e_runs_and_writes_outputs(tmp_path):
    # generate fixture into tmp_path
    fixture = tmp_path / "sample.parquet"
    write_sample_parquet(str(fixture))

    df = pd.read_parquet(str(fixture))
    start_ms = int(df["ts_ms"].min())
    end_ms = int(df["ts_ms"].max())

    start = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    end   = datetime.fromtimestamp(end_ms   / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    market = "SAMPLE.MKT"

    out_dir = tmp_path / "runs"

    run_args = [
        "run",
        "--market", market,
        "--start", start,
        "--end", end,
        "--source", "parquet",
        "--fixture", str(fixture),
        "--out-dir", str(out_dir),
    ]

    backtest_main(run_args)

    run_dir = out_dir / f"run_{safe_slug(market)}_{safe_slug(start)}_{safe_slug(end)}"
    assert run_dir.exists()

    # verify artifact files exist
    import json
    summary = json.loads((run_dir / "summary.json").read_text())
    c = summary["counters"]
    assert (c["modify_count"] + c["cancel_count"]) > 0
    assert summary["events"] > 0
    assert summary["snapshots"] > 0
    assert (run_dir / "summary.json").exists()
    assert (run_dir / "equity.csv").exists()
    assert (run_dir / "fills.csv").exists()
    assert (run_dir / "orders.csv").exists()
