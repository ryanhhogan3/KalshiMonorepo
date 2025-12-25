import sys
from pathlib import Path
import shutil
import os

# ensure src/ is importable
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from backtest.tests.fixtures.generate_fixture import write_sample_parquet
from backtest.cli import main as backtest_main


def test_e2e_runs_and_writes_outputs(tmp_path):
    # generate fixture into tmp_path
    fixture = tmp_path / 'sample.parquet'
    write_sample_parquet(str(fixture))

    market = 'SAMPLE.MKT'
    start = '2025-11-01T10:00:00Z'
    end = '2025-11-01T10:01:00Z'
    run_args = ['run', '--market', market, '--start', start, '--end', end, '--source', 'parquet', '--fixture', str(fixture)]

    # run the CLI
    backtest_main(run_args)

    # check outputs
    run_dir = Path('runs') / f"run_{market}_{start}_{end}"
    assert run_dir.exists()
    assert (run_dir / 'summary.json').exists()
    assert (run_dir / 'equity.csv').exists()
    # fills/orders may be empty depending on fixture; at least equity.csv should have header
    with open(run_dir / 'equity.csv', 'r') as fh:
        lines = fh.readlines()
    assert len(lines) >= 1
