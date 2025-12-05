# Comprehensive Logging Guide

## Overview

Your project now has a complete, production-ready logging system that captures your entire workflow in session files. Everything is automatically saved to disk with multiple log levels and rotating file handlers.

---

## Files Created

### 1. **`src/logging_config.py`**
Central logging configuration with:
- **LogConfig class**: Centralized setup for all loggers
- **Console output**: INFO level (lightweight, user-friendly)
- **File output**: DEBUG level (detailed, technical)
- **Rotating file handlers**: Auto-rotates files when they hit 10MB
- **Session logging**: Creates timestamped session files

### 2. **`src/workflow_logger.py`**
High-level workflow tracking with:
- **WorkflowSession**: Context manager for sync workflows
- **AsyncWorkflowSession**: Async context manager for async workflows
- **@log_workflow decorator**: Function-level logging (sync & async)
- **Automatic execution timing** and error tracking

### 3. **`src/kalshifolder/websocket/streamer_logged.py`**
Enhanced streamer with full logging integration:
- Session-level tracking
- Event counters (snapshots, deltas, total)
- Market subscription logging
- Error/warning tracking
- Periodic progress reports

---

## Quick Start

### Option 1: Use the Logged Streamer (Recommended)

```bash
# Run the fully-logged version
python -m kalshifolder.websocket.streamer_logged
```

**Output:**
- `./logs/SESSION_YYYYMMDD_HHMMSS.log` - Complete session log
- `./logs/kalshifolder_websocket_streamer_logged.log` - Module-specific logs
- Console: Real-time INFO messages

### Option 2: Add Logging to Any Script

```python
from logging_config import get_logger
from workflow_logger import AsyncWorkflowSession

logger = get_logger(__name__)

async def my_workflow():
    async with AsyncWorkflowSession("My Workflow") as session:
        session.log_event("Starting processing...")
        
        try:
            result = await process_data()
            session.log_event(f"Completed successfully: {result}")
        except Exception as e:
            session.log_event(f"Error: {e}", level="error")
            raise
```

### Option 3: Decorate Functions

```python
from workflow_logger import log_workflow

@log_workflow
async def fetch_data():
    # Automatically logged with timing and error handling
    pass

@log_workflow
def process_data():
    # Works with sync functions too
    pass
```

---

## Log Files Location

All logs are saved to `./logs/`:

```
logs/
├── SESSION_20251204_143022.log          # Current session (everything)
├── kalshifolder_websocket_streamer_logged.log  # Module-specific
├── kalshifolder_websocket_ws_runtime.log
├── databases_processing_clickhouse_sink.log
└── [old backups...]
```

Each session file contains:
- **Timestamp**: When each event occurred
- **Logger name**: Which module/component logged it
- **Level**: DEBUG, INFO, WARNING, ERROR
- **Message**: The actual log message

---

## Log Levels Explained

| Level | Use Case | Appears In |
|-------|----------|-----------|
| **DEBUG** | Detailed technical info (deltas, sequences, internal state) | File only |
| **INFO** | Important events (connections, subscriptions, progress) | Console + File |
| **WARNING** | Non-fatal issues (gaps, skipped events, retries) | Console + File |
| **ERROR** | Failures that need attention (auth errors, crashes) | Console + File |

---

## Example Log Output

### Console (INFO only)
```
INFO     | [ENV] MARKETS: FEDFUNDS-DEC25,KXBTCMAXY-25-DEC31-129999.99
INFO     | >>> KalshiMonorepo Streamer started
INFO     | Loaded 2 markets: FEDFUNDS-DEC25, KXBTCMAXY-25-DEC31-129999.99
INFO     | Initialized WebSocket client and sinks
INFO     | Connected to Kalshi WebSocket
INFO     | Subscribing to markets: ['FEDFUNDS-DEC25', 'KXBTCMAXY-25-DEC31-129999.99']
INFO     | Subscription confirmed for sid=123
INFO     | Created orderbook for FEDFUNDS-DEC25 (sid=123)
INFO     | Processed 1000 events (100 snapshots, 900 deltas)
```

### Session File (DEBUG + INFO + WARNING + ERROR)
```
2025-12-04 14:30:22 | SESSION                        | INFO     | === SESSION STARTED: 20251204_143022 ===
2025-12-04 14:30:22 | kalshifolder_websocket_streamer_logged | INFO     | === Environment Configuration ===
2025-12-04 14:30:22 | kalshifolder_websocket_streamer_logged | DEBUG    | Snapshot received for FEDFUNDS-DEC25: seq=1, sides=yes, no
2025-12-04 14:30:23 | kalshifolder_websocket_streamer_logged | DEBUG    | Delta: FEDFUNDS-DEC25 yes 9800 delta=5 => 105
2025-12-04 14:30:25 | kalshifolder_websocket_streamer_logged | WARNING  | Sequence gap or negative level for FEDFUNDS-DEC25 at price 9800
2025-12-04 14:30:27 | kalshifolder_websocket_streamer_logged | INFO     | Processed 1000 events (100 snapshots, 900 deltas)
```

---

## Integration with Your Workflow

### Add Logging to WebSocket Runtime

```python
# In src/kalshifolder/websocket/ws_runtime.py
from logging_config import get_logger

logger = get_logger(__name__)

class KalshiWSRuntime:
    def __init__(self, ...):
        logger.info(f"Initializing WebSocket to {self.ws_url}")
        # ...
    
    async def _run_loop(self):
        logger.info("Starting WS event loop")
        while not self._stop.is_set():
            try:
                # ...
            except ConnectionClosed:
                logger.warning("WebSocket connection closed, reconnecting...")
```

### Add Logging to ClickHouse Sink

```python
# In src/databases/processing/clickhouse_sink.py
from logging_config import get_logger

logger = get_logger(__name__)

class ClickHouseSink:
    async def flush(self):
        if not self._buf_ev["type"]:
            return
        rows = len(self._buf_ev["type"])
        logger.info(f"Flushing {rows} events to ClickHouse")
        # ...
        logger.debug(f"Inserted {rows} rows to {self.table_events}")
```

### Add Logging to Parquet Sink

```python
# In src/databases/processing/parquet_sink.py
from logging_config import get_logger

logger = get_logger(__name__)

class ParquetSink:
    async def flush(self):
        if not self._buf:
            return
        size_mb = self._last_est_bytes / (1024 * 1024)
        logger.info(f"Writing Parquet file: {len(self._buf)} rows, ~{size_mb:.2f}MB")
        # ...
```

---

## Querying Logs

### Find all errors in today's session
```bash
grep "ERROR" logs/SESSION_*.log | head -20
```

### Find WebSocket connection issues
```bash
grep "WebSocket\|connection\|Connection" logs/SESSION_*.log
```

### Find specific market events
```bash
grep "FEDFUNDS-DEC25" logs/SESSION_*.log | tail -50
```

### Monitor in real-time
```bash
tail -f logs/SESSION_*.log
```

---

## Configuration Options

Edit `src/logging_config.py` to customize:

```python
class LogConfig:
    # Change console verbosity
    CONSOLE_LEVEL = logging.DEBUG  # More verbose
    
    # Change file verbosity
    FILE_LEVEL = logging.WARNING  # Less verbose
    
    # Change rotation settings
    # File size before rotation (default 10MB)
    maxBytes=50 * 1024 * 1024  # 50MB
    
    # Number of backup files to keep (default 10)
    backupCount=20
    
    # Change log format
    DETAILED_FORMAT = '%(asctime)s | %(name)-30s | %(levelname)-8s | %(message)s'
```

---

## Docker Integration

When running in Docker, logs are preserved:

```bash
# Build and run
docker build -t kalshi-streamer .
docker run -v ./logs:/app/logs kalshi-streamer

# Logs persist on host in ./logs/
```

### Docker Compose
```yaml
services:
  streamer:
    volumes:
      - ./logs:/app/logs  # Mount logs directory
```

---

## Monitoring & Analysis

### Session Statistics
Extract from logs:
```bash
# Count events
grep "Processed" logs/SESSION_*.log | tail -1

# Count errors
grep -c "ERROR" logs/SESSION_*.log

# Count warnings
grep -c "WARNING" logs/SESSION_*.log

# Total runtime
head -1 logs/SESSION_*.log
tail -1 logs/SESSION_*.log
```

### Performance Analysis
```bash
# Find slowest operations
grep "completed in" logs/SESSION_*.log | sort -t' ' -k4 -rn | head -10
```

---

## Best Practices

### 1. Use Session Logging for Workflows
```python
async with AsyncWorkflowSession("Data Pipeline") as session:
    session.log_event("Step 1: Connecting...")
    # ...
    session.log_event("Step 2: Processing...")
```

### 2. Use Function Decorators for Individual Operations
```python
@log_workflow
async def fetch_market_data():
    pass
```

### 3. Use `get_logger(__name__)` in Modules
```python
logger = get_logger(__name__)
logger.info("Something important happened")
```

### 4. Log Errors with Context
```python
try:
    result = await risky_operation()
except Exception as e:
    session.log_event(f"Failed: {e} (context: {ctx})", level="error")
    raise
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Logs not appearing | Check `./logs/` directory exists; ensure script runs `setup_session_logger()` |
| Logs grow too large | Adjust `maxBytes` and `backupCount` in LogConfig |
| Too much console output | Reduce `CONSOLE_LEVEL` (set to WARNING) |
| Not enough detail in file | Reduce `FILE_LEVEL` (set to DEBUG) |
| Can't find specific events | Use `grep` or `tail` to search session logs |

---

## Summary

Your logging system now:
✅ Captures entire sessions in timestamped files  
✅ Logs to console (real-time) and files (archive)  
✅ Auto-rotates files when they get large  
✅ Tracks execution timing and errors  
✅ Works with sync and async code  
✅ Integrates seamlessly with Docker  
✅ Provides DEBUG, INFO, WARNING, ERROR levels  

**Everything is automatically saved. Just run and all details are logged!**
