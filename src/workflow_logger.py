"""
Session wrapper that logs entire workflow execution.
Use this as a context manager or decorator.
"""
import asyncio
import functools
import logging
import time
import traceback
from datetime import datetime
from typing import Any, Callable, Optional

from logging_config import setup_session_logger


class WorkflowSession:
    """
    Context manager for wrapping entire workflows with logging.
    Captures start/end times, errors, and overall execution summary.
    """
    
    def __init__(self, name: str = "Workflow"):
        self.name = name
        self.session_logger = setup_session_logger()
        self.start_time = None
        self.end_time = None
        self.errors = []
        self.warnings = []
    
    def __enter__(self):
        self.start_time = time.time()
        self.session_logger.info(f">>> {self.name} started")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        duration = self.end_time - self.start_time
        
        if exc_type is not None:
            self.errors.append(f"{exc_type.__name__}: {exc_val}")
            self.session_logger.error(
                f"!!! {self.name} FAILED after {duration:.2f}s: {exc_val}",
                exc_info=(exc_type, exc_val, exc_tb)
            )
        else:
            self.session_logger.info(f"<<< {self.name} completed in {duration:.2f}s")
        
        self._log_summary()
        return False  # Re-raise exceptions
    
    def log_event(self, message: str, level: str = "info"):
        """Log an event during the session."""
        method = getattr(self.session_logger, level.lower(), self.session_logger.info)
        method(f"  - {message}")
        
        if level.lower() == "error":
            self.errors.append(message)
        elif level.lower() == "warning":
            self.warnings.append(message)
    
    def _log_summary(self):
        """Log execution summary."""
        if self.errors or self.warnings:
            self.session_logger.warning(
                f"Session Summary: {len(self.errors)} errors, {len(self.warnings)} warnings"
            )
            for err in self.errors:
                self.session_logger.warning(f"  ERROR: {err}")
            for warn in self.warnings:
                self.session_logger.warning(f"  WARNING: {warn}")


class AsyncWorkflowSession(WorkflowSession):
    """Async version of WorkflowSession for async workflows."""
    
    async def __aenter__(self):
        self.start_time = time.time()
        self.session_logger.info(f">>> {self.name} started (async)")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        duration = self.end_time - self.start_time
        
        if exc_type is not None:
            self.errors.append(f"{exc_type.__name__}: {exc_val}")
            self.session_logger.error(
                f"!!! {self.name} FAILED after {duration:.2f}s: {exc_val}",
                exc_info=(exc_type, exc_val, exc_tb)
            )
        else:
            self.session_logger.info(f"<<< {self.name} completed in {duration:.2f}s")
        
        self._log_summary()
        return False


def log_workflow(func: Callable) -> Callable:
    """
    Decorator to log function execution.
    Works with both sync and async functions.
    """
    if asyncio.iscoroutinefunction(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__module__)
            logger.info(f"Starting {func.__name__} with args={args}, kwargs={kwargs}")
            start = time.time()
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start
                logger.info(f"Completed {func.__name__} in {duration:.2f}s")
                return result
            except Exception as e:
                duration = time.time() - start
                logger.error(f"Failed {func.__name__} after {duration:.2f}s: {e}", exc_info=True)
                raise
        return async_wrapper
    else:
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__module__)
            logger.info(f"Starting {func.__name__} with args={args}, kwargs={kwargs}")
            start = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start
                logger.info(f"Completed {func.__name__} in {duration:.2f}s")
                return result
            except Exception as e:
                duration = time.time() - start
                logger.error(f"Failed {func.__name__} after {duration:.2f}s: {e}", exc_info=True)
                raise
        return sync_wrapper
