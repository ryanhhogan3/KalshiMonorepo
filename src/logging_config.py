"""
Centralized logging configuration for KalshiMonorepo.
Logs to both console and files with rotation.
"""
import logging
import logging.handlers
import os
from datetime import datetime
from pathlib import Path


class LogConfig:
    """Centralized logging configuration."""
    
    LOG_DIR = Path("./logs")
    LOG_DIR.mkdir(exist_ok=True)
    
    # Log levels
    CONSOLE_LEVEL = logging.INFO
    FILE_LEVEL = logging.DEBUG
    
    # Log formats
    DETAILED_FORMAT = '%(asctime)s | %(name)-30s | %(levelname)-8s | %(message)s'
    SIMPLE_FORMAT = '%(levelname)-8s | %(message)s'
    DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    
    @classmethod
    def setup_logger(cls, name: str, log_file: str = None) -> logging.Logger:
        """
        Set up a logger with console and file handlers.
        
        Args:
            name: Logger name (typically __name__)
            log_file: Optional specific log file name. If None, uses module name.
        
        Returns:
            Configured logger instance
        """
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)  # Capture everything
        
        # Avoid duplicate handlers
        if logger.handlers:
            return logger
        
        # Console handler (INFO level)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(cls.CONSOLE_LEVEL)
        console_formatter = logging.Formatter(cls.SIMPLE_FORMAT, datefmt=cls.DATE_FORMAT)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        # File handler (DEBUG level, rotated daily)
        if log_file is None:
            log_file = f"{name.replace('.', '_')}.log"
        
        log_path = cls.LOG_DIR / log_file
        file_handler = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=10 * 1024 * 1024,  # 10MB per file
            backupCount=10,  # Keep 10 backups
        )
        file_handler.setLevel(cls.FILE_LEVEL)
        file_formatter = logging.Formatter(cls.DETAILED_FORMAT, datefmt=cls.DATE_FORMAT)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        return logger
    
    @classmethod
    def setup_session_logger(cls) -> logging.Logger:
        """
        Set up a session-wide logger that captures all events.
        Logs to a session file with timestamp.
        
        Returns:
            Session logger instance
        """
        session_name = datetime.now().strftime("%Y%m%d_%H%M%S")
        session_log_file = f"session_{session_name}.log"
        
        session_logger = logging.getLogger("SESSION")
        session_logger.setLevel(logging.DEBUG)
        
        if session_logger.handlers:
            return session_logger
        
        # Session file handler (all messages)
        log_path = cls.LOG_DIR / session_log_file
        session_handler = logging.FileHandler(log_path)
        session_handler.setLevel(logging.DEBUG)
        session_formatter = logging.Formatter(
            '%(asctime)s | %(name)-30s | %(levelname)-8s | %(message)s',
            datefmt=cls.DATE_FORMAT
        )
        session_handler.setFormatter(session_formatter)
        session_logger.addHandler(session_handler)
        
        # Also console (INFO only)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(cls.CONSOLE_LEVEL)
        console_formatter = logging.Formatter(cls.SIMPLE_FORMAT, datefmt=cls.DATE_FORMAT)
        console_handler.setFormatter(console_formatter)
        session_logger.addHandler(console_handler)
        
        session_logger.info(f"=== SESSION STARTED: {session_name} ===")
        return session_logger
    
    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """Get or create a logger for a module."""
        logger = logging.getLogger(name)
        if not logger.handlers:
            cls.setup_logger(name)
        return logger


# Convenience function
def get_logger(name: str) -> logging.Logger:
    """Get a configured logger for a module."""
    return LogConfig.get_logger(name)


def setup_session_logger() -> logging.Logger:
    """Set up session-wide logging."""
    return LogConfig.setup_session_logger()
