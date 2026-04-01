import os
import logging
from logging.handlers import RotatingFileHandler
import colorlog

# Ensure the logs directory exists at the root of the project
LOGS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

_ANSI_GREEN      = '\x1b[32m'
_ANSI_LIGHT_BLUE = '\x1b[94m'


class _ConnectorAwareFormatter(colorlog.ColoredFormatter):
    """Colors connector INFO lines light blue; everything else follows the normal level colors."""

    def format(self, record: logging.LogRecord) -> str:
        result = super().format(record)
        if record.name.endswith('_connector') and record.levelno == logging.INFO:
            if result.startswith(_ANSI_GREEN):
                result = _ANSI_LIGHT_BLUE + result[len(_ANSI_GREEN):]
        return result


def setup_logger(name: str) -> logging.Logger:
    """Sets up a logger with a colored console handler (INFO) and rotating file handler (DEBUG)."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # Catch all levels; handlers will do the filtering

    # Prevent handler duplication if setup_logger is called multiple times for the same module
    if logger.hasHandlers():
        return logger

    # Log format specified in the Day-1 spec
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    # 1. Console Handler (INFO level with colors)
    console_handler = colorlog.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = _ConnectorAwareFormatter(
        "%(log_color)s" + log_format,
        datefmt=date_format,
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        }
    )
    console_handler.setFormatter(console_formatter)

    # 2. File Handler (DEBUG level, rotating with max 10MB and 5 backups)
    log_file_path = os.path.join(LOGS_DIR, "career_copilot.log")
    file_handler = RotatingFileHandler(
        log_file_path,
        maxBytes=10 * 1024 * 1024,  # 10 MB limit
        backupCount=5,              # Keep up to 5 historical logs
        encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(log_format, datefmt=date_format)
    file_handler.setFormatter(file_formatter)

    # Add Handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
