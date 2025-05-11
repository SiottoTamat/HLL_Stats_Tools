import logging
import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

cfg = yaml.safe_load(Path("config.yaml").read_text())
to_console = cfg["to_console"]


def setup_logger(
    name: str = "hll_logger", level: int = logging.INFO, to_console: bool = True
) -> logging.Logger:
    """
    Sets up a logger with both file and optional console output.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # avoid adding handlers multiple times

    logger.setLevel(level)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Ensure log directory exists
    load_dotenv(".env")
    log_file = Path(os.getenv("log_file"))
    # log_file = Path(log_file)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # File handler
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # Console handler
    if to_console:
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger


def log_debug(logger, message, *args, maxlen=1000):
    """
    Logs a message only if logger level is DEBUG,
    with optional truncation for long args.

    Parameters:
        logger (logging.Logger): The logger instance to use.
        message (str): The format string with `%s` placeholders.
        *args: Arguments to format into the message.
        maxlen (int): Max length for any individual argument;
        longer ones will be truncated.
    """
    if logger.isEnabledFor(logging.DEBUG):
        truncated_args = [
            (str(a)[:maxlen] + "..." if len(str(a)) > maxlen else a) for a in args
        ]
        logger.debug(message, *truncated_args)
