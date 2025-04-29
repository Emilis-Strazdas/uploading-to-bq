import logging
from dataclasses import dataclass, field

def configure_logger(
    logging_level=logging.DEBUG,
    logging_format="%(asctime)s [%(levelname)s] [%(filename)s] %(message)s",
    logging_handlers=None,
    logging_force=True,
):
    """Configure the logger settings."""
    if logging_handlers is None:
        logging_handlers = [logging.StreamHandler()]

    logging.basicConfig(
        level=logging_level,
        format=logging_format,
        handlers=logging_handlers,
        force=logging_force,
    )
