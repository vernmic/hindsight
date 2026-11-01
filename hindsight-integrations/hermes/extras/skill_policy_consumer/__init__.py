"""Native plugin entrypoint; see ``consumer.py`` and ``CONTRACT.md``."""

from .consumer import register

__all__ = ["register"]
