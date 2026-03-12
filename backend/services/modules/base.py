"""
base.py — Module Protocol definitions (type hints only, not enforced at runtime).

A module is any .py file or package directory in services/modules/ that exposes
one of these duck-typed interfaces.  The loader auto-discovers all of them by
scanning the directory — no config file needed.

Type A — Data module (scheduled poll)
    KEY      = "key_name"      # str, OR use KEYS = [...] for multi-key writers
    TIER     = "fast"|"slow"|"custom"
    DEFAULT  = <value>         # written to latest_data on startup (even if disabled)
    ENABLED  = True            # set False to skip loading
    RUN_ONCE = False           # airports: run on startup only, never repeat
    INTERVAL_MINUTES = None    # only when TIER == "custom"
    INTERVAL_HOURS   = None

    def fetch(current_state=None) -> any
        Single-key: return value directly.
        Multi-key:  return dict {key: value}.
        current_state: snapshot of latest_data keys owned by this module
                       (only passed for multi-key modules).

Type B — Lifecycle module (streaming / event-driven)
    LIFECYCLE = True
    def start(app) -> None
    def stop()     -> None
"""

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class DataModule(Protocol):
    KEY: str
    TIER: str
    DEFAULT: Any
    ENABLED: bool

    def fetch(self) -> Any: ...


@runtime_checkable
class MultiKeyModule(Protocol):
    KEYS: list
    TIER: str
    DEFAULT: dict
    ENABLED: bool

    def fetch(self, current_state: dict) -> dict: ...


@runtime_checkable
class LifecycleModule(Protocol):
    LIFECYCLE: bool

    def start(self, app) -> None: ...
    def stop(self) -> None: ...
