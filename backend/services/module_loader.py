"""
module_loader.py — auto-discovers and orchestrates modular data-source plugins.

Discovery: scans services/modules/ for:
  - *.py files (single-file modules), e.g. weather.py
  - subdirectories with __init__.py (package modules), e.g. flights/

A valid module exposes one of:
  Type A — Data (scheduled poll):
    KEY      = "key_name"    # str, OR KEYS = [...] for multi-key writers
    TIER     = "fast"|"slow"|"custom"
    DEFAULT  = <value>
    ENABLED  = True          # optional; default True
    RUN_ONCE = False         # optional; airports-style run-once
    INTERVAL_MINUTES = None  # custom-tier only
    INTERVAL_HOURS   = None

    def fetch(current_state=None) -> any

  Type B — Lifecycle (streaming / event-driven):
    LIFECYCLE = True
    def start(app) -> None
    def stop()     -> None

No config file needed.  To disable a module set ENABLED = False in the module.
"""

import importlib
import inspect
import logging
import threading
import concurrent.futures
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

_MODULES_DIR = Path(__file__).parent / "modules"
_SKIP = {"__init__", "base"}


class ModuleLoader:
    def __init__(self):
        self._modules: list = []
        self._scheduler = None
        self._lifecycle_modules: list = []
        self._data_lock = None
        self._mark_fresh = None
        self._latest_data = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self) -> None:
        """Auto-discover and import all enabled modules from services/modules/."""
        for entry in sorted(_MODULES_DIR.iterdir()):
            if entry.is_file() and entry.suffix == ".py" and entry.stem not in _SKIP:
                mod_path = f"services.modules.{entry.stem}"
            elif entry.is_dir() and (entry / "__init__.py").exists():
                mod_path = f"services.modules.{entry.name}"
            else:
                continue

            try:
                mod = importlib.import_module(mod_path)
            except Exception as exc:
                logger.error(f"ModuleLoader: failed to import '{mod_path}': {exc}")
                continue

            if not (hasattr(mod, "KEY") or hasattr(mod, "KEYS") or getattr(mod, "LIFECYCLE", False)):
                logger.debug(f"ModuleLoader: '{entry.name}' has no KEY/KEYS/LIFECYCLE — skipping")
                continue

            if not getattr(mod, "ENABLED", True):
                logger.info(f"ModuleLoader: '{entry.name}' disabled (ENABLED=False)")
                continue

            self._modules.append(mod)
            logger.info(f"ModuleLoader: loaded '{entry.name}'")

    def init_latest_data(self, latest_data: dict) -> None:
        """Write DEFAULT values for every module (including disabled) into latest_data."""
        for entry in sorted(_MODULES_DIR.iterdir()):
            if entry.is_file() and entry.suffix == ".py" and entry.stem not in _SKIP:
                mod_path = f"services.modules.{entry.stem}"
            elif entry.is_dir() and (entry / "__init__.py").exists():
                mod_path = f"services.modules.{entry.name}"
            else:
                continue
            try:
                mod = importlib.import_module(mod_path)
            except Exception:
                continue
            if hasattr(mod, "KEYS"):
                default = getattr(mod, "DEFAULT", {})
                for k in mod.KEYS:
                    if k not in latest_data:
                        latest_data[k] = default.get(k) if isinstance(default, dict) else default
            elif hasattr(mod, "KEY"):
                if mod.KEY not in latest_data:
                    latest_data[mod.KEY] = getattr(mod, "DEFAULT", None)

    def build_fast_keys(self) -> set:
        keys: set = set()
        for mod in self._modules:
            if getattr(mod, "LIFECYCLE", False):
                continue
            if getattr(mod, "TIER", "slow") == "fast":
                keys.update(getattr(mod, "KEYS", []))
                if hasattr(mod, "KEY"):
                    keys.add(mod.KEY)
        return keys

    def build_slow_keys(self) -> set:
        keys: set = set()
        for mod in self._modules:
            if getattr(mod, "LIFECYCLE", False):
                continue
            if getattr(mod, "TIER", "slow") in ("slow", "custom"):
                keys.update(getattr(mod, "KEYS", []))
                if hasattr(mod, "KEY"):
                    keys.add(mod.KEY)
        return keys

    def start_all(self, app, latest_data: dict, data_lock: threading.Lock,
                  mark_fresh, scheduler) -> None:
        """Register scheduler jobs and start lifecycle modules."""
        self._latest_data = latest_data
        self._data_lock = data_lock
        self._mark_fresh = mark_fresh
        self._scheduler = scheduler

        for mod in self._modules:
            name = _mod_name(mod)

            if getattr(mod, "LIFECYCLE", False):
                self._lifecycle_modules.append(mod)
                try:
                    mod.start(app)
                    logger.info(f"ModuleLoader: started lifecycle '{name}'")
                except Exception as exc:
                    logger.error(f"ModuleLoader: lifecycle start failed for '{name}': {exc}")
                continue

            tier = getattr(mod, "TIER", "slow")
            run_once = getattr(mod, "RUN_ONCE", False)
            job_fn = self._make_job_fn(mod)

            if run_once:
                scheduler.add_job(job_fn, "date", run_date=datetime.now(), id=f"mod:{name}:startup")
                logger.info(f"ModuleLoader: '{name}' scheduled run_once on startup")
            elif tier == "fast":
                # Fast modules run via run_fast_modules(); fire once on startup for initial data
                scheduler.add_job(job_fn, "date", run_date=datetime.now(), id=f"mod:{name}:startup")
                logger.info(f"ModuleLoader: '{name}' (fast) fired on startup; driven by fast-tier loop")
            elif tier == "slow":
                scheduler.add_job(job_fn, "date", run_date=datetime.now(), id=f"mod:{name}:startup")
                scheduler.add_job(job_fn, "interval", minutes=30, id=f"mod:{name}:30min")
                logger.info(f"ModuleLoader: '{name}' (slow) scheduled 30min")
            elif tier == "custom":
                scheduler.add_job(job_fn, "date", run_date=datetime.now(), id=f"mod:{name}:startup")
                interval_min = getattr(mod, "INTERVAL_MINUTES", None)
                interval_hr = getattr(mod, "INTERVAL_HOURS", None)
                if interval_min:
                    scheduler.add_job(job_fn, "interval", minutes=interval_min,
                                      id=f"mod:{name}:{interval_min}min")
                    logger.info(f"ModuleLoader: '{name}' (custom) every {interval_min}min")
                elif interval_hr:
                    scheduler.add_job(job_fn, "interval", hours=interval_hr,
                                      id=f"mod:{name}:{interval_hr}h")
                    logger.info(f"ModuleLoader: '{name}' (custom) every {interval_hr}h")
                else:
                    logger.warning(f"ModuleLoader: '{name}' is custom-tier but has no interval; run-once only")

    def run_fast_modules(self) -> None:
        """Called by data_fetcher's fast-tier loop — runs all enabled fast modules in parallel."""
        fast_mods = [
            m for m in self._modules
            if not getattr(m, "LIFECYCLE", False) and getattr(m, "TIER", "slow") == "fast"
        ]
        if not fast_mods:
            return
        fns = [self._make_job_fn(m) for m in fast_mods]
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(fns)) as ex:
            concurrent.futures.wait([ex.submit(fn) for fn in fns])

    def run_slow_modules(self) -> None:
        """Called by data_fetcher's slow-tier loop — runs all enabled slow modules in parallel."""
        slow_mods = [
            m for m in self._modules
            if not getattr(m, "LIFECYCLE", False) and getattr(m, "TIER", "slow") == "slow"
        ]
        if not slow_mods:
            return
        fns = [self._make_job_fn(m) for m in slow_mods]
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(fns)) as ex:
            concurrent.futures.wait([ex.submit(fn) for fn in fns])

    def stop_all(self) -> None:
        for mod in self._lifecycle_modules:
            try:
                mod.stop()
                logger.info(f"ModuleLoader: stopped lifecycle '{_mod_name(mod)}'")
            except Exception as exc:
                logger.error(f"ModuleLoader: lifecycle stop failed for '{_mod_name(mod)}': {exc}")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _make_job_fn(self, mod):
        """Return a zero-arg callable that runs mod.fetch() and writes results."""
        latest_data = self._latest_data
        data_lock = self._data_lock
        mark_fresh = self._mark_fresh
        name = _mod_name(mod)

        if hasattr(mod, "KEYS"):
            keys = list(mod.KEYS)

            def _multi_key_job():
                try:
                    snapshot = {k: latest_data.get(k) for k in keys}
                    result = mod.fetch(current_state=snapshot)
                    if result and isinstance(result, dict):
                        with data_lock:
                            for k, v in result.items():
                                latest_data[k] = v
                        mark_fresh(*[k for k in result if result[k] is not None])
                except Exception as exc:
                    logger.error(f"ModuleLoader: fetch error in '{name}': {exc}", exc_info=True)

            return _multi_key_job
        else:
            key = mod.KEY

            def _single_key_job():
                try:
                    result = mod.fetch()
                    if result is not None:
                        with data_lock:
                            latest_data[key] = result
                        mark_fresh(key)
                except Exception as exc:
                    logger.error(f"ModuleLoader: fetch error in '{name}': {exc}", exc_info=True)

            return _single_key_job


def _mod_name(mod) -> str:
    return mod.__name__.split(".")[-1]


# Singleton used by data_fetcher and main
loader = ModuleLoader()
