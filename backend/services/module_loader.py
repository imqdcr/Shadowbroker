"""
module_loader.py — orchestrates modular data-source plugins.

Each module in services/modules/ exposes a duck-typed interface:

  Type A — Data module (scheduled poll):
    KEY      = "key_name"    # or KEYS = [...] for multi-key
    TIER     = "fast"|"slow"|"custom"
    DEFAULT  = <value>
    INTERVAL_MINUTES / INTERVAL_HOURS  # only for TIER=="custom"
    RUN_ONCE = False         # set True for run-on-startup-only modules
    def fetch(current_state=None) -> any

  Type B — Lifecycle module (streaming / event-driven):
    LIFECYCLE = True
    def start(app) -> None
    def stop()     -> None

Modules are listed in modules.yaml; only enabled=true entries are loaded.
"""

import importlib
import logging
import threading
import concurrent.futures
from pathlib import Path
from datetime import datetime

import yaml

logger = logging.getLogger(__name__)

_MODULES_DIR = Path(__file__).parent / "modules"
_YAML_PATH = Path(__file__).parent.parent / "modules.yaml"


class ModuleLoader:
    def __init__(self):
        self._modules: list = []          # loaded module objects
        self._scheduler = None            # set by start_all
        self._lifecycle_modules: list = []
        self._data_lock = None
        self._mark_fresh = None
        self._latest_data = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self, yaml_path: Path = _YAML_PATH) -> None:
        """Import all enabled modules listed in modules.yaml."""
        try:
            with open(yaml_path, "r") as fh:
                config = yaml.safe_load(fh)
        except FileNotFoundError:
            logger.warning(f"modules.yaml not found at {yaml_path}; no modules loaded.")
            return

        entries = config.get("modules", [])
        for entry in entries:
            if not entry.get("enabled", False):
                continue
            name = entry["name"]
            try:
                mod = importlib.import_module(f"services.modules.{name}")
                # Attach yaml-level overrides onto the module so the loader can
                # use them without inspecting the yaml again.
                if not hasattr(mod, "TIER") or mod.TIER == "custom":
                    if "interval_minutes" in entry:
                        mod._YAML_INTERVAL_MINUTES = entry["interval_minutes"]
                    if "interval_hours" in entry:
                        mod._YAML_INTERVAL_HOURS = entry["interval_hours"]
                if entry.get("run_once"):
                    mod._YAML_RUN_ONCE = True
                self._modules.append(mod)
                logger.info(f"ModuleLoader: loaded '{name}'")
            except Exception as exc:
                logger.error(f"ModuleLoader: failed to load '{name}': {exc}")

    def init_latest_data(self, latest_data: dict) -> None:
        """Write DEFAULT values for every module into latest_data (even disabled ones)."""
        # Also process disabled modules so keys are always present.
        try:
            yaml_path = _YAML_PATH
            with open(yaml_path, "r") as fh:
                config = yaml.safe_load(fh)
            for entry in config.get("modules", []):
                name = entry["name"]
                try:
                    mod = importlib.import_module(f"services.modules.{name}")
                    if hasattr(mod, "KEYS"):
                        for k in mod.KEYS:
                            if k not in latest_data:
                                default = mod.DEFAULT if not isinstance(mod.DEFAULT, dict) else mod.DEFAULT.get(k)
                                latest_data[k] = default
                    elif hasattr(mod, "KEY"):
                        if mod.KEY not in latest_data:
                            latest_data[mod.KEY] = mod.DEFAULT
                except Exception:
                    pass  # silent — module may not exist yet
        except Exception:
            pass

    def build_fast_keys(self) -> set:
        """Return the set of keys owned by fast-tier modules."""
        keys = set()
        for mod in self._modules:
            if getattr(mod, "LIFECYCLE", False):
                continue
            tier = getattr(mod, "TIER", "slow")
            if tier == "fast":
                if hasattr(mod, "KEYS"):
                    keys.update(mod.KEYS)
                elif hasattr(mod, "KEY"):
                    keys.add(mod.KEY)
        return keys

    def build_slow_keys(self) -> set:
        """Return the set of keys owned by slow/custom-tier modules."""
        keys = set()
        for mod in self._modules:
            if getattr(mod, "LIFECYCLE", False):
                continue
            tier = getattr(mod, "TIER", "slow")
            if tier in ("slow", "custom"):
                if hasattr(mod, "KEYS"):
                    keys.update(mod.KEYS)
                elif hasattr(mod, "KEY"):
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
            if getattr(mod, "LIFECYCLE", False):
                self._lifecycle_modules.append(mod)
                try:
                    mod.start(app)
                    logger.info(f"ModuleLoader: started lifecycle module '{mod.__name__}'")
                except Exception as exc:
                    logger.error(f"ModuleLoader: lifecycle start failed for '{mod.__name__}': {exc}")
                continue

            tier = getattr(mod, "TIER", "slow")
            run_once = getattr(mod, "RUN_ONCE", False) or getattr(mod, "_YAML_RUN_ONCE", False)
            job_fn = self._make_job_fn(mod)

            name = _mod_name(mod)
            if run_once:
                scheduler.add_job(job_fn, "date", run_date=datetime.now(), id=f"mod:{name}:startup")
                logger.info(f"ModuleLoader: '{name}' scheduled run_once on startup")
            elif tier == "fast":
                scheduler.add_job(job_fn, "date", run_date=datetime.now(), id=f"mod:{name}:startup")
                logger.info(f"ModuleLoader: '{name}' (fast) fired on startup; driven by fast-tier loop")
            elif tier == "slow":
                scheduler.add_job(job_fn, "date", run_date=datetime.now(), id=f"mod:{name}:startup")
                scheduler.add_job(job_fn, "interval", minutes=30, id=f"mod:{name}:30min")
                logger.info(f"ModuleLoader: '{name}' (slow) scheduled 30min")
            elif tier == "custom":
                scheduler.add_job(job_fn, "date", run_date=datetime.now(), id=f"mod:{name}:startup")
                interval_min = getattr(mod, "_YAML_INTERVAL_MINUTES", None) \
                    or getattr(mod, "INTERVAL_MINUTES", None)
                interval_hr = getattr(mod, "_YAML_INTERVAL_HOURS", None) \
                    or getattr(mod, "INTERVAL_HOURS", None)
                if interval_min:
                    scheduler.add_job(job_fn, "interval", minutes=interval_min, id=f"mod:{name}:{interval_min}min")
                    logger.info(f"ModuleLoader: '{name}' (custom) scheduled every {interval_min}min")
                elif interval_hr:
                    scheduler.add_job(job_fn, "interval", hours=interval_hr, id=f"mod:{name}:{interval_hr}h")
                    logger.info(f"ModuleLoader: '{name}' (custom) scheduled every {interval_hr}h")
                else:
                    logger.warning(f"ModuleLoader: '{name}' is custom-tier but has no interval; running once only")

    def run_fast_modules(self) -> None:
        """Called by data_fetcher's fast-tier loop to execute all enabled fast modules."""
        fast_mods = [
            m for m in self._modules
            if not getattr(m, "LIFECYCLE", False) and getattr(m, "TIER", "slow") == "fast"
        ]
        if not fast_mods:
            return
        fns = [self._make_job_fn(m) for m in fast_mods]
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(fns)) as ex:
            futures = [ex.submit(fn) for fn in fns]
            concurrent.futures.wait(futures)

    def run_slow_modules(self) -> None:
        """Called by data_fetcher's slow-tier loop to execute all enabled slow modules."""
        slow_mods = [
            m for m in self._modules
            if not getattr(m, "LIFECYCLE", False) and getattr(m, "TIER", "slow") in ("slow",)
        ]
        if not slow_mods:
            return
        fns = [self._make_job_fn(m) for m in slow_mods]
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(fns)) as ex:
            futures = [ex.submit(fn) for fn in fns]
            concurrent.futures.wait(futures)

    def stop_all(self) -> None:
        """Stop all lifecycle modules."""
        for mod in self._lifecycle_modules:
            try:
                mod.stop()
                logger.info(f"ModuleLoader: stopped lifecycle module '{_mod_name(mod)}'")
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
                    logger.error(f"ModuleLoader: fetch error in '{_mod_name(mod)}': {exc}")

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
                    logger.error(f"ModuleLoader: fetch error in '{_mod_name(mod)}': {exc}")

            return _single_key_job


def _mod_name(mod) -> str:
    return mod.__name__.split(".")[-1]


# Singleton used by data_fetcher and main
loader = ModuleLoader()
