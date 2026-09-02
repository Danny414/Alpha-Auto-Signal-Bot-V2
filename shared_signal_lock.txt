"""
Standalone stub for shared_signal_lock.

In the original multi-bot setup this file coordinates signal locks between
the Alpha Bot and NDF Bot via a shared JSON file. Running standalone on
Railway means there is no NDF Bot, so:
  - check_conflict()  → always returns False (nothing to conflict with)
  - set_lock()        → writes a local lock file (harmless, not read by anyone)
  - clear_lock()      → removes the local lock file
"""
import os, json, time

_LOCK_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "signal_locks.json")


def _read_locks() -> dict:
    try:
        with open(_LOCK_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def _write_locks(data: dict) -> None:
    try:
        with open(_LOCK_FILE, "w") as f:
            json.dump(data, f)
    except Exception:
        pass


def check_conflict(symbol: str, direction: str) -> bool:
    """
    Returns True if another bot has an open signal in the OPPOSITE direction
    for this symbol. Standalone → always False.
    """
    return False


def set_lock(symbol: str, direction: str, source: str = "alpha", **kwargs) -> None:
    """Record that this bot has an open signal. No-op if no other bot reads it."""
    locks = _read_locks()
    locks[symbol] = {
        "direction": direction,
        "source": source,
        "ts": time.time(),
        **{k: v for k, v in kwargs.items() if isinstance(v, (str, int, float, bool))},
    }
    _write_locks(locks)


def clear_lock(symbol: str) -> None:
    """Remove the lock for a symbol when its signal closes."""
    locks = _read_locks()
    locks.pop(symbol, None)
    _write_locks(locks)
