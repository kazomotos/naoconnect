# only_one_process.py
import os
import sys
import atexit
import tempfile
from pathlib import Path
from typing import Union

if os.name == "nt":
    import msvcrt
else:
    import fcntl


_LOCK_HANDLE = None
_LOCK_PATH = None


def onlyOneProzess(
    lock_name: str = "desigocc-nao-logging",
    lock_dir: Union[str, None] = None
):
    global _LOCK_HANDLE, _LOCK_PATH

    lock_dir = lock_dir or tempfile.gettempdir()
    Path(lock_dir).mkdir(parents=True, exist_ok=True)
    lockfile = Path(lock_dir) / f"{lock_name}.lock"
    _LOCK_PATH = lockfile

    # Datei immer normal öffnen/anlegen.
    # Die Datei darf dauerhaft existieren.
    f = open(lockfile, "a+b")
    _LOCK_HANDLE = f

    try:
        if os.name == "nt":
            # Für Windows mindestens 1 Byte locken
            f.seek(0)
            if lockfile.stat().st_size == 0:
                f.write(b" ")
                f.flush()

            f.seek(0)
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    except OSError:
        try:
            f.seek(0)
            pid_txt = f.read().decode(errors="ignore").strip()
        except Exception:
            pid_txt = "?"
        print(f"\033[1;31mInstanz '{lock_name}' läuft bereits (PID {pid_txt}).\033[0m")
        f.close()
        sys.exit(0)

    # PID reinschreiben, rein informativ
    try:
        f.seek(0)
        f.truncate()
        f.write(str(os.getpid()).encode())
        f.flush()
        os.fsync(f.fileno())
    except Exception:
        pass

    def _cleanup():
        global _LOCK_HANDLE
        if _LOCK_HANDLE is None:
            return

        try:
            if os.name == "nt":
                _LOCK_HANDLE.seek(0)
                msvcrt.locking(_LOCK_HANDLE.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(_LOCK_HANDLE.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass

        try:
            _LOCK_HANDLE.close()
        except Exception:
            pass

        _LOCK_HANDLE = None

        # Absichtlich KEIN os.remove(lockfile)!
        # Die Datei darf bestehen bleiben.
        # Gesperrt/nicht gesperrt ist entscheidend, nicht Existenz.

    atexit.register(_cleanup)