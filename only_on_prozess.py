# only_one_process.py
import os, sys, atexit, tempfile
from pathlib import Path

try:
    import fcntl  # POSIX flock
except ImportError:
    fcntl = None

def onlyOneProcess(lock_name: str = "desigocc-nao-logging", lock_dir: str | None = None):
    """
    Erzwingt Single-Instance per flock. Beendet das Programm,
    wenn bereits eine andere Instanz läuft.

    Nutzung:
        from only_one_process import only_one_process
        only_one_process("desigocc-nao-logging")
    """
    if fcntl is None:
        # Fallback: best effort Lockfile (ohne flock); besser: psutil/pidfile nutzen
        lock_dir = lock_dir or tempfile.gettempdir()
        lockfile = Path(lock_dir) / f"{lock_name}.lock"
        try:
            fd = os.open(lockfile, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644)
        except FileExistsError:
            print(f"\033[1;31mInstanz '{lock_name}' läuft bereits (Lock existiert).\033[0m")
            sys.exit(0)
        os.write(fd, str(os.getpid()).encode())
        def _cleanup():
            try: os.close(fd)
            except: pass
            try: os.remove(lockfile)
            except: pass
        atexit.register(_cleanup)
        return

    # flock-Variante
    lock_dir = lock_dir or ("/var/lock" if os.access("/var/lock", os.W_OK) else tempfile.gettempdir())
    Path(lock_dir).mkdir(parents=True, exist_ok=True)
    lockfile = Path(lock_dir) / f"{lock_name}.lock"

    # Datei öffnen (bleibt während der Laufzeit offen – wichtig!)
    fd = os.open(lockfile, os.O_RDWR | os.O_CREAT, 0o644)

    try:
        # exklusiv & non-blocking sperren
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        # Jemand hält bereits den Lock
        try:
            with open(lockfile, "r") as f:
                pid_txt = f.read().strip()
        except Exception:
            pid_txt = "?"
        print(f"\033[1;31mInstanz '{lock_name}' läuft bereits (PID {pid_txt}).\033[0m")
        sys.exit(0)

    # Unsere PID reinschreiben (nur informativ)
    try:
        os.ftruncate(fd, 0)
        os.write(fd, str(os.getpid()).encode())
        os.fsync(fd)
    except Exception:
        pass

    # Beim Exit Lock lösen & Datei löschen
    def _cleanup():
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            os.close(fd)
        except Exception:
            pass
        try:
            os.remove(lockfile)
        except Exception:
            pass

    atexit.register(_cleanup)
