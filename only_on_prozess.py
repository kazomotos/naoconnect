from os import getpid
import psutil
import ctypes
import sys

def onlyOneProzess(prozess_name=b"nao-prozess"):
    current_pid = getpid()

    for proc in psutil.process_iter(['pid', 'name']):
        try:
            if proc.pid != current_pid and proc.info['name'] == prozess_name.decode():
                RED_BOLD = "\033[1;31m"
                RESET = "\033[0m"
                print(f"{RED_BOLD}Instanz mit Namen '{prozess_name.decode()}' läuft bereits (PID {proc.pid}). Beende mich selbst.{RESET}")

                sys.exit(0)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    libc = ctypes.cdll.LoadLibrary("libc.so.6")
    libc.prctl(15, prozess_name, 0, 0, 0)