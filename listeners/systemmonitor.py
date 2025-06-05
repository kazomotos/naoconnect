import psutil
import time
from threading import Thread, Event
from collections import deque
from datetime import datetime

class ReturnInterface:

    def __init__(self, avg_cpu_percent:float, avg_ram_percent:float, 
                 free_disk_gb:float, disk_usage_percent:float) -> None:
        avg_cpu_percent = avg_cpu_percent
        avg_ram_percent = avg_ram_percent
        free_disk_gb = free_disk_gb
        disk_usage_percent = disk_usage_percent


class SystemMonitor:
    def __init__(self, interval_seconds=5, window_minutes=15):
        self.interval_seconds = interval_seconds
        self.window_size = int((60 / interval_seconds) * window_minutes)

        # Gleitende Fenster für CPU und RAM
        self.cpu_readings = deque(maxlen=self.window_size)
        self.ram_readings = deque(maxlen=self.window_size)

        # Hintergrund-Thread
        self.stop_event = Event()
        self.thread = Thread(target=self._collectData, daemon=True)
        self.thread.start()

    def _collectData(self):
        while not self.stop_event.is_set():
            cpu = psutil.cpu_percent(interval=self.interval_seconds)
            ram = psutil.virtual_memory().percent

            self.cpu_readings.append(cpu)
            self.ram_readings.append(ram)

    def getAll(self):
        avg_cpu = sum(self.cpu_readings) / len(self.cpu_readings) if self.cpu_readings else 0.0
        avg_ram = sum(self.ram_readings) / len(self.ram_readings) if self.ram_readings else 0.0

        disk = psutil.disk_usage('/')
        free_disk_gb = disk.free / (1024 ** 3)
        disk_usage_percent = disk.percent

        return( ReturnInterface(
            avg_cpu_percent = round(avg_cpu, 2),
            avg_ram_percent = round(avg_ram, 2),
            free_disk_gb = round(free_disk_gb, 2),
            disk_usage_percent = round(disk_usage_percent, 2)
        ) )

    def stop(self):
        self.stop_event.set()
        self.thread.join()