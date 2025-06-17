import psutil
import time
from threading import Thread, Event
from threading import Lock
from collections import deque
from typing import Optional


class ReturnInterface:

    def __init__(self, avg_cpu_percent:float, avg_ram_percent:float, 
                 free_disk_gb:float, disk_usage_percent:float,
                 disk_usage_absolute_gb:float, max_cpu_percent:float) -> None:
        self.max_cpu_percent = max_cpu_percent
        self.avg_cpu_percent = avg_cpu_percent
        self.avg_ram_percent = avg_ram_percent
        self.free_disk_gb = free_disk_gb
        self.disk_usage_percent = disk_usage_percent
        self.disk_usage_absolute_gb = disk_usage_absolute_gb


class LablingInterface:

    def __init__(self, asset_id:str, instance_id:str, avg_cpu_percent_id:Optional[str], 
                 avg_ram_percent_id:Optional[str], free_disk_gb_id:Optional[str], 
                 disk_usage_percent_id:Optional[str],disk_usage_absolute_id: Optional[str],
                 max_cpu_percent_id:Optional[str]) -> None:
        self.asset_id = asset_id
        self.instance_id = instance_id
        self.max_cpu_percent_id = max_cpu_percent_id
        self.avg_cpu_percent_id = avg_cpu_percent_id
        self.avg_ram_percent_id = avg_ram_percent_id
        self.free_disk_gb_id = free_disk_gb_id
        self.disk_usage_percent_id = disk_usage_percent_id
        self.disk_usage_absolute_id = disk_usage_absolute_id
    

class SystemMonitor:
    def __init__(self, sample_interval:int=5, avg_window:int=15) -> None:
        self.sample_interval = sample_interval
        self.window_size = int(( sample_interval) * avg_window)

        # Gleitende Fenster für CPU und RAM
        self.cpu_readings = deque(maxlen=self.window_size)
        self.ram_readings = deque(maxlen=self.window_size)

        # Hintergrund-Thread
        self.stop_event = Event()
        self.thread = Thread(target=self._collectData, args=(), daemon=True)
        self.thread.start()

    def _collectData(self):
        while not self.stop_event.is_set():
            cpu = psutil.cpu_percent(interval=self.sample_interval)
            ram = psutil.virtual_memory().percent

            self.cpu_readings.append(cpu)
            self.ram_readings.append(ram)

    def getAll(self) -> ReturnInterface:
        avg_cpu = sum(self.cpu_readings) / len(self.cpu_readings) if self.cpu_readings else 0.0
        max_cpu = max(self.cpu_readings) if self.cpu_readings else 0.0
        avg_ram = sum(self.ram_readings) / len(self.ram_readings) if self.ram_readings else 0.0

        disk = psutil.disk_usage('/')
        free_disk_gb = disk.free / (1024 ** 3)
        disk_usage_percent = disk.percent
        disk_usage_absolute_gb = disk.used / (1024 ** 3)  # ← NEU

        return( ReturnInterface(
            avg_cpu_percent = round(avg_cpu, 2),
            avg_ram_percent = round(avg_ram, 2),
            max_cpu_percent=round(max_cpu, 2),
            free_disk_gb = round(free_disk_gb, 2),
            disk_usage_percent = round(disk_usage_percent, 2),
            disk_usage_absolute_gb = round(disk_usage_absolute_gb, 2) 
        ) )

    def stop(self):
        self.stop_event.set()
        self.thread.join()

        

class Logger:

    def __init__(self, labling:LablingInterface, logging_interval:int=300, sample_interval:int=5) -> None:
        self.sample_interval = sample_interval
        self.logging_interval = logging_interval
        self.labling = labling

        self.System = SystemMonitor(
            sample_interval=self.sample_interval,
            avg_window=self.logging_interval
        )

        self.telegraf_frame = []
        self._lock = Lock()
        self._stop_event = Event()
        self._thread = Thread(target=self._logLoop, args=(), daemon=True)
        self._thread.start()
    
    def _logLoop(self):
        last_time = time.time()
        while not self._stop_event.is_set():
            time.sleep(self.logging_interval - (time.time()-last_time))
            last_time = time.time()

            with self._lock:
                data = self.System.getAll()
                timestamp = int(time.time() * 1e9)  # Unix-Nanosekunden

                line_parts = []
                if self.labling.avg_cpu_percent_id:
                    line_parts.append(f"{self.labling.avg_cpu_percent_id}={data.avg_cpu_percent}")
                if self.labling.avg_ram_percent_id:
                    line_parts.append(f"{self.labling.avg_ram_percent_id}={data.avg_ram_percent}")
                if self.labling.free_disk_gb_id:
                    line_parts.append(f"{self.labling.free_disk_gb_id}={data.free_disk_gb}")
                if self.labling.disk_usage_percent_id:
                    line_parts.append(f"{self.labling.disk_usage_percent_id}={data.disk_usage_percent}")
                if self.labling.disk_usage_absolute_id:
                    line_parts.append(f"{self.labling.disk_usage_absolute_id}={data.disk_usage_absolute_gb}")
                if self.labling.max_cpu_percent_id:
                    line_parts.append(f"{self.labling.max_cpu_percent_id}={data.max_cpu_percent}")
                if line_parts:
                    line = (
                        f"{self.labling.asset_id},instance={self.labling.instance_id} "
                        + ",".join(line_parts)
                        + f" {timestamp}"
                    )
                    self.telegraf_frame.append(line)

    def getAndClearFrame(self):
        with self._lock:
            frame_copy = self.telegraf_frame.copy()
            self.telegraf_frame.clear()
        return frame_copy

    def stop(self):
        self._stop_event.set()
        self._thread.join()
        self.System.stop()