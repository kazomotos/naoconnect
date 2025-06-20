import psutil
import time
from threading import Thread, Event
from threading import Lock
from collections import deque
from typing import Optional


class ReturnInterface:

    def __init__(self, avg_cpu_percent: float, avg_ram_percent: float,
                 free_disk_gb: float, disk_usage_percent: float,
                 disk_usage_absolute_gb: float, max_cpu_percent: float,
                 avg_net_rx_kbps: Optional[float] = None, avg_net_tx_kbps: Optional[float] = None) -> None:
        self.max_cpu_percent = max_cpu_percent
        self.avg_cpu_percent = avg_cpu_percent
        self.avg_ram_percent = avg_ram_percent
        self.free_disk_gb = free_disk_gb
        self.disk_usage_percent = disk_usage_percent
        self.disk_usage_absolute_gb = disk_usage_absolute_gb
        self.avg_net_rx_kbps = avg_net_rx_kbps
        self.avg_net_tx_kbps = avg_net_tx_kbps


class LablingInterface:

    def __init__(self, asset_id: str, instance_id: str, avg_cpu_percent_id: Optional[str],
                 avg_ram_percent_id: Optional[str], free_disk_gb_id: Optional[str],
                 disk_usage_percent_id: Optional[str], disk_usage_absolute_id: Optional[str],
                 max_cpu_percent_id: Optional[str],
                 avg_net_rx_kbps_id: Optional[str] = None, avg_net_tx_kbps_id: Optional[str] = None) -> None:
        self.asset_id = asset_id
        self.instance_id = instance_id
        self.max_cpu_percent_id = max_cpu_percent_id
        self.avg_cpu_percent_id = avg_cpu_percent_id
        self.avg_ram_percent_id = avg_ram_percent_id
        self.free_disk_gb_id = free_disk_gb_id
        self.disk_usage_percent_id = disk_usage_percent_id
        self.disk_usage_absolute_id = disk_usage_absolute_id
        self.avg_net_rx_kbps_id = avg_net_rx_kbps_id
        self.avg_net_tx_kbps_id = avg_net_tx_kbps_id


class SystemMonitor:
    def __init__(self, sample_interval: int = 5, avg_window: int = 15, net_interface: Optional[str] = None) -> None:
        self.sample_interval = sample_interval
        self.window_size = int(sample_interval * avg_window)
        self.net_interface = net_interface

        self.cpu_readings = deque(maxlen=self.window_size)
        self.ram_readings = deque(maxlen=self.window_size)
        self.net_rx_readings = deque(maxlen=self.window_size)
        self.net_tx_readings = deque(maxlen=self.window_size)

        self.stop_event = Event()
        self.thread = Thread(target=self._collectData, daemon=True)
        self.thread.start()

    def _get_net_bytes(self):
        try:
            with open(f"/sys/class/net/{self.net_interface}/statistics/rx_bytes", "r") as f:
                rx = int(f.read())
            with open(f"/sys/class/net/{self.net_interface}/statistics/tx_bytes", "r") as f:
                tx = int(f.read())
            return rx, tx
        except FileNotFoundError:
            return 0, 0

    def _collectData(self):
        prev_rx, prev_tx = self._get_net_bytes()
        while not self.stop_event.is_set():
            cpu = psutil.cpu_percent(interval=self.sample_interval)
            ram = psutil.virtual_memory().percent

            self.cpu_readings.append(cpu)
            self.ram_readings.append(ram)

            if self.net_interface:
                curr_rx, curr_tx = self._get_net_bytes()
                rx_kbps = (curr_rx - prev_rx) / 1024 / self.sample_interval
                tx_kbps = (curr_tx - prev_tx) / 1024 / self.sample_interval
                self.net_rx_readings.append(rx_kbps)
                self.net_tx_readings.append(tx_kbps)
                prev_rx, prev_tx = curr_rx, curr_tx

    def getAll(self) -> ReturnInterface:
        avg_cpu = sum(self.cpu_readings) / len(self.cpu_readings) if self.cpu_readings else 0.0
        max_cpu = max(self.cpu_readings) if self.cpu_readings else 0.0
        avg_ram = sum(self.ram_readings) / len(self.ram_readings) if self.ram_readings else 0.0

        disk = psutil.disk_usage('/')
        free_disk_gb = disk.free / (1024 ** 3)
        disk_usage_percent = disk.percent
        disk_usage_absolute_gb = disk.used / (1024 ** 3)

        avg_net_rx = sum(self.net_rx_readings) / len(self.net_rx_readings) if self.net_rx_readings else None
        avg_net_tx = sum(self.net_tx_readings) / len(self.net_tx_readings) if self.net_tx_readings else None

        return ReturnInterface(
            avg_cpu_percent=round(avg_cpu, 2),
            avg_ram_percent=round(avg_ram, 2),
            max_cpu_percent=round(max_cpu, 2),
            free_disk_gb=round(free_disk_gb, 2),
            disk_usage_percent=round(disk_usage_percent, 2),
            disk_usage_absolute_gb=round(disk_usage_absolute_gb, 2),
            avg_net_rx_kbps=round(avg_net_rx, 2) if avg_net_rx is not None else None,
            avg_net_tx_kbps=round(avg_net_tx, 2) if avg_net_tx is not None else None
        )

    def stop(self):
        self.stop_event.set()
        self.thread.join()


class Logger:

    def __init__(self, labling: LablingInterface, logging_interval: int = 300, sample_interval: int = 5, net_interface: Optional[str] = None) -> None:
        self.sample_interval = sample_interval
        self.logging_interval = logging_interval
        self.labling = labling

        self.System = SystemMonitor(
            sample_interval=self.sample_interval,
            avg_window=self.logging_interval,
            net_interface=net_interface
        )

        self.telegraf_frame = []
        self._lock = Lock()
        self._stop_event = Event()
        self._thread = Thread(target=self._logLoop, args=(), daemon=True)
        self._thread.start()

    def _logLoop(self):
        last_time = time.time()
        while not self._stop_event.is_set():
            time.sleep(self.logging_interval - (time.time() - last_time))
            last_time = time.time()

            with self._lock:
                data = self.System.getAll()
                timestamp = int(time.time() * 1e9)

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
                if self.labling.avg_net_rx_kbps_id and data.avg_net_rx_kbps is not None:
                    line_parts.append(f"{self.labling.avg_net_rx_kbps_id}={data.avg_net_rx_kbps}")
                if self.labling.avg_net_tx_kbps_id and data.avg_net_tx_kbps is not None:
                    line_parts.append(f"{self.labling.avg_net_tx_kbps_id}={data.avg_net_tx_kbps}")

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
