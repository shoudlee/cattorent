from pathlib import Path
import threading
import queue
from download_worker import DownloadWorker


class DownloadManager:
    def _prepare_target_file(self) -> None:
        self.destination_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.destination_path, "wb") as file_obj:
            file_obj.truncate(self.file_size)

    def __init__(
        self,
        *,
        data_handler,  # 初始data handler, 表示peer那个参数指定的
        filename: str,
        destination_path: Path,
        file_size: int,
        slice_size: int,
        slice_count: int,
        timeout_seconds: float = 3.0,
        max_retries: int = 3,
    ):
        self.data_handler = data_handler
        self.filename = filename
        self.destination_path = Path(destination_path)
        self.file_size = file_size
        self.slice_size = slice_size
        self.slice_count = slice_count
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.stop_event = threading.Event()
        self._prepare_target_file()  # 提前创建空洞文件
        self._filelock = threading.Lock()
        self._state_lock = threading.Lock()
        self.file_slice_queue: queue.Queue[int] = queue.Queue()
        self.completed_slices: set[int] = set()
        self.permanently_failed_slices: set[int] = set()
        self.failed_attempts: dict[int, int] = {}
        self.max_piece_failures = 3
        for i in range(slice_count):
            self.file_slice_queue.put(i)
        self.worker_0 = DownloadWorker(
            data_handler=data_handler,
            filename=filename,
            destination_path=destination_path,
            file_size=file_size,
            slice_size=slice_size,
            slice_count=slice_count,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            filelock=self._filelock,
            state_lock=self._state_lock,
            file_slice_queue=self.file_slice_queue,
            completed_slices=self.completed_slices,
            permanently_failed_slices=self.permanently_failed_slices,
            failed_attempts=self.failed_attempts,
            max_piece_failures=self.max_piece_failures,
        )
        self.download_workers = [self.worker_0]

    def start_download(self) -> tuple[bool, str | None]:
        for worker in self.download_workers:
            worker.start()
        for worker in self.download_workers:
            worker.join()

        with self._state_lock:
            completed_count = len(self.completed_slices)
            failed_list = sorted(self.permanently_failed_slices)

        if completed_count == self.slice_count and not failed_list:
            return True, None

        if failed_list:
            preview = ", ".join(str(i) for i in failed_list[:10])
            if len(failed_list) > 10:
                preview += ", ..."
            return (
                False,
                f"failed pieces: [{preview}] (count={len(failed_list)})",
            )

        remaining = self.slice_count - completed_count
        return False, f"download incomplete, remaining pieces={remaining}"
