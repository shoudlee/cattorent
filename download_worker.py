import queue
import threading
from pathlib import Path

from logging_setup import get_logger


logger = get_logger(__name__)


class DownloadWorker(threading.Thread):
    def __init__(
        self,
        *,
        data_handler,
        filename: str,
        destination_path: Path,
        file_size: int,
        slice_size: int,
        slice_count: int,
        timeout_seconds: float = 3.0,
        max_retries: int = 3,
        file_slice_queue: queue.Queue[int],
        filelock: threading.Lock,
        state_lock: threading.Lock,
        completed_slices: set[int],
        permanently_failed_slices: set[int],
        failed_attempts: dict[int, int],
        max_piece_failures: int = 3,
    ):
        super().__init__(daemon=True)
        self.data_handler = data_handler
        self.filename = filename
        self.destination_path = Path(destination_path)
        self.file_size = file_size
        self.slice_size = slice_size
        self.slice_count = slice_count
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.stop_event = threading.Event()
        self.filelock = filelock
        self.state_lock = state_lock
        self.file_slice_queue = file_slice_queue
        self.completed_slices = completed_slices
        self.permanently_failed_slices = permanently_failed_slices
        self.failed_attempts = failed_attempts
        self.max_piece_failures = max_piece_failures

    def stop(self):
        self.stop_event.set()

    def _piece_size_for_index(self, piece_index: int) -> int:
        if piece_index < 0 or piece_index >= self.slice_count:
            raise IndexError("piece index out of range")
        if piece_index == self.slice_count - 1:
            remaining = self.file_size - piece_index * self.slice_size
            return max(0, remaining)
        return self.slice_size

    def _request_piece_once(self, piece_index: int) -> bytes | None:
        waiter = self.data_handler.send_getp_and_register_waiter(
            self.filename, piece_index
        )
        try:
            if not waiter.wait(self.timeout_seconds):
                self.data_handler.cancel_waiter(self.filename, piece_index)
                return None
            return waiter.get_result()
        except Exception as exc:
            logger.warning(
                "GETP failed for %s piece=%s with error: %s",
                self.filename,
                piece_index,
                exc,
            )
            return None

    def _request_piece_with_retry(self, piece_index: int) -> bytes | None:
        for attempt in range(1, self.max_retries + 1):
            if self.stop_event.is_set():
                return None
            piece_data = self._request_piece_once(piece_index)
            if piece_data is not None:
                return piece_data
            logger.warning(
                "GETP timeout/retry peer=%s file=%s piece=%s attempt=%s/%s",
                self.data_handler.peer_ip,
                self.filename,
                piece_index,
                attempt,
                self.max_retries,
            )
        return None

    def _mark_piece_failure(self, piece_index: int, reason: str) -> None:
        with self.state_lock:
            attempts = self.failed_attempts.get(piece_index, 0) + 1
            self.failed_attempts[piece_index] = attempts

            if attempts >= self.max_piece_failures:
                self.permanently_failed_slices.add(piece_index)
                logger.error(
                    "Giving up piece file=%s piece=%s peer=%s attempts=%s reason=%s",
                    self.filename,
                    piece_index,
                    self.data_handler.peer_ip,
                    attempts,
                    reason,
                )
                return

        self.file_slice_queue.put(piece_index)
        logger.warning(
            "Requeued piece file=%s piece=%s peer=%s attempt=%s/%s reason=%s",
            self.filename,
            piece_index,
            self.data_handler.peer_ip,
            attempts,
            self.max_piece_failures,
            reason,
        )

    def run(self) -> None:
        try:
            with open(self.destination_path, "r+b") as file_obj:
                while True:
                    if self.stop_event.is_set():
                        return

                    try:
                        piece_index = self.file_slice_queue.get(timeout=0.5)
                    except queue.Empty:
                        return
                    try:
                        with self.state_lock:
                            if piece_index in self.completed_slices:
                                continue
                            if piece_index in self.permanently_failed_slices:
                                continue

                        piece_data = self._request_piece_with_retry(piece_index)
                        if piece_data is None:
                            self._mark_piece_failure(
                                piece_index, "request timeout or peer error"
                            )
                            continue

                        expected_size = self._piece_size_for_index(piece_index)
                        if len(piece_data) != expected_size:
                            self._mark_piece_failure(
                                piece_index,
                                f"invalid piece size expected={expected_size} got={len(piece_data)}",
                            )
                            continue

                        offset = piece_index * self.slice_size
                        with self.filelock:
                            file_obj.seek(offset)
                            file_obj.write(piece_data)

                        with self.state_lock:
                            self.completed_slices.add(piece_index)
                            self.failed_attempts.pop(piece_index, None)

                        logger.info(
                            "Downloaded piece file=%s piece=%s/%s",
                            self.filename,
                            piece_index + 1,
                            self.slice_count,
                        )
                    finally:
                        self.file_slice_queue.task_done()
        except Exception:
            logger.exception(
                "Download worker crashed for file=%s, ip=%s",
                self.filename,
                self.data_handler.peer_ip,
            )
