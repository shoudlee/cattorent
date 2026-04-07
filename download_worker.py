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
        self.completed_slices: set[int] = set()

    def stop(self):
        self.stop_event.set()

    def _piece_size_for_index(self, piece_index: int) -> int:
        if piece_index < 0 or piece_index >= self.slice_count:
            raise IndexError("piece index out of range")
        if piece_index == self.slice_count - 1:
            remaining = self.file_size - piece_index * self.slice_size
            return max(0, remaining)
        return self.slice_size

    def _prepare_target_file(self) -> None:
        self.destination_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.destination_path, "wb") as file_obj:
            file_obj.truncate(self.file_size)

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

    def run(self) -> None:
        try:
            self._prepare_target_file()
            with open(self.destination_path, "r+b") as file_obj:
                for piece_index in range(self.slice_count):
                    if self.stop_event.is_set():
                        return
                    if piece_index in self.completed_slices:
                        continue

                    piece_data = self._request_piece_with_retry(piece_index)
                    if piece_data is None:
                        logger.error(
                            "Failed to download piece after retries: file=%s piece=%s",
                            self.filename,
                            piece_index,
                        )
                        return

                    expected_size = self._piece_size_for_index(piece_index)
                    if len(piece_data) > expected_size:
                        logger.error(
                            "Invalid piece length: file=%s piece=%s expected<=%s got=%s",
                            self.filename,
                            piece_index,
                            expected_size,
                            len(piece_data),
                        )
                        return

                    offset = piece_index * self.slice_size
                    file_obj.seek(offset)
                    file_obj.write(piece_data)
                    self.completed_slices.add(piece_index)

                    logger.info(
                        "Downloaded piece file=%s piece=%s/%s",
                        self.filename,
                        piece_index + 1,
                        self.slice_count,
                    )

            logger.info("Download complete: %s -> %s", self.filename, self.destination_path)
        except Exception:
            logger.exception("Download worker crashed for file=%s", self.filename)
