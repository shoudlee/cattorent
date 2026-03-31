import socket
import threading
from dataclasses import dataclass
from queue import Empty, Queue
from typing import Callable

from logging_setup import get_logger
from protocol_codec import (
    build_list_request,
    build_list_response,
    build_meta_request,
    build_meta_response,
    is_valid_meta_payload,
    parse_list_response,
    parse_meta_request,
    unpack_frame_body,
)
from tcp_utils import close_socket_quietly, recv_exact, safe_get_peer_ip


logger = get_logger(__name__)


@dataclass
class ConnectionHandlerCallbacks:
    # 连接层通过回调访问上层业务，避免直接依赖 protocol 内部状态。
    list_local_files: Callable[[], dict[str, int]]
    load_meta_content: Callable[[str], bytes | None]
    on_file_list: Callable[[str, list[tuple[str, int]]], None]
    on_meta_received: Callable[[str, str, bytes], None]


class ControlConnectionHandler(threading.Thread):
    def __init__(
        self,
        manager,
        sock: socket.socket,
        callbacks: ConnectionHandlerCallbacks,
    ):
        super().__init__(daemon=True)
        self.manager = manager
        self.socket = sock
        self.callbacks = callbacks
        self.stop_event = threading.Event()
        self.queue: Queue[dict] = Queue()
        self.pending_meta_filename: str | None = None
        self.peer_ip = safe_get_peer_ip(sock) or "unknown"
        self._pending_meta_lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._closed = False
        self._send_thread: threading.Thread | None = None
        self._recv_thread: threading.Thread | None = None

    def stop(self) -> None:
        self.stop_event.set()
        self._close_socket_once()

    def _close_socket_once(self) -> None:
        # send/recv 两个线程都可能触发关闭，这里保证 socket 只被实际关闭一次。
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
        close_socket_quietly(self.socket)

    def request_list(self) -> None:
        self.queue.put({"command": "LIST"})

    def request_meta(self, filename: str) -> None:
        self.queue.put({"command": "META", "filename": filename})

    def _handle_outgoing_task(self, task: dict) -> None:
        # 所有主动发送的数据都统一从发送线程出队，避免多个线程直接写同一个 socket。
        command = task.get("command")
        if command == "LIST":
            self.socket.sendall(build_list_request())
            return
        if command == "RESPONSE_LIST":
            self.socket.sendall(build_list_response(self.callbacks.list_local_files()))
            return
        if command == "META":
            filename = task["filename"]
            with self._pending_meta_lock:
                self.pending_meta_filename = filename
            self.socket.sendall(build_meta_request(filename))
            return
        if command == "RESPONSE_META":
            filename = task["filename"]
            meta_content = self.callbacks.load_meta_content(filename)
            if meta_content is not None:
                self.socket.sendall(build_meta_response(meta_content))

    def _handle_incoming_frame(self) -> None:
        # 接收线程只负责按帧读取和分发，不直接做阻塞式业务处理。
        data_length = int.from_bytes(recv_exact(self.socket, 4), "big")
        body = recv_exact(self.socket, data_length)
        command, payload = unpack_frame_body(body)

        if command == "LIST":
            self.queue.put({"command": "RESPONSE_LIST"})
            return

        if command == "RLST":
            files = parse_list_response(payload)
            self.callbacks.on_file_list(self.peer_ip, files)
            return

        if command == "META":
            filename = parse_meta_request(payload)
            self.queue.put({"command": "RESPONSE_META", "filename": filename})
            return

        if command == "RMTA":
            if not is_valid_meta_payload(payload):
                logger.warning("Received invalid meta payload from %s.", self.peer_ip)
                return
            with self._pending_meta_lock:
                filename = self.pending_meta_filename
                self.pending_meta_filename = None
            if filename:
                self.callbacks.on_meta_received(self.peer_ip, filename, payload)
            else:
                logger.warning(
                    "Received meta from %s but no pending filename, ignoring",
                    self.peer_ip,
                )

    def _sender_loop(self) -> None:
        try:
            # 同步 IO 模型下，发送单独占用一个线程，避免被 recv 阻塞影响出站请求。
            while not self.stop_event.is_set():
                try:
                    task = self.queue.get(timeout=0.1)
                except Empty:
                    continue
                self._handle_outgoing_task(task)
        except (ConnectionError, OSError):
            self.stop()
        except Exception as exc:
            logger.exception("Sender loop error with %s", self.peer_ip)
            self.stop()

    def _receiver_loop(self) -> None:
        try:
            # 接收线程持续阻塞读 socket，收到完整协议帧后再交给上层回调。
            while not self.stop_event.is_set():
                try:
                    self._handle_incoming_frame()
                except socket.timeout:
                    continue
        except (ConnectionError, OSError):
            self.stop()
        except Exception as exc:
            logger.exception("Receiver loop error with %s", self.peer_ip)
            self.stop()

    def run(self) -> None:
        try:
            # handler 本身只负责托管 send/recv 两个子线程及它们的生命周期。
            self._send_thread = threading.Thread(target=self._sender_loop, daemon=True)
            self._recv_thread = threading.Thread(target=self._receiver_loop, daemon=True)
            self._send_thread.start()
            self._recv_thread.start()
            self._send_thread.join()
            self._recv_thread.join()
        except Exception as exc:
            logger.exception("ControlConnectionHandler error with %s", self.peer_ip)
        finally:
            self.stop_event.set()
            self._close_socket_once()
            self.manager.unregister_connection(self.peer_ip, self)
