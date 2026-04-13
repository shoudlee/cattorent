import socket
import threading
from dataclasses import dataclass
from queue import Empty, Queue
from typing import Callable

from logging_setup import get_logger
from protocol_codec import (
    build_error_response,
    build_getp_request,
    build_getp_response,
    build_list_request,
    build_list_response,
    build_meta_request,
    build_meta_response,
    is_valid_meta_payload,
    parse_error_response,
    parse_getp_request,
    parse_getp_response,
    parse_list_response,
    parse_meta_request,
    unpack_frame_body,
)
from tcp_utils import (
    SimpleEventWaiter,
    close_socket_quietly,
    recv_exact,
    safe_get_peer_ip,
)


logger = get_logger(__name__)


@dataclass
class ConnectionHandlerCallbacks:
    # 连接层通过回调访问上层业务，避免直接依赖 protocol 内部状态。
    list_local_files: Callable[[], dict[str, int]]
    load_meta_content: Callable[[str], bytes | None]
    load_piece_content: Callable[[str, int], bytes | None]
    on_file_list: Callable[[str, list[tuple[str, int]]], None]
    on_meta_received: Callable[[str, str, bytes], None]
    on_piece_received: Callable[[str, str, int, bytes], None]


# 实现为一个线程，可以防止主线程阻塞
# 为了避免出错，data socket遵循：
# 1.先查找当前有无，没有再新建
# 2.如果收到新的连接请求，使用新的socket,关闭当前已有的socket
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
        self.pending_meta_waiter: SimpleEventWaiter | None = None
        self.pending_list_waiter: SimpleEventWaiter | None = None
        self.peer_ip = safe_get_peer_ip(sock) or "unknown"
        self._pending_meta_lock = threading.Lock()
        self._pending_list_lock = threading.Lock()
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

    def request_list_and_wait(self) -> SimpleEventWaiter:
        waiter = SimpleEventWaiter(request_id=f"list:{self.peer_ip}")
        with self._pending_list_lock:
            self.pending_list_waiter = waiter
        self.queue.put({"command": "LIST"})
        return waiter

    def request_meta(self, filename: str) -> None:
        self.queue.put({"command": "META", "filename": filename})

    def request_meta_and_wait(self, filename: str) -> SimpleEventWaiter:
        waiter = SimpleEventWaiter(request_id=f"meta:{self.peer_ip}:{filename}")
        with self._pending_meta_lock:
            self.pending_meta_filename = filename
            self.pending_meta_waiter = waiter
        self.queue.put({"command": "META", "filename": filename})
        return waiter

    def _pop_pending_list_waiter(self) -> SimpleEventWaiter | None:
        with self._pending_list_lock:
            waiter = self.pending_list_waiter
            self.pending_list_waiter = None
        return waiter

    def _pop_pending_meta(self) -> tuple[str | None, SimpleEventWaiter | None]:
        with self._pending_meta_lock:
            filename = self.pending_meta_filename
            waiter = self.pending_meta_waiter
            self.pending_meta_filename = None
            self.pending_meta_waiter = None
        return filename, waiter

    def _flush_pending_waiters_with_error(self, error: Exception) -> None:
        filename, meta_waiter = self._pop_pending_meta()
        list_waiter = self._pop_pending_list_waiter()
        if meta_waiter is not None:
            meta_waiter.set_error(error)
        if list_waiter is not None:
            list_waiter.set_error(error)

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
            else:
                logger.error(
                    f"Meta request from {self.peer_ip}, but {filename} not Found."
                )

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
            waiter = self._pop_pending_list_waiter()
            if waiter is not None:
                waiter.set_result(files)
            else:
                self.callbacks.on_file_list(self.peer_ip, files)
            return

        if command == "META":
            filename = parse_meta_request(payload)
            self.queue.put({"command": "RESPONSE_META", "filename": filename})
            return

        if command == "RMTA":
            if not is_valid_meta_payload(payload):
                logger.error("Received invalid meta payload from %s.", self.peer_ip)
                return
            filename, waiter = self._pop_pending_meta()
            if filename:
                if waiter is not None:
                    waiter.set_result(payload)
                else:
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
        except (ConnectionError, OSError) as exc:
            self._flush_pending_waiters_with_error(exc)
            self.stop()
        except Exception as exc:
            self._flush_pending_waiters_with_error(exc)
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
        except (ConnectionError, OSError) as exc:
            self._flush_pending_waiters_with_error(exc)
            self.stop()
        except Exception as exc:
            self._flush_pending_waiters_with_error(exc)
            logger.exception("Receiver loop error with %s", self.peer_ip)
            self.stop()

    def run(self) -> None:
        try:
            # handler 本身只负责托管 send/recv 两个子线程及它们的生命周期。
            self._send_thread = threading.Thread(target=self._sender_loop, daemon=True)
            self._recv_thread = threading.Thread(
                target=self._receiver_loop, daemon=True
            )
            self._send_thread.start()
            self._recv_thread.start()
            self._send_thread.join()
            self._recv_thread.join()
        except Exception as exc:
            logger.exception("ControlConnectionHandler error with %s", self.peer_ip)
        finally:
            self.stop_event.set()
            self._close_socket_once()
            self.manager.unregister_control_connection(self.peer_ip, self)


class DataConnectionHandler(threading.Thread):
    def __init__(
        self,
        manager,
        peer_ip: str,
        sock: socket.socket,
        callbacks: ConnectionHandlerCallbacks,
    ):
        super().__init__(daemon=True)
        self.manager = manager
        self.peer_ip = peer_ip
        self.socket = sock
        self.callbacks = callbacks
        self.stop_event = threading.Event()
        self._close_lock = threading.Lock()
        self._closed = False
        self.queue: Queue[dict] = Queue()
        self.data_request_waiters: dict[tuple[str, int], SimpleEventWaiter] = {}
        self._waiters_lock = threading.Lock()
        self.socket_send_lock = threading.Lock()
        self._send_thread: threading.Thread | None = None
        self._recv_thread: threading.Thread | None = None

    def _waiter_key(self, filename: str, piece_index: int) -> tuple[str, int]:
        return filename, piece_index

    def send_getp_and_register_waiter(
        self,
        filename: str,
        piece_index: int,
    ) -> SimpleEventWaiter:
        waiter = SimpleEventWaiter(request_id=f"{filename}:{piece_index}")
        key = self._waiter_key(filename, piece_index)
        with self._waiters_lock:
            self.data_request_waiters[key] = waiter
        try:
            with self.socket_send_lock:
                self.socket.sendall(build_getp_request(filename, piece_index))
        except Exception:
            with self._waiters_lock:
                self.data_request_waiters.pop(key, None)
            raise
        return waiter

    def cancel_waiter(self, filename: str, piece_index: int) -> None:
        key = self._waiter_key(filename, piece_index)
        with self._waiters_lock:
            self.data_request_waiters.pop(key, None)

    def _resolve_waiter(
        self,
        filename: str,
        piece_index: int,
    ) -> SimpleEventWaiter | None:
        key = self._waiter_key(filename, piece_index)
        with self._waiters_lock:
            return self.data_request_waiters.pop(key, None)

    def _flush_waiters_with_error(self, error: Exception) -> None:
        with self._waiters_lock:
            waiters = list(self.data_request_waiters.values())
            self.data_request_waiters.clear()
        for waiter in waiters:
            waiter.set_error(error)

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

    def _handle_outgoing_task(self, task: dict) -> None:
        # 这里只处理发送PIEC的情况，发送GETP由download manager线程自己写socket
        command = task.get("command")
        if command == "PIEC":
            filename = task["filename"]
            piece_index = task["piece_index"]
            try:
                file_piece = self.callbacks.load_piece_content(filename, piece_index)
            except ValueError:
                file_piece = None
                self.queue.put(
                    {
                        "command": "ERRO",
                        "error_code": 2,
                        "message": "bad request",
                    }
                )
                return
            except IndexError:
                file_piece = None
                self.queue.put(
                    {
                        "command": "ERRO",
                        "error_code": 3,
                        "message": "invalid piece",
                    }
                )
                return
            except Exception:
                file_piece = None
                self.queue.put(
                    {
                        "command": "ERRO",
                        "error_code": 4,
                        "message": "internal error",
                    }
                )
                return

            if file_piece is None:
                self.queue.put(
                    {
                        "command": "ERRO",
                        "error_code": 1,
                        "message": "file not found",
                    }
                )
                return
            with self.socket_send_lock:
                self.socket.sendall(
                    build_getp_response(filename, piece_index, file_piece)
                )
            return
        if command == "ERRO":
            with self.socket_send_lock:
                self.socket.sendall(
                    build_error_response(task["error_code"], task["message"])
                )
            return
        logger.error("Unknown command at data connection command: %s.", command)

    def _handle_incoming_frame(self) -> None:
        data_length = int.from_bytes(recv_exact(self.socket, 4), "big")
        body = recv_exact(self.socket, data_length)
        command, payload = unpack_frame_body(body)
        if command == "GETP":
            # 具体处理逻辑调用移交给send_handler,不然它太闲(?)
            try:
                filename, piece_index = parse_getp_request(payload)
            except ValueError:
                self.queue.put(
                    {
                        "command": "ERRO",
                        "error_code": 2,
                        "message": "bad request",
                    }
                )
                return
            self.queue.put(
                {
                    "command": "PIEC",
                    "filename": filename,
                    "piece_index": piece_index,
                }
            )
            return
        if command == "PIEC":
            filename, piece_index, file_piece = parse_getp_response(payload)
            waiter = self._resolve_waiter(filename, piece_index)
            if waiter is not None:
                waiter.set_result(file_piece)
            self.callbacks.on_piece_received(
                self.peer_ip, filename, piece_index, file_piece
            )
            return
        if command == "ERRO":
            error_code, message = parse_error_response(payload)
            logger.warning(
                "Received data error from %s: code=%s msg=%s",
                self.peer_ip,
                error_code,
                message,
            )
            self._flush_waiters_with_error(
                RuntimeError(f"Peer error {error_code}: {message}")
            )
            return

    def _sender_loop(self) -> None:
        try:
            # 同步 IO 模型下，发送单独占用一个线程，避免被 recv 阻塞影响出站请求。
            while not self.stop_event.is_set():
                try:
                    task = self.queue.get(timeout=0.1)  # 这里的timeout需要用来暂停
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
            logger.exception("Data receiver loop error with %s", self.peer_ip)
            self.stop()

    def run(self) -> None:
        try:
            # handler 本身只负责托管 send/recv 两个子线程及它们的生命周期。
            self._send_thread = threading.Thread(target=self._sender_loop, daemon=True)
            self._recv_thread = threading.Thread(
                target=self._receiver_loop, daemon=True
            )
            self._send_thread.start()
            self._recv_thread.start()
            self._send_thread.join()
            self._recv_thread.join()
        except Exception as exc:
            logger.exception("ControlConnectionHandler error with %s", self.peer_ip)
        finally:
            self.stop()
            self.manager.unregister_data_connection(self.peer_ip, self)
