import socket
import threading

from connection_handler import (
    ConnectionHandlerCallbacks,
    ControlConnectionHandler,
    DataConnectionHandler,
)
from logging_setup import get_logger


logger = get_logger(__name__)


class ConnectionManager:
    """
    Attributes:
        connection_handlers (dict): {  peer_ip(str):(control_connection_handler(ControlConnectionHandler) }
    """

    def __init__(self, callbacks: ConnectionHandlerCallbacks, port, data_port) -> None:
        """
        初始化connection_handlers为空字典，
        port为TCP连接使用的端口号，默认为9822
        """
        self.connection_handlers: dict[
            str, dict[str, ControlConnectionHandler | DataConnectionHandler | None]
        ] = {}
        self.callbacks = callbacks
        self.port = port
        self.data_port = data_port
        self._lock = threading.Lock()

    def _get_or_create_record(self, ip: str):
        record = self.connection_handlers.get(ip)
        if record is None:
            record = {"control": None, "data": None}
            self.connection_handlers[ip] = record
        return record

    def _is_alive(self, handler) -> bool:
        return handler is not None and handler.is_alive()

    def cleanup_connection(self, ip):
        """
        关闭指定peer_ip的connection handler，如果存在data_connection_handler,
        那么必然首先存在对应的control_connection_handler
        """
        with self._lock:
            handlers = self.connection_handlers.pop(ip, None)
        if handlers is None:
            return
        control_handler = handlers.get("control")
        data_handler = handlers.get("data")
        if control_handler is not None:
            try:
                control_handler.stop()
            except OSError:
                logger.warning(
                    "Failed to close control connection to peer %s at:%s",
                    ip,
                    self.port,
                )
        if data_handler is not None:
            try:
                data_handler.stop()
            except OSError:
                logger.warning(
                    "Failed to close data connection to peer %s at:%s",
                    ip,
                    self.data_port,
                )

    def get_control_handler(self, *, ip):
        """
        确保与ip的连接存在，如果不存在则创建新的control、data handler，
        如果线程已经死了则清理掉旧的handler并创建新的handler，
        并返回control handler
        """
        with self._lock:
            record = self.connection_handlers.get(ip)
            if record is not None:
                existing = record.get("control")
                if self._is_alive(existing):
                    return existing
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)  # 用于connect
            sock.connect((ip, self.port))  # 使用同一个端口号进行TCP连接
        except Exception as e:
            logger.exception(
                "Failed to connect to peer %s at:%s, exception: %s", ip, self.port, e
            )
            return None
        handler = self.register_accepted_connection(ip, sock)
        return handler

    def get_data_handler(self, *, ip):
        with self._lock:
            record = self.connection_handlers.get(ip)
            if record is not None:
                existing = record.get("data")
                if self._is_alive(existing):
                    return existing
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            sock.connect((ip, self.data_port))
        except Exception as e:
            logger.exception(
                "Failed to connect data socket to peer %s at:%s, exception: %s",
                ip,
                self.data_port,
                e,
            )
            return None
        return self.register_accepted_data_connection(ip, sock)

    def register_accepted_connection(self, ip: str, sock: socket.socket):
        """Create a control handler for an accepted inbound TCP connection.这里不用socket.getpeername()是因为以前涉及到多个客户端端口"""
        sock.settimeout(0.1)
        old_handler = None
        with self._lock:
            record = self._get_or_create_record(ip)
            old_handler = record.get("control")
        handler = ControlConnectionHandler(self, sock, self.callbacks)
        with self._lock:
            record = self._get_or_create_record(ip)
            record["control"] = handler
        if old_handler is not None:
            old_handler.stop()
        handler.start()
        return handler

    def register_accepted_data_connection(self, ip: str, sock: socket.socket):
        sock.settimeout(0.1)
        old_handler = None
        with self._lock:
            record = self._get_or_create_record(ip)
            old_handler = record.get("data")
            if old_handler is not None:
                old_handler.stop()
            handler = DataConnectionHandler(self, ip, sock, self.callbacks)
            record["data"] = handler
        handler.start()
        return handler

    def unregister_control_connection(self, ip: str, handler: ControlConnectionHandler):
        with self._lock:
            record = self.connection_handlers.get(ip)
            if record is None:
                return
            if record.get("control") is handler:
                record["control"] = None
            if record.get("control") is None and record.get("data") is None:
                self.connection_handlers.pop(ip, None)

    def unregister_data_connection(self, ip: str, handler: DataConnectionHandler):
        with self._lock:
            record = self.connection_handlers.get(ip)
            if record is None:
                return
            if record.get("data") is handler:
                record["data"] = None
            if record.get("control") is None and record.get("data") is None:
                self.connection_handlers.pop(ip, None)

    def stop_all(self):
        with self._lock:
            ips = list(self.connection_handlers.keys())
        for ip in ips:
            self.cleanup_connection(ip)
