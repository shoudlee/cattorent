import socket
import threading

from connection_handler import ControlConnectionHandler, ConnectionHandlerCallbacks
from logging_setup import get_logger


logger = get_logger(__name__)


class ConnectionManager:
    """
    Attributes:
        connection_handlers (dict): {  peer_ip(str):(control_connection_handler(ControlConnectionHandler),
          data_connection_handler(DataConnectionHandler)) }
    """
    def __init__(self, callbacks: ConnectionHandlerCallbacks, port=9822) -> None:
        """
        初始化connection_handlers为空字典，
        port为TCP连接使用的端口号，默认为9822
        """
        self.connection_handlers = {}
        self.callbacks = callbacks
        self.port = port
        self._lock = threading.Lock()
    
    def cleanup_connection(self, ip):
        """
        关闭指定peer_ip的connection handler，如果存在data_connection_handler,
        那么必然首先存在对应的control_connection_handler
        """
        with self._lock:
            handlers = self.connection_handlers.pop(ip, None)
        if handlers is None:
            return
        control_handler = handlers
        try:
            control_handler.stop() #顺带给data_connection(如果有)一起关了
        except OSError:
            pass
    
    def get_control_handler(self, *, ip):
        """
        确保与ip的连接存在，如果不存在则创建新的control、data handler，
        如果线程已经死了则清理掉旧的handler并创建新的handler，
        并返回control handler
        """
        with self._lock:
            handler = self.connection_handlers.get(ip)
        if handler is not None:
            if handler.is_alive():
                return handler
            self.cleanup_connection(ip)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            sock.connect((ip, self.port))
            sock.settimeout(0.1)
            handler = ControlConnectionHandler(self, sock, self.callbacks)
        except Exception as e:
            logger.exception("Failed to connect to peer %s at:%s", ip, self.port)
            return None
        with self._lock:
            self.connection_handlers[ip] = handler
        handler.start()
        return handler

    def register_accepted_connection(self, ip: str, sock: socket.socket):
        """Create a handler for an accepted inbound TCP connection."""
        sock.settimeout(0.1)
        self.cleanup_connection(ip)
        handler = ControlConnectionHandler(self, sock, self.callbacks)
        with self._lock:
            self.connection_handlers[ip] = handler
        handler.start()
        return handler

    def unregister_connection(self, ip: str, handler: ControlConnectionHandler):
        with self._lock:
            existing = self.connection_handlers.get(ip)
            if existing is handler:
                self.connection_handlers.pop(ip, None)

    def stop_all(self):
        with self._lock:
            ips = list(self.connection_handlers.keys())
        for ip in ips:
            self.cleanup_connection(ip)