import socket
from logging_setup import get_logger

logger = get_logger(__name__)


def recv_exact(sock: socket.socket, size: int) -> bytes:
    """Receive exactly size bytes or raise ConnectionError if peer closes early."""
    data = bytearray()  # bytes对象不可变，所以用bytearray来累积接收的数据
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError("Connection closed before receiving enough data")
        data.extend(chunk)
    return bytes(data)


def safe_get_peer_ip(sock: socket.socket) -> str | None:
    try:
        return sock.getpeername()[0]  # getpeername()会导致各种error
    except OSError:
        return None


def close_socket_quietly(sock: socket.socket | None) -> None:
    if sock is None:
        return
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError as e:
        logger.debug("Socket shutdown error (probably already closed): %s", e)
    try:
        sock.close()
    except OSError as e:
        logger.debug("Socket close error (probably already closed): %s", e)
