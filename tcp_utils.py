import socket


def recv_exact(sock: socket.socket, size: int) -> bytes:
    """Receive exactly size bytes or raise ConnectionError if peer closes early."""
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError("Connection closed before receiving enough data")
        data.extend(chunk)
    return bytes(data)


def safe_get_peer_ip(sock: socket.socket) -> str | None:
    try:
        return sock.getpeername()[0]
    except OSError:
        return None


def close_socket_quietly(sock: socket.socket | None) -> None:
    if sock is None:
        return
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass
