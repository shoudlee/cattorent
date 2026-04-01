import hashlib
import socket
import struct
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from connection_handler import ConnectionHandlerCallbacks
from connections import ConnectionManager


class CattorrentProtocol:
    """
    Core protocol orchestration layer.

    Attributes:
        peers (dict): {peer_id (uuid.UUID): (PeerInfo, last_seen_time)}
    """

    def __init__(
        self,
        ip="0.0.0.0",
        port=9822,
        data_port=9823,
        broadcast_interval=2,
        share_folder="./catshare",
    ):
        self.ip = ip
        self.port = port
        self.data_port = data_port
        self.broadcast_interval = broadcast_interval
        self.peer_id = uuid.uuid4()
        self.peers: dict[uuid.UUID, tuple[PeerInfo, float]] = {}
        self.share_folder = Path(share_folder)
        self.share_folder.mkdir(parents=True, exist_ok=True)
        self.slice_size = 256 * 1024

        # 与其把整个protocol层暴露给connection handler thread，不如通过回调的方式让connection handler访问需要的功能，
        # 避免直接依赖protocol内部状态，降低耦合。
        callbacks = ConnectionHandlerCallbacks(
            list_local_files=self._list_local_files,
            load_meta_content=self._load_meta_content,
            on_file_list=self._on_file_list,
            on_meta_received=self._on_meta_received,
        )
        self.connection_manager = ConnectionManager(
            callbacks=callbacks, port=self.port, data_port=self.data_port
        )

        self.upd_handler: UdpBroadcastWorker | None = None
        self.tcp_recv_handler: TcpListenWorker | None = None

    def get_peer(self, peer_id: uuid.UUID):
        """Ensure a control connection exists for peer_id and return its handler."""
        self.refresh_peers()
        peer_entry = self.peers.get(peer_id)
        if peer_entry is None:
            print(f"Peer {peer_id} not found.")
            return None
        peer_ip = peer_entry[0].ip
        return self.connection_manager.get_control_handler(ip=peer_ip)

    def cleanup_connection(self, ip: str):
        self.connection_manager.cleanup_connection(ip)

    def _list_local_files(self) -> dict[str, int]:
        files: dict[str, int] = {}
        for p in self.share_folder.iterdir():
            if not p.is_file() or p.name.startswith("."):
                continue
            meta_path = self.share_folder / f".{p.name}.meta"
            if not meta_path.exists():
                continue
            files[p.name] = p.stat().st_size
        return files

    def _load_meta_content(self, filename: str) -> bytes | None:
        filepath = self.share_folder / filename
        meta_filepath = self.share_folder / f".{filename}.meta"
        if not (filepath.exists() and meta_filepath.exists()):
            return None
        return meta_filepath.read_bytes()

    def _on_file_list(self, peer_ip: str, files: list[tuple[str, int]]):
        print(f"\nReceived file list from {peer_ip}:")
        for filename, filesize in files:
            print(f"{filename} ({filesize} bytes)")

    def _on_meta_received(self, peer_ip: str, filename: str, meta_content: bytes):
        meta_filename = self.share_folder / f".{filename}.meta"
        meta_filename.write_bytes(meta_content)
        print(f"\nReceived meta for {filename} from {peer_ip}")

    def meta(self, filename: str):
        """Generate .filename.meta from local file."""
        filepath = self.share_folder / filename
        if not filepath.exists():
            print(f"{filename} doesn't exist.")
            return

        filesize = filepath.stat().st_size
        hash_result = hashlib.sha256()
        with open(filepath, "br") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_result.update(chunk)

        bitmap = BitMap(filesize, self.slice_size)
        bitmap.fill_all()

        meta_content = struct.pack(
            f"!QLL32sL{len(bitmap.bitmap)}s",
            filesize,
            self.slice_size,
            bitmap.total_slices,
            hash_result.digest(),
            len(bitmap.bitmap),
            bitmap.bitmap,
        )
        (self.share_folder / f".{filename}.meta").write_bytes(meta_content)

    def online(self):
        self.start_protocol_upd_handler()
        self.start_protocol_tcp_handler()
        print(f"Cattorrent protocol started on {self.ip}:{self.port}")

    def start_protocol_upd_handler(self):
        self.upd_handler = UdpBroadcastWorker(self)
        self.upd_handler.start()

    def start_protocol_tcp_handler(self):
        self.tcp_recv_handler = TcpListenWorker(self)
        self.tcp_recv_handler.start()

    def get_peer_list(self, peer_id) -> None:
        handler = self.get_peer(peer_id)
        if handler is None:
            return
        print(f"Sending LIST to peer {peer_id}")
        handler.request_list()

    def get_peer_meta(self, peer_id, filename):
        handler = self.get_peer(peer_id)
        if handler is None:
            return
        print(f"Requesting META {filename} from peer {peer_id}")
        handler.request_meta(filename)

    def encode_broadcast_message(self, command="ONLI"):
        reserved = 0
        protocol_version = 1
        peer_id = self.peer_id.bytes
        body = struct.pack("!HHI16s", self.port, reserved, protocol_version, peer_id)
        length = len(body) + 4
        return struct.pack("!I4s", length, command.encode("ascii")) + body

    def handle_received_udp_packet(self, ip, port, packet):
        try:
            packet_length = struct.unpack("!I", packet[:4])[0]
            if packet_length != len(packet) - 4:
                print("Invalid packet length")
                return

            command = struct.unpack("!4s", packet[4:8])[0].decode("ascii")
            if command != "ONLI":
                print(f"Unknown command: {command}")
                return

            adv_port, _, protocol_version, peer_id_bytes = struct.unpack(
                "!HHI16s", packet[8:]
            )
            if peer_id_bytes == self.peer_id.bytes:
                return

            peer_info = PeerInfo(ip=ip, port=adv_port, version=protocol_version)
            peer_id = uuid.UUID(bytes=peer_id_bytes)
            self.peers[peer_id] = (peer_info, time.time())
            self.connection_manager.get_control_handler(ip=ip)

        except Exception as e:
            print(f"Failed to handle received UDP packet: {e}")

    def get_peers(self):
        self.refresh_peers()
        return [(peer_id, peer_info) for peer_id, (peer_info, _) in self.peers.items()]

    def refresh_peers(self):
        now = time.time()
        expired_peers = [
            peer_id
            for peer_id, (_, last_seen) in self.peers.items()
            if now - last_seen > self.broadcast_interval * 2
        ]
        for peer_id in expired_peers:
            del self.peers[peer_id]

    def get_peer_key_by_ip(self, ip):
        self.refresh_peers()
        return next(
            (peer_id for peer_id, (info, _) in self.peers.items() if info.ip == ip),
            None,
        )


class UdpBroadcastWorker(threading.Thread):
    def __init__(self, cattorrent_protocol: CattorrentProtocol):
        super().__init__(daemon=True)
        self.cattorrent_protocol = cattorrent_protocol
        self.ip = cattorrent_protocol.ip
        self.port = cattorrent_protocol.port
        self.broadcast_interval = cattorrent_protocol.broadcast_interval
        self.stop_event = threading.Event()
        self.socket: socket.socket | None = None

    def setup_socket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind((self.ip, self.port))
        sock.settimeout(0.5)
        return sock

    def stop(self):
        self.stop_event.set()
        if self.socket:
            try:
                self.socket.close()
            except OSError:
                pass

    def send_broadcast(self, msg):
        if self.socket:
            self.socket.sendto(msg, ("255.255.255.255", self.port))

    def run(self):
        self.socket = self.setup_socket()
        message = self.cattorrent_protocol.encode_broadcast_message()
        broadcast_time = 0.0
        while not self.stop_event.is_set():
            now = time.time()
            try:
                if now - broadcast_time >= self.broadcast_interval:
                    broadcast_time = now
                    self.send_broadcast(message)
                data, addr = self.socket.recvfrom(1024)
                self.cattorrent_protocol.handle_received_udp_packet(
                    addr[0], addr[1], data
                )
            except socket.timeout:
                continue
            except OSError:
                return
            except Exception as e:
                print(f"UDP Broadcast Worker error: {e}")
                return


class TcpListenWorker(threading.Thread):
    def __init__(self, cattorrent_protocol: CattorrentProtocol):
        super().__init__(daemon=True)
        self.cattorrent_protocol = cattorrent_protocol
        self.stop_event = threading.Event()
        self.recv_socket: socket.socket | None = None

    def setup_socket(self):
        self.recv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.recv_socket.bind(
            (self.cattorrent_protocol.ip, self.cattorrent_protocol.port)
        )
        self.recv_socket.listen()
        self.recv_socket.settimeout(0.5)

    def run(self) -> None:
        self.setup_socket()
        while not self.stop_event.is_set():
            try:
                assert self.recv_socket is not None
                conn, addr = self.recv_socket.accept()
                self.cattorrent_protocol.connection_manager.register_accepted_connection(
                    addr[0], conn
                )
            except socket.timeout:
                continue
            except OSError:
                return
            except Exception as e:
                print(f"TCP Listen Worker error: {e}")
                return

    def stop(self):
        self.stop_event.set()
        self.cattorrent_protocol.connection_manager.stop_all()
        if self.recv_socket:
            try:
                self.recv_socket.close()
            except OSError:
                pass


class TcpDataListenWorker(threading.Thread):
    """
    用于监听data端口的连接
    """

    def __init__(self, cattorrent_protocol: CattorrentProtocol):
        super().__init__(daemon=True)
        self.cattorrent_protocol = cattorrent_protocol
        self.stop_event = threading.Event()
        self.recv_socket: socket.socket | None = None

    def setup_socket(self):
        self.recv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.recv_socket.bind(
            (self.cattorrent_protocol.ip, self.cattorrent_protocol.data_port)
        )
        self.recv_socket.listen()
        self.recv_socket.settimeout(0.5)

    def run(self) -> None:
        self.setup_socket()
        while not self.stop_event.is_set():
            try:
                assert self.recv_socket is not None
                conn, addr = self.recv_socket.accept()
                self.cattorrent_protocol.connection_manager.register_accepted_data_connection(
                    addr[0], conn
                )
            except socket.timeout:
                continue
            except OSError:
                return
            except Exception as e:
                print(f"TCP Data Listen Worker error: {e}")
                return

    def stop(self):
        self.stop_event.set()
        self.cattorrent_protocol.connection_manager.stop_all()
        if self.recv_socket:
            try:
                self.recv_socket.close()
            except OSError:
                pass


class BitMap:
    def __init__(self, filesize, slice_size=256 * 1024):
        self.total_slices = (filesize + slice_size - 1) // slice_size
        self.bitmap = bytearray((self.total_slices + 7) // 8)

    def set_slice(self, index):
        if index < 0 or index >= self.total_slices:
            raise IndexError("Slice index out of range")
        byte_index = index // 8
        bit_index = index % 8
        self.bitmap[byte_index] |= 1 << (7 - bit_index)

    def has_slice(self, index):
        if index < 0 or index >= self.total_slices:
            raise IndexError("Slice index out of range")
        byte_index = index // 8
        bit_index = index % 8
        return (self.bitmap[byte_index] & (1 << (7 - bit_index))) != 0

    def fill_all(self):
        for i in range(len(self.bitmap)):
            self.bitmap[i] = 0xFF


class MetaInfo:
    def __init__(self):
        self.filesize = None
        self.slice_size = None
        self.total_slices = None
        self.file_hash = None
        self.bitmap = None

    def from_file(self, filepath, slice_size=256 * 1024):
        with open(filepath, "rb") as f:
            content = f.read()
        self.filesize, self.slice_size, self.total_slices, file_hash, bitmap_length = (
            struct.unpack("!QLL32sL", content[:48])
        )
        self.file_hash = file_hash.hex()
        bitmap_data = content[48 : 48 + bitmap_length]
        self.bitmap = BitMap(self.filesize, self.slice_size)
        self.bitmap.bitmap = bytearray(bitmap_data)


@dataclass
class PeerInfo:
    ip: str
    port: int
    version: int
