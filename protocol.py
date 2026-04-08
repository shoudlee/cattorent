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

from download_manager import DownloadManager


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

        callbacks = ConnectionHandlerCallbacks(
            list_local_files=self._list_local_files,
            load_meta_content=self._load_meta_content,
            load_piece_content=self._load_piece_content,
            on_file_list=self._on_file_list,
            on_meta_received=self._on_meta_received,
            on_piece_received=self._on_piece_received,
        )
        self.connection_manager = ConnectionManager(
            callbacks=callbacks, port=self.port, data_port=self.data_port
        )

        self.upd_handler: UdpBroadcastWorker | None = None
        self.tcp_recv_handler: TcpListenWorker | None = None
        self.tcp_data_recv_handler: TcpDataListenWorker | None = None
        self.download_managers: list[DownloadManager] = []

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

    def _resolve_shared_filepath(self, filename: str) -> Path | None:
        if "/" in filename or "\\" in filename or ".." in filename:
            return None
        base = self.share_folder.resolve()
        candidate = (base / filename).resolve()
        if candidate != base and base not in candidate.parents:
            return None
        return candidate

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
        filepath = self._resolve_shared_filepath(filename)
        if filepath is None:
            return None
        meta_filepath = self.share_folder / f".{filename}.meta"
        if not (filepath.exists() and meta_filepath.exists()):
            return None
        return meta_filepath.read_bytes()

    def _load_piece_content(self, filename: str, piece_index: int) -> bytes | None:
        filepath = self._resolve_shared_filepath(filename)
        if filepath is None:
            raise ValueError("bad request")
        if not filepath.exists() or not filepath.is_file():
            return None

        filesize = filepath.stat().st_size
        piece_count = (filesize + self.slice_size - 1) // self.slice_size
        if piece_index < 0 or piece_index >= piece_count:
            raise IndexError("invalid piece")

        offset = piece_index * self.slice_size
        read_size = min(self.slice_size, filesize - offset)
        with open(filepath, "rb") as file_obj:
            file_obj.seek(offset)
            return file_obj.read(read_size)

    def _on_file_list(self, peer_ip: str, files: list[tuple[str, int]]):
        print(f"\nReceived file list from {peer_ip}:")
        for filename, filesize in files:
            print(f"{filename} ({filesize} bytes)")

    def _on_meta_received(self, peer_ip: str, filename: str, meta_content: bytes):
        meta_filename = self.share_folder / f".{filename}.meta"
        meta_filename.write_bytes(meta_content)
        print(f"\nReceived meta for {filename} from {peer_ip}")

    def _on_piece_received(
        self,
        peer_ip: str,
        filename: str,
        piece_index: int,
        data: bytes,
    ) -> None:
        print(
            f"Received piece {piece_index} for {filename} from {peer_ip} ({len(data)} bytes)"
        )

    def meta(self, filename: str):
        """Generate .filename.meta from local file."""
        filepath = self._resolve_shared_filepath(filename)
        if filepath is None or not filepath.exists():
            print(f"{filename} doesn't exist.")
            return

        filesize = filepath.stat().st_size
        hash_result = hashlib.sha256()
        with open(filepath, "br") as file_obj:
            for chunk in iter(lambda: file_obj.read(8192), b""):
                hash_result.update(chunk)

        bitmap = BitMap(filesize, self.slice_size)
        bitmap.fill_all()

        meta_content = struct.pack(
            f"!QII32sI{len(bitmap.bitmap)}sI",
            filesize,
            self.slice_size,
            bitmap.total_slices,
            hash_result.digest(),
            len(bitmap.bitmap),
            bitmap.bitmap,
            len(filename.encode("utf-8")),
        )
        (self.share_folder / f".{filename}.meta").write_bytes(meta_content)

    def online(self):
        self.start_protocol_upd_handler()
        self.start_protocol_tcp_handler()
        self.start_protocol_data_tcp_handler()
        print(
            f"Cattorrent protocol started on {self.ip}:{self.port} (data port {self.data_port}) with peer ID {self.peer_id}"
        )

    def start_protocol_upd_handler(self):
        self.upd_handler = UdpBroadcastWorker(self)
        self.upd_handler.start()

    def start_protocol_tcp_handler(self):
        self.tcp_recv_handler = TcpListenWorker(self)
        self.tcp_recv_handler.start()

    def start_protocol_data_tcp_handler(self):
        self.tcp_data_recv_handler = TcpDataListenWorker(self)
        self.tcp_data_recv_handler.start()

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

    def start_download(self, peer_id, filename: str, destination: str):
        peer_entry = self.peers.get(peer_id)
        if peer_entry is None:
            print(f"Peer {peer_id} not found.")
            return False, "peer not found"

        peer_ip = peer_entry[0].ip
        data_handler = self.connection_manager.get_data_handler(ip=peer_ip)
        if data_handler is None:
            print(f"Cannot establish data connection to {peer_ip}")
            return False, f"cannot establish data connection to {peer_ip}"

        meta_path = self.share_folder / f".{filename}.meta"
        if not meta_path.exists():
            print(f"Missing meta file for {filename}, request meta first.")
            return False, f"missing meta file for {filename}"

        meta_info = MetaInfo()
        meta_info.from_file(meta_path)

        download_manager = DownloadManager(
            data_handler=data_handler,
            filename=filename,
            destination_path=Path(destination),
            file_size=meta_info.filesize,
            slice_size=meta_info.slice_size,
            slice_count=meta_info.total_slices,
            timeout_seconds=3.0,
            max_retries=3,
        )
        self.download_managers.append(download_manager)
        try:
            return download_manager.start_download()
        finally:
            if download_manager in self.download_managers:
                self.download_managers.remove(download_manager)

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
        self.bitmap[byte_index] |= 1 << bit_index

    def has_slice(self, index):
        if index < 0 or index >= self.total_slices:
            raise IndexError("Slice index out of range")
        byte_index = index // 8
        bit_index = index % 8
        return (self.bitmap[byte_index] & (1 << bit_index)) != 0

    def fill_all(self):
        for i in range(self.total_slices):
            self.set_slice(i)


class MetaInfo:
    def __init__(self):
        self.filesize = None
        self.slice_size = None
        self.total_slices = None
        self.file_hash = None
        self.bitmap = None
        self.filename_size = None

    def from_file(self, filepath):
        with open(filepath, "rb") as file_obj:
            content = file_obj.read()
        if len(content) < 56:
            raise ValueError("Invalid meta file")
        self.filesize, self.slice_size, self.total_slices, file_hash, bitmap_length = (
            struct.unpack("!QII32sI", content[:52])
        )
        expected_length = 56 + bitmap_length
        if len(content) < expected_length:
            raise ValueError("Invalid meta bitmap length")
        self.file_hash = file_hash.hex()
        bitmap_data = content[52 : 52 + bitmap_length]
        self.filename_size = struct.unpack(
            "!I", content[52 + bitmap_length : 56 + bitmap_length]
        )[0]
        self.bitmap = BitMap(self.filesize, self.slice_size)
        self.bitmap.bitmap = bytearray(bitmap_data)


@dataclass
class PeerInfo:
    ip: str
    port: int
    version: int
