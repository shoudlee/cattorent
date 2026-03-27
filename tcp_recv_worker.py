import threading
import socket
import struct
import time 
import uuid
from dataclasses import dataclass
from queue import Queue, Empty
from pathlib import Path

class TcpRecvWorker(threading.Thread):
    def __init__(self, cattorrent_protocol: CattorrentProtocol, socket: socket.socket): # type: ignore
        super().__init__()
        self.cattorrent_protocol = cattorrent_protocol
        self.stop_event = threading.Event()
        self.socket = socket

    def get_peer_key(self):
        try:
            return self.socket.getpeername()[0]
        except OSError:
            return None
    
    def handle_list_response(self, packet):
        file_count = struct.unpack('!I', packet[:4])[0]
        files = []
        offset = 4
        for _ in range(file_count):
            filename_length = struct.unpack('!H', packet[offset:offset+2])[0]
            offset += 2
            filename = packet[offset:offset+filename_length].decode()
            offset += filename_length
            filesize = struct.unpack('!Q', packet[offset:offset+8])[0]
            offset += 8
            files.append((filename, filesize))
        return files
    
    def recv_exact(self, n):
        data = bytearray()
        while len(data) < n:
            chunk = self.socket.recv(n - len(data))
            if not chunk:
                raise ConnectionError("Connection closed before receiving enough data")
            data += chunk
        return bytes(data)
    
    def stop(self):
        self.stop_event.set()

    def cleanup_connection(self):
        try:
            peer_key = self.socket.getpeername()[0]
        except OSError:
            peer_key = None
        if peer_key:
            self.cattorrent_protocol.cleanup_connection(peer_key)

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                data_length = struct.unpack('!I', self.recv_exact(4))[0]
                data = self.recv_exact(data_length)
            except socket.timeout:
                continue
            except (ConnectionError, OSError):
                self.stop()
                self.cleanup_connection()
                return
            except Exception as e:
                print(f"TcpRecvWorker parse error: {e}")
                continue
            command = struct.unpack('!4s', data[:4])[0].decode()
            if command == 'LIST':
                peer_key = self.get_peer_key()
                if peer_key and peer_key in self.cattorrent_protocol.tcp_connections:
                    self.cattorrent_protocol.tcp_connections[peer_key].queue.put({'command': 'RESPONSE_LIST'})
            elif command == 'RLST':
                files = self.handle_list_response(data[4:])
                print("\nReceived file list:")
                for filename, filesize in files:
                    print(f"{filename} ({filesize} bytes)")
            elif command == 'META':
                filename_length = struct.unpack('!H', data[4:6])[0]
                filename = data[6:6+filename_length].decode()
                peer_key = self.get_peer_key()
                if peer_key and peer_key in self.cattorrent_protocol.tcp_connections:
                    self.cattorrent_protocol.tcp_connections[peer_key].queue.put({'command': 'RESPONSE_META', 'filename': filename})
            elif command == 'RMTA':
                meta_content = data[4:]
                meta_bitmap_length = struct.unpack("!I", meta_content[48:52])[0]
                if len(meta_content) < 52 + meta_bitmap_length:
                    print("Received invalid meta payload.")
                    continue

                peer_ip = self.socket.getpeername()[0]
                if self.cattorrent_protocol.pending_meta_filename:
                    filename = self.cattorrent_protocol.pending_meta_filename
                    self.cattorrent_protocol.pending_meta_filename = None
                    meta_filename = f'.{filename}.meta'
                    with open(Path(self.cattorrent_protocol.share_folder) / meta_filename, 'wb') as f:
                        f.write(meta_content)
                    print(f"\nReceived meta for {filename} from {peer_ip}")
                else:
                    print(f"\nReceived meta from {peer_ip} but no pending filename, ignoring")