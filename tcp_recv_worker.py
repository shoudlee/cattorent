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
            return self.cattorrent_protocol.get_peer_key_by_ip(self.socket.getpeername()[0])
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
    
    def stop(self):
        self.stop_event.set()

    def run(self) -> None:
        while not self.stop_event.is_set():
            # 处理收到的消息
            try:
                data_length_bytes = self.socket.recv(4)
                if not data_length_bytes:
                    # 连接被对方关闭了，这里还需要停止掉对应的tcp_sender_worker，并从tcp_connections里删除这个连接
                    self.stop()
                    peer_key = self.get_peer_key()
                    if peer_key and peer_key in self.cattorrent_protocol.tcp_connections:
                        self.cattorrent_protocol.tcp_connections[peer_key].stop()
                        del self.cattorrent_protocol.tcp_connections[peer_key]
                    return
                data_length = struct.unpack('!I', data_length_bytes)[0]
            except socket.timeout: 
                continue
            data = bytes()
            while len(data) < data_length:
                try:
                    chunk = self.socket.recv(data_length - len(data))
                    data += chunk
                except socket.timeout:
                    continue
            command = struct.unpack('!4s', data[:4])[0].decode()
            if command == 'LIST':
                peer_key = self.get_peer_key()
                if peer_key and peer_key in self.cattorrent_protocol.tcp_connections:
                    self.cattorrent_protocol.tcp_connections[peer_key].queue.put({'command': 'RESPONSE_LIST'})
            if command == 'RLST':
                files = self.handle_list_response(data[4:])
                print("\nReceived file list:")
                for filename, filesize in files:
                    print(f"{filename} ({filesize} bytes)")
