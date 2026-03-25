import threading
import socket
import struct
import time 
import uuid
from dataclasses import dataclass
from queue import Queue, Empty
from pathlib import Path

class CattorrentProtocol:
    def __init__(self, ip='0.0.0.0', port=9822, broadcast_interval=2, share_folder="./catshare"):
        self.ip = ip
        self.port = port
        self.broadcast_interval = broadcast_interval
        self.peer_id = uuid.uuid4()
        self.peers= {}
        self.tcp_connections = {}
        self.share_folder = share_folder
    
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
        
        
    def get_peer_list(self, peer_id)->None:
        # 是否在peers里
        self.refresh_peers()
        if peer_id not in self.peers:
            print(f"Peer {peer_id} not found.")
            return
        peer_info = self.peers[peer_id][0]
        
        # 是否已经建立了TCP连接
        if handler := self.tcp_connections.get(peer_info.ip):
            handler.queue.put({'command': 'LIST'})
            return
        
        # 没有TCP连接，建立一个新的连接并放入tcp_connections
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((peer_info.ip, peer_info.port))
            handler = TcpSendWorker(self, sock)
        except Exception as e:
            print(f"Failed to connect to peer {peer_id} at {peer_info.ip}:{peer_info.port}: {e}")
            return
        # 统一使用对方的监听端口作为key
        self.tcp_connections[peer_info.ip] = handler
        handler.start()
        handler.queue.put({'command': 'LIST'})
        recv_handler = TcpRecvWorker(self, sock)
        recv_handler.start()


    def encode_broadcast_message(self, command="ONLI"):
        reserved = 0
        protocol_version = 1
        peer_id = self.peer_id.bytes
        body = struct.pack('!HHI16s', self.port, reserved, protocol_version, peer_id)
        length = len(body) + 4  # 4 bytes for the command
        message = struct.pack('!I4s', length, command.encode()) + body
        return message
    
    def handle_received_udp_packet(self, ip, port, packet):
        try:
            # 对于udp来说，每次recv都是一个完整的packet，所以length时冗余的，但为了协议的完整性，我们还是保留它
            packet_length = struct.unpack('!I', packet[:4])[0]
            if packet_length != len(packet) - 4:
                print("Invalid packet length")
                return
            command = struct.unpack('!4s', packet[4:8])[0].decode()
            if command != 'ONLI':
                print(f"Unknown command: {command}")
                return
            port, reserved, protocol_version, peer_id = struct.unpack('!HHI16s', packet[8:])
            if peer_id == self.peer_id.bytes:
                # 收到自己的广播，忽略
                return
            peer_info = PeerInfo(ip=ip, port=port, version=protocol_version)
            peer_id = uuid.UUID(bytes=peer_id)
            self.peers[peer_id] = peer_info, time.time()
        except Exception as e:
            print(f"Failed to handle received UDP packet: {e}")
    
    def get_peers(self):
        self.refresh_peers()
        return [(peer_id, peer_info) for peer_id, (peer_info, last_seen) in self.peers.items()]

    def refresh_peers(self):
        now = time.time()
        expired_peers = [peer_id for peer_id, (peer_info, last_seen) in self.peers.items() if now - last_seen > self.broadcast_interval * 2]
        for peer_id in expired_peers:
            del self.peers[peer_id]        

    def get_peer_key_by_ip(self, ip):
        self.refresh_peers()
        return next((info.ip for info, _ in self.peers.values() if info.ip == ip), None)

@dataclass
class PeerInfo:
    ip: str
    port: int
    version: int


class UdpBroadcastWorker(threading.Thread):
    def __init__(self, cattorrent_protocol: CattorrentProtocol):
        super().__init__()
        self.cattorrent_protocol = cattorrent_protocol
        self.ip = cattorrent_protocol.ip
        self.port = cattorrent_protocol.port
        self.broadcast_interval = cattorrent_protocol.broadcast_interval
        self.stop_event = threading.Event()
        # just a try
        self.socket:socket.socket | None = None

    def setup_socket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Enable broadcasting mode
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind((self.ip, self.port))
        sock.settimeout(0.5)
        return sock
    
    def stop(self):
        self.stop_event.set()

    def send_broadcast(self, msg):
        if self.socket:
            self.socket.sendto(msg, ('255.255.255.255', self.port))
            # print(f"Broadcasted: {msg}")
    
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
                # 这里的recvfrom是阻塞的，但是之前设置了timeout，所以它会在没有数据时抛出socket.timeout异常，
                # 我们捕获这个异常并继续循环，以便定期发送广播消息
                data, addr = self.socket.recvfrom(1024)
                self.cattorrent_protocol.handle_received_udp_packet(addr[0], addr[1], data)
            
            except socket.timeout:
                continue
            except Exception as e:
                print(f"UDP Broadcast Worker error: {e}")
                return
            
# 更确切叫TcpListenWorker，因为它只负责监听和接受TCP连接，真正的发送和接收数据的工作由TcpSendWorker来做
class TcpListenWorker(threading.Thread):
    def __init__(self, cattorrent_protocol: CattorrentProtocol):
        super().__init__()
        self.cattorrent_protocol = cattorrent_protocol
        self.stop_event = threading.Event()
    
    def setup_socket(self):
        self.recv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.recv_socket.bind((self.cattorrent_protocol.ip, self.cattorrent_protocol.port))
        self.recv_socket.listen()
        self.recv_socket.settimeout(0.5)

    def run(self) -> None:
        self.setup_socket()
        while not self.stop_event.is_set():
            try:
                conn, addr = self.recv_socket.accept()
                peer_key = self.cattorrent_protocol.get_peer_key_by_ip(addr[0])
                if peer_key is None:
                    print("??????????")
                    conn.close()
                    continue
                # 不知道有什么用，但是感觉加一个timeout比较好，防止某些异常情况导致线程一直阻塞在recv上
                conn.settimeout(0.1)
                send_worker = TcpSendWorker(self.cattorrent_protocol, conn)
                self.cattorrent_protocol.tcp_connections[peer_key] = send_worker
                send_worker.start() 
                TcpRecvWorker(self.cattorrent_protocol, conn).start() 

            except socket.timeout:
                continue
            except Exception as e:
                print(f"TCP Receive Worker error: {e}")
                return
    
    def stop(self):
        for handler in self.cattorrent_protocol.tcp_connections.values():
            handler.stop()
        self.stop_event.set()

class TcpRecvWorker(threading.Thread):
    def __init__(self, cattorrent_protocol: CattorrentProtocol, socket: socket.socket):
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

class TcpSendWorker(threading.Thread):
    def __init__(self, cattorrent_protocol: CattorrentProtocol, socket: socket.socket):
        super().__init__()
        self.cattorrent_protocol = cattorrent_protocol
        self.stop_event = threading.Event()
        self.queue = Queue()
        # socket.setblocking(False)
        # 后期改为select或epoll来处理多个连接，现在先简单地把它设置为非阻塞，并在recv时捕获BlockingIOError异常
        self.socket = socket
        # self.socket.settimeout(0.1)
    
    def stop(self):
        self.stop_event.set()
    
    def handle_list_request(self):
        # 获取share文件夹中的filename和size
        file = {}
        for p in Path(self.cattorrent_protocol.share_folder).iterdir():
            if p.is_file():
                file[p.name] = p.stat().st_size
        # 构造响应
        file_count = len(file)
        file_content = bytes()
        if file_count > 0:
            for filename, filesize in file.items():
                filename_bytes = filename.encode()
                filename_length = len(filename_bytes)
                file_content += struct.pack('!H', filename_length) + filename_bytes + struct.pack('!Q', filesize)
        
        return struct.pack('!I4sI', 4 + 4 + len(file_content), b'RLST', file_count) + file_content

    def run(self) -> None:
        task = None
        while not self.stop_event.is_set():
            # 从queue中获取任务并处理，来自于TCP接收线程的任务会被放到这个queue里
            try:
                if task is None:
                    task = self.queue.get()
                # 任务的格式应该是一个字典，包含command和其他必要的信息
                if task['command'] == 'LIST':
                    msg = struct.pack('!I4s', 4, b'LIST')
                    self.socket.sendall(msg)
                    task = None
                elif task['command'] == 'RESPONSE_LIST':
                    msg = self.handle_list_request()
                    self.socket.sendall(msg)
                    task = None
            # 防止sendall超时，保留现存的任务继续尝试发送
            except socket.timeout:
                continue
            