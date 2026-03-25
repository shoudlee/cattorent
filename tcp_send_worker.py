import threading
import socket
import struct
import time 
import uuid
from dataclasses import dataclass
from queue import Queue, Empty
from pathlib import Path

class TcpSendWorker(threading.Thread):
    def __init__(self, cattorrent_protocol: CattorrentProtocol, socket: socket.socket): # type: ignore
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
   