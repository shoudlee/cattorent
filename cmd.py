from logging_setup import get_logger
from protocol import CattorrentProtocol, UdpBroadcastWorker
import uuid
import pathlib


logger = get_logger(__name__)


class Client:
    def __init__(self):
        self.online_status = False
        self.cattorrent_protocol = CattorrentProtocol()

    def online(self):
        if self.online_status:
            logger.info("Client is already online.")
        else:
            self.online_status = True
            self.cattorrent_protocol.online()

    def peer(self):
        if not self.online_status:
            logger.warning("Client is offline. Please go online first.")
            return
        else:
            print(self.cattorrent_protocol.peers)

    def list(self, peer_id):
        if not self.online_status:
            logger.warning("Client is offline. Please go online first.")
            return
        try:
            peer_id = uuid.UUID(peer_id)
        except ValueError:
            logger.warning("Invalid peer ID format. Please provide a valid UUID.")
            return
        if peer_id not in self.cattorrent_protocol.peers:
            logger.warning("Peer %s not found.", peer_id)
            return
        self.cattorrent_protocol.get_peer_list(peer_id)

    def meta(self, filename):
        """
        计算share文件夹下的filename的hash值，并将其写入.filename.meta中
        """
        result = self.cattorrent_protocol.meta(filename)
        if result:
            logger.info("Metafile for %s has been regenerated.", filename)

    def get_meta(self, peer_id, filename):
        """
        测试用，检测是否能正确处理meta file的收发流程
        """
        try:
            peer_id = uuid.UUID(peer_id)
        except ValueError:
            logger.warning("Invalid peer ID format. Please provide a valid UUID.")
            return
        self.cattorrent_protocol.get_peer_meta(peer_id, filename)

    def exit(self):
        # 退出时按监听线程、广播线程的顺序收尾，避免后台线程残留。
        if self.cattorrent_protocol.upd_handler is not None:
            self.cattorrent_protocol.upd_handler.stop()  # 停止UDP广播线程
            self.cattorrent_protocol.upd_handler.join()  # 等待UDP广播线程结束
        if self.cattorrent_protocol.tcp_recv_handler is not None:
            self.cattorrent_protocol.tcp_recv_handler.stop()  # 停止TCP监听线程
            self.cattorrent_protocol.tcp_recv_handler.join()  # 等待TCP监听线程结束

    def file(self, dst, filename):
        """ """
        pass


def main():
    client = Client()
    # 目前默认启动即上线，便于本地联调；如果需要手动控制可以去掉这一行。
    client.online()
    while True:
        command = input("Enter a command (or 'exit' to quit): ")
        if command.lower() == "exit":
            logger.info("Exiting the program.")
            if client.online_status:
                client.exit()
            exit(0)
        match command.split():
            case ["greet", name]:
                logger.info("Hello, %s!", name)
            case ["online"]:
                client.online()
            case ["peer"]:
                client.peer()
            case ["list", peer_id]:
                client.list(peer_id)
            case ["meta", filename]:
                client.meta(filename)
            # 测试用
            case ["meta", peer_id, filename]:
                client.get_meta(peer_id, filename)
            case ["file", dst, filename]:
                client.file(dst, filename)
            case _:
                logger.warning("Unknown command. Please try again.")


if __name__ == "__main__":
    main()
