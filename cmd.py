from protocol import CattorrentProtocol, UdpBroadcastWorker
import uuid


class ClientStatus:
    def __init__(self):
        self.online_status = False
        self.cattorrent_protocol = CattorrentProtocol()

    def online(self):
        if self.online_status:
            print("Client is already online.")
        else:
            self.online_status = True
            self.cattorrent_protocol.online()

    def peer(self):
        if not self.online_status:
            print("Client is offline. Please go online first.")
            return
        else:
            print(self.cattorrent_protocol.get_peers())

    def list(self, peer_id):
        if not self.online_status:
            print("Client is offline. Please go online first.")
            return
        try:
            peer_id = uuid.UUID(peer_id)
        except ValueError:
            print("Invalid peer ID format. Please provide a valid UUID.")
            return
        if peer_id not in self.cattorrent_protocol.peers:
            print(f"Peer {peer_id} not found.")
            return
        self.cattorrent_protocol.get_peer_list(peer_id)

    # 计算share文件夹下的filename的hash值，并将其写入filename.meta中
    def meta(self, filename):
        self.cattorrent_protocol.meta(filename)
        # TODO

    def file(self, dst, filename):
        pass


def main():
    client = ClientStatus()

    while True:
        command = input("Enter a command (or 'exit' to quit): ")
        if command.lower() == "exit":
            print("Exiting the program.")
            if client.online_status:
                client.cattorrent_protocol.upd_handler.stop()  # 停止UDP广播线程
                client.cattorrent_protocol.tcp_recv_handler.stop()  # 停止TCP监听线程
                client.cattorrent_protocol.upd_handler.join()  # 等待UDP广播线程结束
                client.cattorrent_protocol.tcp_recv_handler.join()  # 等待TCP监听线程结束
            exit(0)

        match command.split():
            case ["greet", name]:
                print(f"Hello, {name}!")
            case ["online"]:
                client.online()
            case ["peer"]:
                client.peer()
            case ["list", peer_id]:
                client.list(peer_id)
            case ["file", dst, filename]:
                client.file(dst, filename)
            case _:
                print("Unknown command. Please try again.")


if __name__ == "__main__":
    main()
