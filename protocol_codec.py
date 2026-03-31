import struct


def pack_frame(command: str, payload: bytes = b"") -> bytes:
    command_bytes = command.encode("ascii")
    if len(command_bytes) != 4:
        raise ValueError("Command must be exactly 4 ASCII characters")
    return struct.pack("!I4s", 4 + len(payload), command_bytes) + payload


def unpack_frame_body(body: bytes) -> tuple[str, bytes]:
    if len(body) < 4:
        raise ValueError("Frame body too short")
    command = body[:4].decode("ascii")
    return command, body[4:]


def build_list_request() -> bytes:
    return pack_frame("LIST")


def build_list_response(file_map: dict[str, int]) -> bytes:
    payload = struct.pack("!I", len(file_map))
    for filename, filesize in file_map.items():
        filename_bytes = filename.encode("utf-8")
        payload += struct.pack("!H", len(filename_bytes))
        payload += filename_bytes
        payload += struct.pack("!Q", filesize)
    return pack_frame("RLST", payload)


def parse_list_response(payload: bytes) -> list[tuple[str, int]]:
    if len(payload) < 4:
        raise ValueError("Invalid RLST payload")
    file_count = struct.unpack("!I", payload[:4])[0]
    offset = 4
    files: list[tuple[str, int]] = []
    for _ in range(file_count):
        if offset + 2 > len(payload):
            raise ValueError("Invalid RLST payload while reading filename length")
        filename_length = struct.unpack("!H", payload[offset:offset + 2])[0]
        offset += 2
        if offset + filename_length + 8 > len(payload):
            raise ValueError("Invalid RLST payload while reading file record")
        filename = payload[offset:offset + filename_length].decode("utf-8")
        offset += filename_length
        filesize = struct.unpack("!Q", payload[offset:offset + 8])[0]
        offset += 8
        files.append((filename, filesize))
    return files


def build_meta_request(filename: str) -> bytes:
    filename_bytes = filename.encode("utf-8")
    payload = struct.pack("!H", len(filename_bytes)) + filename_bytes
    return pack_frame("META", payload)


def parse_meta_request(payload: bytes) -> str:
    if len(payload) < 2:
        raise ValueError("Invalid META payload")
    filename_length = struct.unpack("!H", payload[:2])[0]
    if len(payload) != 2 + filename_length:
        raise ValueError("Invalid META payload length")
    return payload[2:].decode("utf-8")


def build_meta_response(meta_content: bytes) -> bytes:
    return pack_frame("RMTA", meta_content)


def is_valid_meta_payload(meta_content: bytes) -> bool:
    if len(meta_content) < 52:
        return False
    bitmap_length = struct.unpack("!I", meta_content[48:52])[0]
    return len(meta_content) >= 52 + bitmap_length
