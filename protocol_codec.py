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
        filename_length = struct.unpack("!H", payload[offset : offset + 2])[0]
        offset += 2
        if offset + filename_length + 8 > len(payload):
            raise ValueError("Invalid RLST payload while reading file record")
        filename = payload[offset : offset + filename_length].decode("utf-8")
        offset += filename_length
        filesize = struct.unpack("!Q", payload[offset : offset + 8])[0]
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
    if len(meta_content) < 56:
        return False
    bitmap_length = struct.unpack("!I", meta_content[48:52])[0]
    return len(meta_content) >= 56 + bitmap_length


def build_getp_request(filename: str, piece_index: int) -> bytes:
    filename_bytes = filename.encode("utf-8")
    payload = struct.pack("!H", len(filename_bytes))
    payload += filename_bytes
    payload += struct.pack("!I", piece_index)
    return pack_frame("GETP", payload)


def parse_getp_request(payload: bytes) -> tuple[str, int]:
    if len(payload) < 6:
        raise ValueError("Invalid GETP payload")
    filename_length = struct.unpack("!H", payload[:2])[0]
    expected_length = 2 + filename_length + 4
    if len(payload) != expected_length:
        raise ValueError("Invalid GETP payload length")
    filename = payload[2 : 2 + filename_length].decode("utf-8")
    piece_index = struct.unpack("!I", payload[2 + filename_length : expected_length])[0]
    return filename, piece_index


def build_getp_response(filename: str, piece_index: int, data: bytes) -> bytes:
    filename_bytes = filename.encode("utf-8")
    payload = struct.pack("!H", len(filename_bytes))
    payload += filename_bytes
    payload += struct.pack("!I", piece_index)
    payload += struct.pack("!I", len(data))
    payload += data
    return pack_frame("PIEC", payload)


def parse_getp_response(payload: bytes) -> tuple[str, int, bytes]:
    if len(payload) < 10:
        raise ValueError("Invalid PIEC payload")
    filename_length = struct.unpack("!H", payload[:2])[0]
    header_length = 2 + filename_length + 4 + 4
    if len(payload) < header_length:
        raise ValueError("Invalid PIEC payload length")
    filename = payload[2 : 2 + filename_length].decode("utf-8")
    piece_index = struct.unpack(
        "!I", payload[2 + filename_length : 6 + filename_length]
    )[0]
    data_length = struct.unpack(
        "!I", payload[6 + filename_length : 10 + filename_length]
    )[0]
    if len(payload) != header_length + data_length:
        raise ValueError("Invalid PIEC data length")
    data = payload[header_length : header_length + data_length]
    return filename, piece_index, data


def build_error_response(error_code: int, message: str) -> bytes:
    msg_bytes = message.encode("utf-8")
    payload = struct.pack("!HH", error_code, len(msg_bytes)) + msg_bytes
    return pack_frame("ERRO", payload)


def parse_error_response(payload: bytes) -> tuple[int, str]:
    if len(payload) < 4:
        raise ValueError("Invalid ERRO payload")
    error_code, msg_len = struct.unpack("!HH", payload[:4])
    if len(payload) != 4 + msg_len:
        raise ValueError("Invalid ERRO payload length")
    return error_code, payload[4:].decode("utf-8")
