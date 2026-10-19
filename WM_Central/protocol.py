STX = b'\x02'
ETX = b'\x03'
ACK = b'\x06'
NACK = b'\x15'


def lrc(data):
    result = 0
    for byte in data:       # iterating over bytes gives integers 0-255
        result ^= byte      # XOR the running result with each byte
    return result


def build_frame(text):
    data = text.encode()
    return STX + data + ETX + bytes([lrc(data)])

def recv_byte(sock):
    byte = sock.recv(1)
    if byte == b"":
        raise ConnectionError("peer closed the connection")
    return byte

def recv_message(sock):
    # 1. skip bytes until STX
    byte = recv_byte(sock)
    while byte != STX:
        byte = recv_byte(sock)

    # 2. collect DATA until ETX
    data = b""
    byte = recv_byte(sock)
    while byte != ETX:
        data += byte
        byte = recv_byte(sock)

    # 3. read the LRC and compare
    received_lrc = recv_byte(sock)[0]
    if received_lrc == lrc(data):
        sock.sendall(ACK)
        return data.decode()
    else:
        sock.sendall(NACK)
        return None


def send_message(sock, text):
    sock.sendall(build_frame(text))
    answer = recv_byte(sock)
    return answer == ACK
