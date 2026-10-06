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

def recv_message(sock):
    # 1. skip bytes until STX
    byte = sock.recv(1)
    while byte != STX:
        byte = sock.recv(1)

    # 2. collect DATA until ETX
    data = b""
    byte = sock.recv(1)
    while byte != ETX:
        data += byte
        byte = sock.recv(1)

    # 3. read the LRC and compare
    received_lrc = sock.recv(1)[0]
    if received_lrc == lrc(data):
        sock.sendall(ACK)
        return data.decode()
    else:
        sock.sendall(NACK)
        return None


def send_message(sock, text):
    sock.sendall(build_frame(text))
    answer = sock.recv(1)
    return answer == ACK
