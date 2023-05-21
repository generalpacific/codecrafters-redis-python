import socket

BUFFER_SIZE = 100


def __decode_received_msg(msg):
    print("Decoding: " + str(msg))

    msg_str = msg.decode()
    print("Msg str: " + msg_str)

    if "ping" in msg_str:
        return "ping"
    else:
        return "unknown_command"


def __encode_ping_response():
    response = "+PONG\r\n"
    return response.encode("utf-8")


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        (conn, address) = server_socket.accept()  # wait for client

        print("Received conn from: " + str(address))

        msg = conn.recv(BUFFER_SIZE)
        print("Received " + str(msg) + " type: " + str(type(msg)) + " from " + str(address))

        command = __decode_received_msg(msg)

        if command == "ping":
            conn.send(__encode_ping_response())
        else:
            print("unknown command closing connection.")
        conn.close()


if __name__ == "__main__":
    main()
