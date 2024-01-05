import socketserver

BUFFER_SIZE = 100


class RedisHandler(socketserver.BaseRequestHandler):
    def handle(self):
        # self.request is the TCP socket connected to the client
        self.data = self.request.recv(BUFFER_SIZE).strip()
        # We presume the client will only send 'PING'
        print("Received request: " + str(self.data))
        self.request.sendall(b'+PONG\r\n')


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    with socketserver.ThreadingTCPServer(("localhost", 6379), RedisHandler) as server:
        server.serve_forever()


if __name__ == "__main__":
    main()
