import asyncio

BUFFER_SIZE = 1024


async def handle_client(reader, writer):
    while True:
        request = await reader.read(BUFFER_SIZE)
        print("Got request: " + str(request))
        if not request:
            break
        writer.write("+PONG\r\n".encode())
        await writer.drain()
    writer.close()


async def start_server(host, port):
    srv = await asyncio.start_server(
        handle_client, host, port)
    async with srv:
        await srv.serve_forever()


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    asyncio.run(start_server("localhost", 6379))


if __name__ == "__main__":
    main()
