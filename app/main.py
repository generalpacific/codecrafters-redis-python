import asyncio
from enum import Enum

BUFFER_SIZE = 1024


class RedisCommand(Enum):
    ECHO = 1,
    PING = 2,
    UNKNOWN = 3


async def decode_size_array(arg):
    return int(arg[1:])


async def decode_echo(request):
    parts = request.split("\r\n")
    print("Debug parts: \n")
    for part in parts:
        print(part)
    size = await decode_size_array(parts[0])
    if size == 1:
        return "".encode()
    return parts[4].encode()


async def get_command(request):
    if "ECHO" in request:
        return RedisCommand.ECHO
    elif "PING" in request:
        return RedisCommand.PING
    else:
        return RedisCommand.UNKNOWN


async def handle_client(reader, writer):
    while True:
        request = await reader.read(BUFFER_SIZE)
        print("Got request: " + str(request))
        if not request:
            break
        request = request.decode()
        command = await get_command(request)
        if command is RedisCommand.PING:
            writer.write("+PONG\r\n".encode())
        elif command is RedisCommand.ECHO:
            argument = await decode_echo(request)
            writer.write(b"$" + str(len(argument)).encode() + b'\r\n' + argument + b"\r\n")
        else:
            writer.write("+UNKNOWN\r\n".encode())
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
