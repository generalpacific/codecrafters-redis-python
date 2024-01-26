import asyncio
import datetime
from enum import Enum

BUFFER_SIZE = 1024

IN_MEM_DATABASE = {}


class RedisCommand(Enum):
    ECHO = 1,
    PING = 2,
    SET = 3,
    GET = 4,
    UNKNOWN = 5


async def decode_size_array(arg):
    return int(arg[1:])


async def decode_echo(request):
    parts = request.split("\r\n")
    size = await decode_size_array(parts[0])
    if size == 1:
        return "".encode()
    return parts[4].encode()


async def decode_set(request):
    parts = request.split("\r\n")
    size = await decode_size_array(parts[0])
    print("parts: ")
    for part in parts:
        print(part)
    if size == 1:
        return "".encode()
    if len(parts) > 7:
        return [parts[4], parts[6], datetime.datetime.now() + datetime.timedelta(milliseconds=int(parts[10]))]
    else:
        return [parts[4], parts[6], datetime.datetime.max]


async def decode_get(request):
    parts = request.split("\r\n")
    size = await decode_size_array(parts[0])
    if size == 1:
        return "".encode()
    return parts[4]


async def get_command(request):
    if "ECHO" in request or "echo" in request:
        return RedisCommand.ECHO
    elif "PING" in request or "ping" in request:
        return RedisCommand.PING
    elif "SET" in request or "set" in request:
        return RedisCommand.SET
    elif "GET" in request or "get" in request:
        return RedisCommand.GET
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
        elif command is RedisCommand.SET:
            arguments = await decode_set(request)
            IN_MEM_DATABASE[arguments[0]] = (arguments[1], arguments[2])
            writer.write(b'$2\r\nOK\r\n')
        elif command is RedisCommand.GET:
            argument = await decode_get(request)
            db_value = IN_MEM_DATABASE[argument]
            ttl = db_value[1]
            return_value = db_value[0]
            if ttl >= datetime.datetime.now():
                writer.write(b"$" + str(len(return_value)).encode() + b'\r\n' + return_value.encode() + b"\r\n")
            else:
                writer.write(b"$-1\r\n")

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
