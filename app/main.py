import argparse
import asyncio
import datetime
import sys
from enum import Enum
import struct

BUFFER_SIZE = 1024
IN_MEM_DATABASE = {}
DIRECTORY = ""
DBFILENAME = ""


class RedisCommand(Enum):
    ECHO = 1,
    PING = 2,
    SET = 3,
    GET = 4,
    CONFIG = 5,
    KEYS = 6,
    UNKNOWN = 7


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
    if size == 1:
        return "".encode()
    if len(parts) > 8:
        return [parts[4], parts[6], datetime.datetime.now() + datetime.timedelta(milliseconds=int(parts[10]))]
    else:
        return [parts[4], parts[6], datetime.datetime.max]


async def decode_get(request):
    parts = request.split("\r\n")
    print("parts: ")
    for part in parts:
        print(part)
    return ""


async def read_til_kvs(file):
    while operand := file.read(1):
        print(operand)
        if operand == b'\xfb':
            # we reached the byte just before kv pairs
            break
    # Skip next to bytes which are hashtable size.
    file.read(3)


async def read_unsigned_char(file):
    return struct.unpack("B", file.read(1))[0]


async def get_string_len(file):
    first_byte = await read_unsigned_char(file)
    print("firstbyte")
    print(first_byte)
    # Check if the two most significant bits are 00
    if first_byte >> 6 == 0b00:
        # 00 -> The next 6 bits represent the length of the string
        return first_byte & 0b00111111
    else:
        # Handle other cases
        print("Invalid first byte", first_byte)
        return None


async def read_string(file):
    length = await get_string_len(file)
    print("length is " + str(length))
    return file.read(length).decode()


async def decode_file_content(file):
    await read_til_kvs(file)
    return await read_string(file)


async def decode_config(request):
    parts = request.split("\r\n")
    size = await decode_size_array(parts[0])
    if size == 1:
        return "".encode()
    return parts[6]


async def get_command(request):
    parts = request.split("\r\n")
    command = parts[2]
    print(f"Decoded command string: {command}")
    if command == "ECHO" or command == "echo":
        return RedisCommand.ECHO
    elif command == "PING" or command == "ping":
        return RedisCommand.PING
    elif command == "GET" or command == "get":
        return RedisCommand.GET
    elif command == "SET" or command == "set":
        return RedisCommand.SET
    elif command == "CONFIG" or command == "config":
        return RedisCommand.CONFIG
    elif command == "KEYS" or command == "keys":
        return RedisCommand.KEYS
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
            if argument not in IN_MEM_DATABASE:
                writer.write(b"$-1\r\n")
            else:
                db_value = IN_MEM_DATABASE[argument]
                ttl = db_value[1]
                return_value = db_value[0]
                if ttl >= datetime.datetime.now():
                    writer.write(b"$" + str(len(return_value)).encode() + b'\r\n' + return_value.encode() + b"\r\n")
                else:
                    writer.write(b"$-1\r\n")
        elif command is RedisCommand.CONFIG:
            argument = await decode_config(request)
            print(f"Argument for config: {argument}")
            if argument == "dir":
                writer.write(
                    b"*2\r\n$3\r\ndir\r\n$" + str(len(DIRECTORY)).encode() + b'\r\n' + DIRECTORY.encode() + b"\r\n")
            else:
                writer.write(
                    b"*2\r\n$10\r\ndbfilename\r\n$" + str(
                        len(DBFILENAME)).encode() + b'\r\n' + DBFILENAME.encode() + b"\r\n")
        elif command is RedisCommand.KEYS:
            with open(DIRECTORY + "/" + DBFILENAME, 'rb') as file:
                key = await decode_file_content(file)
            with open(DIRECTORY + "/" + DBFILENAME, 'rb') as file:
                file_content = file.read()
                print(file_content)
            print("key: " + key)
            writer.write(b"$" + str(len(key)).encode() + b'\r\n' + key.encode() + b"\r\n")
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
    if len(sys.argv) > 1:
        for i, arg in enumerate(sys.argv[1:], start=1):
            print(f"Argument {i}: {arg}")

        parser = argparse.ArgumentParser()
        parser.add_argument('--dir', type=str, required=True, help='The directory path')
        parser.add_argument('--dbfilename', type=str, required=True, help='The database file name')

        args = parser.parse_args()

        global DIRECTORY
        global DBFILENAME
        DIRECTORY = args.dir
        DBFILENAME = args.dbfilename

        print(f"Directory: {DIRECTORY}")
        print(f"Database Filename: {DBFILENAME}")
    else:
        print("No additional command-line arguments were provided.")
    asyncio.run(start_server("localhost", 6379))


if __name__ == "__main__":
    main()
