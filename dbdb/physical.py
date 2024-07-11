import os
import struct
from io import TextIOWrapper
from typing import Any

import portalocker


class Storage(object):
    SUPERBLOCK_SIZE = 4096
    INTEGER_FORMAT = "!Q"
    INTEGER_LENGTH = 8

    def __init__(self, f):
        self._f = f
        self.locked = False
        self._ensure_superblock()

    def _ensure_superblock(self):
        """
        确保文件大小是 SUPERBLOCK_SIZE 字节
        :return:
        """
        self.lock()
        self._seek_end()
        end_address = self._f.tell()
        if end_address < self.SUPERBLOCK_SIZE:
            self._f.write(b'\x00' * (self.SUPERBLOCK_SIZE - end_address))
        self.unlock()

    def lock(self):
        if not self.locked:
            portalocker.lock(self._f, portalocker.LOCK_EX)
            self.locked = True
            return True
        else:
            return False

    def unlock(self):
        if self.locked:
            self._f.flush()
            portalocker.unlock(self._f)
            self.locked = False

    def _seek_end(self):
        """
        到文件结尾
        :return:
        """
        self._f.seek(0, os.SEEK_END)

    def _seek_superblock(self):
        """
        到文件开头
        :return:
        """
        self._f.seek(0)

    def _bytes_to_integer(self, integer_bytes):
        return struct.unpack(self.INTEGER_FORMAT, integer_bytes)[0]

    def _integer_to_bytes(self, integer):
        return struct.pack(self.INTEGER_FORMAT, integer)

    def _read_integer(self):
        return self._bytes_to_integer(self._f.read(self.INTEGER_LENGTH))

    def _write_integer(self, integer):
        self.lock()
        self._f.write(self._integer_to_bytes(integer))

    def get_root_address(self):
        """
        获取元数据长度
        :return:
        """
        self._seek_superblock()
        root_address = self._read_integer()
        return root_address

    def commit_root_address(self, root_address):
        """
        更新元数据长度
        :param root_address:
        :return:
        """
        self.lock()
        self._f.flush()
        self._seek_superblock()
        self._write_integer(root_address)
        self._f.flush()
        self.unlock()

    def close(self):
        self.unlock()
        self._f.close()

    @property
    def closed(self):
        return self._f.closed

    def read(self, address):
        self._f.seek(address)
        length = self._read_integer()
        data = self._f.read(length)
        return data

    def write(self, data):
        self.lock()
        self._seek_end()
        object_address = self._f.tell()
        self._write_integer(len(data))
        self._f.write(data)
        return object_address

