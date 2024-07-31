import io
import os
import pickle
import struct
from datetime import datetime

from filelock import FileLock


class Storage:
    INTEGER_FORMAT = "!Q"
    INTEGER_LENGTH = 8

    def __init__(self, dbname: str):
        try:
            self._f = open(dbname, 'r+b')
        except IOError:
            fd = os.open(dbname, os.O_RDWR | os.O_CREAT)
            self._f = os.fdopen(fd, 'r+b')
        self.dbname = dbname

    def _bytes_to_integer(self, integer_bytes) -> int:
        return struct.unpack(self.INTEGER_FORMAT, integer_bytes)[0]

    def _integer_to_bytes(self, integer) -> bytes:
        return struct.pack(self.INTEGER_FORMAT, integer)

    def _read_integer(self):
        return self._bytes_to_integer(self._f.read(self.INTEGER_LENGTH))

    def _write_integer(self, integer):
        self._f.write(self._integer_to_bytes(integer))

    def _seek_end(self):
        """
        到文件结尾
        :return:
        """
        self._f.seek(0, os.SEEK_END)

    def _seek_head(self):
        """
        到文件开头
        :return:
        """
        self._f.seek(0)

    def get_head_address(self):
        """
        获取根节点位置
        :return:
        """
        self._seek_head()
        return 0

    def close(self):
        self._f.close()

    @property
    def closed(self):
        return self._f.closed

    def read(self, address: int):
        self._f.seek(address)
        length = self._read_integer()
        data = self._f.read(length)
        return data

    def write(self, data):
        lock = FileLock(self.dbname + '.lock')
        with lock:
            self._seek_end()
            object_address = self._f.tell()
            self._write_integer(len(data))
            self._f.write(data)
            return object_address

    def travel_data(self):
        length = len(self)
        address = self.get_head_address()
        self._seek_head()
        while self._f.tell() < length:
            data_bytes = self.read(address)
            yield Data.bytes_to_obj(data_bytes), address
            address = self._f.tell()

    def commit(self, data):
        tmp_dbname = 'tmp_' + self.dbname
        dbname = self.dbname
        new_storage = Storage(tmp_dbname)
        for value_pos in data:
            new_storage.write(self.read(value_pos))
        new_storage.close()
        self.close()
        os.remove(dbname)
        os.rename(tmp_dbname, dbname)
        self.__init__(dbname)

    def __len__(self):
        self._seek_end()
        return self._f.tell()


class Data:

    def __init__(self, key, value, tstamp=None):
        # if tstamp is None:
        #     self.tstamp = datetime.now().timestamp()
        self.key = key
        self.value = value

    @staticmethod
    def obj_to_bytes(obj) -> bytes:
        return pickle.dumps((obj.key, obj.value))

    @staticmethod
    def bytes_to_obj(obj_bytes):
        d = pickle.loads(obj_bytes)
        return Data(
            d[0],
            d[1],
            # d['tstamp'],
        )

    def store(self, storage: Storage):
        if self.key is not None and self.value is not None:
            return storage.write(self.obj_to_bytes(self))
        else:
            raise ValueError

    def __str__(self):
        return f'[{self.key}: {self.value}]'


if __name__ == '__main__':
    s = Storage('123')
    # s.write(b'1')
    # s.write(b'2')
    for data, address in s.travel_data():
        print(data, address)
