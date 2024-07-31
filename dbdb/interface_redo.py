from dbdb.physical_redo import Storage
from dbdb.skip_list import SkipList


class DBDB:

    def __init__(self, dbname: str):
        self._storage = Storage(dbname)
        self._list = SkipList(self._storage)

    def _assert_not_closed(self):
        if self._storage.closed:
            return ValueError('Database closed.')

    def close(self):
        self._storage.close()

    def commit(self):
        self._list.commit()

    def __getitem__(self, key):
        self._assert_not_closed()
        if self.__contains__(key):
            return self._list.get(key).value
        else:
            return False

    def __setitem__(self, key, value):
        self._assert_not_closed()
        if self.__contains__(key):
            self.__delitem__(key)
        return self._list.set(key, value)

    def __delitem__(self, key):
        self._assert_not_closed()
        if self.__contains__(key):
            return self._list.pop(key)
        else:
            return False

    def __contains__(self, key):
        try:
            return self._list.contain(key)
        except KeyError:
            return False

    def __len__(self):
        pass


if __name__ == '__main__':
    db = DBDB('123')
    db['1'] = '1'
    db['2'] = '2'
    print(db['1'])