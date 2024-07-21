from dbdb.physical_redo import Storage


class LogicalBase:

    def __init__(self, storage: Storage):
        self._storage = storage

    def commit(self):
        pass

    def get(self, key):
        pass

    def set(self, key, value):
        pass

    def pop(self, key):
        self.commit()
        self._storage.close()


if __name__ == '__main__':
    pass
