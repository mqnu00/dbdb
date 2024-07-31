import os
import random
from typing import List

from dbdb.logical_redo import LogicalBase
from dbdb.physical_redo import Storage, Data


class KeyData:

    def __init__(self, key, value_pos: int = None):
        self.data = (key, value_pos)

    @property
    def key(self):
        return self.data[0]

    @property
    def value_pos(self):
        return self.data[1]

    @staticmethod
    def store(key, value, storage):
        data = Data(key, value)
        data_address = data.store(storage)
        item = KeyData(key, data_address)
        return item


class Node:
    def __init__(self, level, data: KeyData = None) -> None:
        if data is None:
            self._data = KeyData(None, None)
        else:
            self._data = data
        self.next: List[Node] = [None] * (level + 1)

    @property
    def key(self):
        return self._data.key

    @property
    def value_pos(self):
        return self._data.value_pos

    def __str__(self):
        return f'[{self.key}:{self.value_pos}]'


class SkipList(LogicalBase):

    def __init__(self, storage: Storage, max_level=16, p=0.5):
        super().__init__(storage)
        self.max_level = max_level
        self.p = p
        self.head = Node(max_level)
        self.level = 0
        self._load()

    def _load(self):
        for data, address in self._storage.travel_data():
            now = KeyData(data.key, address)
            self.set(item=now)

    def random_level(self):
        level = 0
        while random.random() < self.p and level < self.max_level:
            level += 1
        return level

    def get(self, key):
        cur = self.head
        for i in range(self.level, -1, -1):
            while cur.next[i] and cur.next[i].key < key:
                cur = cur.next[i]
        cur: Node = cur.next[0]
        if cur and cur.key == key:
            return cur
        return False

    def get_key_value(self, key) -> Data:
        return Data.bytes_to_obj(self._storage.read(self.get(key).value_pos))

    def _do_find(self, target: KeyData):
        update: List[Node] = [None] * (self.max_level + 1)
        cur = self.head

        for i in range(self.level, -1, -1):
            while cur.next[i] and cur.next[i].key < target.key:
                cur = cur.next[i]
            update[i] = cur
        return update, cur.next[0]

    def set(self, key=None, value=None, item=None) -> None:
        if key is not None and value is not None and item is None:
            item = KeyData.store(key, value, self._storage)
        elif key is None and value is None and item is not None:
            pass
        else:
            raise ValueError

        update, cur = self._do_find(item)

        # 没有这个key 或 key已存在，value不一样
        if cur is None or cur.key != key:
            rlevel = self.random_level()

            if rlevel > self.level:
                for i in range(self.level + 1, rlevel + 1):
                    update[i] = self.head
                self.level = rlevel

            n = Node(rlevel, item)
            for i in range(rlevel + 1):
                n.next[i] = update[i].next[i]
                update[i].next[i] = n
        elif self.get_key_value(key).value != value:
            self.pop(key)
            self.set(key, value)

    def pop(self, key) -> bool:
        item = KeyData(key)
        update, cur = self._do_find(item)

        if cur is None or cur.key != key:
            return False
        for i in range(self.level + 1):
            if update[i].next[i] != cur:
                break
            update[i].next[i] = cur.next[i]
        while self.level > 1 and self.head.next[self.level] is None:
            self.level -= 1
        return True

    def travel_key(self):
        p = self.head.next[0]
        while p is not None:
            yield p.value_pos
            p = p.next[0]

    def commit(self):
        self._storage.commit(self.travel_key())


if __name__ == '__main__':
    s = Storage('123')
    a = SkipList(s)
    a.set('1', '1')
    a.set('2', '2')
    res = a.get('2')
    print()
