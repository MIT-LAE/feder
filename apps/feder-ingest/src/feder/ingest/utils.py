from collections import OrderedDict


class LastUpdatedOrderedDict[K, V](OrderedDict):
    def __setitem__(self, key: K, value: V):
        super().__setitem__(key, value)
        self.move_to_end(key)
