from lstore.partition import Partition
import os
import pickle


# TODO:
#   mark dirty and add

class Bufferpool:
    def __init__(self, size, n_cols, key_column, path):
        self.PATH = path # path of the table
        self.MAX_PARTITIONS = size
        self.N_TOTAL_COLS = n_cols
        self.COL_KEY = key_column

        self.count = 0  # number of partitions that are in the BP
        # Vals:
        #    - Partition obj: partition is in bufferpool
        #    - None: partition on disk
        self.partitions = []
        # Vals:
        #    - 1: if dirty; 0: clean
        self.dirty = []

        # index of the partitions in the buffer sorted by least recently used
        # Length = MAX_PARTITIONS
        # [3, 5, 4] means partition 3 was least recently used
        self.LRU = []

        self.pin_pages = {}
        self.new_partition()

    def __getitem__(self, idx_part):
        p = self.partitions[idx_part]

        if p is None:
            # not currently in bufferpool
            # kick one out and load it in
            idx_evict = self.__recently_used(idx_part)
            self.__evict(idx_evict)  # it should assign None to the evicted

            with open(os.path.join(self.PATH, str(idx_part)), 'rb') as f:
                p = pickle.load(f)

            self.partitions[idx_part] = p
        else:
            return p

    def new_partition(self):
        # create a partition first and store it in BP
        p = Partition(n_cols=self.N_TOTAL_COLS, key_column=self.COL_KEY)
        self.dirty.append(1)
        self.partitions.append(p)
        self.count += 1

        # now decide how if we need to evict
        if self.__full():
            # BP full; have to evict
            idx_part = len(self.partitions)
            idx_evict = self.__recently_used(idx_part)
            self.__evict(idx_evict)
            self.count -= 1
        else:
            pass

    def __recently_used(self, idx_part):
        """
        note: should be called after adding partition
        # return the partition index that needs to be evicted
        # if no need to evict, None is returned
        """
        if self.__full():
            if idx_part not in self.LRU:
                idx_evict = self.LRU[0]
                self.LRU.append(idx_part)
                self.LRU = self.LRU[1:]
                return idx_evict
            else:
                self.LRU.remove(idx_part)
                self.LRU.append(idx_part)
        else:
            if idx_part not in self.LRU:
                self.LRU.append(idx_part)
            else:
                self.LRU.remove(idx_part)
                self.LRU.append(idx_part)
        return

    def __evict(self, idx_evict):
        """
        """
        if self.dirty[idx_evict]:
            # merge i guess
            pass
        else:
            with open(os.path.join(self.PATH, str(idx_evict)), 'wb') as f:
                pickle.dump(self.paritions[idx_evict], f)
        self.dirty[idx_evict] = 0
        self.partitions[idx_evict] = None

    def __full(self):
        return self.count >= self.MAX_PARTITIONS