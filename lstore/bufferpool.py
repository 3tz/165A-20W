from lstore.partition import Partition
import os
import pickle


# TODO:
#   mark dirty and add

class Bufferpool:
    def __init__(self, size, n_cols, key_column, path):
        self.PATH = path # path of the table
        if not os.path.exists(path):
            os.makedirs(path)

        self.MAX_PARTITIONS = size
        self.N_TOTAL_COLS = n_cols
        self.COL_KEY = key_column

        # Vals:
        #    - Partition obj: partition is in bufferpool
        #    - None: partition on disk
        # EX: MAX_PARTITIONS = 2
        # [None, None, Partition obj, Partition obj, None]
        self.partitions = []

        # index of the partitions in the buffer sorted by least recently used
        # This is the core of tracking what partitions are in the BP
        # Max Length = MAX_PARTITIONS
        # EX: means partition 3 was least recently used
        # [3, 2]
        self.LRU = []
        # TODO: implmenet PIN PAGE
        self.pin_pages = {}

        self.new_partition()

    def __getitem__(self, idx_part):
        """ Return the partition with index @idx_part
            If partition not in BP
                partition will be added to buffer pool
            if partition is in
                idx_part in LRU move to the end
            X LRU
            X dirty[idx]
            X Partitions[idx]
        """
        # convert negative index to non neg
        if idx_part < 0:
            idx_part += len(self.partitions)
        # trying to access a partition that doesn't exist
        if idx_part >= len(self.partitions):
            raise IndexError
        # If already in the LRU array, move it to the end
        # Don't need to care about limit since we are not adding things here
        if idx_part in self.LRU:
            self.LRU.remove(idx_part)
            self.LRU.append(idx_part)
            assert(len(self.LRU) <= self.MAX_PARTITIONS)
        # not in the LRU, load from the disk
        else:
            # if buffer limit reached, evict
            if len(self.LRU) == self.MAX_PARTITIONS:
                self.__evict()
            # buffer has space now, just append to the end
            self.LRU.append(idx_part)
            with open(os.path.join(self.PATH, str(idx_part)), 'rb') as f:
                self.partitions[idx_part] = pickle.load(f)

        return self.partitions[idx_part]

    def new_partition(self):
        """ Add a new partition to the DB. New partition will be added to the
        BP and marked as dirty. If BP's limit is reached, the LRU partition
        will be evicted.
        """
        idx_part = len(self.partitions)

        # It is definitely not in the LRU, no need to check
        # if buffer limit reached, evict
        if len(self.LRU) == self.MAX_PARTITIONS:
            self.__evict()
        self.LRU.append(idx_part)
        # Now BP has space, add the new partition
        self.partitions.append(
            Partition(n_cols=self.N_TOTAL_COLS, key_column=self.COL_KEY))

    def __evict(self):
        """ Evict the LRU partition.
            - idx = LRU[0] is removed;
            - partitions[idx] is marked as clean
                - write to disk if dirty
            - partitions[idx] will be replaced with None
        """
        idx_evict = self.LRU.pop(0)
        if self.partitions[idx_evict].is_dirty():
            # TODO: ADD MERGE HERE BEFORE WRITING TO DISK
            self.partitions[idx_evict].set_clean()
            # it's dirty; # write to disk
            with open(os.path.join(self.PATH, str(idx_evict)), 'wb') as f:
                pickle.dump(self.partitions[idx_evict], f)
        self.partitions[idx_evict] = None
