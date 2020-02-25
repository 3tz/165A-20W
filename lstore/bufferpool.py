fromt lstore.page import *

class Bufferpool():
    def __init__(self, num_of_paritions, n_cols):
        self.NUM_OF_PARTITIONS = num_of_paritions
        self.paritions = {}
        self.dirty_pages = {}
        self.pin_pages = {}
        self.LRU = {}

    def update(parition_idx):
        "idx: the current parition index of the record"
        self.LeastRecentUse(parition_idx)
        if(partition_idx in self.partitions): pass
        else:
            if(len(self.partitions) < self.NUM_OF_PARTITIONS):
                "read from the disk"
                pass
            else:
                max = -1
                evict_parition = -1
                for i in self.LRU:
                    if(LRU[i] > max):
                        max = LRU[i]
                        evict_parition = i;
                if(dirty_pages[i] == 1):
                    "write to the disk"
                    pass
                "read from the disk"

    def read(idx, query_columns):
        cur_partition_idx = idx / 512
        self.update(cur_partition_idx)]

        self.pin_pages[cur_partition_idx] += 1
        result = self.partitions[cur_partition_idx].read(idx,query_columns)
        self.pin_pages[cur_partition_idx] -= 1
        return result

    def write(idx, *columns):
        cur_partition_idx = idx / 512
        self.update(cur_partition_idx)

        self.pin_pages[cur_partition_idx] += 1
        self.dirty_pages[cur_partition_idx] = 1
        self.partitions[cur_partition_idx].write(*columns)
        self.pin_pages[cur_partition_idx] -= 1
        return

    def LeastRecentUse(partition_idx):
        "partition_idx: the index of the page that is just used"
        old_val = LRU[cur_partition_idx]
        for i in self.LRU:
            if(i == cur_partition_idx): LRU[cur_partition_idx] == 1
            else:
                if(LRU[i] <= old_val): LRU[i] += 1
                else: pass
