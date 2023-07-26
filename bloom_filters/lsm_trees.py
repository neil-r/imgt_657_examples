import os
import pickle
import tempfile
import bloom_filter


'''
Fixed ChatGPT generated code.

Demonstrates a simple LSM tree with only two levels: a memory-based component
(Level 0) and a disk-based component (Level 1). The memory-based component is
implemented as a Python dictionary, and the disk-based component is 
represented as a list of key-value pairs stored in a file.

Keep in mind that this is a basic implementation for demonstration purposes, 
and real-world LSM trees are more complex, with optimizations and additional 
features to handle data efficiently.

TODO: the SSTable class is not actually implemented as an string sorted table, 
just a placeholder for demonstration for the LSM tree data structure.
'''

class SSTable:
    def __init__(self, file_path):
        self.file_path = file_path

    def write(self, data):
        with open(self.file_path, 'wb') as f:
            pickle.dump(data, f)

    def read(self):
        with open(self.file_path, 'rb') as f:
            return pickle.load(f)

    def delete(self):
        os.remove(self.file_path)


class LSMTree:
    def __init__(self, memtable_size=3):
        self.memtable = {}   # In-memory dictionary acting as Level 0
        self.memtable_size = memtable_size
        self.ss_tables_file_paths = []  # Disk-based list acting as Level 1

        # a bloom filter is used to avoid looking up keys in ss tables that
        # don't exist
        self.bloom_filter = bloom_filter.BloomFilter(32, 1)

    def insert(self, key, value):
        self.memtable[key] = value
        self.bloom_filter.add(key)
        if len(self.memtable) >= self.memtable_size:
            self.flush()

    def flush(self):
        if not self.memtable:
            return

        # Writing memtable to disk as a new SSTable (Sorted String Table)
        with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
            # Persist the in-memory data to disk as a pickle file
            sstable = SSTable(tmpfile.name)
            sstable.write(self.memtable)
            self.ss_tables_file_paths.append(tmpfile.name)

        self.memtable.clear()

    def get(self, key):
        # use bloom filter to avoid unnesscary io
        if key not in self.bloom_filter:
            print("Skipping lookup due to bloom filter check.")
            return None

        # Check in memtable first
        if key in self.memtable:
            return self.memtable[key]

        # If not found in memtable, check in disktable
        for ss_table_path in reversed(self.ss_tables_file_paths):
            sstable = SSTable(ss_table_path).read()
            if key in sstable:
                return sstable[key]

        return None

    def __del__(self):
        # Ensure the last batch of data is flushed to disk before deletion
        self.flush()


if __name__ == "__main__":
    lsm = LSMTree()
    lsm.insert('apple', 'red')
    lsm.insert('banana', 'yellow')
    lsm.insert('grape', 'purple')

    # Retrieve values
    print(lsm.get('apple'))    # Output: 'red'
    print(lsm.get('banana'))   # Output: 'yellow'
    print(lsm.get('orange'))   # Output: None

    # After flushing, new data will be merged into disktable
    lsm.flush()

    # Insert additional data
    lsm.insert('orange', 'orange')
    lsm.insert('kiwi', 'green')

    # Retrieve values again
    print(lsm.get('orange'))   # Output: 'orange'
    print(lsm.get('kiwi'))     # Output: 'green'
