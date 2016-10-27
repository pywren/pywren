import numpy as np
import time

class RandomDataGenerator(object):
    """
    A file-like object which generates random data.
    1. Never actually keeps all the data in memory so
    can be used to generate huge files. 
    2. Actually generates random data to eliminate
    false metrics based on compression. 

    It does this by generating data in 1MB blocks
    from np.random where each block is seeded with
    the block number. 
    """

    def __init__(self, bytes_total):
        self.bytes_total = bytes_total
        self.pos = 0
        self.current_block_id = None
        self.current_block_data = ""
        self.BLOCK_SIZE_BYTES = 1024*1024

        self.block_random = np.random.randint(0, 256, dtype=np.uint8, 
                                              size=self.BLOCK_SIZE_BYTES)

    def tell(self):
        print "tell", self.pos
        return self.pos

    def seek(self, pos, whence=0):
        print "seek","pos=", pos, "whence=", whence
        if whence == 0:
            self.pos = pos
        elif whence == 1:
            self.pos += pos
        elif whence == 2:
            self.pos = self.bytes_total - pos

    def get_block(self, block_id):
        if block_id == self.current_block_id:
            return self.current_block_data

        self.current_block_id = block_id
        self.current_block_data = (block_id + self.block_random).tostring()
        return self.current_block_data
    
    def get_block_coords(self, abs_pos):
        block_id = abs_pos // self.BLOCK_SIZE_BYTES
        within_block_pos = abs_pos - block_id * self.BLOCK_SIZE_BYTES
        return block_id, within_block_pos
    

    def read(self, bytes_requested):
        remaining_bytes = self.bytes_total - self.pos
        if remaining_bytes == 0:
            return ""
        
        bytes_out = min(remaining_bytes, bytes_requested)
        start_pos = self.pos

        byte_data = ""
        byte_pos = 0
        while byte_pos < bytes_out:
            abs_pos = start_pos + byte_pos
            bytes_remaining = bytes_out - byte_pos
            
            block_id, within_block_pos = self.get_block_coords(abs_pos)
            block = self.get_block(block_id)
            # how many bytes can we copy? 
            chunk = block[within_block_pos:within_block_pos + bytes_remaining]

            
            byte_data += chunk

            byte_pos += len(chunk)

        self.pos += bytes_out

        return byte_data


if __name__ == "__main__":
    """
    basic benchmark of data generation
    """
    rdg = RandomDataGenerator(1024*1024*1024)
    read_size = 8192
    read_count = 10000000
    bytes_read = 0
    t1 = time.time()
    for r in range(read_count):
        a = rdg.read(read_size)
        bytes_read += len(a)
    t2 = time.time()

    print t2-t1
    print bytes_read / (t2-t1)/1e6, "MB/sec"
