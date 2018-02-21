from __future__ import division
from subprocess import call, PIPE
import argparse
import sys
import os
import threading
from queue import Queue

class CopyThread(threading.Thread):
    def __init__(self, source, dest, partSize, blockSize, queue):
        super(CopyThread, self).__init__()
        self.source = source
        self.dest = dest
        self.partSize = partSize
        self.blockSize = blockSize
        self.queue = queue
    
    def run(self):
        partBlockCount = self.partSize // self.blockSize
        index = 0
        
        while True:
            partPath = os.path.join(self.dest, 'part_%08d' % index)
            sys.stdout.write("Copying part index %s to: %s\n" % (index, partPath))
            
            status = call(['dd', 'if=%s' % self.source, 'of=%s' % partPath, 'bs=%s' % self.blockSize, 'count=%s' % partBlockCount,
                           'skip=%s' % (index*partBlockCount)], stdout=PIPE, stderr=PIPE)
            
            if status != 0:
                #TODO: what do I do?!
                sys.stderr.write('dd failed!\n')
                break
            
            self.queue.put(partPath)
            stats = os.stat(partPath)
            
            if stats.st_size != self.partSize:
                break
            
            index += 1
        
        sys.stdout.write("Done copying!\n")
        self.queue.put('')
    
class CompressThread(threading.Thread):
    def __init__(self, queue):
        super(CompressThread, self).__init__()
        self.queue = queue
    
    def run(self):
        while True:
            partPath = self.queue.get()
            
            if len(partPath) == 0:
                break
            
            sys.stdout.write('Processing: %s\n' % partPath)
            call(['gzip', '-k', partPath])
            
        
        sys.stdout.write("Done processing!\n")
        

def backup(source, dest, partSize, blockSize):
    if partSize % blockSize != 0:
        raise ValueError('Part size must be integer multiple of block size')
    
    queue = Queue()
    copyThread = CopyThread(source, dest, partSize, blockSize, queue)
    compressThread = CompressThread(queue)
    copyThread.start()
    compressThread.start()
    
    copyThread.join()
    compressThread.join()
    sys.stdout.write("All done\n")


def main():
    parser = argparse.ArgumentParser(description="Iteratively backup file or device to multi-part file")
    parser.add_argument('source', help="Source file or device")
    parser.add_argument('dest', help="Destination folder for multi-part backup")
    args = parser.parse_args()

    partSize = 100 * 1024 * 1024
    blockSize = 1024 * 1024
    
    try:
        backup(args.source, args.dest, partSize, blockSize)
    except ValueError as e:
        sys.stderr.write('Error: %s\n' % e)
        return 1

if __name__ == "__main__":
    status = main()
    sys.exit(status)