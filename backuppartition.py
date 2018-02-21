from __future__ import division
from subprocess import call, Popen, PIPE
import argparse
import sys
import os
import threading
from queue import Queue
import time

_nullBlock = '\0'

def isFileAllZeros(path, blockSize):
    global _nullBlock
    
    # Quick optimization so that we don't have to recreate _nullBlock more than necessary
    if len(_nullBlock) != blockSize:
        _nullBlock = '\0' * blockSize
    
    with open(path, 'rb') as f:
        while True:
            block = f.read(blockSize)
            
            if len(block) == 0:
                break
            
            if len(block) == blockSize and block != _nullBlock:
                return False
            elif block != ('\0' * len(block)):
                return False
    
    return True

def humanReadableSize(bytes):
    if bytes < 1024:
        return '%db' % bytes
    elif bytes < (1024*1024):
        return '%.1fK' % (bytes / 1024)
    elif bytes < (1024*1024*1024):
        return '%.1fM' % (bytes / (1024*1024))
    else:
        return '%.1fG' % (bytes / (1024*1024*1024))

def outputStatus(str, lastSize):
    if len(str) < lastSize:
        str = str + (' ' * (lastSize-len(str)))
    
    sys.stdout.write(str + '\r')
    sys.stdout.flush()
    
    return len(str)

class CopyThread(threading.Thread):
    def __init__(self, source, dest, partSize, blockSize, queue):
        super(CopyThread, self).__init__()
        self.source = source
        self.dest = dest
        self.partSize = partSize
        self.blockSize = blockSize
        self.queue = queue
    
    def run(self):
        timingSamples = 5
        timings = []
        averageSpeed = None
        
        partBlockCount = self.partSize // self.blockSize
        index = 0
        lastStatusSize = 0
        
        while True:
            startTime = time.time()
            
            partPath = os.path.join(self.dest, 'part_%08d' % index)
            
            if averageSpeed is not None:
                lastSize = outputStatus("Copying part index %s to: %s ... speed: %s/sec" %
                                        (index, partPath, humanReadableSize(averageSpeed)), lastStatusSize)
            else:
                lastSize = outputStatus("Copying part index %s to: %s ..." % (index, partPath), lastStatusSize)
            
            sys.stdout.flush()
            
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
            
            endTime = time.time()
            timings.append(endTime-startTime)
            
            if len(timings) >= timingSamples:
                timings = timings[-timingSamples:] 
                averageSpeed = (self.partSize * timingSamples) / sum(timings)
            
            if index > 50:
                break
        
        sys.stdout.write("\n")
        self.queue.put('')

class CompressThread(threading.Thread):
    def __init__(self, partSize, blockSize, queue):
        super(CompressThread, self).__init__()
        self.queue = queue
        self.partSize = partSize
        self.blockSize = blockSize
    
    def run(self):
        while True:
            partPath = self.queue.get()
            
            if len(partPath) == 0:
                break
            
            # sys.stdout.write('Checking if part is all zeros: %s\n' % partPath)
            
            stats = os.stat(partPath)
            
            # Only want to consider files that are of size partSize
            if stats.st_size != self.partSize:
                continue
            
            if isFileAllZeros(partPath, self.blockSize):
                # sys.stdout.write('... %s is null!\n' % partPath)
                
                with open(partPath, 'wb') as f:
                    pass
        

def backup(source, dest, partSize, blockSize):
    if partSize % blockSize != 0:
        raise ValueError('Part size must be integer multiple of block size')
    
    queue = Queue()
    copyThread = CopyThread(source, dest, partSize, blockSize, queue)
    compressThread = CompressThread(partSize, blockSize, queue)
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