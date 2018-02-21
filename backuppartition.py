from __future__ import division
from subprocess import call, Popen, PIPE
import argparse
import sys
import os
import threading
from queue import Queue
import time

_nullBlock = '\0'

class DDError(Exception):
    pass

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

def areFilesIdentical(path1, path2, blockSize):
    with open(path1, 'rb') as f1:
        with open(path2, 'rb') as f2:
            while True:
                block1 = f1.read(blockSize)
                block2 = f2.read(blockSize)
                
                if block1 != block2:
                    return False
                
                if len(block1) == 0:
                    break
    
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
        try:
            timingSamples = 5
            timings = []
            averageSpeed = None
            
            partBlockCount = self.partSize // self.blockSize
            index = 0
            lastStatusSize = 0
            
            while True:
                startTime = time.time()
                
                partPath = os.path.join(self.dest, 'part_%08d.new' % index)
                
                if averageSpeed is not None:
                    lastSize = outputStatus("Copying part %s ... speed: %s/sec" % (index+1, humanReadableSize(averageSpeed)), lastStatusSize)
                else:
                    lastSize = outputStatus("Copying part %s ..." % (index+1), lastStatusSize)
                
                sys.stdout.flush()
                
                p = Popen(['dd', 'if=%s' % self.source, 'of=%s' % partPath, 'bs=%s' % self.blockSize, 'count=%s' % partBlockCount,
                               'skip=%s' % (index*partBlockCount)], stdout=PIPE, stderr=PIPE)
                out, err = p.communicate()
                
                if p.returncode != 0:
                    sys.stderr.write('dd failed! Output:\n%s\n' % err)
                    raise DDError('dd failed on index %s with status %s' % (index, p.returncode))
                
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
        finally:
            self.queue.put('')

class CompareThread(threading.Thread):
    def __init__(self, partSize, blockSize, queue):
        super(CompareThread, self).__init__()
        self.queue = queue
        self.partSize = partSize
        self.blockSize = blockSize
        self.changedFiles = 0
    
    def areOldAndNewPartsIdentical(self, prevPartPath, newPartPath):
        newPartSize = os.stat(newPartPath).st_size
        prevPartSize = os.stat(prevPartPath).st_size
        
        if prevPartSize == 0 and isFileAllZeros(newPartPath, self.blockSize):
            return True
        else:
            return areFilesIdentical(prevPartPath, newPartPath, self.blockSize)
    
    def run(self):
        while True:
            newPartPath = self.queue.get()
            
            if len(newPartPath) == 0:
                break
            
            prevPartPath = os.path.splitext(newPartPath)[0]
            
            if os.path.exists(prevPartPath):
                if self.areOldAndNewPartsIdentical(prevPartPath, newPartPath):
                    os.remove(newPartPath)
                    continue
                else:
                    os.remove(prevPartPath)
            
            os.rename(newPartPath, prevPartPath)
            self.changedFiles += 1
            
            # Only want to consider files that are of size partSize
            if os.stat(prevPartPath).st_size != self.partSize:
                continue
            
            if isFileAllZeros(prevPartPath, self.blockSize):
                # Blank out file, signaling that its size is blockSize and it is all zeros
                with open(prevPartPath, 'wb') as f:
                    pass

def backup(source, dest, partSize, blockSize):
    if partSize % blockSize != 0:
        raise ValueError('Part size must be integer multiple of block size')
    
    queue = Queue()
    copyThread = CopyThread(source, dest, partSize, blockSize, queue)
    compareThread = CompareThread(partSize, blockSize, queue)
    copyThread.start()
    compareThread.start()
    
    copyThread.join()
    compareThread.join()
    sys.stdout.write("Finished. Changed files: %s\n" % compareThread.changedFiles)


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