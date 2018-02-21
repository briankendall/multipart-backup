from __future__ import division
from subprocess import call, Popen, PIPE
import argparse
import sys
import os
import threading
from queue import Queue
import time
import ctypes

_nullBlock = '\0'

class DDError(Exception):
    pass

def isFileAllZeros(path, blockSize):
    global _nullBlock
    
    # Quick optimization so that we don't have to recreate _nullBlock more than necessary
    if len(_nullBlock) != blockSize:
        _nullBlock = '\0' * blockSize
    
    result = False
    
    with open(path, 'rb') as f:
        while True:
            block = f.read(blockSize)
            
            if len(block) == 0:
                break
            
            result = True
            
            if len(block) == blockSize and block != _nullBlock:
                return False
            elif block != ('\0' * len(block)):
                return False
    
    return result

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

def humanReadableSizeToBytes(value):
    validSuffixes = {'b':512, 'k':1024, 'm':1048576, 'g':1073741824, 'w':ctypes.sizeof(ctypes.c_int)}
    value = value.lower().strip()
    
    if value[-1] in validSuffixes:
        numberPart = value[:-1]
        suffix = value[-1]
    else:
        numberPart = value
        suffix = None
    
    if numberPart.startswith('0x'):
        number = int(numberPart, 16)
    elif numberPart.startswith('0'):
        number = int(numberPart, 8)
    else:
        number = int(numberPart, 10)
    
    if suffix is None:
        return number
    else:
        return number * validSuffixes[suffix]

def partPathAtIndex(dest, index):
    return os.path.join(dest, 'part_%08d' % index)
    
def newPartPathAtIndex(dest, index):
    return os.path.join(dest, 'part_%08d.new' % index)

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
        self.totalParts = 0
    
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
                
                partPath = newPartPathAtIndex(self.dest, index)
                
                if averageSpeed is not None:
                    lastStatusSize = outputStatus("Copying part %s ... speed: %s/sec" % (index+1, humanReadableSize(averageSpeed)), lastStatusSize)
                else:
                    lastStatusSize = outputStatus("Copying part %s ..." % (index+1), lastStatusSize)
                
                p = Popen(['dd', 'if=%s' % self.source, 'of=%s' % partPath, 'bs=%s' % self.blockSize, 'count=%s' % partBlockCount,
                               'skip=%s' % (index*partBlockCount)], stdout=PIPE, stderr=PIPE)
                out, err = p.communicate()
                
                if p.returncode != 0:
                    sys.stderr.write('dd failed! Output:\n%s\n' % err)
                    raise DDError('dd failed on index %s with status %s' % (index, p.returncode))
                
                partSize = os.stat(partPath).st_size
                
                if partSize == 0:
                    os.remove(partPath)
                    break
                
                self.totalParts += 1
                self.queue.put(partPath)
                
                if partSize != self.partSize:
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
    def __init__(self, partSize, blockSize, keepNullParts, queue):
        super(CompareThread, self).__init__()
        self.queue = queue
        self.partSize = partSize
        self.blockSize = blockSize
        self.keepNullParts = keepNullParts
        self.changedFiles = 0
    
    def areOldAndNewPartsIdentical(self, prevPartPath, newPartPath, newPartIsAllZeros):
        newPartSize = os.stat(newPartPath).st_size
        prevPartSize = os.stat(prevPartPath).st_size
        
        if not self.keepNullParts and prevPartSize == 0 and newPartIsAllZeros:
            return True
        else:
            result = areFilesIdentical(prevPartPath, newPartPath, self.blockSize)
            return result
    
    def run(self):
        while True:
            newPartPath = self.queue.get()
            
            if len(newPartPath) == 0:
                break
            
            newPartIsAllZeros = isFileAllZeros(newPartPath, self.blockSize)
            prevPartPath = os.path.splitext(newPartPath)[0]
            
            if os.path.exists(prevPartPath):
                if self.areOldAndNewPartsIdentical(prevPartPath, newPartPath, newPartIsAllZeros):
                    os.remove(newPartPath)
                    continue
                else:
                    os.remove(prevPartPath)
            
            os.rename(newPartPath, prevPartPath)
            self.changedFiles += 1
            
            # Only want to consider files that are of size partSize
            if os.stat(prevPartPath).st_size != self.partSize:
                continue
            
            if not self.keepNullParts and newPartIsAllZeros:
                # Blank out file, signaling that its size is blockSize and it is all zeros
                with open(prevPartPath, 'wb') as f:
                    pass

def removeExcessPartsInDestStartingAtIndex(dest, index):
    deletedFiles = 0
    
    while os.path.exists(partPathAtIndex(dest, index)):
        os.remove(partPathAtIndex(dest, index))
        index += 1
        deletedFiles += 1
    
    return deletedFiles

def backup(source, dest, partSize, blockSize, keepNullParts):
    if partSize % blockSize != 0:
        raise ValueError('Part size must be integer multiple of block size')
    
    queue = Queue()
    copyThread = CopyThread(source, dest, partSize, blockSize, queue)
    compareThread = CompareThread(partSize, blockSize, keepNullParts, queue)
    copyThread.start()
    compareThread.start()
    
    copyThread.join()
    compareThread.join()
    
    deletedFiles = removeExcessPartsInDestStartingAtIndex(dest, copyThread.totalParts)
    
    sys.stdout.write("Finished. Changed files: %s\n" % (compareThread.changedFiles + deletedFiles))


def main():
    parser = argparse.ArgumentParser(description="Iteratively backup file or device to multi-part file")
    parser.add_argument('source', help="Source file or device")
    parser.add_argument('dest', help="Destination folder for multi-part backup")
    parser.add_argument('-bs', '--block-size', help='Block size for dd and comparing files. Uses same format for sizes as dd.',
                        type=str, default=str(1024*1024))
    parser.add_argument('-ps', '--part-size', help='Size of each part of the backup. Uses same format for sizes as dd.',
                        type=str, default=str(100*1024*1024))
    parser.add_argument('-k', '--keep-null-parts', help='Keep parts that contain all zeros at full size', action='store_true')
    args = parser.parse_args()
    
    try:
        partSize = humanReadableSizeToBytes(args.part_size)
        blockSize = humanReadableSizeToBytes(args.block_size)
        backup(args.source, args.dest, partSize, blockSize, args.keep_null_parts)
    except ValueError as e:
        sys.stderr.write('Error: %s\n' % e)
        return 1

if __name__ == "__main__":
    status = main()
    sys.exit(status)