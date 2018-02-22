from __future__ import division
from subprocess import call, Popen, PIPE
import argparse
import sys
import os
import threading
from queue import Queue
import time
import ctypes
import datetime
import re

_nullBlock = '\0'
_outputStatusLastSize = 0

class DDError(Exception):
    pass

class BackupError(Exception):
    pass

def isFileAllZeros(path, blockSize):
    """Returns true if the file at path contains no data other than 0. Will
    check data in increments of blockSize."""
    global _nullBlock
    
    # Quick optimization so that we don't have to recreate _nullBlock more than
    # necessary
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
    """Returns true if both files contain identical data. Will check data in
    increments of blockSize."""
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
    """Returns a nicer human readable representation of the given size in
    bytes"""
    if bytes < 1024:
        return '%db' % bytes
    elif bytes < (1024*1024):
        return '%.1fK' % (bytes / 1024)
    elif bytes < (1024*1024*1024):
        return '%.1fM' % (bytes / (1024*1024))
    else:
        return '%.1fG' % (bytes / (1024*1024*1024))

def humanReadableSizeToBytes(value):
    """Converts a human readable size value into an exact number of bytes. Uses
    the same format as dd."""
    validSuffixes = {'b':512, 'k':1024, 'm':1048576, 'g':1073741824,
                     'w':ctypes.sizeof(ctypes.c_int)}
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
    """Returns the path of a backup part for the given backup destination and
    index"""
    return os.path.join(dest, 'part_%08d' % index)
    
def newPartPathAtIndex(dest, index):
    """Returns the path of a newly created backup part for the given backup
    destination and index. A new part has not yet been compared to an existing
    part to see if they're identical or if the new part contains all zeros"""
    return os.path.join(dest, 'part_%08d.new' % index)

def isPartFile(filename):
    return (len(filename) == 13 and filename.startswith('part_') and
            filename[-8:].isdigit())

def outputStatus(str):
    """Prints a line to the console that overwrites the previous line, allowing
    for status updates."""
    global _outputStatusLastSize
    
    if len(str) < _outputStatusLastSize:
        str = str + (' ' * (_outputStatusLastSize-len(str)))
    
    sys.stdout.write(str + '\r')
    sys.stdout.flush()
    _outputStatusLastSize = len(str)

class AverageSpeedCalculator(object):
    """Class for calculating average copy speed of several copy operations"""
    def __init__(self, maxSamples):
        self.startTime = None
        self.currentAverageSpeed = None
        self.maxSamples = maxSamples
        self.timingList = []
        self.bytesCopiedList = []
    
    def startOfCycle(self):
        self.startTime = time.time()
    
    def endOfCycle(self, bytesCopied):
        self.timingList.append(time.time()-self.startTime)
        self.bytesCopiedList.append(bytesCopied)
        self.timingList = self.timingList[-self.maxSamples:] 
        self.bytesCopiedList = self.bytesCopiedList[-self.maxSamples:]
        self.currentAverageSpeed = sum(self.bytesCopiedList) / sum(self.timingList)
    
    def averageSpeed(self):
        return self.currentAverageSpeed

class CopyThread(threading.Thread):
    """Thread that copies source into dest in partSize chunks. Each part that
    finishes copying is appended to queue for processing in another thread."""
    
    def __init__(self, source, dest, partSize, blockSize, queue):
        super(CopyThread, self).__init__()
        self.source = source
        self.dest = dest
        self.partSize = partSize
        self.blockSize = blockSize
        self.queue = queue
        self.totalParts = 0
        self.error = False
    
    def run(self):
        try:
            speedCalculator = AverageSpeedCalculator(5)
            partBlockCount = self.partSize // self.blockSize
            index = 0
            
            while True:
                speedCalculator.startOfCycle()
                partPath = newPartPathAtIndex(self.dest, index)
                
                if speedCalculator.averageSpeed() is not None:
                    outputStatus("Copying part %s ... speed: %s/sec" %
                                 (index+1, humanReadableSize(speedCalculator.averageSpeed())))
                else:
                    outputStatus("Copying part %s ..." % (index+1))
                
                p = Popen(['dd', 'if=%s' % self.source, 'of=%s' % partPath,
                           'bs=%s' % self.blockSize, 'count=%s' % partBlockCount,
                           'skip=%s' % (index*partBlockCount)],
                          stdout=PIPE, stderr=PIPE)
                out, err = p.communicate()
                
                if p.returncode != 0:
                    sys.stderr.write('dd failed! Output:\n%s\n' % err)
                    raise DDError('dd failed on index %s with status %s' %
                                  (index, p.returncode))
                
                partSize = os.stat(partPath).st_size
                
                # If the part size is zero, that means we've gone past the
                # end of the file or device we're copying and we need to stop
                if partSize == 0:
                    os.remove(partPath)
                    break
                
                self.totalParts += 1
                self.queue.put(partPath)
                
                # If the size of this part is not equal to the target size,
                # that means we've hit the end of the file or device that
                # we're copying.
                if partSize != self.partSize:
                    break
                
                index += 1
                speedCalculator.endOfCycle(partSize)
            
            sys.stdout.write("\n")
        except:
            self.error = True
            raise
        finally:
            self.queue.put('')

class CompareThread(threading.Thread):
    """Thread that compares freshly completed parts to the previously existing
    parts (if any exist) as well as checking if the part is all zeros"""
    
    def __init__(self, partSize, blockSize, keepNullParts, queue):
        super(CompareThread, self).__init__()
        self.queue = queue
        self.partSize = partSize
        self.blockSize = blockSize
        self.keepNullParts = keepNullParts
        self.changedFiles = 0
        self.error = False
    
    def areOldAndNewPartsIdentical(self, prevPartPath, newPartPath,
                                   newPartIsAllZeros):
        newPartSize = os.stat(newPartPath).st_size
        prevPartSize = os.stat(prevPartPath).st_size
        
        if not self.keepNullParts and prevPartSize == 0 and newPartIsAllZeros:
            return True
        else:
            result = areFilesIdentical(prevPartPath, newPartPath, self.blockSize)
            return result
    
    def run(self):
        try:
            while True:
                newPartPath = self.queue.get()
                
                if len(newPartPath) == 0:
                    # Signals that we're done and the thread can exit
                    break
                
                newPartIsAllZeros = isFileAllZeros(newPartPath, self.blockSize)
                prevPartPath = os.path.splitext(newPartPath)[0]
                
                if os.path.exists(prevPartPath):
                    if self.areOldAndNewPartsIdentical(prevPartPath, newPartPath,
                                                       newPartIsAllZeros):
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
                    # Blank out file, signaling that its size is blockSize and it
                    # is all zeros
                    with open(prevPartPath, 'wb') as f:
                        pass
        except:
            self.error = True
            raise

def removeExcessPartsInDestStartingAtIndex(dest, index):
    """Used to remove parts that are no longer needed for the given backup
    destination."""
    deletedFiles = 0
    
    while os.path.exists(partPathAtIndex(dest, index)):
        os.remove(partPathAtIndex(dest, index))
        index += 1
        deletedFiles += 1
    
    return deletedFiles

def snapshotTimestamp():
    return "snapshot-%s" % datetime.datetime.now().strftime("%Y-%m-%d-%H%M%S")

def inProgressSnapshotName():
    return 'snapshot-inprogress'

def isSnapshotDir(dirName):
    return (dirName == inProgressSnapshotName() or
            re.search(r"^snapshot-\d{4}-\d{2}-\d{2}-\d{6}$", dirName) is not None)

def previousSnapshots(destRoot):
    return map(lambda x: os.path.join(destRoot, x),
               sorted(filter(isSnapshotDir,os.listdir(destRoot))))

def findIncompleteSnapshot(snapshots):
    incompletes = filter(lambda x: os.path.basename(x) == inProgressSnapshotName(),
                         snapshots)
    
    if len(incompletes) > 0:
        return incompletes[0]
    else:
        return None

def partsInSnapshot(dest):
    return sorted(filter(isPartFile, os.listdir(dest)))

def createNewSnapshotWithLinksToOld(destRoot, lastSnapshot):
    dest = os.path.join(destRoot, inProgressSnapshotName())
    os.mkdir(dest)
    
    for part in partsInSnapshot(lastSnapshot):
        os.link(os.path.join(lastSnapshot, part), os.path.join(dest, part))
    
    return dest

def setupAndReturnDestination(destRoot, snapshotCount):
    """If snapshotCount > 0, either returns a new snapshot containing hard
    links to the previous snapshot's parts, or returns an existing in-progress
    snapshot. If snapshotCount is 0, then returns destRoot."""
    if snapshotCount > 0:
        prevs = previousSnapshots(destRoot)
        incompleteSnapshot = findIncompleteSnapshot(prevs)
        
        if incompleteSnapshot is not None:
            sys.stdout.write("NOTE: last snapshot is complete! Will attempt to "
                             "finish it...\n")
            dest = incompleteSnapshot
        else:
            sys.stdout.write("Setting up new snapshot...\n")
            dest = createNewSnapshotWithLinksToOld(destRoot, prevs[-1])
    else:
        dest = destRoot
        
    return dest

def removeEmptyDirectoryEvenIfItHasAnAnnoyingDSStoreFileInIt(dir):
    try:
        os.rmdir(dir)
        return
    except OSError:
        if os.path.exists(os.path.join(dir, '.DS_Store')):
            os.remove(os.path.join(dir, '.DS_Store'))
            
            try:
                os.rmdir(dir)
            except OSError:
                pass

def removeOldSnapshots(destRoot, snapshotCount):
    """If the backup at the given root folder contains more snapshots than
    snapshotCount, removes the oldest extra snapshots."""
    prevs = previousSnapshots(destRoot)
    snapshotsToRemove = prevs[:-snapshotCount]
    
    if len(snapshotsToRemove) > 0:
        sys.stdout.write("Removing old snapshots...\n")
        
        for oldSnapshot in snapshotsToRemove:
            for part in partsInSnapshot(oldSnapshot):
                os.remove(os.path.join(oldSnapshot, part))
            
            removeEmptyDirectoryEvenIfItHasAnAnnoyingDSStoreFileInIt(oldSnapshot)

def renameSnapshotToFinalName(dest):
    os.rename(dest, os.path.join(os.path.dirname(dest), snapshotTimestamp()))

def backup(source, destRoot, partSize, blockSize, keepNullParts, snapshotCount):
    if partSize % blockSize != 0:
        raise ValueError('Part size must be integer multiple of block size')
    
    dest = setupAndReturnDestination(destRoot, snapshotCount)
    queue = Queue()
    copyThread = CopyThread(source, dest, partSize, blockSize, queue)
    compareThread = CompareThread(partSize, blockSize, keepNullParts, queue)
    copyThread.start()
    compareThread.start()
    
    copyThread.join()
    compareThread.join()
    
    if copyThread.error or compareThread.error:
        raise BackupError('The backup failed to complete')
    
    deletedFiles = removeExcessPartsInDestStartingAtIndex(dest, copyThread.totalParts)
    renameSnapshotToFinalName(dest)
    
    if snapshotCount > 0:
        removeOldSnapshots(destRoot, snapshotCount)
    
    sys.stdout.write("Finished! Changed files: %s\n" %
                     (compareThread.changedFiles + deletedFiles))

def main():
    parser = argparse.ArgumentParser(description="Iteratively backup file or "
                                     "device to multi-part file")
    parser.add_argument('source', help="Source file or device")
    parser.add_argument('dest', help="Destination folder for multi-part backup")
    parser.add_argument('-bs', '--block-size', help='Block size for dd and '
                        'comparing files. Uses same format for sizes as dd.',
                        type=str, default=str(1024*1024))
    parser.add_argument('-ps', '--part-size', help='Size of each part of the '
                        'backup. Uses same format for sizes as dd.',
                        type=str, default=str(100*1024*1024))
    parser.add_argument('-k', '--keep-null-parts', help='Keep parts that '
                        'contain all zeros at full size', action='store_true')
    parser.add_argument('-s', '--snapshots', type=int, default=4, help='Number '
                        'of snapshots to maintain. Default is 4.') 
    args = parser.parse_args()
    
    try:
        partSize = humanReadableSizeToBytes(args.part_size)
        blockSize = humanReadableSizeToBytes(args.block_size)
        backup(args.source, args.dest, partSize, blockSize,
               args.keep_null_parts, args.snapshots)
        return 0
    except (DDError, ValueError, BackupError) as e:
        sys.stderr.write('Error: %s\n' % e)
        return 1

if __name__ == "__main__":
    status = main()
    sys.exit(status)
