from __future__ import division
import argparse
from subprocess import call, Popen, PIPE
import os
import ctypes
import sys
import time

class BackupDataError(Exception):
    pass

class DDError(Exception):
    pass

def outputStatus(str, lastSize):
    if len(str) < lastSize:
        str = str + (' ' * (lastSize-len(str)))
    
    sys.stdout.write(str + '\r')
    sys.stdout.flush()
    
    return len(str)

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

def isPart(filename):
    return len(filename) == 13 and filename.startswith('part_') and filename[-8:].isdigit()

def checkPartsAndGetPartSize(backupPath, parts, blockSize):
    backupPartSize = None
    
    for i in xrange(len(parts)-1):
        part = parts[i]
        partPath = os.path.join(backupPath, part)
        partSize = os.stat(partPath).st_size
        
        if partSize == 0:
            continue
        
        if backupPartSize is None:
            backupPartSize = partSize
        else:
            if partSize != backupPartSize:
                raise BackupDataError('Parts in backup have inconsistent sizes. Backup may be corrupted!')
        
            if partSize % blockSize != 0:
                print partSize, blockSize
                raise BackupDataError('Parts in backup have a size that is not an integer multiple of the block size. Please '
                                      'specify a compatible block size.')
    
    return backupPartSize

def restore(backupPath, dest, blockSize):
    parts = sorted(filter(isPart, os.listdir(backupPath)))
    backupPartSize = checkPartsAndGetPartSize(backupPath, parts, blockSize)
    
    if backupPartSize is None:
        raise BackupDataError('Could not deduce part size... are all of your parts 0 bytes in size?')
    
    partBlockCount = backupPartSize // blockSize
    
    lastStatusSize = 0
    timingSamples = 5
    timings = []
    averageSpeed = None
    
    for i in xrange(len(parts)):
        startTime = time.time()
        
        partPath = os.path.join(backupPath, parts[i])
        partSize = os.stat(partPath).st_size
        
        if averageSpeed is not None:
            lastStatusSize = outputStatus("Restoring part %s ... speed: %s/sec" % (i+1, humanReadableSize(averageSpeed)), lastStatusSize)
        else:
            lastStatusSize = outputStatus("Restoring part %s ..." % (i+1), lastStatusSize)
        
        if partSize == 0:
            partPathToUse = '/dev/zero'
        else:
            partPathToUse = partPath
        
        p = Popen(['dd', 'if=%s' % partPathToUse, 'of=%s' % dest, 'bs=%s' % blockSize, 'count=%s' % partBlockCount,
                  'oseek=%s' % (i*partBlockCount)], stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
                
        if p.returncode != 0:
            sys.stderr.write('dd failed! Output:\n%s\n' % err)
            raise DDError('dd failed on index %s with status %s' % (index, p.returncode))
        
        endTime = time.time()
        timings.append(endTime-startTime)
        
        if len(timings) >= timingSamples:
            timings = timings[-timingSamples:] 
            averageSpeed = (partSize * timingSamples) / sum(timings)
    
    sys.stdout.write("\nRestore completed\n")
        

def main():
    parser = argparse.ArgumentParser(description="Iteratively backup file or device to multi-part file")
    parser.add_argument('backup', help="Folder containing multi-part backup")
    parser.add_argument('dest', help="Destination file or device")
    parser.add_argument('-bs', '--block-size', help='Block size for dd and comparing files. Uses same format for sizes as dd.',
                        type=str, default=str(1024*1024))
    args = parser.parse_args()
    
    try:
        blockSize = humanReadableSizeToBytes(args.block_size)
        restore(args.backup, args.dest, blockSize)
        return 0
    except (DDError, BackupDataError) as e:
        sys.stderr.write('Error: %s\n' % e.message)
        return 1

if __name__ == "__main__":
    status = main()
    sys.exit(status)
