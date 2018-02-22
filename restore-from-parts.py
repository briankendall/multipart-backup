#!/usr/bin/env python2.7
from __future__ import division
import argparse
from subprocess import call, Popen, PIPE
import os
import sys
from shared import (BackupDataError, DDError, AverageSpeedCalculator, outputStatus, humanReadableSize,
                    humanReadableSizeToBytes, partsInSnapshot)

def checkPartsAndGetPartSize(backupPath, parts, blockSize):
    """Checks to make sure all the parts in the given backup are a consistent size, and returns that size."""
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
                raise BackupDataError('Parts in backup have a size that is not an integer multiple of the block size. '
                                      'Please specify a compatible block size.')
    
    return backupPartSize

def restore(backupPath, dest, blockSize):
    parts = partsInSnapshot(backupPath)
    backupPartSize = checkPartsAndGetPartSize(backupPath, parts, blockSize)
    
    if backupPartSize is None:
        raise BackupDataError('Could not deduce part size... are all of your parts 0 bytes in size?')
    
    partBlockCount = backupPartSize // blockSize
    speedCalculator = AverageSpeedCalculator(5)
    
    for i in xrange(len(parts)):
        speedCalculator.startOfCycle()
        
        partPath = os.path.join(backupPath, parts[i])
        partSize = os.stat(partPath).st_size
        
        if speedCalculator.averageSpeed() is not None:
            outputStatus("Restoring part %s ... speed: %s/sec" %
                         (i+1, humanReadableSize(speedCalculator.averageSpeed())))
        else:
            outputStatus("Restoring part %s ..." % (i+1))
        
        if partSize == 0:
            # If the file size is 0, that indicates that it was a full size part that contained only zeros, so we
            # can pull data from /dev/zero for this part.
            partPathToUse = '/dev/zero'
        else:
            partPathToUse = partPath
        
        p = Popen(['dd', 'if=%s' % partPathToUse, 'of=%s' % dest, 'bs=%s' % blockSize, 'count=%s' % partBlockCount,
                  'oseek=%s' % (i*partBlockCount)], stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
                
        if p.returncode != 0:
            sys.stderr.write('dd failed! Output:\n%s\n' % err)
            raise DDError('dd failed on index %s with status %s' % (i, p.returncode))
        
        speedCalculator.endOfCycle(partSize)
    
    sys.stdout.write("\nRestore completed\n")

def main():
    parser = argparse.ArgumentParser(description="Iteratively backup file or device to multi-part file")
    parser.add_argument('backup', help="Folder containing multi-part backup")
    parser.add_argument('dest', help="Destination file or device")
    parser.add_argument('-bs', '--block-size', help='Block size for dd and comparing files. Uses same format for sizes '
                        'as dd. Defaults to 1MB.', type=str, default=str(1024*1024))
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
