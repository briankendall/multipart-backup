from __future__ import division
import sys
import ctypes
import os
import time
from subprocess import check_output
import platform
import uuid

_outputStatusLastSize = 0

class BackupDataError(Exception):
    pass

class DDError(Exception):
    pass
    
class BackupError(Exception):
    pass
    
class UnimplementedPlatformError(Exception):
    pass

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

def outputStatus(str):
    """Prints a line to the console that overwrites the previous line, allowing for status updates."""
    global _outputStatusLastSize
    
    if len(str) < _outputStatusLastSize:
        str = str + (' ' * (_outputStatusLastSize-len(str)))
    
    sys.stdout.write(str + '\r')
    sys.stdout.flush()
    _outputStatusLastSize = len(str)

def humanReadableSize(bytes):
    """Returns a nicer human readable representation of the given size in bytes"""
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

def isPartFile(filename):
    return len(filename) == 13 and filename.startswith('part_') and filename[-8:].isdigit()

def partsInSnapshot(dest):
    return sorted(filter(isPartFile, os.listdir(dest)))

def normalizeUUID(uuidString):
    return str(uuid.UUID(uuidString)).lower()

def findDiskDeviceIdentifierByUUIDMacOS(uuidString):
    import plistlib
    
    diskUtilPlistData = check_output(['diskutil', 'list', '-plist'])
    diskUtilData = plistlib.readPlistFromString(diskUtilPlistData)
    allDisksAndPartitions = diskUtilData['AllDisksAndPartitions']
    
    def findDiskUUIDInList(partitionList, targetUUIDString):
        for partition in partitionList:
            if partition['DiskUUID'].lower() == targetUUIDString:
                # Want to provide the unbuffered device identifier for better performance, hence the r
                return '/dev/r' + partition['DeviceIdentifier']
        
        return None
    
    for data in allDisksAndPartitions:
        if 'Partitions' in data:
            result = findDiskUUIDInList(data['Partitions'], uuidString)
            
            if result is not None:
                return result
                
        if 'APFSVolumes' in data:
            result = findDiskUUIDInList(data['APFSVolumes'], uuidString)
            
            if result is not None:
                return result
    
    return None

def findDiskDeviceIdentifierByUUID(uuidString):
    uuidString = normalizeUUID(uuidString)
    
    if platform.system() == 'Darwin':
        return findDiskDeviceIdentifierByUUIDMacOS(uuidString)
    else:
        raise UnimplementedPlatformError('Finding a device by UUID is not implemented for platform: %s' % platform.system())

def isUUID(uuidString):
    try:
        uuid.UUID(uuidString)
        return True
    except ValueError:
        return False
