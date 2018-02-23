# multipart-backup

A Python script for macOS and (theoretically) Linux that utilizes `dd` to create incremental backups of entire partitions, regardless of their contents or filesystem.

For example, after backing up a 1 GB partition into 100 MB chunks, the folder containing the backup may look like this:

    -rw-r--r--  4 root  staff  104857600 Feb 21 23:15 part_00000000
    -rw-r--r--  4 root  staff  104857600 Feb 21 23:15 part_00000001
    -rw-r--r--  4 root  staff  104857600 Feb 21 23:15 part_00000002
    -rw-r--r--  4 root  staff  104857600 Feb 21 23:15 part_00000004
    -rw-r--r--  4 root  staff  104857600 Feb 21 23:15 part_00000005
    -rw-r--r--  4 root  staff  104857600 Feb 21 23:15 part_00000006
    -rw-r--r--  4 root  staff  104857600 Feb 21 23:15 part_00000007
    -rw-r--r--  4 root  staff  104857600 Feb 21 23:15 part_00000008
    -rw-r--r--  4 root  staff  104857600 Feb 21 23:15 part_00000009
    -rw-r--r--  4 root  staff  104857600 Feb 21 23:15 part_00000010
    -rw-r--r--  4 root  staff   25165824 Feb 21 23:15 part_00000011

Then, when the script is run again and the backup is updated, only the parts containing data that has changed will be updated. The other files will be left as-is.

The script can also optionally be used to create snapshots, where each backup is contained within its own timestamped folder. Each time a new snapshot is made, the script hard links the contents of the previous snapshot folder into the new snapshot folder, and then the multi-part files are updated. (Similar to macOS's Time Machine feature.) That way, multiple snapshots of the partition may be kept around while still utilizing space efficiently.

### Requirements:

- Python 2.7 (Currently untested in Python 3)
- `dd` is installed and in your PATH.

### Usage for backing up:

    backup-to-parts.py [arguments] source backup-root

* source: the file or device to backup, e.g. `/dev/rdisk1s2` or `/dev/sda2`

* backup-root: the path to the folder that will contain the backup

* `-bs SIZE` `--block-size SIZE`    
Block size used with `dd` and when comparing files. Defaults to 1 MB.

* `-ps SIZE` `--part-size SIZE`:    
The size of the parts the source file or device is split into. Defaults to 100 MB.

* `-k` `--keep-null-parts`:    
The default behavior is any part of the backup that contains no data other than null bytes (zero) are represented by 0 bytes files. When this is used, all parts are kept at full size.

* `-s COUNT` `--snapshots COUNT`    
Specifies how many snapshots are kept in the backup root. When set to 1 or higher, the script will create the snapshot folders in the backup root named with a timestamp. When set to 0, no snapshots are made and the backup root just contains all of the parts. The default is 4.

##### Example:

    backup-to-parts.py -ps 50m -bs 1m -c 10 /dev/rdisk4s1 /Volumes/Backups/external-drive-backup/

### Usage for restoring:

    restore-from-parts.py [arguments] snapshot-path destination
    
* snapshot-path: path to a folder containing all of the parts of a backup. When `-s` is non-zero when creating the backup, this is the path to a particular snapshot, otherwise it's the path to the backup root itself.

* destination: the file or device to restore onto, e.g. `/dev/rdisk1s2` or `/dev/sda2`

* `-bs SIZE` `--block-size SIZE`    
Block size used with `dd`. Defaults to 1 MB.

##### Example:

    restore-from-parts -bs 1m /Volumes/Backups/external-drive-backup/snapshot-2018-04-20-001337

### Sizes

Similar to `dd`, where sizes are specified, a decimal, octal, or hexadecimal number of bytes is expected.  If the number ends with a `b`, `k`, `m`, `g`, or `w`, the number is multiplied by 512, 1024 (1K), 1048576 (1M), 1073741824 (1G) or the number of bytes in an integer, respectively.

### Some macOS notes:

It's much faster to specify a disk using `/dev/rdisk` rather than `/dev/disk`. The explanation can be found in the `hdiutil` manpage:

> `/dev/rdisk` nodes are character-special devices, but are "raw" in the BSD sense and force block-aligned I/O. They are closer to the physical disk than the buffer cache. `/dev/disk` nodes, on the other hand, are buffered block-special devices and are used primarily by the kernel's filesystem code.


### Why this exists

I spent a great many hours setting up an external drive for my mac containing several partitions with different operating systems on them. It took a lot of work both to get the systems set up the way I wanted and to get all of their bootloaders set up correctly on the EFI partition. While my mac's main drive is backed up regularly by Time Machine and my choice of online backup service, I didn't have a good plan on how to back up these other systems, especially given that I've encrypted their filesystems.

It's important to me that I be able to restore those systems to a fully working and bootable state should the need arise, so I eventually decided I wanted to dump the contents of their partitions to my backup disk. The trouble was that a huge file that's hundreds of gigabytes in size is inefficient, and also will wreck havoc when processed by my online backup software -- a file that size would take a week to upload with my current internet connection! I could just exclude it from the online backup, but I've learned from experience that having an on-site and off-site backup of my data is *really* important.

So I figured that if I could split that huge backup into multiple parts and then incrementally update it, then the online backup would only upload the parts of that backup that have changed, solving the problem. Furthermore, since a lot of sections of these partitions is still blank, I figured I could avoid backing up that data. But I wasn't aware of any software that exists to do that. And slapping something together in Python that accomplishes this with `dd` didn't seem to daunting. So I went ahead and did it.

Maybe it will be of use to someone else!

### Issues and future work

- Doesn't yet have a way to specify partitions by GUID, but probably will eventually.

- Not sure if it's Python 3 compatible, but I may go out of my way to ensure that in the future.

- There's no progress indicator other than how many parts have been copied

- No option for compression: I experimented early on with compressing each part of a backup using gzip, but it causes it to take a lot longer to perform the backup, and since my partitions are encrypted gzip didn't really save me any disk space anyway. So I settled instead of excluding parts that are totally blank. I may add an option later that allows using gzip.