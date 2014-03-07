package MogileFS::Client::Fuse::Constants;

use strict;
use base qw{Exporter};

our $VERSION = '0.05';

use constant CALLBACKS => qw{
	getattr readlink getdir mknod mkdir unlink rmdir symlink
	rename link chmod chown truncate utime open read write
	statfs flush release fsync setxattr getxattr listxattr
	removexattr opendir readdir releasedir fsyncdir init
	destroy access create ftruncate fgetattr lock utimens
	bmap
};

#log levels
use constant LOG_OFF    => -1;
use constant LOG_NOTICE => 0;
use constant ERROR      => 1;
use constant DEBUG      => 2;
use constant DEBUGMFS   => 3;
use constant DEBUGFUSE  => 4;

#are threads enabled
use constant THREADS => $threads::threads;

# attribute position constants
use constant ATTR_DEV     =>  0; # device number of filesystem
use constant ATTR_INO     =>  1; # inode number
use constant ATTR_MODE    =>  2; # file mode (type and permissions)
use constant ATTR_NLINK   =>  3; # number of (hard) links to the file
use constant ATTR_UID     =>  4; # numeric user ID of file's owner
use constant ATTR_GID     =>  5; # numeric group ID of file's owner
use constant ATTR_RDEV    =>  6; # the device identifier (special files only)
use constant ATTR_SIZE    =>  7; # total size of file, in bytes
use constant ATTR_ATIME   =>  8; # last access time in seconds since the epoch
use constant ATTR_MTIME   =>  9; # last modify time in seconds since the epoch
use constant ATTR_CTIME   => 10; # inode change time (NOT creation time!) in seconds since the epoch
use constant ATTR_BLKSIZE => 11; # preferred block size for file system I/O
use constant ATTR_BLOCKS  => 12; # actual number of blocks allocated

our @EXPORT_OK = qw{
	CALLBACKS
	LOG_OFF
	LOG_NOTICE
	ERROR
	DEBUG
	DEBUGMFS
	DEBUGFUSE
	THREADS

	ATTR_DEV
	ATTR_INO
	ATTR_MODE
	ATTR_NLINK
	ATTR_UID
	ATTR_GID
	ATTR_RDEV
	ATTR_SIZE
	ATTR_ATIME
	ATTR_MTIME
	ATTR_CTIME
	ATTR_BLKSIZE
	ATTR_BLOCKS
};
our %EXPORT_TAGS = (
	ATTRS => [qw{
		ATTR_DEV
		ATTR_INO
		ATTR_MODE
		ATTR_NLINK
		ATTR_UID
		ATTR_GID
		ATTR_RDEV
		ATTR_SIZE
		ATTR_ATIME
		ATTR_MTIME
		ATTR_CTIME
		ATTR_BLKSIZE
		ATTR_BLOCKS
	}],
	LEVELS => [qw{
		LOG_OFF
		LOG_NOTICE
		ERROR
		DEBUG
		DEBUGMFS
		DEBUGFUSE
	}],
);

1;
