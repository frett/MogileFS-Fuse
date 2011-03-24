package MogileFS::Fuse::Constants;

use strict;
use base qw{Exporter};

use constant CALLBACKS => qw{
	getattr readlink getdir mknod mkdir unlink rmdir symlink
	rename link chmod chown truncate utime open read write statfs
	flush release fsync setxattr getxattr listxattr removexattr
};

#log levels
use constant ERROR => 0;
use constant DEBUG => 1;

#are threads enabled
use constant THREADS => $threads::threads;

our @EXPORT_OK = qw{
	CALLBACKS
	ERROR
	DEBUG
	THREADS
};
our %EXPORT_TAGS = (
	LEVELS => [qw{
		ERROR
		DEBUG
	}],
);

1;
