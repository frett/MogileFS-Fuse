package MogileFS::Fuse;

use strict;
use utf8;
use base qw{Exporter};
use threads;
use threads::shared;

#log levels
use constant ERROR => 0;
use constant DEBUG => 1;

#list of functions available for export
our @EXPORT_OK = qw{
	mountMogileFS
};

#flag that will control log verbosity
our $VERBOSITY :shared = ERROR;

use Fuse 0.09_4;
use MogileFS::Client::FilePaths;
use Params::Validate qw{validate ARRAYREF SCALAR};
use POSIX qw{
	EEXIST
	EIO
	ENOENT
	EOPNOTSUPP
};

##Private static variables

#MogileFS configuration
my %config :shared;

#state variables
my $mounted :shared;
my $instance :shared = 0;

#objects used for Fuse binding
my $mogc;

#file objects
my %files :shared;
my $nextfile :shared = 1;

#Function to mount the specified MogileFS domain to the filesystem
#	class      => the class to store files as in MogileFS
#	domain     => the domain to use in MogileFS
#	mountpoint => where to mount the filesystem
#	trackers   => the addresses for the MogileFS trackers
sub mount(%) {
	my %opt = validate(@_, {
		'class'      => {'type' => SCALAR, 'default' => undef},
		'domain'     => {'type' => SCALAR},
		'mountpoint' => {'type' => SCALAR},
		'trackers'   => {'type' => ARRAYREF},
	});

	#short-circuit if a MogileFS file system was already mounted
	{
		lock($mounted);
		return if($mounted);
		$mounted = 1;
	}

	#process the MogileFS config
	$config{'mountpoint'} = $opt{'mountpoint'};
	$config{'class'} = $opt{'class'};
	$config{'domain'} = $opt{'domain'};
	$config{'trackers'} = shared_clone([]);
	push @{$config{'trackers'}}, @{$opt{'trackers'}};

	#increment the instance id of this mount
	{
		lock($instance);
		$instance++;
	}

	#mount the MogileFS file system
	Fuse::main(
		'mountpoint' => $config{'mountpoint'},
		'threaded' => 1,

		#callback functions
		'getattr'     => __PACKAGE__ . '::e_getattr',
		'getdir'      => __PACKAGE__ . '::e_getdir',
		'getxattr'    => __PACKAGE__ . '::e_getxattr',
		'link'        => __PACKAGE__ . '::e_link',
		'listxattr'   => __PACKAGE__ . '::e_listxattr',
		'mknod'       => __PACKAGE__ . '::e_mknod',
		'open'        => __PACKAGE__ . '::e_open',
		'readlink'    => __PACKAGE__ . '::e_readlink',
		'rename'      => __PACKAGE__ . '::e_rename',
		'symlink'     => __PACKAGE__ . '::e_symlink',
		'unlink'      => __PACKAGE__ . '::e_unlink',
	);

	#reset static variables
	%config = ();
	$mounted = 0;

	#return
	return;
}
*mountMogileFS = *mount;

##Support Functions

#function that will return a MogileFS client for the current config
sub MogileFS() {
	if(ref($mogc) ne 'HASH' || $mogc->{'version'} != $instance) {
		$mogc = {
			'client'  => MogileFS::Client::FilePaths->new(
				'hosts'  => [@{$config{'trackers'}}],
				'domain' => $config{'domain'},
			),
			'version' => $instance,
		};
	}

	return $mogc->{'client'};
}

#fetch meta-data about the specified file
sub get_file_info($) {
	my ($path) = @_;

	#short-circuit if this is the root directory
	return {
		'name' => '/',
		'is_directory' => 1,
	} if($path eq '/');

	#process the specified path
	$path =~ m!^(.*/)([^/]+)$!;
	my ($dir, $file) = ($1, $2);

	#look up meta-data for the directory containing the specified file
	#TODO: maybe cache this lookup
	my $finfo = eval {
		my $mogc = MogileFS();
		my @files = $mogc->list($dir);
		foreach(@files) {
			return $_ if($_->{'name'} eq $file);
		}
		return undef;
	};

	#return the found file info
	return $finfo;
}

#function that will output a log message
sub logmsg($$) {
	my ($level, $msg) = @_;
	return if($level > $VERBOSITY);

	print STDERR $msg, "\n";
}

sub sanitize_path($) {
	my ($path) = @_;

	# Make sure we start everything from '/'
	$path = '/' unless(length($path));
	$path = '/' if($path eq '.');
	$path = '/' . $path unless($path =~ m!^/!so);

	return $path;
}

##Callback Functions

sub e_getattr($) {
	my ($path) = @_;
	$path = sanitize_path($path);
	logmsg(DEBUG, "e_getattr: $path");

	# short-circuit if the file doesn't exist
	my $finfo = get_file_info($path);
	return -ENOENT() if(!defined $finfo);

	# Cook some permissions since we don't store this information in mogile
	#TODO: how should we set file/dir permissions?
	my $modes =
		$finfo->{'is_directory'} ? (0040 << 9) + 0777 :
		(0100 << 9) + 0666;
	my $size = $finfo->{'size'} || 0;

	#set some generic attributes
	#TODO: set more sane values for file attributes
	my ($dev, $ino, $rdev, $blocks, $gid, $uid, $nlink, $blksize) = (0,0,0,1,0,0,1,1024);
	my ($atime, $ctime, $mtime);
	$atime = $ctime = $mtime = $finfo->{'mtime'} || time;

	#return the attribute values
	return (
		$dev,
		$ino,
		$modes,
		$nlink,
		$uid,
		$gid,
		$rdev,
		$size,
		$atime,
		$mtime,
		$ctime,
		$blksize,
		$blocks,
	);
}

sub e_getdir($) {
	my ($path) = @_;
	$path = sanitize_path($path);
	logmsg(DEBUG, "e_getdir: $path");

	#fetch all the files in the specified directory
	my @files = eval {
		my $mogc = MogileFS();
		return $mogc->list($path);
	};

	#return this directory listing
	return ('.', '..', map {$_->{'name'}} @files), 0;
}

sub e_getxattr($$) {
	logmsg(DEBUG, "e_getxattr: $_[0]: $_[1]");
	return -EOPNOTSUPP();
}

sub e_link($$) {
	logmsg(DEBUG, "e_link: $_[0] $_[1]");
	return -EOPNOTSUPP();
}

sub e_listxattr($) {
	logmsg(DEBUG, "e_listxattr: $_[0]");
	return -EOPNOTSUPP();
}

sub e_mknod($) {
	my ($path) = @_;
	$path = sanitize_path($path);
	logmsg(DEBUG, "e_mknod: $path");

	#attempt creating an empty file
	my $mogc = MogileFS();
	my ($errcode, $errstr) = (-1, '');
	my $response = eval {$mogc->new_file($path, $config{'class'})->close};
	if($@ || !$response) {
		#set the error code and string if we have a MogileFS::Client object
		if($mogc) {
			$errcode = $mogc->errcode || -1;
			$errstr = $mogc->errstr || '';
		}
		logmsg(ERROR, "Error creating file: $errcode: $errstr");
		$! = $errstr;
		$? = $errcode;
		return -EIO();
	}

	#return success
	return 0;
}

sub e_open($$) {
	my ($path, $flags) = @_;
	$path = sanitize_path($path);
	logmsg(DEBUG, "e_open: $path, $flags");

	#create a new file handle
	my $file = shared_clone({});

	#store the new file in the opened files hash
	{
		lock($nextfile);
		$files{$nextfile} = $file;
		$nextfile++;
	}

	#return success
	return 0;
}

sub e_readlink($) {
	logmsg(DEBUG, "e_readlink: $_[0]");
	return 0;
}

sub e_rename {
	my ($old, $new) = @_;
	$old = sanitize_path($old);
	$new = sanitize_path($new);
	logmsg(DEBUG, "e_rename: $old -> $new");

	#attempt renaming the specified file
	my $mogc = MogileFS();
	my ($errcode, $errstr) = (-1, '');
	my $response = eval {$mogc->rename($old, $new)};
	if($@) {
		#set the error code and string if we have a MogileFS::Client object
		if($mogc) {
			$errcode = $mogc->errcode || -1;
			$errstr = $mogc->errstr || '';
		}
		logmsg(ERROR, "Error renaming file: $errcode: $errstr");
		$! = $errstr;
		$? = $errcode;
		return -EIO();
	}
	return -EEXIST() if(!$response);

	#return success
	return 0;
}

sub e_symlink($$) {
	logmsg(DEBUG, "e_symlink: $_[0] $_[1]");
	return -EOPNOTSUPP();
}

sub e_unlink($) {
	my ($path) = @_;
	$path = sanitize_path($path);
	logmsg(DEBUG, "e_unlink: $path");

	#attempt deleting the specified file
	my $mogc = MogileFS();
	my ($errcode, $errstr) = (-1, '');
	eval {$mogc->delete($path)};
	if($@) {
		#set the error code and string if we have a MogileFS::Client object
		if($mogc) {
			$errcode = $mogc->errcode || -1;
			$errstr = $mogc->errstr || '';
		}
		logmsg(ERROR, "Error unlinking file: $errcode: $errstr");
		$! = $errstr;
		$? = $errcode;
		return -EIO();
	}

	#return success
	return 0;
}

1;
