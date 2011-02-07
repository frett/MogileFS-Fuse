package MogileFS::Fuse;

use strict;
use utf8;
use threads;
use threads::shared;

#log levels
use constant ERROR => 0;
use constant DEBUG => 1;

#flag that will control log verbosity
our $VERBOSITY :shared = ERROR;

use Fuse 0.09_4;
use MogileFS::Client;
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

#variables to track unshared instance objects
my $instance :shared = 1;
my %unshared;

#objects used for Fuse binding
my $mogc;

#file objects
my %files :shared;
my $nextfile :shared = 1;

##Static Methods

#constructor
#	class      => the class to store files as in MogileFS
#	domain     => the domain to use in MogileFS
#	mountpoint => where to mount the filesystem
#	trackers   => the addresses for the MogileFS trackers
sub new {
	#create the new MogileFS::Fuse object
	my $self = shift;
	$self = shared_clone(bless({}, ref($self) || $self));

	#initialize and return the new object
	return $self->_init(@_);
}

##Instance Methods

#method that will initialize the MogileFS::Fuse object
sub _init {
	my $self = shift;
	my %opt = validate(@_, {
		'class'      => {'type' => SCALAR, 'default' => undef},
		'domain'     => {'type' => SCALAR},
		'mountpoint' => {'type' => SCALAR},
		'trackers'   => {'type' => ARRAYREF},
	});

	#die horribly if we are trying to reinit an existing object
	die 'You are trying to reinitialize an existing MogileFS::Fuse object, this could introduce race conditions and is unsupported' if($self->{'id'});

	#set the instance id
	{
		lock($instance);
		$self->{'id'} = $instance;
		$instance++;
	}

	#process the MogileFS config
	$self->{'config'} = shared_clone({
		'mountpoint' => $opt{'mountpoint'},
		'class'  => $opt{'class'},
		'domain' => $opt{'domain'},
		'trackers' => $opt{'trackers'},
	});

	#return the initialized object
	return $self;
}

#method that will access unshared object elements
sub _localElem {
	my $self = ($unshared{shift->id} ||= {});
	my $elem = shift;
	my $old = $self->{$elem};
	$self->{$elem} = $_[0] if(@_);
	return $old;
}

#method that will return a MogileFS object
sub client {
	my $client = $_[0]->_localElem('client');

	#create and store a new client if one doesn't exist already
	if(!defined $client) {
		$client = MogileFS::Client->new(
			'hosts'  => [@{$config{'trackers'}}],
			'domain' => $config{'domain'},
		);
		$_[0]->_localElem('client', $client);
	}

	#return the MogileFS client
	return $client;
}

#return the instance id for this object
sub id {
	return $_[0]->{'id'};
}

#Method to mount the specified MogileFS domain to the filesystem
sub mount {
	my $self = shift;

	#short-circuit if this MogileFS file system is currently mounted
	{
		lock($self);
		return if($self->{'mounted'});
		$self->{'mounted'} = 1;
	}

	#mount the MogileFS file system
	Fuse::main(
		'mountpoint' => $self->{'mountpoint'},
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
		'removexattr' => __PACKAGE__ . '::e_removexattr',
		'rename'      => __PACKAGE__ . '::e_rename',
		'setxattr'    => __PACKAGE__ . '::e_setxattr',
		'statfs'      => __PACKAGE__ . '::e_statfs',
		'symlink'     => __PACKAGE__ . '::e_symlink',
		'unlink'      => __PACKAGE__ . '::e_unlink',
	);

	#reset mounted state
	{
		lock($self);
		$self->{'mounted'} = 0;
	}

	#return
	return;
}

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

sub e_removexattr($$) {
	logmsg(DEBUG, "e_removexattr: $_[0]: $_[1]");
	return -EOPNOTSUPP();
}

sub e_rename {
	my ($old, $new) = @_;
	$old = sanitize_path($old);
	$new = sanitize_path($new);
	logmsg(DEBUG, "e_rename: $old -> $new");

	#throw an error if the new file already exists
	return -EEXIST() if(defined get_file_info($new));

	#attempt renaming the specified file
	my $mogc = MogileFS();
	my ($errcode, $errstr) = (-1, '');
	my $response = eval {$mogc->rename($old, $new)};
	if($@ || !$response) {
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

	#return success
	return 0;
}

sub e_setxattr($$$) {
	logmsg(DEBUG, "e_setxattr: $_[0]: $_[1] => $_[2]");
	return -EOPNOTSUPP();
}

sub e_statfs() {
	logmsg(DEBUG, "e_statfs");
	return 255, 1, 1, 1, 1, 1024;
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
