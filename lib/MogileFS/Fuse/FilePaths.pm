package MogileFS::Fuse::FilePaths;

use strict;
use mro 'c3';
use threads::shared;
use base qw{MogileFS::Fuse};

our $VERSION = v0.1.0;

use Errno qw{EEXIST EIO ENOENT};
use MogileFS::Client::FilePaths;
use MogileFS::Fuse::Constants qw{:LEVELS};
use Params::Validate qw{validate_with BOOLEAN SCALAR};

##Instance Methods

#method that will initialize the MogileFS::Fuse::FilePaths object
sub _init {
	my $self = shift;
	my %opt = validate_with(
		'allow_extra' => 1,
		'params' => \@_,
		'spec'   => {
			'filepaths.dircache'          => {'type' => BOOLEAN, 'default' => 1},
			'filepaths.dircache.duration' => {'type' => SCALAR, 'default' => 2},
		},
	);

	#initialize any ancestor classes
	$self = $self->next::method(%opt);

	#initialize this object
	$self->{'dirs'} = shared_clone({});

	#return the initialized object
	return $self;
}

#method that returns a directory listing for the current directory
sub _listDir {
	my $self = shift;
	my ($path) = @_;
	$path .= '/' if($path !~ m!/$!so);
	my $config = $self->_config;

	#short-circuit if the dir cache is disabled
	return $self->MogileFS->list($path) if(!$config->{'filepaths.dircache'});

	#check to see if the specified path is cached
	my $cache = $self->{'dirs'};
	my $dir = $cache->{$path} || {};

	#load the directory listing if the current cached listing is stale
	if($dir->{'expires'} <= time) {
		#fetch and store the files in the dir cache
		$dir = $cache->{$path} = shared_clone({
			'expires' => time + $config->{'filepaths.dircache.duration'},
			'files' => [$self->MogileFS->list($path)],
		});
	}

	#return the files for the current directory
	return @{$dir->{'files'}};
}

#method that flushes the specified dir from the dir cache
sub _flushDir {
	my $self = shift;
	my ($path, $flushParent) = @_;
	$path .= '/' if($path !~ m!/$!so);
	delete $self->{'dirs'}->{$path};

	#flush the parent directory from the cache as well
	$self->_flushDir($1, 1) if($flushParent && $path =~ m!^(.*/)[^/]*/$!so);

	return;
}

#fetch meta-data about the specified file
sub get_file_info($) {
	my $self = shift;
	my ($path) = @_;

	#short-circuit if this is the root directory
	return {
		'name' => '/',
		'is_directory' => 1,
	} if($path eq '/');

	#split the path into the directory and the file
	$path =~ m!^(.*/)([^/]+)$!so;
	my ($dir, $file) = ($1, $2);

	#look up meta-data in the directory containing the specified file
	my $finfo = eval {
		my $mogc = $self->MogileFS();
		foreach($self->_listDir($dir)) {
			return $_ if($_->{'name'} eq $file);
		}
		return undef;
	};

	#return the found file info
	return $finfo;
}

#method that will return a MogileFS object
sub MogileFS {
	my $client = $_[0]->_localElem('MogileFS');

	#create and store a new client if one doesn't exist already
	if(!defined $client) {
		my $config = $_[0]->_config;
		$client = MogileFS::Client::FilePaths->new(
			'hosts'  => [@{$config->{'trackers'}}],
			'domain' => $config->{'domain'},
		);
		$_[0]->_localElem('MogileFS', $client);
	}

	#return the MogileFS client
	return $client;
}

##Fuse callbacks

sub fuse_flush {
	my $self = shift;
	my ($path, $file) = @_;

	#does the directory cache need a flush after this file is flushed
	my $needsFlush = eval{$file->writable && $file->dirty};

	#issue actual flush
	my $resp = $self->next::method(@_);

	#flush the directory cache if necessary
	eval {$self->_flushDir($file->path, 1)} if($needsFlush);

	#return the response for the flush
	return $resp;
}

sub fuse_getattr {
	my $self = shift;
	my ($path) = @_;
	$path = $self->sanitize_path($path);

	# short-circuit if the file doesn't exist
	my $finfo = $self->get_file_info($path);
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
	$atime = $ctime = $mtime = $finfo->{'modified'} || time;

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

sub fuse_getdir {
	my $self = shift;
	my ($path) = @_;
	$path = $self->sanitize_path($path);

	#fetch all the files in the specified directory
	my @files = eval {$self->_listDir($path)};
	return -EIO() if($@);

	#return this directory listing
	return ('.', '..', map {$_->{'name'}} @files), 0;
}

sub fuse_mkdir {
	my $self = shift;
	my ($path, $mode) = @_;
	$path = $self->sanitize_path($path);

	#create and delete a file to force path vivification
	eval{
		my $file = $path . '/.mkdir_tmp_' . join('', map {chr(int(rand(26)) + 97)} (0..9));
		my $mogc = $self->MogileFS();
		die unless($mogc->new_file($file, $self->_config->{'class'})->close);
		$mogc->delete($file);

		#flush the directory cache
		$self->_flushDir($path, 1);
	};
	if($@) {
		$self->log(ERROR, 'Error creating new directory: ' . $path);
		return -EIO();
	}

	return 0;
}

sub fuse_mknod {
	my $self = shift;
	my ($path) = @_;
	$path = $self->sanitize_path($path);

	#issue actual mknod callback
	my $resp = $self->next::method(@_);

	#flush affected entries from the dir cache
	eval {$self->_flushDir($path, 1)};

	#return the actual response
	return $resp;
}

sub fuse_release {
	my $self = shift;
	my ($path, $flags, $file) = @_;

	#does the directory cache need a flush after this file is released
	my $needsFlush = eval{$file->writable && $file->dirty};

	#issue actual release
	my $resp = $self->next::method(@_);

	#flush the directory cache if necessary
	eval {$self->_flushDir($file->path, 1)} if($needsFlush);

	#return the response for the release
	return $resp;
}

sub fuse_rename {
	my $self = shift;
	my ($old, $new) = @_;
	$old = $self->sanitize_path($old);
	$new = $self->sanitize_path($new);

	#throw an error if the new file already exists
	return -EEXIST() if(defined $self->get_file_info($new));

	#attempt renaming the specified file
	my $mogc = $self->MogileFS();
	my $response = eval {
		$mogc->rename($old, $new);
		$self->_flushDir($old, 1);
		$self->_flushDir($new, 1);
	};
	if($@ || !$response) {
		($?, $!) = (-1, '');
		#set the error code and string if we have a MogileFS::Client object
		if($mogc) {
			$? = $mogc->errcode || -1;
			$! = $mogc->errstr || '';
		}
		$self->log(ERROR, "Error renaming file: $?: $!");
		return -EIO();
	}

	#return success
	return 0;
}

sub fuse_truncate {
	my $self = shift;
	my ($path, $size) = @_;
	$path = $self->sanitize_path($path);

	#issue actual truncate callback
	my $resp = $self->next::method(@_);

	#flush affected entries from the dir cache
	eval {$self->_flushDir($path, 1)};

	#return the actual response
	return $resp;
}

sub fuse_unlink {
	my $self = shift;
	my ($path) = @_;
	$path = $self->sanitize_path($path);

	#issue actual unlink callback
	my $resp = $self->next::method(@_);

	#flush affected entries from the dir cache
	eval {$self->_flushDir($path, 1)};

	#return the actual response
	return $resp;
}

1;
