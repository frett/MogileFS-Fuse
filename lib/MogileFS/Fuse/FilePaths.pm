package MogileFS::Fuse::FilePaths;

use strict;
use utf8;
use base qw{MogileFS::Fuse};

use MogileFS::Client::FilePaths;
use MogileFS::Fuse::Constants qw{:LEVELS};
use POSIX qw{EEXIST EIO ENOENT};

##Instance Methods

#method that will return a MogileFS object
sub client {
	my $client = $_[0]->_localElem('client');

	#create and store a new client if one doesn't exist already
	if(!defined $client) {
		my $config = $_[0]->{'config'};
		$client = MogileFS::Client::FilePaths->new(
			'hosts'  => [@{$config->{'trackers'}}],
			'domain' => $config->{'domain'},
		);
		$_[0]->_localElem('client', $client);
	}

	#return the MogileFS client
	return $client;
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

	#process the specified path
	$path =~ m!^(.*/)([^/]+)$!;
	my ($dir, $file) = ($1, $2);

	#look up meta-data for the directory containing the specified file
	#TODO: maybe cache this lookup
	my $finfo = eval {
		my $mogc = $self->client();
		my @files = $mogc->list($dir);
		foreach(@files) {
			return $_ if($_->{'name'} eq $file);
		}
		return undef;
	};

	#return the found file info
	return $finfo;
}

##Fuse callbacks

sub e_getattr {
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

sub e_getdir {
	my $self = shift;
	my ($path) = @_;
	$path = $self->sanitize_path($path);

	#fetch all the files in the specified directory
	my @files = eval {
		return $self->client()->list($path);
	};

	#return this directory listing
	return ('.', '..', map {$_->{'name'}} @files), 0;
}

sub e_rename {
	my $self = shift;
	my ($old, $new) = @_;
	$old = $self->sanitize_path($old);
	$new = $self->sanitize_path($new);

	#throw an error if the new file already exists
	return -EEXIST() if(defined $self->get_file_info($new));

	#attempt renaming the specified file
	my $mogc = $self->client();
	my ($errcode, $errstr) = (-1, '');
	my $response = eval {$mogc->rename($old, $new)};
	if($@ || !$response) {
		#set the error code and string if we have a MogileFS::Client object
		if($mogc) {
			$errcode = $mogc->errcode || -1;
			$errstr = $mogc->errstr || '';
		}
		$self->log(ERROR, "Error renaming file: $errcode: $errstr");
		$! = $errstr;
		$? = $errcode;
		return -EIO();
	}

	#return success
	return 0;
}

1;
