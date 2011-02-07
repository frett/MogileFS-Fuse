package MogileFS::Fuse::FilePaths;

use strict;
use utf8;
use base qw{MogileFS::Fuse};

use MogileFS::Client::FilePaths;
use MogileFS::Fuse qw{:LEVELS};
use POSIX qw{EEXIST EIO};

##Instance Methods

#method that will return a MogileFS object
sub client {
	my $client = $_[0]->_localElem('client');

	#create and store a new client if one doesn't exist already
	if(!defined $client) {
		$client = MogileFS::Client::FilePaths->new(
			'hosts'  => [@{$config{'trackers'}}],
			'domain' => $config{'domain'},
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
