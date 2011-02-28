package MogileFS::Fuse::File;

use strict;
use threads::shared;

use Errno qw{EIO};
use Fcntl;
use HTTP::Request;
use HTTP::Status qw{HTTP_REQUEST_RANGE_NOT_SATISFIABLE};
use MogileFS::Fuse::Constants qw{:LEVELS};
use Scalar::Util qw{refaddr};

##Static Methods

#Constructor
#	flags => the open flags for this file
#	fuse  => the MogileFS::Fuse object controlling this file
#	path  => the path of this file
sub new {
	#create the new MogileFS::Fuse::File object
	my $self = shift;
	$self = bless(shared_clone({}), ref($self) || $self);

	#initialize and return the new object
	return $self->_init(@_);
}

##Instance Methods

#method that will copy existing data from the old handle to the new handle
sub _cow {
	my $self = shift;
	my ($newPtr, $limit) = @_;

	#sanitize limit
	$limit = $newPtr if(defined $limit && $limit < $newPtr);

	while(defined $self->{'cowPtr'} && $self->{'cowPtr'} < $newPtr) {
		#set the buffer size for the next block being copied, limit it as necessary
		my $bufSize = 1024 * 1024;
		$bufSize = $limit - $self->{'cowPtr'} if(defined($limit) && $self->{'cowPtr'} + $bufSize > $limit);

		#copy a block of data
		my $bytes = $self->_write($self->{'cowPtr'}, $self->_read($self->{'cowPtr'}, $bufSize));
		$self->{'cowPtr'} += $bytes;
		delete $self->{'cowPtr'} if(!$bytes);
	}

	return;
}

#method to initialize this file object
sub _init {
	my $self = shift;
	my (%opt) = @_;

	#set all the specified options
	$self->{'flags'} = $opt{'flags'};
	$self->{'fuse'} = $opt{'fuse'};
	$self->{'id'} = is_shared($self) || refaddr($self);
	$self->{'path'} = $opt{'path'};

	#short-circuit if the file isn't opened for writing and doesn't exist in MogileFS
	return if(!($self->flags & (O_WRONLY | O_RDWR)) && !$self->getPaths());

	#initialize the cow pointer for COW when the file is in write mode and a previous version exists
	$self->{'cowPtr'} = 0 if($self->flags & (O_WRONLY | O_RDWR) && $self->getPaths());

	#return the initialized object
	return $self;
}

#method to read the requested data directly from a file in MogileFS
#	output => is this read request being performed on an output file instead of an input file
sub _read {
	my $self = shift;
	my ($offset, $len, %opt) = @_;

	#iterate over all paths attempting to read data
	my $ua = $self->fuse->ua;
	my $headers = ['Range' => 'bytes=' . $offset . '-' . ($offset + $len - 1)];
	my $res;
	foreach my $uri ($opt{'output'} ? $self->getOutputDest->{'path'} : $self->getPaths()) {
		#attempt retrieving the requested data
		$res = $ua->request(HTTP::Request->new('GET' => $uri, $headers));

		#check for errors
		if($res->is_error) {
			#have we reached the end of this file?
			return undef if($res->code == HTTP_REQUEST_RANGE_NOT_SATISFIABLE);

			#try the next uri
			next;
		}

		#exit the loop
		last;
	}

	#was there an error satisfying this read request?
	if(!$res || $res->is_error) {
		$self->fuse->log(ERROR, 'Error reading data from: ' . $self->path);
		die;
	}

	#return the fetched content
	$res->decode;
	return $res->content_ref;
}

#method to write the specified data to a file in MogileFS
sub _write {
	my $self = shift;
	my ($offset, $buf) = @_;

	#attempt writing the buffer to the output destination
	if(my $dest = $self->getOutputDest()) {
		#short-circuit if an invalid buffer was provided
		if(defined($buf) && ref($buf) ne 'SCALAR') {
			$self->fuse->log(ERROR, 'Invalid buffer passed to _write');
			$dest->{'error'} = 1;
			die;
		}

		#write buffer if it contains any data
		my $len = 0;
		if(ref($buf) eq 'SCALAR' && ($len = length($$buf))) {
			#build request
			my $req = HTTP::Request->new('PUT' => $dest->{'path'}, [
				'Content-Range' => 'bytes ' . $offset . '-' . ($offset + $len - 1) . '/*',
			]);
			$req->content_ref($buf);

			#attempt this raw write
			my $res = $self->fuse->ua->request($req);
			if(!$res || $res->is_error) {
				$self->fuse->log(ERROR, 'Error writing data to: ' . $self->path);
				$dest->{'error'} = 1;
				die;
			}

			#update the output size
			{
				lock($dest);
				$dest->{'size'} = $offset + $len if($offset + $len > $dest->{'size'});
			}
		}

		#return the number of bytes written
		return $len;
	}
	else {
		$self->fuse->log(ERROR, 'Cannot write to file: ' . $self->path);
		die;
	}
}

sub close {
	my $self = shift;

	#close an open output handle if we are in a write mode
	if($self->flags & (O_WRONLY | O_RDWR)) {
		my $dest = $self->getOutputDest();

		#copy any data that hasn't been copied yet
		$self->_cow($self->{'cowPtr'} + 1024*1024) while(defined $self->{'cowPtr'});

		#TODO: need to make sure there are no current writes happening (this should be probably be handled by flush eventually)

		my $res = eval {
			my $config = $self->fuse->{'config'};
			$self->MogileFS->{'backend'}->do_request('create_close', {
				'fid'    => $dest->{'fid'},
				'devid'  => $dest->{'devid'},
				'domain' => $config->{'domain'},
				'size'   => $dest->{'size'},
				'key'    => ($dest->{'error'} ? '' : $self->path),
				'path'   => $dest->{'path'},

				# these attributes are specific to MogileFS::Client::FilePaths which utilizes the MetaData MogileFS plugin
				#TODO: move this into a FilePaths specific file object
				'plugin.meta.keys'   => 1,
				'plugin.meta.key0'   => 'mtime',
				'plugin.meta.value0' => scalar time,
			});
		};
		if($@ || !$res) {
			$self->fuse->log(ERROR, 'Error closing open file: ' . $self->path);
			die;
		}
	}

	return;
}

sub flags {
	return $_[0]->{'flags'};
}

sub fuse {
	return $_[0]->{'fuse'};
}

#method that will return an output path for writing to this file
sub getOutputDest {
	my $self = shift;

	#short-circuit if we are in a read only mode
	return if(!($self->flags & (O_WRONLY | O_RDWR)));

	#create an output path if one doesn't exist already
	{
		lock($self);
		if(!$self->{'dest'}) {
			#create a new temporary file in MogileFS
			my $tmpFile = eval{
				my $config = $self->fuse->{'config'};
				$self->MogileFS->{'backend'}->do_request('create_open', {
					'domain'     => $config->{'domain'},
					'class'      => $config->{'class'},
					'key'        => $self->path,
					'fid'        => 0,
					'multi_dest' => 0,
				});
			};
			if($@ || !$tmpFile) {
				$self->fuse->log(ERROR, 'Error creating temporary file in MogileFS: ' . $self->path);
				die;
			}

			#attempt creating a file at the specified location
			my $res = $self->fuse->ua->request(HTTP::Request->new('PUT' => $tmpFile->{'path'}));
			if(!$res->is_success()) {
				$self->fuse->log(ERROR, 'Error creating temporary file in MogileFS: ' . $self->path);
				die;
			}

			#store the destination
			$self->{'dest'} = shared_clone({
				'devid' => $tmpFile->{'devid'},
				'fid'   => $tmpFile->{'fid'},
				'path'  => $tmpFile->{'path'},
				'size'  => 0,
			});
		}
	}

	return $self->{'dest'};
}

#method that will return the paths for the current file
sub getPaths {
	my $self = shift;

	#load the file paths
	{
		#lock here to make sure the paths are only loaded once and a thread doesn't
		#happen to get different paths for a request than other threads
		lock($self);
		if(!$self->{'paths'}) {
			my $mogc = $self->MogileFS();
			$self->{'paths'} = shared_clone([]);
			push @{$self->{'paths'}}, eval {$mogc->get_paths($self->path)};
			if($@) {
				#set the error code and string if we have a MogileFS::Client object
				($?, $!) = (-1, '');
				if($mogc) {
					$? = $mogc->errcode || -1;
					$! = $mogc->errstr || '';
				}
				$self->fuse->log(ERROR, 'Error opening file: ' . $? . ': ' . $!);
				die;
			}
		}
	}

	#return the paths for this file
	return @{$self->{'paths'}};
}

sub id {
	return $_[0]->{'id'};
}

sub MogileFS {
	return $_[0]->fuse->MogileFS();
}

sub path {
	return $_[0]->{'path'};
}

sub read {
	my $self = shift;
	my ($len, $offset) = @_;
	my $output = $self->flags & (O_WRONLY | O_RDWR);

	#make sure the read request from the output file is satisfiable
	$self->_cow($offset + $len) if($output);

	return $self->_read($offset, $len,
		'output' => $output,
	);
}

#method that will truncate this file to the specified byte
sub truncate {
	my $self = shift;
	my ($size) = @_;

	#throw an error if it is not possible to truncate the file to the specified size
	if(!defined $self->{'cowPtr'} || $self->{'cowPtr'} > $size) {
		$self->fuse->log(ERROR, 'Cannot truncate ' . $self->path . ' to ' . $size);
		die;
	}

	#copy up to $size bytes of the file
	$self->_cow($size, $size);
	delete $self->{'cowPtr'};

	return;
}

sub write {
	my $self = shift;
	my ($buf, $offset) = @_;

	#make sure data is copied from the old file past the specified write buffer
	$self->_cow($offset + length($$buf));

	#write the raw data
	return $self->_write($offset, $buf);
}

1;
