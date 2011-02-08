package MogileFS::Fuse::File;

use strict;
use threads::shared;

use Errno qw{EIO};
use Fcntl;
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

	#return the initialized object
	return $self;
}

#method to set/return an error number for the current open file
sub errno {
	$_[0]->{'errno'} = $_[1] if(@_ > 1);
	return $_[0]->{'errno'};
}

sub flags {
	return $_[0]->{'flags'};
}

sub fuse {
	return $_[0]->{'fuse'};
}

#method that will return the paths for the current file
sub getPaths {
	my $self = shift;

	#load the file paths
	if(!exists $self->{'paths'}) {
		my $mogc = $self->fuse->client();
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
			$self->errno(-EIO());
			die $@;
		}
	}

	#return the paths for this file
	return @{$self->{'paths'}};
}

sub id {
	return $_[0]->{'id'};
}

sub path {
	return $_[0]->{'path'};
}

#method to read the requested data from the file into the specified buffer
sub read {
	my $self = shift;
	my ($len, $offset) = @_;

	#iterate over all paths attempting to read data
	my $ua = $self->fuse->ua;
	my $headers = ['Range' => 'bytes=' . $offset . '-' . ($offset + $len)];
	my $res;
	foreach my $uri ($self->getPaths()) {
		#attempt retrieving the requested data
		$res = $ua->request(HTTP::Request->new('GET', $uri, $headers));

		#check for errors
		if($res->is_error) {
			#have we reached the end of this file?
			return if($res->code == HTTP_REQUEST_RANGE_NOT_SATISFIABLE);

			#try the next uri
			next;
		}

		#exit the loop
		last;
	}

	#was there an error satisfying this read request?
	if(!$res || $res->is_error) {
		$self->fuse->log(ERROR, 'Error reading data from: ' . $self->path);
		$self->errno(-EIO());
		die;
	}

	#return the fetched content
	return $res->content
}

1;
