package MogileFS::Fuse::File;

use strict;
use threads::shared;

use Errno qw{EIO};
use Fcntl;
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

1;
