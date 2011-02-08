package MogileFS::Fuse;

use strict;
use threads::shared;

use Errno qw{EEXIST EIO ENOENT EOPNOTSUPP};
use Fuse 0.09_4;
use LWP;
use MogileFS::Client;
use MogileFS::Fuse::Constants qw{CALLBACKS :LEVELS};
use MogileFS::Fuse::File;
use Params::Validate qw{validate ARRAYREF BOOLEAN SCALAR UNDEF};
use Scalar::Util qw{blessed refaddr};

##Private static variables

#variables to track the currently mounted Fuse object
my %unshared;
my $mountedObject :shared;

##Fuse callback wrappers
#TODO: this is a hack because the Fuse library doesn't support method callbacks or coderef callbacks in threads
BEGIN {
	no strict "refs";
	foreach (CALLBACKS) {
		my $callback = __PACKAGE__ . '::_' . $_;
		my $method = 'e_' . $_;

		*$callback = sub {
			my $self = $mountedObject;
			$self->log(DEBUG, $method . '(' . join(', ', map {'"' . $_ . '"'} @_) . ')') if($self->{'config'}->{'loglevel'} >= DEBUG);
			$self->$method(@_);
		};
	}
}

##Static Methods

#constructor
#	class      => the class to store files as in MogileFS
#	domain     => the domain to use in MogileFS
#	loglevel   => the log level to use for output
#	mountpoint => where to mount the filesystem
#	threaded   => flag indicating if this MogileFS file system should be threaded or not
#	trackers   => the addresses for the MogileFS trackers
sub new {
	#create the new MogileFS::Fuse object
	my $self = shift;
	$self = bless(shared_clone({}), ref($self) || $self);

	#initialize and return the new object
	return $self->_init(@_);
}

##Instance Methods

#method that will initialize the MogileFS::Fuse object
sub _init {
	my $self = shift;
	my %opt = validate(@_, {
		'class'      => {'type' => SCALAR | UNDEF, 'default' => undef},
		'domain'     => {'type' => SCALAR},
		'loglevel'   => {'type' => SCALAR, 'default' => ERROR},
		'mountpoint' => {'type' => SCALAR},
		'threaded'   => {'type' => BOOLEAN, 'default' => $threads::threads},
		'trackers'   => {'type' => ARRAYREF},
	});

	#die horribly if we are trying to reinit an existing object
	die 'You are trying to reinitialize an existing MogileFS::Fuse object, this could introduce race conditions and is unsupported' if($self->{'id'});

	#disable threads if they aren't loaded
	$opt{'threaded'} = 0 if(!$threads::threads);

	#initialize this object
	$self->{'config'} = shared_clone({%opt});
	$self->{'files'} = shared_clone({});
	$self->{'id'} = is_shared($self) || refaddr($self);

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
		my $config = $_[0]->{'config'};
		$client = MogileFS::Client->new(
			'hosts'  => [@{$config->{'trackers'}}],
			'domain' => $config->{'domain'},
		);
		$_[0]->_localElem('client', $client);
	}

	#return the MogileFS client
	return $client;
}

sub CLONE {
	#destroy all unshared objects to prevent non-threadsafe objects from being accessed by multiple threads
	%unshared = ();
	return 1;
}

#method that will look up the requested file
sub find_file {
	my $self = shift;
	my ($file) = @_;
	$file = $self->{'files'}->{$file} if(!blessed($file));
	$self->log(ERROR, 'Something went wrong finding a file') if(!defined $file);
	return $file;
}

#return the instance id for this object
sub id {
	return $_[0]->{'id'};
}

#function that will output a log message
sub log {
	my $self = shift;
	my ($level, $msg) = @_;
	return if($level > $self->{'config'}->{'loglevel'});
	print STDERR $msg, "\n";
}

#Method to mount the specified MogileFS domain to the filesystem
sub mount {
	my $self = shift;

	#short-circuit if a MogileFS file system is currently mounted
	{
		lock($self);
		lock($mountedObject);
		return if($self->{'mounted'} || $mountedObject);
		$self->{'mounted'} = 1;
		$mountedObject = $self;
	}

	#mount the MogileFS file system
	Fuse::main(
		'mountpoint' => $self->{'config'}->{'mountpoint'},
		'threaded' => $self->{'config'}->{'threaded'},

		#callback functions
		(map {$_ => __PACKAGE__ . '::_' . $_} grep {$self->can('e_' . $_)} CALLBACKS),
	);

	#reset mounted state
	{
		lock($self);
		lock($mountedObject);
		$mountedObject = undef;
		$self->{'mounted'} = 0;
	}

	#return
	return;
}

sub sanitize_path {
	my $self = shift;
	my ($path) = @_;

	# Make sure we start everything from '/'
	$path = '/' unless(length($path));
	$path = '/' if($path eq '.');
	$path = '/' . $path unless($path =~ m!^/!so);

	return $path;
}

#method that will return an LWP UserAgent object
sub ua {
	my $ua = $_[0]->_localElem('ua');

	#create and store a new ua if one doesn't exist already
	if(!defined $ua) {
		$ua = LWP::UserAgent->new(
			'keep_alive' => 60,
			'timeout'    => 5,
		);
		$_[0]->_localElem('ua', $ua);
	}

	#return the UserAgent
	return $ua;
}

##Callback Functions

sub e_getattr {
	return -EOPNOTSUPP();
}

sub e_getdir {
	return -EOPNOTSUPP();
}

sub e_link {
	return -EOPNOTSUPP();
}

sub e_mknod {
	my $self = shift;
	my ($path) = @_;
	$path = $self->sanitize_path($path);

	#attempt creating an empty file
	my $mogc = $self->client();
	my ($errcode, $errstr) = (-1, '');
	my $response = eval {$mogc->new_file($path, $self->{'config'}->{'class'})->close};
	if($@ || !$response) {
		#set the error code and string if we have a MogileFS::Client object
		if($mogc) {
			$errcode = $mogc->errcode || -1;
			$errstr = $mogc->errstr || '';
		}
		$self->log(ERROR, "Error creating file: $errcode: $errstr");
		$! = $errstr;
		$? = $errcode;
		return -EIO();
	}

	#return success
	return 0;
}

sub e_open {
	my $self = shift;
	my ($path, $flags) = @_;
	$path = $self->sanitize_path($path);

	#open the requested file
	my $file = eval {MogileFS::Fuse::File->new(
		'fuse'  => $self,
		'path'  => $path,
		'flags' => $flags,
	)};
	return -EIO() if($@);
	return -EEXIST() if(!$file);

	#store the file in the list of open files
	{
		my $files = $self->{'files'};
		lock($files);
		return -EIO() if(defined $files->{$file->id});
		$files->{$file->id} = $file;
	};

	#return success and the open file id
	return 0, $file->id;
}

sub e_read {
	my $self = shift;
	my ($path, $len, $off, $file) = @_;

	my $buf = eval{$self->find_file($file)->read($len, $off)};
	return -EIO() if($@);

	return $buf;
}

sub e_readlink {
	return 0;
}

sub e_rename {
	return -EOPNOTSUPP();
}

sub e_statfs {
	return 255, 1, 1, 1, 1, 1024;
}

sub e_symlink {
	return -EOPNOTSUPP();
}

sub e_unlink {
	my $self = shift;
	my ($path) = @_;
	$path = $self->sanitize_path($path);

	#attempt deleting the specified file
	my $mogc = $self->client();
	my ($errcode, $errstr) = (-1, '');
	eval {$mogc->delete($path)};
	if($@) {
		#set the error code and string if we have a MogileFS::Client object
		if($mogc) {
			$errcode = $mogc->errcode || -1;
			$errstr = $mogc->errstr || '';
		}
		$self->log(ERROR, "Error unlinking file: $errcode: $errstr");
		$! = $errstr;
		$? = $errcode;
		return -EIO();
	}

	#return success
	return 0;
}

1;
