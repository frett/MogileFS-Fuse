package MogileFS::Fuse;

use strict;
use threads::shared;

use Errno qw{EEXIST EIO ENOENT EOPNOTSUPP};
use Fcntl qw{O_WRONLY};
use Fuse 0.11;
use LWP;
use MogileFS::Client;
use MogileFS::Fuse::BufferedFile;
use MogileFS::Fuse::Constants qw{CALLBACKS :LEVELS};
use MogileFS::Fuse::File;
use Params::Validate qw{validate ARRAYREF BOOLEAN SCALAR UNDEF};
use Scalar::Util qw{blessed refaddr};

##Private static variables

#variables to track the currently mounted Fuse object
my %unshared;

##Static Methods

#constructor
#	buffered   => boolean indicating if open file handles should utilize write buffering, defaults to true
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
		'buffered'   => {'type' => BOOLEAN, 'default' => 1},
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

sub CLONE {
	#destroy all unshared objects to prevent non-threadsafe objects from being accessed by multiple threads
	%unshared = ();
	return 1;
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

#method that will return a MogileFS object
sub MogileFS {
	my $client = $_[0]->_localElem('MogileFS');

	#create and store a new client if one doesn't exist already
	if(!defined $client) {
		my $config = $_[0]->{'config'};
		$client = MogileFS::Client->new(
			'hosts'  => [@{$config->{'trackers'}}],
			'domain' => $config->{'domain'},
		);
		$_[0]->_localElem('MogileFS', $client);
	}

	#return the MogileFS client
	return $client;
}

#Method to mount the specified MogileFS domain to the filesystem
sub mount {
	my $self = shift;

	#short-circuit if a MogileFS file system is currently mounted
	{
		lock($self);
		return if($self->{'mounted'});
		$self->{'mounted'} = 1;
	}

	#generate closures for supported callbacks
	my %callbacks;
	foreach(CALLBACKS) {
		#skip unsupported callbacks
		my $method = 'fuse_' . $_;
		next if(!$self->can($method));

		#create closure for this callback
		no strict "refs";
		$callbacks{$_} = sub {
			$self->log(DEBUG, $method . '(' . join(', ', map {'"' . $_ . '"'} ($method eq 'fuse_write' ? ($_[0], length($_[1]).' bytes', @_[2,3]) : @_)) . ')') if($self->{'config'}->{'loglevel'} >= DEBUG);
			$self->$method(@_);
		};
	}

	#mount the MogileFS file system
	Fuse::main(
		'mountpoint' => $self->{'config'}->{'mountpoint'},
		'threaded' => $self->{'config'}->{'threaded'},

		#callback functions
		%callbacks,
	);

	#reset mounted state
	{
		lock($self);
		$self->{'mounted'} = 0;
	}

	#return
	return;
}

#thin wrapper for opening a file that can be overriden by subclasses as necessary
sub openFile {
	my $self = shift;
	my ($path, $flags) = @_;

	#pick the file class to use based on whether buffering is enabled or not
	my $class =
		$self->{'buffered'} ? 'MogileFS::Fuse::BufferedFile' :
		'MogileFS::Fuse::File';

	#create a file object for the file being opened
	return $class->new(
		'fuse'  => $self,
		'path'  => $path,
		'flags' => $flags,
	);
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

sub fuse_flush {
	my $self = shift;
	my ($path, $file) = @_;

	eval {$file->flush()};
	return -EIO() if($@);

	return 0;
}

sub fuse_getattr {
	return -EOPNOTSUPP();
}

sub fuse_getdir {
	return -EOPNOTSUPP();
}

sub fuse_link {
	return -EOPNOTSUPP();
}

sub fuse_mknod {
	my $self = shift;
	my ($path) = @_;
	$path = $self->sanitize_path($path);

	#attempt creating an empty file
	eval {$self->openFile($path, O_WRONLY)->close()};
	return -EIO() if($@);

	#return success
	return 0;
}

sub fuse_open {
	my $self = shift;
	my ($path, $flags) = @_;
	$path = $self->sanitize_path($path);

	#open the requested file
	my $file = eval {$self->openFile($path, $flags)};
	return -EIO() if($@);
	return -EEXIST() if(!$file);

	#store the file in the list of open files
	{
		my $files = $self->{'files'};
		lock($files);
		return -EIO() if(defined $files->{$file->id});
		$files->{$file->id} = $file;
	};

	#return success and the open file handle
	return 0, $file;
}

sub fuse_read {
	my $self = shift;
	my ($path, $len, $off, $file) = @_;

	my $buf = eval{$file->read($len, $off)};
	return -EIO() if($@);

	return defined($buf) ? $$buf : '';
}

sub fuse_readlink {
	return 0;
}

sub fuse_release    {
	my $self = shift;
	my ($path, $flags, $file) = @_;

	eval {
		delete $self->{'files'}->{$file->id};
		$file->close();
	};
	return -EIO() if($@);

	return 0;
}

sub fuse_rename {
	return -EOPNOTSUPP();
}

sub fuse_statfs {
	return 255, 1, 1, 1, 1, 1024;
}

sub fuse_symlink {
	return -EOPNOTSUPP();
}

sub fuse_truncate {
	my $self = shift;
	my ($path, $size) = @_;

	#attempt to truncate the specified file
	eval{
		my $file = $self->openFile($path, O_WRONLY);
		$file->truncate($size);
		$file->close;
	};
	return -EIO() if($@);

	#return success
	return 0;
}

sub fuse_unlink {
	my $self = shift;
	my ($path) = @_;
	$path = $self->sanitize_path($path);

	#attempt deleting the specified file
	my $mogc = $self->MogileFS();
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

sub fuse_write {
	my $self = shift;
	my $buf = \$_[1];
	my $offset = $_[2];
	my $file = $_[3];

	my $bytesWritten = eval{$file->write($buf, $offset)};
	return -EIO() if($@);

	return $bytesWritten;
}

1;
