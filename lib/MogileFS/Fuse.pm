package MogileFS::Fuse;

use strict;
use utf8;
use base qw{Exporter};
use threads;
use threads::shared;

#list of functions available for export
our @EXPORT_OK = qw{
	mountMogileFS
};

#flag that will control log verbosity
our $VERBOSITY :shared = 0;

use Fuse 0.09_4;
use MogileFS::Client::FilePaths;
use Params::Validate qw{validate ARRAYREF SCALAR};

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
#	domain     => the domain to use in MogileFS
#	mountpoint => where to mount the filesystem
#	trackers   => the addresses for the MogileFS trackers
sub mount(%) {
	my %opt = validate(@_, {
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
		'open'    => __PACKAGE__ . '::e_open',
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

sub e_open($$) {
	my ($path, $flags) = @_;
	$path = sanitize_path($path);
	logmsg(1, "e_open: $path, $flags");

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

1;
