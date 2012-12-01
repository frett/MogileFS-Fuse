package MogileFS::Client::Fuse::ChecksumedFile;

use strict;
use warnings;
use mro 'c3';
use threads::shared;
use base qw{MogileFS::Client::Fuse::File};

use Digest;
use MogileFS::Client::Fuse::Constants qw{THREADS DEBUG};
require Thread::Queue if(THREADS);

sub _flush($%) {
	my $self = shift;
	my (%opt) = @_;

	# sanitize opts
	$opt{'close_args'} = {} if(ref($opt{'close_args'}) ne 'HASH');

	# finalize checksum
	my $checksum = $self->{'checksum'};
	if($checksum->{'enabled'}) {
		my $config = $self->fuse->_config;
		if($config->{'threaded'}) {
			#TODO: need threaded support
		}
		else {
			$opt{'close_args'}->{'checksumverify'} = 1;
			$opt{'close_args'}->{'checksum'} = $checksum->{'type'} . ':' . $checksum->{'digest'}->hexdigest();
			$self->fuse->log(DEBUG, 'file checksum: ' . $opt{'close_args'}->{'checksum'});

			# checksum state has been destroyed, disable checksums for now
			$checksum->{'enabled'} = 0;
		}
	}

	return $self->next::method(%opt);
}

sub _init {
	my $self = shift;
	my (%opt) = @_;

	$self->{'checksum'} = shared_clone({
		'type' => 'MD5',
	});

	# initialize the base object
	$self = $self->next::method(%opt);
	return undef if(!$self);

	return $self;
}

sub _initIo {
	my $self = shift;
	my (%opt) = @_;

	# (re)initialize the checksum data structure
	my $checksum = $self->{'checksum'};
	$checksum->{'enabled'} = 1;
	$checksum->{'pos'}     = 0;
	$checksum->{'digest'}  = Digest->new($checksum->{'type'}) if(!$self->fuse->_config->{'threaded'});

	# (re)initialize the base object
	return $self->next::method(%opt);
}

# checksum data as it is being written
sub _write {
	my $self = shift;
	my ($offset, $buf, %opt) = @_;

	# process checksums
	if(defined($buf) && ref($buf) eq 'SCALAR') {
		my $checksum = $self->{'checksum'};
		if($checksum->{'enabled'}) {
			if($checksum->{'pos'} == $offset) {
				my $config = $self->fuse->_config;

				# checksum the current buffer
				if($config->{'threaded'}) {
					#XXX: disable checksums when threaded for now
					$checksum->{'enabled'} = 0;
				}
				else {
					$checksum->{'digest'}->add($$buf);
				}

				# update the checksum position
				$checksum->{'pos'} += length($$buf);
			}
			# it's currently impossible to calculate non-sequential checksums
			else {
				$checksum->{'enabled'} = 0;
			}
		}
	}

	return $self->next::method($offset, $buf, %opt);
}

1;
