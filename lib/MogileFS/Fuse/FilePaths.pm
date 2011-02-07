package MogileFS::Fuse::FilePaths;

use strict;
use utf8;
use base qw{MogileFS::Fuse};

use MogileFS::Client::FilePaths;

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

1;
