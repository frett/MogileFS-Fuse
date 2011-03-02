#!/usr/bin/perl

use strict;

use Module::Build;

Module::Build->new(
	'module_name'        => 'MogileFS::Fuse',
	'create_makefile_pl' => 'traditional',
	'dist_abstract'      => 'Fuse binding for MogileFS',
	'license'            => 'mit',
	'configure_requires' => {
		'Module::Build'               => '0.36',
	},
	'requires'           => {
		'Fuse'                        => '0.11',
		'MogileFS::Client::FilePaths' => 0,
		'Params::Validate'            => 0,
		'Scalar::Util'                => 0,
	},
)->create_build_script();
