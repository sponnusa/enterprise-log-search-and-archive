#!/usr/bin/perl
use strict;
use lib qw(/usr/local/elsa);
use ELSA::Indexer;
my $indexer = new ELSA::Indexer("/etc/elsa.conf");
print $indexer->get_sphinx_conf("/usr/local/elsa/conf/sphinx.conf.template");
