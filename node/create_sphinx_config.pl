#!/usr/bin/perl
use strict;
use lib qw(/usr/local/elsa/node);
use ELSA::Indexer;
my $indexer = new ELSA::Indexer("/usr/local/elsa/etc/elsa.conf");
open(FH, '>' . $indexer->conf->get('sphinx/config_file')) or die("Cannot open config file for writing: $!");
print FH $indexer->get_sphinx_conf($indexer->conf->get('sphinx/config_template_file'));
close(FH);
print 'Wrote new config file using template at ' . $indexer->conf->get('sphinx/config_template_file') . ' to file ' . $indexer->conf->get('sphinx/config_file') . "\n";
