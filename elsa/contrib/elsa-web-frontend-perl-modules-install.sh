#!/bin/sh
cpanm YAML::Syck &&
cpanm Moose &&
cpanm IO::Socket &&
cpanm Data::Serializer &&
cpanm POE::Event::Message &&
cpanm Config::JSON &&
cpanm Net::LDAP::Express &&
cpanm Net::LDAP::FilterBuilder &&
cpanm Module::Pluggable &&
cpanm URI::Escape &&
cpanm DBD::mysql &&
cpanm POE::Filter::Reference &&
cpanm Digest::HMAC_SHA1 &&
cpanm Mail::Internet &&
cpanm File::Slurp &&
cpanm MIME::Base64 &&
cpanm EV &&
cpanm Time::HiRes &&
cpanm Plack::Builder &&
cpanm Plack::Session &&
cpanm Plack::Middleware::CrossOrigin &&
cpanm Plack::Middleware::ForwardedHeaders &&
# For auth plugins (optional, but enabled by default)
cpanm -n Authen::Simple::PAM &&
cpanm -n Authen::Simple::LDAP &&
# For plugins (optional, but enabled by default)
cpanm Module::Install &&
cpanm PDF::API2::Simple &&
cpanm XML::Writer &&
cpanm Spreadsheet::WriteExcel &&
cpanm Parse::Snort