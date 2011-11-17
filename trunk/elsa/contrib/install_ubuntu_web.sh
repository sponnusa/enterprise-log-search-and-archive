#!/bin/sh

# CONFIG VARIABLES
BASE_DIR="/usr/local"
TMP_DIR="/tmp"
MYSQL_HOST="localhost"
MYSQL_PORT="3306"
MYSQL_DB="elsa_web"
MYSQL_USER="elsa"
MYSQL_PASS="biglog"

ntpdate time.nist.gov
apt-get update

# Make debconf noninteractive
echo "debconf debconf/frontend select noninteractive" | debconf-set-selections

# Install required packages
apt-get -qy install curl subversion gcc g++ mysql-client libmysqlclient-dev apache2-mpm-prefork libapache2-mod-perl2

# Get the latest code from Google Code
cd $BASE_DIR
svn export "https://enterprise-log-search-and-archive.googlecode.com/svn/trunk/elsa"

# Install required Perl modules
cd $TMP_DIR && curl -L http://cpanmin.us | perl - App::cpanminus
# Now cpanm is available to install the rest
cpanm Moose Config::JSON Plack::Builder Plack::App::File Date::Manip Digest::SHA1 MIME::Base64 URI::Escape Socket Net::DNS Sys::Hostname::FQDN String::CRC32 CHI CHI::Driver::RawMemory Search::QueryParser AnyEvent::DBI EV Sys::Info Sys::MemInfo MooseX::Traits Authen::Simple Authen::Simple::PAM Plack::Middleware::CrossOrigin URI::Escape Module::Pluggable Module::Install PDF::API2::Simple XML::Writer Parse::Snort Spreadsheet::WriteExcel IO::String Mail::Internet Plack::Middleware::Static Log::Log4perl Email::LocalDelivery Plack::Session

# Install mysql schema
mysqladmin "-h$MYSQL_HOST" "-P$MYSQL_PORT" -uroot create elsa_web &&
mysql "-h$MYSQL_HOST" "-P$MYSQL_PORT" -uroot -e "GRANT ALL ON $MYSQL_DB.* TO \"$MYSQL_USER\"\@\"%\" IDENTIFIED BY \"$MYSQL_PASS\"" &&
mysql "-h$MYSQL_HOST" "-P$MYSQL_PORT" "-u$MYSQL_USER" "-p$MYSQL_PASS" $MYSQL_DB -e "source $BASE_DIR/elsa/web/conf/meta_db_schema.mysql"

# Copy elsa.conf to /etc/
cp "$BASE_DIR/elsa/web/conf/elsa.conf" /etc/elsa_web.conf

# For Apache, locations vary, but this is the gist:
cpanm Plack::Handler::Apache2
cp "$BASE_DIR/elsa/web/conf/apache_site.conf" /etc/apache2/sites-available/elsa.conf
# Enable the site
a2ensite elsa
a2dissite default
a2enmod rewrite
sudo service apache2 restart

# Setup alerts (optional)
echo "Adding cron entry for alerts..."
# Edit /etc/elsa_web.conf and set the "smtp_server" and "to" fields under "email"
echo "* * * * * perl $BASE_DIR/elsa/web/cron.pl -c /etc/elsa_web.conf 2>&1 > /dev/null" >> /var/spool/cron/crontabs/root

