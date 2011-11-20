#!/bin/sh

# CONFIG VARIABLES
BASE_DIR="/usr/local"
DATA_DIR="/data"
TMP_DIR="/tmp"
EVENTLOG_VER="0.2.12"
SYSLOG_VER="3.2.4"
MYSQL_ROOT_PASS="biglog"

ntpdate time.nist.gov
apt-get update

# Don't ask for mysql password
echo "debconf debconf/frontend select noninteractive" | debconf-set-selections

# Install required packages
apt-get -qy install curl subversion gcc g++ mysql-server libmysqlclient-dev pkg-config libglib2.0-dev libpcre3-dev libcap-dev libnet1-dev libssl-dev

# Make debconf interactive again
echo "debconf debconf/frontend select readline" | debconf-set-selections

# Get the latest code from Google Code
cd $BASE_DIR
svn export "https://enterprise-log-search-and-archive.googlecode.com/svn/trunk/elsa"
mkdir -p "$BASE_DIR/elsa/node/tmp/locks" && 
touch "$BASE_DIR/elsa/node/tmp/locks/directory"

# Install required Perl modules
cd $TMP_DIR && curl -L http://cpanmin.us | perl - App::cpanminus
# Now cpanm is available to install the rest
cpanm Moose Config::JSON String::CRC32 Log::Log4perl DBD::mysql Date::Manip Sys::Info Sys::MemInfo

# Get and build sphinx on nodes
cd $TMP_DIR
svn export "https://sphinxsearch.googlecode.com/svn/trunk/" sphinx-svn
cd sphinx-svn
./configure --enable-id64 "--prefix=$BASE_DIR/sphinx" && make && make install
touch "$BASE_DIR/etc/sphinx_stopwords.txt"
cp $BASE_DIR/elsa/contrib/searchd /etc/init.d/ &&
update-rc.d searchd defaults

# Get and build syslog-ng
cd $TMP_DIR
wget "http://www.balabit.com/downloads/files/syslog-ng/open-source-edition/$SYSLOG_VER/source/eventlog_$EVENTLOG_VER.tar.gz" &&
tar xzvf "eventlog_$EVENTLOG_VER.tar.gz" &&
cd "eventlog-$EVENTLOG_VER" &&
./configure && make && make install &&
ldconfig &&
cd $TMP_DIR &&
wget "http://www.balabit.com/downloads/files/syslog-ng/open-source-edition/$SYSLOG_VER/source/syslog-ng_$SYSLOG_VER.tar.gz" &&
tar xzvf "syslog-ng_$SYSLOG_VER.tar.gz" &&
cd "syslog-ng-$SYSLOG_VER" &&
./configure "--prefix=$BASE_DIR/syslog-ng-$SYSLOG_VER" --enable-ipv6 && 
make && make install && 
ln -s "$BASE_DIR/syslog-ng-$SYSLOG_VER" "$BASE_DIR/syslog-ng"
# Copy the syslog-ng.conf
cp "$BASE_DIR/elsa/node/conf/syslog-ng.conf" "$BASE_DIR/syslog-ng/etc/syslog-ng.conf" &&
mkdir "$BASE_DIR/syslog-ng/var"
cp $BASE_DIR/elsa/contrib/syslog-ng /etc/init.d/ &&
update-rc.d syslog-ng defaults

# Make data directories on node
mkdir -p "$DATA_DIR/elsa/log" && mkdir -p "$DATA_DIR/elsa/tmp/buffers" &&
mkdir -p "$DATA_DIR/sphinx/log"

# Install mysql schema
mysqladmin -uroot create syslog && mysqladmin -uroot create syslog_data && 
mysql -uroot -e 'GRANT ALL ON syslog.* TO "elsa"@"localhost" IDENTIFIED BY "biglog"' &&
mysql -uroot -e 'GRANT ALL ON syslog.* TO "elsa"@"%" IDENTIFIED BY "biglog"' &&
mysql -uroot -e 'GRANT ALL ON syslog_data.* TO "elsa"@"localhost" IDENTIFIED BY "biglog"' &&
mysql -uroot -e 'GRANT ALL ON syslog_data.* TO "elsa"@"%" IDENTIFIED BY "biglog"' &&  
mysql -uelsa -pbiglog syslog -e "source $BASE_DIR/elsa/node/conf/schema.sql"

# Copy elsa.conf to /etc/
cat "$BASE_DIR/elsa/node/conf/elsa.conf" | sed -e "s|\/usr\/local|$BASE_DIR|g" | sed -e "s|\/data|$DATA_DIR|g" > /etc/elsa_node.conf

# Run elsa.pl for initial creation of sphinx config
echo "" | perl "$BASE_DIR/elsa/node/elsa.pl" -on -c /etc/elsa_node.conf

# Initialize empty sphinx indexes
"$BASE_DIR/sphinx/bin/indexer" --config "$BASE_DIR/etc/sphinx.conf" --rotate --all
# Start sphinx
service searchd start

# Start syslog-ng using the ELSA config
service syslog-ng start

# Sleep to allow ELSA to initialize and validate its directory
echo "Sleeping for 60 seconds to allow ELSA to init..."
sleep 60

# Test
echo "Sending test log messages..."
"$BASE_DIR/syslog-ng/bin/loggen" -Di -I 1 localhost 514

# Sleep to allow ELSA to initialize and validate its directory
echo "Sleeping for 60 seconds to allow ELSA to load batch..."
sleep 60

# Watch the log file to make sure it's working (after wiping indexes you should see batches processed and rows indexed)
grep "Indexed temp_1 " "$DATA_DIR/elsa/log/node.log"

