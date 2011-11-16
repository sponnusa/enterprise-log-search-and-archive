#!/bin/sh
# Install required packages
sudo apt-get install subversion gcc g++ mysql-server libmysqlclient-dev pkg-config libglib2.0-dev libpcre3-dev

# Get the latest code from Google Code
cd /usr/local
sudo svn export "https://enterprise-log-search-and-archive.googlecode.com/svn/trunk/elsa"
sudo mkdir -p /usr/local/elsa/node/tmp/locks && 
sudo touch /usr/local/elsa/node/tmp/locks/directory

# Install required Perl modules
sudo cpan
o conf prerequisites_policy follow
o conf commit
install App::cpanminus
quit

# Now cpanm is available to install the rest
sudo cpanm Moose Config::JSON String::CRC32 Log::Log4perl DBD::mysql Date::Manip Sys::Info Sys::MemInfo

# Get and build sphinx on nodes
cd /tmp
sudo svn export "https://sphinxsearch.googlecode.com/svn/trunk/" sphinx-svn
cd sphinx-svn
./configure --enable-id64 --prefix=/usr/local/sphinx && make && sudo make install
sudo touch /usr/local/etc/sphinx_stopwords.txt

# For Syslog-NG You have two choices, build from source, or install from .deb file:
# For the .deb file:
# Edit the sources file to get syslog-ng
echo "deb      http://packages.madhouse-project.org/ubuntu lucid syslog-ng" >> /etc/apt/sources.list
# Install syslog-ng 3.3 from packages
sudo apt-get install syslog-ng
# Copy the syslog-ng.conf
cp /usr/local/elsa/node/conf/syslog-ng.conf /etc/syslog-ng/conf.d/elsa.conf

# To build from source:
# Get and build syslog-ng
cd /tmp
wget "http://www.balabit.com/downloads/files/syslog-ng/open-source-edition/3.2.4/source/eventlog_0.2.12.tar.gz" &&
tar xzvf eventlog_0.2.12.tar.gz &&
cd eventlog-0.2.12 &&
./configure && make && sudo make install &&
cd /tmp &&
wget "http://www.balabit.com/downloads/files/syslog-ng/open-source-edition/3.2.4/source/syslog-ng_3.2.4.tar.gz" &&
tar xzvf syslog-ng_3.2.4.tar.gz &&
cd syslog-ng-3.2.4 &&
./configure --prefix=/usr/local/syslog-ng-3.2.4 --enable-ipv6 && 
make && sudo make install && 
sudo ln -s /usr/local/syslog-ng-3.2.4 /usr/local/syslog-ng
# Copy the syslog-ng.conf
sudo mkdir /usr/local/syslog-ng/etc/conf.d &&
sudo cp /usr/local/elsa/node/conf/syslog-ng.conf /usr/local/syslog-ng/etc/conf.d/elsa.conf &&
sudo mkdir /usr/local/syslog-ng/var

# Make data directories on node
sudo mkdir -p /data/elsa/log && sudo mkdir -p /data/elsa/tmp/buffers
sudo mkdir -p /data/sphinx/log

# Install mysql schema
mysqladmin -uroot create syslog && mysqladmin -uroot create syslog_data && 
mysql -uroot -e 'GRANT ALL ON syslog.* TO "elsa"@"%" IDENTIFIED BY "biglog"' &&
mysql -uroot -e 'GRANT ALL ON syslog_data.* TO "elsa"@"%" IDENTIFIED BY "biglog"' && 
mysql -uelsa -pbiglog syslog -e "source /usr/local/elsa/node/conf/schema.sql"

# Copy elsa.conf to /etc/
sudo cp /usr/local/elsa/node/conf/elsa.conf /etc/elsa_node.conf

# Edit the elsa_node.conf for any customizations
# Edit database and make user/pass match the web node install above
# Edit log_size_limit to be maximum space you'll allow for logs
# The other settings should be fine if you're using the dirs referred to in this doc.

# Run elsa.pl for initial creation of sphinx config
echo "" | perl /usr/local/elsa/node/elsa.pl -on -c /etc/elsa_node.conf

# Initialize empty sphinx indexes
/usr/local/sphinx/bin/indexer --config /usr/local/etc/sphinx.conf --rotate --all
# Start sphinx
/usr/local/sphinx/bin/searchd --config /usr/local/etc/sphinx.conf

# Start syslog-ng using the ELSA config
/usr/local/syslog-ng/sbin/syslog-ng -f /usr/local/elsa/node/conf/syslog-ng.conf

# Watch the log file to make sure it's working (after wiping indexes you should see batches processed and rows indexed)
tail -f /data/elsa/log/node.loga

# Test
loggen -Di -I 10 localhost 514
