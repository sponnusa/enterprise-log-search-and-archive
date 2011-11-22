#!/bin/sh

# EDIT CONFIG VARIABLES
BASE_DIR="/usr/local"
DATA_DIR="/data"
TMP_DIR="/tmp"

# Web DB settings
MYSQL_HOST="localhost"
MYSQL_PORT="3306"
MYSQL_DB="elsa_web"
MYSQL_USER="elsa"
MYSQL_PASS="biglog"

# These should be fine
EVENTLOG_VER="0.2.12"
SYSLOG_VER="3.2.4"

########################################

# Determine type of install
INSTALL=""
if [ "$1" = "node" ]; then
	INSTALL="node";
elif [ "$1" = "web" ]; then
	INSTALL="web";
else
	echo "Invoke with either $0 web or $0 node"
	exit;
fi

OP="ALL"
if [ "a$2" != "a" ]; then
	OP=$2
fi 

DISTRO="ubuntu"
MYSQL_SERVICE_NAME="mysql"
CRONTAB_DIR="crontabs"
WEB_USER="www-data"
CRON_SERVICE="cron"
if [ -f /etc/redhat-release ] || [ -f /etc/fedora-release ]; then
	DISTRO="centos"
	MYSQL_SERVICE_NAME="mysqld"
	CRONTAB_DIR=""
	WEB_USER="apache"
	CRON_SERVICE="crond"
elif [ -f /etc/SuSE-release ]; then
	DISTRO="suse"
	CRONTAB_DIR="tabs"
	WEB_USER="wwwrun"
fi
echo "Assuming distro to be $DISTRO"

centos_get_node_packages(){
	# Install required packages
	yum -y update
	yum -yq install flex bison ntpdate perl perl-devel curl make subversion gcc gcc-c++ mysql-server mysql-libs mysql-devel pkg-config pcre-devel libcap-devel libnet-devel libopenssl-devel glib2-devel
	return $?
}

suse_get_node_packages(){
	# Install required packages
	zypper -n update &&
	zypper -qn install ntp perl curl make subversion gcc gcc-c++ mysql-community-server libmysqlclient-devel pkg-config pcre-devel libcap-devel libnet-devel libopenssl-devel glib2-devel pam-devel
	return $?
}

ubuntu_get_node_packages(){
	apt-get update
	# Don't ask for mysql password
	echo "debconf debconf/frontend select noninteractive" | debconf-set-selections &&
	
	# Install required packages
	apt-get -qy install curl subversion gcc g++ mysql-server libmysqlclient-dev pkg-config libglib2.0-dev libpcre3-dev libcap-dev libnet1-dev libssl-dev &&
	
	# Make debconf interactive again
	echo "debconf debconf/frontend select readline" | debconf-set-selections
	return $?
}

set_date(){
	ntpdate time.nist.gov
	# we don't care about the error code, and sometimes ntpd blocks this
	return 0
}

get_elsa(){
	# Get the latest code from Google Code
	cd $BASE_DIR
	svn --force export "https://enterprise-log-search-and-archive.googlecode.com/svn/trunk/elsa" &&
	mkdir -p "$BASE_DIR/elsa/node/tmp/locks" && 
	touch "$BASE_DIR/elsa/node/tmp/locks/directory"
	return $?
}

build_node_perl(){
	# Install required Perl modules
	cd $TMP_DIR && curl -L http://cpanmin.us | perl - App::cpanminus
	# Now cpanm is available to install the rest
	cpanm Time::HiRes CGI Moose Config::JSON String::CRC32 Log::Log4perl DBD::mysql Date::Manip Sys::MemInfo &&
	# Force this because of a bug in the CentOS-specific distro detection
	cpanm -f Sys::Info
	return $?
}

enable_service(){
	if [ "$DISTRO" = "centos" ] || [ "$DISTRO" = "suse" ]; then
		chkconfig $1 on
		return $?
	fi
	update-rc.d $1 defaults
	return $?
}	

build_sphinx(){
	# Get and build sphinx on nodes
	cd $TMP_DIR &&
	svn --force export "https://sphinxsearch.googlecode.com/svn/trunk/" sphinx-svn &&
	cd sphinx-svn &&
	./configure --enable-id64 "--prefix=$BASE_DIR/sphinx" && make && make install &&
	mkdir -p $BASE_DIR/etc &&
	touch "$BASE_DIR/etc/sphinx_stopwords.txt" &&
	cp $BASE_DIR/elsa/contrib/searchd /etc/init.d/ &&
	enable_service "searchd"
	return $?
}

build_syslogng(){
	# Get and build syslog-ng
	cd $TMP_DIR &&
	curl "http://www.balabit.com/downloads/files/syslog-ng/open-source-edition/$SYSLOG_VER/source/eventlog_$EVENTLOG_VER.tar.gz" > "eventlog_$EVENTLOG_VER.tar.gz" &&
	tar xzvf "eventlog_$EVENTLOG_VER.tar.gz" &&
	cd "eventlog-$EVENTLOG_VER" &&
	./configure && make && make install &&
	echo "/usr/local/lib" >> /etc/ld.so.conf &&
	ln -fs "$BASE_DIR/lib/pkgconfig/eventlog.pc" /usr/lib/pkgconfig/ &&
	ldconfig &&
	cd $TMP_DIR &&
	curl "http://www.balabit.com/downloads/files/syslog-ng/open-source-edition/$SYSLOG_VER/source/syslog-ng_$SYSLOG_VER.tar.gz" > "syslog-ng_$SYSLOG_VER.tar.gz" &&
	tar xzvf "syslog-ng_$SYSLOG_VER.tar.gz" &&
	cd "syslog-ng-$SYSLOG_VER" &&
	./configure "--prefix=$BASE_DIR/syslog-ng-$SYSLOG_VER" --enable-ipv6 && 
	make && make install && 
	ln -fs "$BASE_DIR/syslog-ng-$SYSLOG_VER" "$BASE_DIR/syslog-ng" &&
	# Copy the syslog-ng.conf
	cat "$BASE_DIR/elsa/node/conf/syslog-ng.conf" | sed -e "s|\/usr\/local|$BASE_DIR|g" | sed -e "s|\/data|$DATA_DIR|g" > "$BASE_DIR/syslog-ng/etc/syslog-ng.conf" &&
	mkdir -p "$BASE_DIR/syslog-ng/var" &&
	cp $BASE_DIR/elsa/contrib/syslog-ng /etc/init.d/ &&
	enable_service "syslog-ng"
	return $?
}

mk_node_dirs(){
	# Make data directories on node
	mkdir -p "$DATA_DIR/elsa/log" && mkdir -p "$DATA_DIR/elsa/tmp/buffers" &&
	mkdir -p "$DATA_DIR/sphinx/log"
	return $?
}

set_node_mysql(){
	# Test to see if schema is already installed
	mysql -uelsa -pbiglog syslog -e "select count(*) from programs"
	if [ $? -eq 0 ]; then
		echo "MySQL and schema already installed."
		return 0;
	fi
	
	# Install mysql schema
	service $MYSQL_SERVICE_NAME start
	mysqladmin -uroot create syslog && mysqladmin -uroot create syslog_data && 
	mysql -uroot -e 'GRANT ALL ON syslog.* TO "elsa"@"localhost" IDENTIFIED BY "biglog"' &&
	mysql -uroot -e 'GRANT ALL ON syslog.* TO "elsa"@"%" IDENTIFIED BY "biglog"' &&
	mysql -uroot -e 'GRANT ALL ON syslog_data.* TO "elsa"@"localhost" IDENTIFIED BY "biglog"' &&
	mysql -uroot -e 'GRANT ALL ON syslog_data.* TO "elsa"@"%" IDENTIFIED BY "biglog"'
	
	# Above could fail with db already exists, but this is the true test for success
	mysql -uelsa -pbiglog syslog -e "source $BASE_DIR/elsa/node/conf/schema.sql"
	return $?
}

init_elsa(){
	# Copy elsa.conf to /etc/
	cat "$BASE_DIR/elsa/node/conf/elsa.conf" | sed -e "s|\/usr\/local|$BASE_DIR|g" | sed -e "s|\/data|$DATA_DIR|g" > /etc/elsa_node.conf &&
	
	# Run elsa.pl for initial creation of sphinx config
	echo "" | perl "$BASE_DIR/elsa/node/elsa.pl" -on -c /etc/elsa_node.conf &&
	
	# Initialize empty sphinx indexes
	"$BASE_DIR/sphinx/bin/indexer" --config "$BASE_DIR/etc/sphinx.conf" --rotate --all &&
	# Start sphinx
	service searchd restart &&
	
	# Start syslog-ng using the ELSA config
	service syslog-ng restart &&
	pgrep -f "elsa.pl" &&
	
	# Sleep to allow ELSA to initialize and validate its directory
	echo "Sleeping for 60 seconds to allow ELSA to init..."
	sleep 60
	return $?
}

test_elsa(){
	# Test
	echo "Sending test log messages..."
	"$BASE_DIR/syslog-ng/bin/loggen" -Di -I 1 127.0.0.1 514 &&
	
	# Sleep to allow ELSA to initialize and validate its directory
	echo "Sleeping for 60 seconds to allow ELSA to load batch..."
	sleep 60
	
	# Watch the log file to make sure it's working (after wiping indexes you should see batches processed and rows indexed)
	grep "Indexed temp_" "$DATA_DIR/elsa/log/node.log" | tail -1 | perl -e '$l = <>; $l =~ /Indexed temp_\d+ with (\d+)/; if ($1 > 1){ exit 0; } exit 1;'
	return $?
}

suse_get_web_packages(){
	# Install required packages
	zypper -n update &&
	zypper -qn install curl subversion make gcc gcc-c++ mysql-community-server-client libmysqlclient-devel apache2-prefork apache2-mod_perl apache2-mod_perl-devel
	return $?
}

ubuntu_get_web_packages(){
	apt-get update
	# Make debconf noninteractive
	echo "debconf debconf/frontend select noninteractive" | debconf-set-selections &&
	
	# Install required packages
	apt-get -qy install curl subversion gcc g++ mysql-client libmysqlclient-dev apache2-mpm-prefork libapache2-mod-perl2 libpam0g-dev &&
	
	# Make debconf interactive again
	echo "debconf debconf/frontend select readline" | debconf-set-selections
	return $?
}

centos_get_web_packages(){
	yum update &&
	yum -yq install curl subversion make gcc gcc-c++ mysql mysql-libs mysql-devel httpd mod_perl pam-devel setools-console
	return $?
}

build_web_perl(){
	# Install required Perl modules
	cd $TMP_DIR && curl -L http://cpanmin.us | perl - App::cpanminus &&
	# Now cpanm is available to install the rest
	cpanm -f Sys::Info
	# PAM requires some user input for testing, and we don't want that
	cpanm --notest Authen::PAM &&
	cpanm Time::HiRes Moose Config::JSON Plack::Builder Plack::Util Plack::App::File Date::Manip Digest::SHA1 MIME::Base64 URI::Escape Socket Net::DNS Sys::Hostname::FQDN String::CRC32 CHI CHI::Driver::RawMemory Search::QueryParser AnyEvent::DBI DBD::mysql EV Sys::Info Sys::MemInfo MooseX::Traits Authen::Simple Authen::Simple::PAM Plack::Middleware::CrossOrigin URI::Escape Module::Pluggable Module::Install PDF::API2::Simple XML::Writer Parse::Snort Spreadsheet::WriteExcel IO::String Mail::Internet Plack::Middleware::Static Log::Log4perl Email::LocalDelivery Plack::Session
	return $?
}

set_web_mysql(){
	# Test to see if schema is already installed
	mysql "-h$MYSQL_HOST" "-P$MYSQL_PORT" "-u$MYSQL_USER" "-p$MYSQL_PASS" $MYSQL_DB -e "select count(*) from users"
	if [ $? -eq 0 ]; then
		echo "MySQL and schema already installed."
		return 0;
	fi
	
	# Install mysql schema
	mysqladmin "-h$MYSQL_HOST" "-P$MYSQL_PORT" -uroot create elsa_web &&
	mysql "-h$MYSQL_HOST" "-P$MYSQL_PORT" -uroot -e "GRANT ALL ON $MYSQL_DB.* TO \"$MYSQL_USER\"@\"localhost\" IDENTIFIED BY \"$MYSQL_PASS\"" &&
	mysql "-h$MYSQL_HOST" "-P$MYSQL_PORT" -uroot -e "GRANT ALL ON $MYSQL_DB.* TO \"$MYSQL_USER\"@\"%\" IDENTIFIED BY \"$MYSQL_PASS\"" &&
	mysql "-h$MYSQL_HOST" "-P$MYSQL_PORT" "-u$MYSQL_USER" "-p$MYSQL_PASS" $MYSQL_DB -e "source $BASE_DIR/elsa/web/conf/meta_db_schema.mysql"
	return $?
}

mk_web_dirs(){
	# Copy elsa.conf to /etc/
	cat "$BASE_DIR/elsa/web/conf/elsa.conf" | sed -e "s|\/usr\/local|$BASE_DIR|g" | sed -e "s|\/data|$DATA_DIR|g" > /etc/elsa_web.conf
	
	# Make data directories on node
	mkdir -p "$DATA_DIR/elsa/log" &&
	touch "$DATA_DIR/elsa/log/web.log" &&
	chown -R $WEB_USER "$DATA_DIR/elsa/log"
	return $?
}

suse_set_apache(){
	# For Apache, locations vary, but this is the gist:
	cpanm Plack::Handler::Apache2 &&
	cat "$BASE_DIR/elsa/web/conf/apache_site.conf" | sed -e "s|\/usr\/local|$BASE_DIR|g" | sed -e "s|\/data|$DATA_DIR|g" > /etc/apache2/vhosts.d/elsa.conf &&
	# Allow firewall port for apache web server
	echo "opening firewall port 80" &&
	cp /etc/sysconfig/SuSEfirewall2 /etc/sysconfig/SuSEfirewall2.bak_by_elsa && 
	cat /etc/sysconfig/SuSEfirewall2.bak_by_elsa | sed -e "s|FW_CONFIGURATIONS_EXT=\"|FW_CONFIGURATIONS_EXT=\"apache2 |" > /etc/sysconfig/SuSEfirewall2 &&
	SuSEfirewall2 &&
	 
	# Enable the site
	a2enmod rewrite &&
	a2enmod perl &&
	echo "LoadModule perl_module                 /usr/lib/apache2/mod_perl.so" >> /etc/apache2/sysconfig.d/loadmodule.conf &&
	# Verify that we can write to logs
	chown -R $WEB_USER "$DATA_DIR/elsa/log" &&
	service apache2 restart
	return $?
}

ubuntu_set_apache(){
	# For Apache, locations vary, but this is the gist:
	cpanm Plack::Handler::Apache2 &&
	cat "$BASE_DIR/elsa/web/conf/apache_site.conf" | sed -e "s|\/usr\/local|$BASE_DIR|g" | sed -e "s|\/data|$DATA_DIR|g" > /etc/apache2/sites-available/elsa &&
	# Enable the site
	a2ensite elsa &&
	a2dissite default &&
	a2enmod rewrite &&
	chown -R $WEB_USER "$DATA_DIR/elsa/log" &&
	service apache2 restart
	return $?
}

centos_set_apache(){
	# For Apache, locations vary, but this is the gist:
	cpanm Plack::Handler::Apache2 &&
	cat "$BASE_DIR/elsa/web/conf/apache_site.conf" | sed -e "s|\/usr\/local|$BASE_DIR|g" | sed -e "s|\/data|$DATA_DIR|g" > /etc/httpd/conf.d/ZZelsa.conf &&
	
	# Verify that we can write to logs
	chown -R $WEB_USER "$DATA_DIR/elsa/log" &&
	echo "Enabling SELINUX policies for Apache..." &&
	chcon --reference=/var/log/httpd/error_log -R $DATA_DIR &&
	setsebool -P httpd_can_network_connect on &&
	service httpd restart
	# Set firewall
	echo "opening firewall port 80" &&
	cp /etc/sysconfig/iptables /etc/sysconfig/iptables.bak.elsa &&
	cat /etc/sysconfig/iptables.bak.elsa | sed -e "s|-A INPUT -i lo -j ACCEPT|-A INPUT -i lo -j ACCEPT\n-A INPUT -m state --state NEW -m tcp -p tcp --dport 80 -j ACCEPT|" > /etc/sysconfig/iptables &&
	service iptables restart
	return $?
}

set_cron(){
	# Setup alerts (optional)
	echo "Adding cron entry for alerts..."
	# Edit /etc/elsa_web.conf and set the "smtp_server" and "to" fields under "email"
	grep "elsa/web/cron.pl" /var/spool/cron/$CRONTAB_DIR/root
	if [ $? -eq 0 ]; then
		echo "Cron already installed"
		return 0;
	fi
	
	echo "* * * * * perl $BASE_DIR/elsa/web/cron.pl -c /etc/elsa_web.conf 2>&1 > /dev/null" >> /var/spool/cron/$CRONTAB_DIR/root &&
	chmod 600 /var/spool/cron/$CRONTAB_DIR/root &&
	service $CRON_SERVICE restart
	return $?
}

exec_func(){
	RETVAL=1
	FUNCTION=$1
	echo "Executing $FUNCTION"
	$FUNCTION
	RETVAL=$?
	if [ $RETVAL -eq 0 ]; then
	        echo "$FUNCTION success"
	else
	        echo "$FUNCTION FAIL" && exit
	fi
}

if [ "$INSTALL" = "node" ]; then
	if [ "$OP" = "ALL" ]; then
		for FUNCTION in $DISTRO"_get_node_packages" "set_date" "get_elsa" "build_node_perl" "build_sphinx" "build_syslogng" "mk_node_dirs" "set_node_mysql" "init_elsa" "test_elsa"; do
			exec_func $FUNCTION
		done
	else
		exec_func $OP
	fi
elif [ "$INSTALL" = "web" ]; then
	if [ "$OP" = "ALL" ]; then
		for FUNCTION in $DISTRO"_get_web_packages" "set_date" "get_elsa" "build_web_perl" "set_web_mysql" "mk_web_dirs" $DISTRO"_set_apache" "set_cron"; do
			exec_func $FUNCTION
		done
	else
		exec_func $OP
	fi
fi

