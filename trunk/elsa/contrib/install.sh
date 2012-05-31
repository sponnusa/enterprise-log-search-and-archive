#!/bin/sh

export PATH=$PATH:/usr/local/bin

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
MYSQL_ROOT_USER="root"
MYSQL_ROOT_PASS=""

# These should be fine
EVENTLOG_VER="0.2.12"
SYSLOG_VER="3.2.4"
GEOIP_DIR="/usr/local/share/GeoIP/"

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
CRONTAB_DIR="/var/spool/cron/crontabs"
WEB_USER="www-data"
CRON_SERVICE="cron"
INIT_DIR=/etc/init.d/
if [ -f /etc/redhat-release ] || [ -f /etc/fedora-release ]; then
	DISTRO="centos"
	MYSQL_SERVICE_NAME="mysqld"
	CRONTAB_DIR="/var/spool/cron"
	WEB_USER="apache"
	CRON_SERVICE="crond"
elif [ -f /etc/SuSE-release ]; then
	DISTRO="suse"
	CRONTAB_DIR="/var/spool/cron/tabs"
	WEB_USER="wwwrun"
elif [ -f /etc/freebsd-update.conf ]; then
	DISTRO="freebsd"
	CRONTAB_DIR="/var/cron/tabs"
	INIT_DIR=/usr/local/etc/rc.d/
	WEB_USER="www"
	# FreeBSD does better over HTTP than FTP
	export PACKAGEROOT="http://ftp.freebsd.org"
fi
echo "Assuming distro to be $DISTRO"

MYSQL_PASS_SWITCH=""
if [ "$MYSQL_ROOT_PASS" != "" ]; then
    MYSQL_PASS_SWITCH="-p$MYSQL_ROOT_PASS"
fi

centos_get_node_packages(){
	# Install required packages
	yum -y update
	yum -yq install flex bison ntpdate perl perl-devel curl make subversion gcc gcc-c++ mysql-server mysql-libs mysql-devel pkg-config pkgconfig pcre-devel libcap-devel libnet-devel openssl-devel libopenssl-devel glib2-devel
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

freebsd_get_node_packages(){
	pkg_add -Fr subversion wget curl mysql55-server perl syslog-ng p5-App-cpanminus &&
	enable_service "mysql" &&
	service mysql-server start &&
	disable_service "syslogd" &&
	# This could fail if it's already disabled
	service syslogd stop
	
	# Check to see if we got syslog-ng v3 from pkg_add
	pkg_info -E -x syslog-ng | cut -d\- -f3 | egrep "^3\."
	if [ $? -eq 1 ]; then
		echo "Added old syslog-ng, correcting with syslog-ng3"
		pkg_delete $(pkg_info -E -x syslog-ng) &&
		pkg_add -r syslog-ng3
	fi
	
	if [ \! -f /usr/local/etc/syslog-ng.conf ]; then
		cp /usr/local/etc/syslog-ng.conf.dist /usr/local/etc/syslog-ng.conf
	fi
	if [ \! -f /usr/local/etc/elsa_syslog-ng.conf ]; then
		# Copy the syslog-ng.conf
		echo "Creating elsa_syslog-ng.conf"
		cat "$BASE_DIR/elsa/node/conf/syslog-ng.conf" | sed -e "s|\/usr\/local|$BASE_DIR|g" | sed -e "s|\/data|$DATA_DIR|g" > "/usr/local/etc/elsa_syslog-ng.conf" &&
		echo "@include \"elsa_syslog-ng.conf\"" >> /usr/local/etc/syslog-ng.conf
	fi
	enable_service "syslog-ng" &&
	service syslog-ng start
	pgrep syslog-ng
	
	return $?
}	

freebsd_get_node_packages_ports(){
	portsnap update
	if [ $? -ne 0 ]; then
		portsnap extract
	fi
		
	# Install subversion
	if [ \! -f /usr/local/bin/svn ]; then
		cd /usr/ports/devel/subversion && make install clean
	fi
	
	# Install curl
	if [ \! -f /usr/local/bin/curl ]; then
		cd /usr/ports/ftp/curl && make install clean
	fi
	
	# Install MySQL client and server
	if [ \! -f /usr/local/bin/mysql ] || [ \! -f /usr/local/bin/mysqld_safe ]; then
		cd /usr/ports/databases/mysql55-server &&
		make install clean;
	
		# Enable MySQL
		echo 'mysql_enable="YES"' >> /etc/rc.conf
		service mysql-server start
		# Turn on ARCHIVE engine
		mysql -e "install plugin archive soname 'ha_archive.so'"
	fi
	
	# Install Perl
	if [ \! -f /usr/local/bin/perl ]; then
		cd /usr/ports/lang/perl5.10 && make install clean
	fi
	
	# These should happen automatically because of the syslog-ng install
	## Install libnet
	#if [ \! -f /usr/local/include/libnet115/libnet.h ]; then
	#	cd /usr/ports/net/libnet-devel && make install clean
	#fi
	
	## Install glib-2.0
	#if [ \! -f /usr/local/include/glib-2.0/glib.h ]; then
	#	cd /usr/ports/devel/glib20 && make install clean
	#fi
	
	## Install OpenSSL
	#if [ \! -d /usr/local/include/openssl ]; then
	#	cd /usr/ports/security/openssl && make install clean
	#fi
	
	# Install Syslog-NG
	if [ \! -f /usr/local/sbin/syslog-ng ]; then
		cd /usr/ports/sysutils/syslog-ng && make install clean
	fi
	
	if [ \! -f /usr/local/etc/elsa_syslog-ng.conf ]; then
		# Copy the syslog-ng.conf
		echo "Creating elsa_syslog-ng.conf"
		cat "$BASE_DIR/elsa/node/conf/syslog-ng.conf" | sed -e "s|\/usr\/local|$BASE_DIR|g" | sed -e "s|\/data|$DATA_DIR|g" > "/usr/local/etc/elsa_syslog-ng.conf" &&
		echo "@include \"elsa_syslog-ng.conf\"" >> /usr/local/etc/syslog-ng.conf
	fi
	
	disable_service "syslogd" &&
	# This could fail if it's already disabled
	service syslogd stop
	enable_service "syslog-ng" &&
	service syslog-ng restart
	
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
	svn --non-interactive --trust-server-cert --force export "https://enterprise-log-search-and-archive.googlecode.com/svn/trunk/elsa" &&
	mkdir -p "$BASE_DIR/elsa/node/tmp/locks" && 
	touch "$BASE_DIR/elsa/node/tmp/locks/directory"
	return $?
}

build_node_perl(){
	# Install required Perl modules
	
	if [ \! -f /usr/local/bin/cpanm ]; then
		cd $TMP_DIR && curl --insecure -L http://cpanmin.us | perl - App::cpanminus
	fi
	
	# FreeBSD has trouble testing with the current version of ExtUtils
	if [ "$DISTRO" = "freebsd" ]; then
		cpanm -n ExtUtils::MakeMaker
		# This can fail when installing via cpanm, so we'll have ports build it
		cd /usr/ports/devel/p5-Sys-MemInfo && make install clean
	else 
		cpanm Sys::MemInfo
	fi
	
	if [ "$DISTRO" = "centos" ]; then
		# No test because of a bug in the CentOS-specific distro detection
		cpanm -n Sys::Info
	fi
	
	RETVAL=0
	# Now cpanm is available to install the rest
	for RETRY in 1 2 3; do
		cpanm Time::HiRes CGI Moose Config::JSON String::CRC32 Log::Log4perl DBD::mysql Date::Manip Sys::Info
		RETVAL=$?
		if [ "$RETVAL" = 0 ]; then
			break;
		fi
		echo "Retry $RETRY"
	done
	
	return $RETVAL
}

enable_service(){
	if [ "$DISTRO" = "centos" ] || [ "$DISTRO" = "suse" ]; then
		chkconfig $1 on
		return $?
	elif [ "$DISTRO" = "ubuntu" ]; then
		update-rc.d $1 defaults
	elif [ "$DISTRO" = "freebsd" ]; then
		SVC_NAME=$(echo $1 | sed -e "s|\-|\_|g")
		grep $SVC_NAME"_enable=\"YES\"" /etc/rc.conf
		if [ $? -ne 0 ]; then
			echo "Editing /etc/rc.conf to enable $1"
			echo $SVC_NAME"_enable=\"YES\"" >> /etc/rc.conf
		fi
	fi
	return $?
}	

disable_service(){
	if [ "$DISTRO" = "centos" ] || [ "$DISTRO" = "suse" ]; then
		chkconfig $1 off
		return $?
	elif [ "$DISTRO" = "ubuntu" ]; then
		update-rc.d $1 disable
	elif [ "$DISTRO" = "freebsd" ]; then
		SVC_NAME=$(echo $1 | sed -e "s|\-|\_|g")
		grep $SVC_NAME"_enable=\"NO\"" /etc/rc.conf
		if [ $? -ne 0 ]; then
			echo "Editing /etc/rc.conf to disable $1"
			echo $SVC_NAME"_enable=\"NO\"" >> /etc/rc.conf
		fi
	fi
	return $?
}	

build_sphinx(){
	# Get and build sphinx on nodes
	cd $TMP_DIR &&
	svn --non-interactive --trust-server-cert --force export "https://sphinxsearch.googlecode.com/svn/trunk/" sphinx-svn &&
	cd sphinx-svn &&
	./configure --enable-id64 "--prefix=$BASE_DIR/sphinx" && make && make install &&
	mkdir -p $BASE_DIR/etc &&
	touch "$BASE_DIR/etc/sphinx_stopwords.txt"
	if [ "$DISTRO" = "freebsd" ]; then
		cp $BASE_DIR/elsa/contrib/searchd.freebsd $INIT_DIR/searchd
	else
		cp $BASE_DIR/elsa/contrib/searchd $INIT_DIR
	fi
	enable_service "searchd"
	return $?
}

build_syslogng(){
	# we already installed on FreeBSD
	if [ "$DISTRO" = "freebsd" ]; then
		grep "elsa_syslog-ng.conf" /usr/local/etc/syslog-ng.conf
		if [ $? -eq 1 ]; then
			# Copy the syslog-ng.conf
			echo "Creating elsa_syslog-ng.conf"
			cat "$BASE_DIR/elsa/node/conf/syslog-ng.conf" | sed -e "s|\/usr\/local|$BASE_DIR|g" | sed -e "s|\/data|$DATA_DIR|g" > "/usr/local/etc/elsa_syslog-ng.conf" &&
			echo "@include \"elsa_syslog-ng.conf\"" >> /usr/local/etc/syslog-ng.conf &&
			service syslog-ng restart
		fi
		return $?
	fi
	# Get and build syslog-ng
	cd $TMP_DIR &&
	curl "http://www.balabit.com/downloads/files/syslog-ng/open-source-edition/$SYSLOG_VER/source/eventlog_$EVENTLOG_VER.tar.gz" > "eventlog_$EVENTLOG_VER.tar.gz" &&
	tar xzvf "eventlog_$EVENTLOG_VER.tar.gz" &&
	cd "eventlog-$EVENTLOG_VER" &&
	./configure && make && make install &&
	echo "/usr/local/lib" >> /etc/ld.so.conf
	if [ -d /usr/lib64/pkgconfig ]; then
		ln -fs "$BASE_DIR/lib/pkgconfig/eventlog.pc" /usr/lib64/pkgconfig/
	fi 
	if [ -d /usr/lib/pkgconfig ]; then
		ln -fs "$BASE_DIR/lib/pkgconfig/eventlog.pc" /usr/lib/pkgconfig/
	fi 
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
	cp $BASE_DIR/elsa/contrib/syslog-ng $INIT_DIR &&
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
	mysql -uelsa -p$MYSQL_PASS syslog -e "select count(*) from programs"
	if [ $? -eq 0 ]; then
		echo "MySQL and schema already installed."
		return 0;
	fi
	
	# Install mysql schema
	service $MYSQL_SERVICE_NAME start
	mysqladmin -u$MYSQL_ROOT_USER $MYSQL_PASS_SWITCH create syslog && mysqladmin -u$MYSQL_ROOT_USER $MYSQL_PASS_SWITCH create syslog_data && 
	mysql -u$MYSQL_ROOT_USER $MYSQL_PASS_SWITCH -e 'GRANT ALL ON syslog.* TO "elsa"@"localhost" IDENTIFIED BY "'$MYSQL_PASS'"' &&
	mysql -u$MYSQL_ROOT_USER $MYSQL_PASS_SWITCH -e 'GRANT ALL ON syslog.* TO "elsa"@"%" IDENTIFIED BY "'$MYSQL_PASS'"' &&
	mysql -u$MYSQL_ROOT_USER $MYSQL_PASS_SWITCH -e 'GRANT ALL ON syslog_data.* TO "elsa"@"localhost" IDENTIFIED BY "'$MYSQL_PASS'"' &&
	mysql -u$MYSQL_ROOT_USER $MYSQL_PASS_SWITCH -e 'GRANT ALL ON syslog_data.* TO "elsa"@"%" IDENTIFIED BY "'$MYSQL_PASS'"'
	
	# Above could fail with db already exists, but this is the true test for success
	mysql -uelsa -p$MYSQL_PASS syslog -e "source $BASE_DIR/elsa/node/conf/schema.sql" &&
	enable_service "$MYSQL_SERVICE_NAME"
	return $?
}

update_node_mysql(){
	echo "Updating MySQL..."
}

init_elsa(){
	# Copy elsa.conf to /etc/
	cat "$BASE_DIR/elsa/node/conf/elsa.conf" | sed -e "s/biglog/$MYSQL_PASS/g" | sed -e "s|\/usr\/local|$BASE_DIR|g" | sed -e "s|\/data|$DATA_DIR|g" > /etc/elsa_node.conf &&
	
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

restart_elsa(){
	service syslog-ng restart
	service searchd restart
	pgrep -f "elsa.pl" && pgrep searchd
	return $?
}

test_elsa(){
	# Test
	echo "Sending test log messages..."
	if [ "$DISTRO" = "freebsd" ]; then
		loggen -Di -I 1 127.0.0.1 514
	else
		"$BASE_DIR/syslog-ng/bin/loggen" -Di -I 1 127.0.0.1 514
	fi
	
	# Sleep to allow ELSA to initialize and validate its directory
	echo "Sleeping for 60 seconds to allow ELSA to load batch..."
	sleep 60
	
	# Watch the log file to make sure it's working (after wiping indexes you should see batches processed and rows indexed)
	grep "Indexed temp_" "$DATA_DIR/elsa/log/node.log" | tail -1 | perl -e '$l = <>; $l =~ /Indexed temp_\d+ with (\d+)/; if ($1 > 1){ exit 0; } exit 1;'
	return $?
}

set_logrotate(){
	if [ -d /etc/logrotate.d ]; then
		echo "$DATA_DIR/elsa/log/*log {
	size 100M
	create 640 $WEB_USER root
	rotate 4
	missingok
	notifempty
	compress
	maxage 60
}" > /etc/logrotate.d/elsa
	else
		echo "WARNING: No /etc/logrotate.d directory not found, not installing ELSA utility log rotation"
	fi
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
	apt-get -qy install curl subversion gcc g++ mysql-client libmysqlclient-dev apache2-mpm-prefork libapache2-mod-perl2 libpam0g-dev make libgeoip-dev &&
	
	# Make debconf interactive again
	echo "debconf debconf/frontend select readline" | debconf-set-selections
	return $?
}

centos_get_web_packages(){
	yum update &&
	yum -yq install curl subversion make gcc gcc-c++ mysql mysql-libs mysql-devel httpd mod_perl pam-devel setools-console
	return $?
}

freebsd_get_web_packages(){
	cd /usr/ports/www/mod_perl2 && make install clean
	pkg_add -vFr subversion curl mysql55-client perl p5-App-cpanminus
	RET=$?
	# pkg_add will return 6 when packages were already present
	if [ "$RET" -ne 0 ] && [ "$RET" -ne 6 ]; then
		echo "retval was $RET"
		return 1
	fi
	
	# Edit the load modules file to disable unique_id, as it causes problems when host does not have FQDN
	cp /usr/local/etc/apache2/httpd.conf /usr/local/etc/apache2/httpd.conf.bak &&
	cat /usr/local/etc/apache2/httpd.conf.bak | sed -e "s|LoadModule unique_id_module|#LoadModule unique_id_module|" > /usr/local/etc/apache2/httpd.conf &&
	
	enable_service "apache2" &&
	service apache2 start
	pgrep httpd
		
	return $?
}	

build_web_perl(){
	# Install required Perl modules
	
	if [ \! -f /usr/local/bin/cpanm ]; then
		cd $TMP_DIR && curl --insecure -L http://cpanmin.us | perl - App::cpanminus
	fi
	
	# FreeBSD has trouble testing with the current version of ExtUtils
	if [ "$DISTRO" = "freebsd" ]; then
		cpanm -n ExtUtils::MakeMaker
	fi
	
	if [ "$DISTRO" = "centos" ]; then
		# No test because of a bug in the CentOS-specific distro detection
		cpanm -n Sys::Info
	fi
		
	# Now cpanm is available to install the rest
	RETVAL=0
	# Now cpanm is available to install the rest
	for RETRY in 1 2 3; do
		# PAM requires some user input for testing, and we don't want that
		cpanm -n Authen::PAM &&
		cpanm Time::HiRes Moose Config::JSON Plack::Builder Plack::Util Plack::App::File Date::Manip Digest::SHA1 MIME::Base64 URI::Escape Socket Net::DNS Sys::Hostname::FQDN String::CRC32 CHI CHI::Driver::RawMemory Search::QueryParser AnyEvent::DBI DBD::mysql EV Sys::Info Sys::MemInfo MooseX::Traits Authen::Simple Authen::Simple::PAM Authen::Simple::DBI Authen::Simple::LDAP Net::LDAP::Express Net::LDAP::FilterBuilder Plack::Middleware::CrossOrigin URI::Escape Module::Pluggable Module::Install PDF::API2::Simple XML::Writer Parse::Snort Spreadsheet::WriteExcel IO::String Mail::Internet Plack::Middleware::Static Log::Log4perl Email::LocalDelivery Plack::Session Sys::Info CHI::Driver::DBI Plack::Builder::Conditionals AnyEvent::HTTP URL::Encode MooseX::ClassAttribute Data::Serializable MooseX::Log::Log4perl Authen::Simple::DBI Plack::Middleware::NoMultipleSlashes MooseX::Storage MooseX::Clone
		RETVAL=$?
		if [ "$RETVAL" = 0 ]; then
			break;
		fi
		echo "Retry $RETRY"
	done
	
	echo "Retrieving GeoIP databases..."
	mkdir -p /usr/local/share/GeoIP &&
	wget -O $TMP_DIR/GeoLiteCity.dat.gz "http://geolite.maxmind.com/download/geoip/database/GeoLiteCity.dat.gz" &&
	gunzip $TMP_DIR/GeoLiteCity.dat.gz &&
	cp $TMP_DIR/GeoLiteCity.dat $GEOIP_DIR/GeoIPCity.dat
	wget -O $TMP_DIR/GeoIP.dat.gz "http://geolite.maxmind.com/download/geoip/database/GeoLiteCountry/GeoIP.dat.gz" &&
	gunzip $TMP_DIR/GeoIP.dat.gz &&
	cp $TMP_DIR/GeoIP.dat $GEOIP_DIR/ &&
	echo "...done."
	
	if [ "$DISTRO" = "ubuntu" ]; then
		# C API was installed already, proceed normally
		cpanm Geo::IP
	else
		echo "Using slower pure-Perl GeoIP library, install GeoIP C library for faster version" 
		wget -O $TMP_DIR/Geo-IP-1.40.tar.gz "http://search.cpan.org/CPAN/authors/id/B/BO/BORISZ/Geo-IP-1.40.tar.gz" &&
		cd $TMP_DIR && tar xzvf Geo-IP-1.40.tar.gz && cd Geo-IP-1.40 &&
		perl Makefile.PL PP=1 && make && make test && make install
	fi
	
	return $RETVAL
}

set_web_mysql(){
	# Test to see if schema is already installed
	mysql "-h$MYSQL_HOST" "-P$MYSQL_PORT" "-u$MYSQL_USER" "-p$MYSQL_PASS" $MYSQL_DB -e "select count(*) from users"
	if [ $? -eq 0 ]; then
		echo "MySQL and schema already installed."
		return 0;
	fi
	
	# Install mysql schema
	mysqladmin "-h$MYSQL_HOST" "-P$MYSQL_PORT" -u$MYSQL_ROOT_USER $MYSQL_PASS_SWITCH create $MYSQL_DB &&
	mysql "-h$MYSQL_HOST" "-P$MYSQL_PORT" -u$MYSQL_ROOT_USER $MYSQL_PASS_SWITCH -e "GRANT ALL ON $MYSQL_DB.* TO \"$MYSQL_USER\"@\"localhost\" IDENTIFIED BY \"$MYSQL_PASS\"" &&
	mysql "-h$MYSQL_HOST" "-P$MYSQL_PORT" -u$MYSQL_ROOT_USER $MYSQL_PASS_SWITCH -e "GRANT ALL ON $MYSQL_DB.* TO \"$MYSQL_USER\"@\"%\" IDENTIFIED BY \"$MYSQL_PASS\"" &&
	mysql "-h$MYSQL_HOST" "-P$MYSQL_PORT" "-u$MYSQL_USER" "-p$MYSQL_PASS" $MYSQL_DB -e "source $BASE_DIR/elsa/web/conf/meta_db_schema.mysql"
	return $?
}

update_web_mysql(){
	echo "Updating web MySQL, please ignore any errors for this section..."
	mysql "-h$MYSQL_HOST" "-P$MYSQL_PORT" "-u$MYSQL_USER" "-p$MYSQL_PASS" $MYSQL_DB -e "ALTER TABLE query_schedule DROP COLUMN action_params" &&
	mysql "-h$MYSQL_HOST" "-P$MYSQL_PORT" "-u$MYSQL_USER" "-p$MYSQL_PASS" $MYSQL_DB -e "ALTER TABLE query_schedule DROP FOREIGN KEY `query_schedule_ibfk_2`" &&
	mysql "-h$MYSQL_HOST" "-P$MYSQL_PORT" "-u$MYSQL_USER" "-p$MYSQL_PASS" $MYSQL_DB -e "ALTER TABLE query_schedule DROP COLUMN action_id" &&
	mysql "-h$MYSQL_HOST" "-P$MYSQL_PORT" "-u$MYSQL_USER" "-p$MYSQL_PASS" $MYSQL_DB -e "ALTER TABLE query_schedule ADD COLUMN connector VARCHAR(255)" &&
	mysql "-h$MYSQL_HOST" "-P$MYSQL_PORT" "-u$MYSQL_USER" "-p$MYSQL_PASS" $MYSQL_DB -e "ALTER TABLE query_schedule ADD COLUMN params VARCHAR(8000)" > /dev/null 2>&1
	mysql "-h$MYSQL_HOST" "-P$MYSQL_PORT" "-u$MYSQL_USER" "-p$MYSQL_PASS" $MYSQL_DB -e "ALTER TABLE query_log ADD KEY(archive)" > /dev/null 2>&1
	# The above can all fail for perfectly fine reasons
	echo "Finished updating MySQL"
	return 0
}

mk_web_dirs(){
	# Copy elsa.conf to /etc/
	cat "$BASE_DIR/elsa/web/conf/elsa.conf" | sed -e "s/biglog/$MYSQL_PASS/g" | sed -e "s|\/usr\/local|$BASE_DIR|g" | sed -e "s|\/data|$DATA_DIR|g" > /etc/elsa_web.conf
	
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
	#echo "opening firewall port 80" &&
	#cp /etc/sysconfig/SuSEfirewall2 /etc/sysconfig/SuSEfirewall2.bak_by_elsa && 
	#cat /etc/sysconfig/SuSEfirewall2.bak_by_elsa | sed -e "s|FW_CONFIGURATIONS_EXT=\"|FW_CONFIGURATIONS_EXT=\"apache2 |" > /etc/sysconfig/SuSEfirewall2 &&
	#SuSEfirewall2 &&
	 
	# Enable the site
	a2enmod rewrite &&
	a2enmod perl &&
	echo "LoadModule perl_module                 /usr/lib/apache2/mod_perl.so" >> /etc/apache2/sysconfig.d/loadmodule.conf &&
	# Verify that we can write to logs
	chown -R $WEB_USER "$DATA_DIR/elsa/log" &&
	service apache2 restart
	enable_service "apache2"
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
	enable_service "apache2"
	return $?
}

centos_set_apache(){
	# For Apache, locations vary, but this is the gist:
	cpanm Plack::Handler::Apache2 &&
	cat "$BASE_DIR/elsa/web/conf/apache_site.conf" | sed -e "s|\/usr\/local|$BASE_DIR|g" | sed -e "s|\/data|$DATA_DIR|g" > /etc/httpd/conf.d/ZZelsa.conf &&
	
	# Verify that we can write to logs
	chown -R $WEB_USER "$DATA_DIR/elsa/log"
	echo "Enabling SELINUX policies for Apache..."
	chcon --reference=/var/log/httpd -R $DATA_DIR
	setsebool -P httpd_can_network_connect on
	setsebool -P httpd_can_network_connect_db on
	service httpd restart
	enable_service "httpd"
	# Set firewall
	#echo "opening firewall port 80" &&
	#cp /etc/sysconfig/iptables /etc/sysconfig/iptables.bak.elsa &&
	#cat /etc/sysconfig/iptables.bak.elsa | sed -e "s|-A INPUT -i lo -j ACCEPT|-A INPUT -i lo -j ACCEPT\n-A INPUT -m state --state NEW -m tcp -p tcp --dport 80 -j ACCEPT|" > /etc/sysconfig/iptables &&
	#service iptables restart
	return $?
}

freebsd_set_apache(){
	# For Apache, locations vary, but this is the gist:
	egrep "^LoadModule perl_module" /usr/local/etc/apache2/httpd.conf
	if [ $? -ne 0 ]; then
		echo "Enabling mod_perl"
		echo "LoadModule perl_module libexec/apache2/mod_perl.so" >> /usr/local/etc/apache2/httpd.conf
	fi
	cpanm Plack::Handler::Apache2 &&
	cat "$BASE_DIR/elsa/web/conf/apache_site.conf" | sed -e "s|\/usr\/local|$BASE_DIR|g" | sed -e "s|\/data|$DATA_DIR|g" > /usr/local/etc/apache2/Includes/elsa.conf &&
	chown -R $WEB_USER "$DATA_DIR/elsa/log" &&
	service apache2 restart
	
	return $?
}

set_cron(){
	# Setup alerts (optional)
	echo "Adding cron entry for alerts..."
	# Edit /etc/elsa_web.conf and set the "smtp_server" and "to" fields under "email"
	grep "elsa/web/cron.pl" $CRONTAB_DIR/root
	if [ $? -eq 0 ]; then
		echo "Cron already installed"
		return 0;
	fi
	
	echo "* * * * * perl $BASE_DIR/elsa/web/cron.pl -c /etc/elsa_web.conf > /dev/null 2>&1" >> $CRONTAB_DIR/root &&
	chmod 600 $CRONTAB_DIR/root &&
	service $CRON_SERVICE restart
	return $?
}

check_svn_proxy(){
	if [ "$http_proxy" != "" ] || [ "$https_proxy" != "" ]; then
		echo "http_proxy set, verifying subversion is setup accordingly..."
		grep "http-proxy-host" /etc/subversion/servers | grep -v "#"
		if [ $? -eq 1 ]; then
			echo "ERROR: Please set the proxy settings in /etc/subversion/servers before continuing"
			return 1
		fi
		if [ "$https_proxy" == "" ]; then
			echo "ERROR: Please set the $https_proxy environment variable"
			return 1
		fi
	fi
	return 0
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
		for FUNCTION in $DISTRO"_get_node_packages" "set_date" "check_svn_proxy" "build_node_perl" "get_elsa" "build_sphinx" "build_syslogng" "mk_node_dirs" "set_node_mysql" "init_elsa" "test_elsa" "set_logrotate"; do
			exec_func $FUNCTION
		done
	elif [ "$OP" = "update" ]; then
		for FUNCTION in $DISTRO"_get_node_packages" "set_date" "check_svn_proxy" "build_node_perl" "get_elsa" "update_node_mysql" "restart_elsa"; do
			exec_func $FUNCTION
		done
	else
		exec_func $OP
	fi
elif [ "$INSTALL" = "web" ]; then
	if [ "$OP" = "ALL" ]; then
		for FUNCTION in $DISTRO"_get_web_packages" "set_date" "check_svn_proxy" "build_web_perl" "get_elsa" "set_web_mysql" "mk_web_dirs" $DISTRO"_set_apache" "set_cron" "set_logrotate"; do
			exec_func $FUNCTION
		done
	elif [ "$OP" = "update" ]; then
		for FUNCTION in $DISTRO"_get_web_packages" "set_date" "check_svn_proxy" "build_web_perl" "get_elsa" "update_web_mysql"; do
			exec_func $FUNCTION
		done
	else
		exec_func $OP
	fi
fi

echo "!!!!!! IMPORTANT !!!!!!!!!"
echo "If you have a host-based firewall like IPTables running, remember to allow ports 80 (and/or 443) for the web server and ports 514 (syslog), 3306 (MySQL), and 3307 (Sphinx) for log nodes"

