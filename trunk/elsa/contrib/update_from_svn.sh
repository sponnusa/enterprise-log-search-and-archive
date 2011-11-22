#!/bin/sh

# EDIT CONFIG VARIABLES
BASE_DIR="/usr/local"
DATA_DIR="/data"
TMP_DIR="/tmp"

svn export --force https://enterprise-log-search-and-archive.googlecode.com/svn/trunk/elsa "$BASE_DIR/elsa"
if [ $? -ne 0 ]; then
	echo "Error updating from svn"
	exit
fi

if [ -f /etc/elsa_web.conf ]; then
	service apache2 restart
fi

if [ -f /etc/elsa_node.conf ]; then
	service syslog-ng restart
fi