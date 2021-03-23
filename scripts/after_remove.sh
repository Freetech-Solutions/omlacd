#!/bin/bash
# Script that runs after asterisk remove
echo "Removing asterisk symbolic links and folders"
rm -rf /usr/sbin/asterisk
rm -rf /opt/omnileads/asterisk
rm -rf /etc/odbc.ini
rm -rf /usr/lib64/psqlodbcw.so
