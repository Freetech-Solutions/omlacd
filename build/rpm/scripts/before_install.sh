#!/bin/bash
# Script that runs before install of asterisk
echo "Checking if omnileads user/group exists"
existe=$(grep -c '^omnileads:' /etc/passwd)
if [ $existe -eq 0 ]; then
  echo "Creating omnileads group"
  groupadd omnileads
  echo "Creating omnileads user"
  useradd omnileads -d /opt/omnileads -s /bin/bash -g omnileads
else
  echo "The user/group omnileads already exists"
fi
