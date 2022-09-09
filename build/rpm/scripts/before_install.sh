#!/bin/bash
# Script that runs before install of asterisk
echo "Checking if omnileads user/group exists"
existe=$(grep -c '^omnileads:' /etc/passwd)
if [ $existe -eq 0 ]; then
  echo "The user/group omnileads not exists"
  exit 1
else
  echo "The user/group omnileads already exists"
fi
