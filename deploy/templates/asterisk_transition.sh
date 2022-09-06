#!/bin/bash

echo "$(date): transition to master" >> /var/log/keepalive_transition.log
source /etc/omnileads-asterisk.env && source /opt/omnileads/asterisk/virtualenv/bin/activate && /opt/omnileads/asterisk/virtualenv/bin/python3 /opt/omnileads/asterisk/virtualenv/asterisk_transition.py >> /var/log/keepalive_transition.log 2>&1

exit 0
