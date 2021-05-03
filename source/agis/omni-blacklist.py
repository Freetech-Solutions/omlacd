#!/opt/omnileads/asterisk/virtualenv/bin/python3
# -*- coding: utf-8 -*-

# Copyright (C) 2018 Freetech Solutions

# This file is part of OMniLeads

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see http://www.gnu.org/licenses/.

# este script es invocado como AGI desde 'oml_extensions_sub.conf' para detectar si un télefono
# discado está en una lista negra (no está permitido llamar), cuyo valor escribe en el canal
# correspondiente en la variable BLACKLIST

import os
import sys
import redis
from socket import setdefaulttimeout

from asterisk.agi import AGI
from utiles import write_time_stderr

ASTERISK_LOCATION = os.getenv('ASTERISK_LOCATION')
BLACKLIST_AGI_LOG = '{0}/var/log/asterisk/blacklist-agi-errors.log'.format(ASTERISK_LOCATION)
BLACKLIST_ERROR_CODE = 2

if os.path.exists(BLACKLIST_AGI_LOG):
    append_write = 'a'  # append if already exists
else:
    append_write = 'w'  # make a new file if not

sys.stderr = open(BLACKLIST_AGI_LOG, append_write)

setdefaulttimeout(20)

agi = AGI()

phone_number = sys.argv[1]
black_list_key = 'OML:BLACKLIST'
redis_connection = redis.Redis(
    host=os.getenv('REDIS_HOSTNAME'),  # settings.REDIS_HOSTNAME
    port=6379,  # settings.CONSTANCE_REDIS_CONNECTION['port'],
    decode_responses=True)

try:
    is_black_listed = int(redis_connection.sismember(black_list_key, phone_number))
except redis.exceptions.RedisError as e:
    write_time_stderr("Error executing redis command SISMEMBER: {0}".format(e))
    # Si falla el servicio devuelvo codigo de error
    is_black_listed = BLACKLIST_ERROR_CODE

try:
    agi.set_variable('BLACKLIST', str(is_black_listed))
except Exception as e:
    write_time_stderr("Unable to set variable BLACKLIST in channel due to {0}".format(e))
    raise e
