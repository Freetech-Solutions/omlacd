#!/etc/asterisk/virtualenv/bin/python3

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

# Este script se ejecuta como AGI para obtener datos de Families en Redis

import os
import sys
from socket import setdefaulttimeout
import json

import redis
from asterisk.agi import AGI
from utiles import write_time_stderr

ASTERISK_LOCATION = os.getenv('ASTERISK_LOCATION')
REDIS_GET_FAMILY_LOG = '{0}/var/log/asterisk/redis-survey-answer-errors.log'.format(ASTERISK_LOCATION)

if os.path.exists(REDIS_GET_FAMILY_LOG):
    append_write = 'a'  # append if already exists
else:
    append_write = 'w'  # make a new file if not

sys.stderr = open(REDIS_GET_FAMILY_LOG, append_write)

setdefaulttimeout(20)

agi = AGI()
redis_connection = redis.Redis(
    host=os.getenv('REDIS_HOSTNAME'),  # settings.REDIS_HOSTNAME
    port=6379,  # settings.CONSTANCE_REDIS_CONNECTION['port'],
    decode_responses=True)

# Validate args: Optionals receive -1
# campaign_id = sys.argv[1]
# question_id = sys.argv[2]
# option_id = sys.argv[3]
# date = sys.argv[4]
# callid = sys.argv[5]
# telephone = sys.argv[6]
# contact_id = sys.argv[7]
# agent_id = sys.argv[8]
# recording = sys.argv[9]

data = json.dumps(sys.argv[1:10])
family_key = 'OML:QUEUE:SURVEY_ANSWERS'

try:
    family_data = redis_connection.rpush(family_key, data)
except redis.exceptions.RedisError as e:
    write_time_stderr("Error executing redis command RPUSH: {0}".format(e))
