#!/opt/asterisk/virtualenv/bin/python3
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


import os
import sys

from socket import setdefaulttimeout
from utiles import write_time_stderr

from rethinkdb import r
from asterisk.agi import AGI

OML_RETHINKDB_HOST = 'oml-rethinkdb_1-devenv'
OML_RETHINKDB_PORT = 28015

connection = r.connect(OML_RETHINKDB_HOST, OML_RETHINKDB_PORT, db='omnileads')

ASTERISK_LOCATION = os.getenv('ASTERISK_LOCATION')
RETHINKDB_ERROR_LOG = '{0}/var/log/asterisk/rethinkdb_agi_llamadalog.log'.format(ASTERISK_LOCATION)

if os.path.exists(RETHINKDB_ERROR_LOG):
    append_write = 'a'  # append if already exists
else:
    append_write = 'w'  # make a new file if not

sys.stderr = open(RETHINKDB_ERROR_LOG, append_write)

setdefaulttimeout(20)
agi = AGI()

campana_id = sys.argv[1]
callid = sys.argv[2]
agente_id = sys.argv[3]
event = sys.argv[4]
numero_marcado = sys.argv[5]
contacto_id = sys.argv[6]
tipo_llamada = sys.argv[7]
tipo_campana_id = sys.argv[8]
bridge_wait_time = sys.argv[9]
duracion_llamada = sys.argv[10]
archivo_grabacion = sys.argv[11]
channel = sys.argv[12]
transfer_id_agent = sys.argv[13]
transfer_id_camp = sys.argv[14]
transfer_contact_address = sys.argv[15]

llamadalog_dict = {
    'CHANNEL': channel,
    'ID_TRANSACTION': callid,
    'ID_CAMP': campana_id,
    'ID_AGENT': agente_id,
    'ID_CONTACT': contacto_id,
    'CONTACT_ADDRESS': numero_marcado,
    'EVENT': event,
    'RECFILENAME': archivo_grabacion,
    'CAMPTYPE': tipo_campana_id,
    'CALLTYPE': tipo_llamada,
    'WAIT4CONNECTION': bridge_wait_time,
    'TRANSACTIONDURATION': duracion_llamada,
    'TRANSFER_ID_AGENT': transfer_id_agent,
    'TRANSFER_ID_CAMP': transfer_id_camp,
    'TRANSFER_CONTACT_ADDRESS': transfer_contact_address
}

try:
    r.table('omnilogs').insert(llamadalog_dict).run(connection)
except Exception as e:
    write_time_stderr('Error due to: {0}'.format(e))
