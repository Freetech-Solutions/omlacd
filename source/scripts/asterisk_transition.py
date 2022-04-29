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
import logging
import requests
from asterisk.manager import (
    Manager, ManagerSocketException, ManagerAuthException, ManagerException)


ASTERISK_LOCATION = os.environ.get('ASTERISK_LOCATION') or ''
logging.basicConfig(
    filename=f'{ASTERISK_LOCATION}/var/log/asterisk/transition.log',
    format='[%(asctime)s] {line:%(lineno)d} %(levelname)s - %(message)s'
)
logger = logging.getLogger("asyncio")
logger.setLevel(logging.INFO)


class QueueMembersLoader(object):
    def __init__(self, logger):
        self.manager = Manager()
        self.logger = logger

    def connect(self):
        error = False
        ami_manager_user = os.environ.get('AMI_USER')
        ami_manager_pass = os.environ.get('AMI_PASSWORD')
        ami_manager_host = os.environ.get('HOSTNAME') or 'localhost'
        try:
            self.manager.connect(ami_manager_host)
            self.manager.login(ami_manager_user, ami_manager_pass)
        except ManagerSocketException as e:
            self.logger.exception("Error connecting to the manager: {0}".format(e))
            error = True
        except ManagerAuthException as e:
            self.logger.exception("Error logging in to the manager: {0}".format(e))
            error = True
        except ManagerException as e:
            self.logger.exception("Error {0}".format(e))
            error = True
        return error

    def disconnect(self):
        # Atención: El Manager solo permite una sola conexión
        self.manager.close()

    def load_members(self):
        oml_server = os.environ.get('OMNILEADS_HOSTNAME') or 'localhost'
        url = f'https://{oml_server}/api/v1/asterisk/queues_data/'

        response = requests.get(url, verify=False)
        if not response.status_code == 200:
            self.logger.error('Request Error. Status Code: {0} - {1} '.format(
                response.status_code, response.reason))
            return
        agents_data = response.json()
        for agent_data in agents_data:
            [agent_id, member_name, interface, paused, queues, penalties] = agent_data
            for i in range(len(queues)):
                dict = {
                    'Action': 'QueueAdd',
                    'Queue': queues[i],
                    'Interface': interface,
                    'Penalty': penalties[i],
                    'Paused': paused,
                    'MemberName': member_name
                }
                self.manager.send_action(dict)


if __name__ == "__main__":
    loader = QueueMembersLoader(logger)
    connect_error = loader.connect()
    if not connect_error:
        loader.load_members()
        loader.disconnect()
