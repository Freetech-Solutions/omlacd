#!/usr/bin/env python

# -*- coding: utf-8 -*-

# Copyright (C) 2024 Freetech Solutions

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
from asterisk.manager import (
    Manager, ManagerSocketException, ManagerAuthException, ManagerException)


class AMIManager(object):
    def __init__(self, logger):
        self.logger = logger

    def connect(self):
        self.manager = Manager()
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
        self.manager.close()
        # Atención: El Manager solo permite una sola conexión por eso se borra.
        self.manager = None

    def module_reload(self):
        return self.manager.command('module reload').data
