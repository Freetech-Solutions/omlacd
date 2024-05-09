#!/usr/bin/env python3.12

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
import hashlib
from asterisk.agi import AGI
import subprocess

def execute_command(command):
    """ Ejecutar un comando en el shell y capturar la salida. """
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    return out, err, process.returncode

# Inicializar AGI
agi = AGI()
text = agi.env['agi_arg_1']

if not text:
    agi.verbose("No text passed for synthesis.")
    agi.hangup()

# Config
lang_asterisk = agi.env["agi_language"]

# Preparo la var lang como la requiere pico2wave
if lang_asterisk == "en":
    lang = "en-US"
elif lang_asterisk == "es":
    lang = "es-ES"
elif lang_asterisk == "it":
    lang = "it-IT"
elif lang_asterisk == "fr":
    lang = "fr-FR"
else:
    lang = "es-ES"

temp_dir = "/tmp"
hash_object = hashlib.md5(text.encode())
filename = f"{temp_dir}/{hash_object.hexdigest()}.wav"
temp_filename = f"{filename}_temp.wav"

# Comando para generar el archivo de audio usando pico2wave
command = f"pico2wave -l {lang} -w {temp_filename} '{text}'"
out, err, return_code = execute_command(command)

if return_code != 0:
    agi.verbose(f"Failed to synthesize speech: {err.decode()}")
    agi.hangup()

# Ahora convertir para Asterisk PCM 8000 Hz
command = f"sox {temp_filename} -r 8000 -c 1 -b 16 {filename}"
out, err, return_code = execute_command(command)
if return_code != 0:
    agi.verbose(f"Failed to convert audio file: {err.decode()}")
    agi.hangup()

# Limpiar archivo temporal
os.remove(temp_filename)

# Reproducir el archivo de audio generado
agi.stream_file(filename[:-4])  # Asterisk no necesita el '.wav' del final

# Limpiar: Eliminar el archivo después de usarlo
os.remove(filename)

