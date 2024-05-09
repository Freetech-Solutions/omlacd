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

import sys
import os
from os import remove
from time import sleep
import requests
import soundfile as sf
import speech_recognition as sr
from asterisk.agi import AGI
import google.generativeai as genai

agi = AGI()
callerId = agi.env["agi_callerid"]
uniqueId = agi.env["agi_uniqueid"]
lang = agi.env["agi_language"]

genai.configure(api_key=os.getenv('GOOGLE_API_KEY'))


temp_file = f"/tmp/{uniqueId}_{callerId}"
transcoded_file = f"{temp_file}.flac"

agi.verbose(f"Starting Google Cloud Speech to Text for {uniqueId}  on language {lang}")
agi.verbose(f"call from {callerId}")

temp_file = f"/tmp/{uniqueId}_{callerId}"
#agi.record_file(temp_file, silence="2", escape_digits="#")
agi.record_file(temp_file, 'wav', escape_digits='#', timeout=10000, silence=3)


# waiting for recorded file
while not os.path.exists(f"{temp_file}.wav"):
    agi.verbose(f"waiting recorded file  for {uniqueId}")
    sleep(1)

agi.verbose(f"Transcoding audio in FLAC  for {uniqueId}")

data, samplerate = sf.read(f"{temp_file}.wav")
sf.write(transcoded_file, data, samplerate)

# use the audio file as the audio source
r = sr.Recognizer()
with sr.AudioFile(transcoded_file) as source:
    audio = r.record(source)  # read the entire audio file

# recognize speech using Google Cloud Speech
try:
    recognized_text = r.recognize_google_cloud(audio, language=lang)
    agi.verbose(f'Google Cloud Speech thinks you said in call {uniqueId}: "{recognized_text}"')

    #recognized_text = recognized_text.replace(" ", "")
    agi.set_variable("response", recognized_text.strip())

except sr.UnknownValueError:
    agi.verbose(f"Google Cloud Speech could not understand audio in call {uniqueId}")
    sys.exit(1)
except sr.RequestError as e:
    agi.verbose(f"Could not request results from Google Cloud Speech service in call {uniqueId}: {e}")
    sys.exit(1)
except Exception as e:
    agi.verbose(f"{type(e).__name__}: {e}")
    sys.exit(1)
    
# remove files
agi.verbose(f"Removing speech to text files for {uniqueId}")

for to_be_removed in [f"{temp_file}.wav", transcoded_file]:
    remove(to_be_removed)
