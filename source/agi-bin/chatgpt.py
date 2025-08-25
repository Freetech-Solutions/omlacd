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

import sys
import os
from time import sleep
import requests
import soundfile as sf
from asterisk.agi import AGI
import openai
from google.cloud import texttospeech as tts

# Todo lo inherente a TTS y Google Speech
agi = AGI()
callerId = agi.env["agi_callerid"]
uniqueId = agi.env["agi_uniqueid"]
lang = agi.env["agi_language"]
temp_file = f"/tmp/{uniqueId}_{callerId}"
google_tts_file = f"{temp_file}_tts.wav"

# OpenAI chatgpt integration
openai.api_key = os.environ.get('OPENAI_API_KEY')

# Obtener el prompt de la variable de canal AGI
voice_prompt = agi.get_variable('response')

completion = openai.completions.create(
  model="gpt-3.5-turbo-instruct", 
  prompt=voice_prompt,
  max_tokens=200,
  temperature=1
)

# Instantiates a client
client = tts.TextToSpeechClient()

# Set the text input to be synthesized
synthesis_input = tts.SynthesisInput(text=completion.choices[0].text)

voice = tts.VoiceSelectionParams(
    language_code="es-AR",  # Asume español de España como ejemplo
    name="es-ES-Standard-A",  # Especifica una voz en particular
    ssml_gender=tts.SsmlVoiceGender.FEMALE  # Género de la voz
)

# Select the type of audio file you want returned
audio_config = tts.AudioConfig(
    audio_encoding=tts.AudioEncoding.LINEAR16, sample_rate_hertz=8000
)

# Perform the text-to-speech request on the text input with the selected
# voice parameters and audio file type
tts_response = client.synthesize_speech(
    input=synthesis_input, voice=voice, audio_config=audio_config
)
# The response's audio_content is binary.
with open(google_tts_file, "wb") as out:
    # Write the response to the output file.
    out.write(tts_response.audio_content)
    agi.verbose(f'Audio content written to file "{google_tts_file}"')


while not os.path.exists(google_tts_file):
    agi.verbose(f"waiting tts file  for {uniqueId}")
    sleep(1)

agi.stream_file(os.path.splitext(google_tts_file)[0])

# remove files
agi.verbose(f"Removing text to speech files for {uniqueId}")

for to_be_removed in [google_tts_file]:
    os.remove(to_be_removed)
