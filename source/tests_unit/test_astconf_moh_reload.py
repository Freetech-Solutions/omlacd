"""
Tests del worker astconf: reload MOH tras audio y path de playlists.
"""
import base64
import logging
import os
import sys
from unittest.mock import MagicMock, call

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'workers'))

import astconf as astconf_mod  # noqa: E402
from astconf import RegenerarConfiguracion  # noqa: E402


@pytest.fixture
def regenerar(tmp_path, monkeypatch):
    conf_dir = tmp_path / "astconf"
    sounds_dir = tmp_path / "sounds"
    conf_dir.mkdir()
    sounds_dir.mkdir()

    monkeypatch.setattr(astconf_mod, "CONF_FILES_PATH", str(conf_dir) + "/")
    monkeypatch.setattr(astconf_mod, "ASTERISK_SOUND_FILES", str(sounds_dir) + "/")
    monkeypatch.setattr(
        astconf_mod, "CUSTOM_AUDIO_FILES_PATH", str(sounds_dir / "oml") + "/"
    )
    monkeypatch.setattr(
        astconf_mod, "MOH_AUDIO_FILES_PATH", str(sounds_dir / "moh") + "/"
    )

    regen = RegenerarConfiguracion(logging.getLogger("test_astconf_moh"))
    regen.ari = MagicMock()
    regen.ari.reload_module.return_value = True
    regen.ari.unload_module.return_value = True
    regen.ari.load_module.return_value = True
    return regen, conf_dir, sounds_dir


def _run_ops(regen, ops):
    """Bypassa el parser legacy del stream y ejecuta procesa_stream con ops listas."""
    regen._json_string_a_lista = MagicMock(return_value=ops)
    regen.procesa_stream("ignored")


def _assert_moh_unload_load(regen):
    regen.ari.unload_module.assert_called_once_with(regen.RELOAD_RES_MOH)
    regen.ari.load_module.assert_called_once_with(regen.RELOAD_RES_MOH)


def test_audio_custom_copy_writes_file_and_reloads_moh_once(regenerar):
    regen, _conf_dir, sounds_dir = regenerar
    payload = base64.b64encode(b"RIFF....WAV").decode("ascii")

    _run_ops(
        regen,
        [
            {
                "archivo": "moh/test1/1-test1.wav",
                "type": "AUDIO_CUSTOM",
                "action": "COPY",
                "content": payload,
                "stream_id": 1,
            }
        ],
    )

    wav_path = sounds_dir / "moh" / "test1" / "1-test1.wav"
    assert wav_path.is_file()
    assert wav_path.read_bytes() == b"RIFF....WAV"
    _assert_moh_unload_load(regen)


def test_playlist_dir_delete_uses_sounds_moh_path(regenerar):
    regen, _conf_dir, sounds_dir = regenerar
    playlist_dir = sounds_dir / "moh" / "test1"
    playlist_dir.mkdir(parents=True)
    (playlist_dir / "1-test1.wav").write_bytes(b"x")

    # Directorio legacy incorrecto no debe tocarse
    legacy_dir = sounds_dir / "oml" / "test1"
    legacy_dir.mkdir(parents=True)
    (legacy_dir / "keep.wav").write_bytes(b"keep")

    _run_ops(
        regen,
        [
            {
                "archivo": "test1",
                "type": "ASTERISK_PLAY_LIST_DIR",
                "action": "DELETE",
                "stream_id": 2,
            }
        ],
    )

    assert not playlist_dir.exists()
    assert legacy_dir.exists()
    _assert_moh_unload_load(regen)


def test_oml_moh_conf_creates_directory_before_reload(regenerar):
    regen, conf_dir, sounds_dir = regenerar
    moh_dir = sounds_dir / "moh" / "test2"
    assert not moh_dir.exists()

    _run_ops(
        regen,
        [
            {
                "archivo": "oml_moh.conf",
                "type": "CONF_FILE",
                "content": (
                    "[test2]\nmode=files\ndirectory=sounds/moh/test2\n"
                    "[test1]\nmode=files\ndirectory=sounds/moh/test1\n"
                ),
                "stream_id": 3,
            }
        ],
    )

    assert (conf_dir / "oml_moh.conf").is_file()
    assert moh_dir.is_dir()
    assert (sounds_dir / "moh" / "test1").is_dir()
    _assert_moh_unload_load(regen)


def test_oml_moh_conf_plus_audio_dedupes_moh_reload(regenerar):
    regen, conf_dir, sounds_dir = regenerar
    payload = base64.b64encode(b"audio").decode("ascii")

    _run_ops(
        regen,
        [
            {
                "archivo": "oml_moh.conf",
                "type": "CONF_FILE",
                "content": "[test1]\nmode=files\ndirectory=sounds/moh/test1\n",
                "stream_id": 3,
            },
            {
                "archivo": "moh/test1/1-test1.wav",
                "type": "AUDIO_CUSTOM",
                "action": "COPY",
                "content": payload,
                "stream_id": 4,
            },
        ],
    )

    assert (conf_dir / "oml_moh.conf").is_file()
    assert (sounds_dir / "moh" / "test1" / "1-test1.wav").is_file()
    _assert_moh_unload_load(regen)
    # Un solo ciclo unload+load aunque conf+audio vengan juntos
    assert regen.ari.unload_module.call_count == 1
    assert regen.ari.load_module.call_count == 1


def test_moh_reload_falls_back_to_reload_if_unload_fails(regenerar):
    regen, _conf_dir, _sounds_dir = regenerar
    regen.ari.unload_module.return_value = False

    _run_ops(
        regen,
        [
            {
                "archivo": "oml_moh.conf",
                "type": "CONF_FILE",
                "content": "[x]\nmode=files\ndirectory=sounds/moh/x\n",
                "stream_id": 1,
            }
        ],
    )

    regen.ari.unload_module.assert_called_once_with(regen.RELOAD_RES_MOH)
    regen.ari.load_module.assert_not_called()
    regen.ari.reload_module.assert_called_once_with(regen.RELOAD_RES_MOH)
