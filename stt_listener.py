# © 2025 Colin Bond
# All rights reserved.

import sys
import json
import logging
import os
import speech_recognition as sr
from threading import Thread, Event
from typing import Callable, Optional

class STTListener:
    """Headless speech-to-text listener that uses `speech_recognition`.

    API:
      - callback: Callable[[str], None] for receiving transcribed text
      - log_callback: Optional[Callable[[str], None]] to receive log messages
      - config: loaded from `config.json` by default, can be passed in

    Modes supported: speech (manual start/stop).
    """
    version = "0.0.2"
    # - Version 0.0.2: Modified STTListener to explicitly stop listening after a 'phrase' (speech within time limit or silence timeout)

    def __init__(self, callback: Callable[[str], None], log_callback: Optional[Callable[[str], None]] = None, config: dict | None = None, on_stop: Optional[Callable[[], None]] = None):
        self.recognizer = sr.Recognizer()
        self.microphone = None
        self.is_listening = False
        self.stop_listening = Event()
        self.speech_thread: Optional[Thread] = None
        self.callback = callback
        self.log_callback = log_callback
        self.on_stop = on_stop
        self.config = config or self.load_config()
        self.init_microphone()

    def load_config(self):
        try:
            with open("config.json", "r") as f:
                return json.load(f)
        except FileNotFoundError:
            config = {
                "device_index": 0,
                "energy_threshold": 5,
                "pause_threshold": 1.0,
                "phrase_time_limit": 40,
                "timeout": 1
            }
            with open("config.json", "w") as f:
                json.dump(config, f, indent=4)
            return config

    def init_microphone(self):
        try:
            mic_list = sr.Microphone.list_microphone_names()
            logging.debug(f"Available microphones: {mic_list}")
            self.microphone = sr.Microphone(device_index=self.config["device_index"])
            with self.microphone as source:
                self.recognizer.adjust_for_ambient_noise(source, duration=3)
                self.recognizer.dynamic_energy_threshold = True
                self.recognizer.energy_threshold = self.config["energy_threshold"]
                self.recognizer.pause_threshold = self.config["pause_threshold"]
                logging.debug(f"Ambient noise level: {self.recognizer.energy_threshold}")
            logging.debug("Microphone initialized successfully.")
        except Exception as e:
            logging.error(f"Microphone Error: {str(e)}")

    def list_microphones(self):
        """Return a list of available microphone names."""
        return sr.Microphone.list_microphone_names()

    def set_device_index(self, index: int):
        """Set the microphone device index and reinitialize microphone."""
        self.config["device_index"] = index
        self.init_microphone()

    def set_config(self, config: dict):
        """Replace the configuration and re-initialize microphone if needed."""
        self.config = config
        self.init_microphone()

    def set_callback(self, callback: Callable[[str], None]):
        """Update the transcription callback function."""
        self.callback = callback

    def set_log_callback(self, log_callback: Callable[[str], None]):
        """Update the log callback function."""
        self.log_callback = log_callback

    def set_stop_callback(self, on_stop: Callable[[], None]):
        """Set or update the stop callback invoked when the listener stops.

        This callback is called whenever listening stops due to silence, max
        phrase length, or errors.
        """
        self.on_stop = on_stop

    def _on_transcription(self, text: str):
        """Internal callback invoked when text is transcribed.

        This calls the external `callback` with the transcribed text.
        """
        try:
            if self.callback:
                self.callback(text)
        except Exception as e:
            logging.exception(f"Error in transcription callback: {e}")

    def _log(self, message: str):
        if self.log_callback:
            try:
                self.log_callback(message)
            except Exception:
                logging.exception("log_callback raised an exception")
        else:
            logging.info(message)

    def toggle_speech(self):
        if not self.is_listening:
            if not self.microphone:
                self._log("Error: Microphone not initialized")
                logging.error("Microphone not initialized.")
                return
            self.is_listening = True
            # start listen thread
            self.stop_listening.clear()
            self.speech_thread = Thread(target=self.listen_speech, daemon=True)
            self.speech_thread.start()
            logging.debug("Started speech listening.")
        else:
            self.is_listening = False
            self.stop_listening.set()
            if self.speech_thread:
                self.speech_thread.join(timeout=1.0)
                self.speech_thread = None
            logging.debug("Stopped speech listening.")

    # Helper wrappers for programmatic control
    def start_speech(self):
        if not self.is_listening:
            self.toggle_speech()

    def stop_speech(self):
        if self.is_listening:
            self.toggle_speech()

    def shutdown(self):
        """Stop all listeners and clean up threads."""
        self.stop_listening.set()
        self.is_listening = False
        if self.speech_thread:
            self.speech_thread.join(timeout=1.0)
        # Notify interested parties that listener stopped
        if self.on_stop:
            try:
                self.on_stop()
            except Exception:
                logging.exception('on_stop callback raised an exception')

    def listen_speech(self):
        with self.microphone as source:
            while self.is_listening and not self.stop_listening.is_set():
                try:
                    self._log("Listening for speech...")
                    logging.debug("Listening for speech...")
                    audio = self.recognizer.listen(source, timeout=self.config["timeout"], phrase_time_limit=self.config["phrase_time_limit"])
                    text = self.recognizer.recognize_google(audio)
                    self._on_transcription(text)
                    self._log(f"Transcribed: {text}")
                    logging.debug(f"Transcribed: {text}")
                    # Stop listening after every word within the phrase time limit is transcribed, assuming no major pauses
                    self.is_listening = False
                    self.stop_listening.set()
                    if self.on_stop:
                        try:
                            self.on_stop()
                        except Exception:
                            logging.exception('on_stop callback raised an exception')
                    break
                except sr.WaitTimeoutError:
                    # No speech heard during silence timeout, stop listening
                    logging.debug("WaitTimeoutError (silence) - stopping listener")
                    self._log("Stopped listening due to silence")
                    self.is_listening = False
                    self.stop_listening.set()
                    if self.on_stop:
                        try:
                            self.on_stop()
                        except Exception:
                            logging.exception('on_stop callback raised an exception')
                    break
                except sr.UnknownValueError:
                    # Unknown audio, stop listening and notify UI
                    self._log("Speech Error: Could not understand audio.")
                    logging.error("UnknownValueError: Could not understand audio.")
                    self.is_listening = False
                    self.stop_listening.set()
                    if self.on_stop:
                        try:
                            self.on_stop()
                        except Exception:
                            logging.exception('on_stop callback raised an exception')
                    break
                except sr.RequestError as e:
                    self._log(f"Speech Error: {str(e)}")
                    logging.error(f"RequestError: {str(e)}")
                    self.is_listening = False
                    self._log("Stopping speech recognition due to error.")
                    if self.on_stop:
                        try:
                            self.on_stop()
                        except Exception:
                            logging.exception('on_stop callback raised an exception')
                    break

