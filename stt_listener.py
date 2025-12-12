import sys
import json
import re
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
      - config: loaded from `config.json` by default; can be passed in

    Modes supported: speech (manual start/stop), wake (listen for wake words),
    auto_wake_send (wake and automatically send callback when detected).
    """
    def __init__(self, callback: Callable[[str], None], log_callback: Optional[Callable[[str], None]] = None, config: dict | None = None):
        self.recognizer = sr.Recognizer()
        self.microphone = None
        self.is_listening = False
        self.is_wake_mode = False
        self.is_auto_wake_send = False
        self.stop_listening = Event()
        self.speech_thread: Optional[Thread] = None
        self.callback = callback
        self.log_callback = log_callback
        self.config = config or self.load_config()
        self.init_microphone()

        # Example usage:
        #
        # def write_to_text_widget(text):
        #     text_widget.delete("1.0", "end")
        #     text_widget.insert("1.0", text)
        #
        # stt = STTListener(callback=write_to_text_widget)
        # stt.start_speech()

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
            self._log(f"Available microphones: {mic_list}")
            logging.debug(f"Available microphones: {mic_list}")
            self.microphone = sr.Microphone(device_index=self.config["device_index"])
            with self.microphone as source:
                self.recognizer.adjust_for_ambient_noise(source, duration=3)
                self.recognizer.dynamic_energy_threshold = True
                self.recognizer.energy_threshold = self.config["energy_threshold"]
                self.recognizer.pause_threshold = self.config["pause_threshold"]
                self._log(f"Ambient noise level: {self.recognizer.energy_threshold}")
                logging.debug(f"Ambient noise level: {self.recognizer.energy_threshold}")
            self._log("Microphone initialized successfully.")
            logging.debug("Microphone initialized successfully.")
        except Exception as e:
            self._log(f"Microphone Error: {str(e)}")
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

    def _on_transcription(self, text: str):
        """Internal callback invoked when text is transcribed.

        This calls the external `callback` with the transcribed text.
        """
        try:
            if self.callback:
                self.callback(text)
            if self.is_auto_wake_send:
                # If auto send is enabled, invoke the callback again to indicate send
                # or rely on the application to automatically send when receiving text.
                pass
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
        self.is_wake_mode = False
        self.is_auto_wake_send = False
        if self.speech_thread:
            self.speech_thread.join(timeout=1.0)

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
                except sr.WaitTimeoutError:
                    logging.debug("WaitTimeoutError")
                    continue
                except sr.UnknownValueError:
                    self._log("Speech Error: Could not understand audio.")
                    logging.error("UnknownValueError: Could not understand audio.")
                except sr.RequestError as e:
                    self._log(f"Speech Error: {str(e)}")
                    logging.error(f"RequestError: {str(e)}")
                    self.is_listening = False
                    self._log("Stopping speech recognition due to error.")
                    break
                if self.stop_listening.is_set():
                    break

    def test_microphone(self):
        if not self.microphone:
            self._log("Error: Microphone not initialized")
            logging.error("Microphone not initialized.")
            return False
        try:
            with self.microphone as source:
                self.recognizer.adjust_for_ambient_noise(source, duration=3)
                self._log("Testing microphone... Speak now.")
                logging.debug("Testing microphone...")
                audio = self.recognizer.listen(source, timeout=5)
                self._log("Microphone test: Audio detected successfully.")
                logging.debug("Microphone test successful.")
            return True
        except sr.WaitTimeoutError:
            self._log("Microphone test: No audio detected within 5 seconds.")
            logging.error("WaitTimeoutError in mic test.")
        except Exception as e:
            self._log(f"Microphone test error: {str(e)}")
            logging.error(f"Mic test error: {str(e)}")
        return False

    # send_text removed; the application should handle sending logic by receiving
    # the transcription callback and processing/sending as needed.

    # handle_drop removed - this class is headless and should not handle GUI drop events

if __name__ == "__main__":
    # Quick CLI demo that prints transcriptions to stdout
    def print_cb(text: str):
        print(f"Transcription: {text}")

    def log_cb(msg: str):
        print(f"LOG: {msg}")

    listener = STTListener(callback=print_cb, log_callback=log_cb)
    print("STTListener created. Toggle modes in code or press Ctrl-C to exit.")
    try:
        while True:
            import time
            time.sleep(1)
    except KeyboardInterrupt:
        print("Exiting")
        listener.stop_listening.set()

