import tkinter as tk
from tkinter import messagebox
from tkinter import ttk
from mariadb_login import MariaDBLogin
from stt_listener import STTListener
from ailib import Payload
import os
import tkinter.font as tkfont
import re
import logging


class aiEnglishQuery(tk.Frame):
	"""
    Window that contains an interface for submitting plain English queries to an AI agent who generates SQL, which is executed by itself.
    """
	# Current aiEnglishQuery.py Version
	version = "0.0.2"
	# - Version 0.0.2: Implemented STTListener integration for dynamic speech to text input, push to talk but recognizes pauses
	# - Version 0.0.1: Initial release with basic UI that calls MariaDB login, accepts plain English input, generates SQL, executes against MariaDB, and displays results in grid.

	# Provide a default placeholder for the connection object (None until 'initialize_connection' runs)
	db = None
	# Provide a default placeholder for results to avoid later NameError when the UI populates the grid
	results = []

	# Startup Functions

	def __init__(self, prompt_file: str, history_file: str, master=None, db=None):
		super().__init__(master)
		# Set debug mode to true in logs, displaying more information
		self.configure_logging(True)						

		# Initialize the MariaDB connection with required parameters (to be replaced with MariaDB login dialog)
		# Keep reference to application master window
		self.master = master
		# If a database connection was provided, use it; otherwise expect
		# the caller to set it before calling db-related methods
		self.db = db

		# Initialize Open AI Key, Payload, and an AI agent with desired parameters for every analysis in the session
		api_key = self.load_openai_key()
		self.payload = Payload(prompt_file, history_file, api_key)
		logging.info(f"AIEnglishQuery v{self.version} class currently using ModelConnection v{self.payload.connection.version}, PromptBuilder v{self.payload.prompts.version}, ChatHistoryManager v{self.payload.history.version}, Payload v{self.payload.version}, MariaDBConnector v{self.db.version if self.db else 'N/A'}")
		# Configure AI for session
		self.initialize_ai("gpt-5-nano", "low", "aiEnglishQuery_AIDB", "aiEnglishQuery", True)
		# Initialize STT listener and callback to write to text input
		try:
			self.stt_listener = STTListener(callback=self._stt_transcribed, log_callback=self._stt_log)
		except Exception as e:
			logging.warning(f"STTListener initialization failed: {e}")
			self.stt_listener = None
		self.create_widgets()		

	def load_openai_key(self):
		api_key = os.getenv("OPENAI_API_KEY")
		if api_key is None:
			logging.error("OPENAI_API_KEY environment variable not found. An OpenAI API Key is required to run this application")
			raise RuntimeError("OPENAI_API_KEY environment variable not found. An OpenAI API Key is required to run this application")
		else:
			return api_key

	def configure_logging(self, debug_mode: bool):
		"""TRUE for DEBUG mode, False for INFO"""
		if debug_mode:
			logging.basicConfig(
				filename='aioperator.log',
				level=logging.DEBUG,
				format='%(asctime)s - %(levelname)s - %(message)s'
			)
		else:
			logging.basicConfig(
				filename='aioperator.log',
				level=logging.INFO,
				format='%(asctime)s - %(levelname)s - %(message)s'
			)	

	def initialize_ai(self, model: str, verbosity, prompt: str, history: str, resetHistory: bool):
		"""Instantly prepare an AI agent with all of the required parameters to receive calls."""		
		self.payload.connection.set_model(model)
		self.payload.connection.set_verbosity(verbosity)               
		self.payload.prompts.load_prompt(prompt)
		self.payload.history.load_history(history)
		if resetHistory:
			self.payload.history.reset_history()
			logging.info("Chat History has been reset")


	# Runtime Functions

	def generate_sql(self, user_msg: str):
		"""Return an AI reply with SQL guard rails in place to prevent database modification and sanitization."""
		logging.debug(f"PROMPT: {self.payload.prompts.get_prompt()}")
		logging.info(f"User Message: [{user_msg}]")
		logging.debug(f"Model: {self.payload.connection.model}, Verbosity: {self.payload.connection.verbosity}, Reasoning: {self.payload.connection.reasoning_effort}, Max Tokens: {self.payload.connection.maximum_tokens}")
		reply = self.payload.send_message(user_msg)
		logging.info(f"Assistant Raw Reply: {reply}")
		return self.clean_ai_response(reply)

	def clean_ai_response(self, ai_response):
		# --- Clean AI response (remove ```json or ``` etc.) ---
		cleaned = re.sub(r'^```[a-zA-Z]*\n?', '', ai_response.strip())
		cleaned = re.sub(r'\n?```$', '', cleaned.strip())
		response_text = cleaned
		return response_text

	def execute_sql(self, sql: str):
		"""Execute SQL using the instance's DB connection. If connection missing, raise.
		Returns list of rows (as dicts) or raises an exception."""
		print(f"SQL to be executed: {sql}")
		if not hasattr(self, 'db') or self.db is None:
			logging.error("No database connection configured. Call initialize_connection first.")
			raise RuntimeError("No database connection configured. Call initialize_connection first.")
		try:
			results = self.db.execute(sql)
			return results
		except Exception as e:
			logging.info(f"   ERROR: {e}\n")
			raise

	
	# User Interface

	def create_widgets(self):
		# Label for the text box
		lbl = tk.Label(self, text="Plain English Query:")
		lbl.grid(row=1, column=0, sticky="w", padx=6, pady=(6, 0))

		# Text widget with a vertical scrollbar
		self.text_frame = tk.Frame(self)
		self.text_frame.grid(row=2, column=0, columnspan=2, sticky="nsew", padx=6)

		self.text = tk.Text(self.text_frame, wrap="word", width=60, height=12)
		self.text.grid(row=0, column=0, sticky="nsew")

		self.scrollbar = tk.Scrollbar(self.text_frame, orient="vertical", command=self.text.yview)
		self.scrollbar.grid(row=0, column=1, sticky="ns")
		self.text.configure(yscrollcommand=self.scrollbar.set)

		# Buttons
		self.submit_btn = tk.Button(self, text="Submit", command=self.on_submit)
		self.submit_btn.grid(row=3, column=0, sticky="w", padx=6, pady=6)

		self.clear_btn = tk.Button(self, text="Clear", command=self.on_clear)
		self.clear_btn.grid(row=3, column=1, sticky="e", padx=6, pady=6)

		# Speech toggle button — will be disabled if STTListener not available
		self.speech_toggle_btn = tk.Button(self, text="Start Speech", command=self._toggle_speech)
		self.speech_toggle_btn.grid(row=3, column=2, sticky="e", padx=6, pady=6)
		if not hasattr(self, 'stt_listener') or self.stt_listener is None:
			self.speech_toggle_btn.config(state='disabled')

		# Status label
		self.status_var = tk.StringVar(value="Ready")
		self.status_lbl = tk.Label(self, textvariable=self.status_var, anchor="w")
		self.status_lbl.grid(row=4, column=0, columnspan=2, sticky="we", padx=6, pady=(0, 6))

		# Make grid expandable
		# Make the result grid row expand by default (row 0)
		self.grid_rowconfigure(0, weight=1)
		self.grid_columnconfigure(0, weight=1)
		self.text_frame.grid_rowconfigure(0, weight=1)
		self.text_frame.grid_columnconfigure(0, weight=1)

		# Result grid (hidden until submission)
		self.result_frame = tk.Frame(self)
		# Use a treeview to show tabular results
		self.result_tree = ttk.Treeview(self.result_frame, columns=("name", "population"), show="headings")
		self.result_tree.heading("name", text="Name")
		self.result_tree.heading("population", text="Population")
		# Set fixed widths and disable "stretch" so columns cannot be resized by user
		# Setting minwidth equal to width reduces the chance of user-resize
		self.result_tree.column("name", anchor="w", width=200, minwidth=200, stretch=False)
		self.result_tree.column("population", anchor="e", width=120, minwidth=120, stretch=False)

		self.result_tree.grid(row=0, column=0, sticky="nsew")
		self.result_scroll = tk.Scrollbar(self.result_frame, orient="vertical", command=self.result_tree.yview)		
		self.result_scroll.grid(row=0, column=1, sticky="ns")
		self.result_tree.configure(yscrollcommand=self.result_scroll.set)
		# Add a horizontal scrollbar for wide tables
		self.result_hscroll = tk.Scrollbar(self.result_frame, orient="horizontal", command=self.result_tree.xview)
		self.result_hscroll.grid(row=1, column=0, columnspan=2, sticky="ew")
		self.result_tree.configure(xscrollcommand=self.result_hscroll.set)

		self.result_frame.grid_rowconfigure(0, weight=1)
		self.result_frame.grid_columnconfigure(0, weight=1)
		# Place the result grid above the input area; hide until populated
		self.result_frame.grid(row=0, column=0, columnspan=2, sticky="nsew", padx=6, pady=(0, 6))
		self.grid_rowconfigure(0, weight=1)
		self.result_frame.grid_remove()

		# Keyboard binding for Ctrl+Enter to submit
		self.text.bind('<Control-Return>', lambda event: self.on_submit())

		# Pack the frame (we're using grid inside frame)
		self.pack(fill="both", expand=True)

		# Bind resize event on the top-level window to enforce a maximum height
		# for the input text: it should never exceed 25% of the total form height.
		if self.master is not None:
			# Keep a reference font object for line height metrics
			self._text_font = tkfont.Font(font=self.text["font"]) if self.text["font"] else tkfont.nametofont("TkDefaultFont")
			self.master.bind('<Configure>', self._on_root_resize)

	def _on_root_resize(self, event):
		"""Resize handler to ensure the text widget never grows larger than 25% of
		the total window height. Height is enforced by adjusting the widget's
		`height` in number of text lines using the configured font metrics.
		"""
		try:
			total_height = self.master.winfo_height()
			if total_height <= 0:
				return
			# 25% of total height
			max_pixels = int(total_height * 0.25)
			line_height = self._text_font.metrics("linespace") or 1
			max_lines = max(1, max_pixels // line_height)
			# Respect a small minimum of lines so the widget doesn't collapse
			min_lines = 2
			desired_lines = max(min_lines, max_lines)
			# Only update if different to avoid continuous reconfiguration
			current_height = int(self.text.cget("height"))
			if current_height != desired_lines:
				self.text.configure(height=desired_lines)

			# Ensure the results grid has at least 50% of total height when visible
			try:
				min_grid_pixels = int(total_height * 0.5)
				# If the result_frame is mapped (visible via grid) enforce minsize
				if hasattr(self, 'result_frame') and self.result_frame.winfo_ismapped():
					# Only reconfigure if different to avoid layout thrashing
					current_minsize = self.grid_rowconfigure(0).get('minsize', 0) or 0
					if current_minsize != min_grid_pixels:
						self.grid_rowconfigure(0, minsize=min_grid_pixels)
				else:
					# If not visible, remove enforced minsize
					if current_minsize != 0:
						self.grid_rowconfigure(0, minsize=0)
			except Exception:
				pass
		except Exception:
			# Don't crash UI if resize logic fails; just skip
			return

	def load_to_tkinter_grid(self, results):
		"""Populate the existing `self.result_tree` with results.

		This avoids creating a new Treeview widget repeatedly and keeps the
		geometry manager consistent (grid).
		"""
		if not results:
			return

		# Clear any existing rows
		for item in self.result_tree.get_children():
			self.result_tree.delete(item)

		# Ensure columns are set in case results shape changed
		columns = list(results[0].keys())
		self.result_tree["columns"] = columns
		for col in columns:
			# Set heading text. Use a larger width for a "name" column if present
			self.result_tree.heading(col, text=col)
			width = 200 if col.lower() == "name" else 120
			# Prevent user resize by setting minwidth equal to width and setting stretch=False
			self.result_tree.column(col, anchor="w", width=width, minwidth=width, stretch=False)

		# Insert rows
		for row in results:
			# Order values according to column headings
			self.result_tree.insert("", "end", values=[row.get(col, "") for col in columns])

		# Make sure results frame (and tree) are visible
		self.result_frame.grid()
		# Recalculate layout constraints (max text size, min grid size) now that the grid is visible
		self._on_root_resize(None)

	def on_submit(self):
		# Toggle off speech if active
		if self.stt_listener.is_listening:
			self._toggle_speech()
		content = self.text.get("1.0", "end-1c")  # 'end-1c' removes final newline
		if not content.strip():
			messagebox.showwarning("Empty", "Please enter some text before submitting.")
			self.status_var.set("Nothing to submit")
			return				
		self.status_var.set(f"Submitted ({len(content)} chars)")	
		# Come up with an SQL command based on the plain text entry and given schema
		sql = self.generate_sql(content)
		# Get results of SQL command from given live database
		results = self.execute_sql(sql)
		# Populate and show results grid		
		self.load_to_tkinter_grid(results)
		self.result_frame.grid()

	def on_clear(self):
		self.text.delete("1.0", "end")
		self.status_var.set("Cleared")
		# Hide results grid when cleared
		self.result_frame.grid_remove()
		# Recalculate layout constraints now the grid is hidden
		self._on_root_resize(None)

	# STT listener integration helpers
	def _stt_transcribed(self, text: str):
		# Write transcription into the text widget (replace contents)
		try:
			self.text.delete('1.0', 'end')
			self.text.insert('1.0', text)
			self.status_var.set('Transcribed')
		except Exception:
			logging.exception('Error writing transcription into widget')

	def _stt_log(self, message: str):
		self.status_var.set(message)

	def _toggle_speech(self):
		# Toggle speech listening using the stt_listener instance
		if not getattr(self, 'stt_listener', None):
			return
		if not self.stt_listener.is_listening:
			self.stt_listener.start_speech()
			self.speech_toggle_btn.config(text='Stop Speech')
			self.status_var.set('Listening')
		else:
			self.stt_listener.stop_speech()
			self.speech_toggle_btn.config(text='Start Speech')
			self.status_var.set('Ready')

def main():
	logging.info("💡")
	# Show the login window first and only create the main root on success
	login = MariaDBLogin()
	# Optionally pre-fill default values as desired
	try:
		login.host_var.set("192.168.123.244")
		login.database_var.set("ai_db")
		login.username_var.set("ai")
		login.password_var.set("ai")
	except Exception:
		pass
	# Optionally pre-fill default values as desired
	# login.host_var.set("192.168.123.244")
	# login.database_var.set("ai_db")
	# login.username_var.set("ai")
	# login.password_var.set("ai")
	conn = login.run()
	# Ensure login window is properly closed
	try:
		login.destroy()
	except Exception:
		pass
	if conn is None:
		# User cancelled login; exit application
		logging.info("MariaDB login cancelled by user; exiting application.")
		return

	# Only initialize the main application GUI if login succeeded
	root = tk.Tk()
	# Set a sensible default window size
	root.title("AI English Query")
	root.geometry("640x420")
	root.minsize(640, 420)
	app = aiEnglishQuery("prompts.json", "chat_history.json", master=root, db=conn)
	root.mainloop()

if __name__ == '__main__':
	main()


