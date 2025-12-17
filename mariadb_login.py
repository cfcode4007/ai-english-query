# © 2025 Colin Bond
# All rights reserved.

import tkinter as tk
from tkinter import messagebox
from tkinter import ttk
from mariadb_connector import MariaDBConnection

class MariaDBLogin:
    """Unified MariaDB login UI that can be used either as a standalone
    Tk window (no parent) or as a modal Toplevel (parent supplied).
    """
    version = "0.0.2"
    # - Version 0.0.2: Added cancel button

    def __init__(self, parent: tk.Misc | None = None, initial_values: dict | None = None):
        # Create window either as Toplevel (if parent given) or Tk
        if parent is None:
            self.win = tk.Tk()
            self._is_root = True
        else:
            self.win = tk.Toplevel(parent)
            self._is_root = False
            # set modal and transient behavior later in run()

        self.win.title("MariaDB Login")
        self.win.minsize(375, 200)
        self.win.resizable(False, False)

        self.username_var = tk.StringVar()
        self.password_var = tk.StringVar()
        self.host_var = tk.StringVar(value="localhost")
        self.port_var = tk.IntVar(value=3306)
        self.database_var = tk.StringVar()

        if initial_values:
            self.username_var.set(initial_values.get("user", ""))
            self.password_var.set(initial_values.get("password", ""))
            self.host_var.set(initial_values.get("host", "localhost"))
            self.port_var.set(initial_values.get("port", 3306))
            self.database_var.set(initial_values.get("database", ""))

        self._result: MariaDBConnection | None = None
        self.connection: MariaDBConnection | None = None
        self._build_ui()

    def _build_ui(self):
        frm = ttk.Frame(self.win, padding=12)
        # If we're a root (full window), center to 0,0; else place in central 3x3
        if self._is_root:
            frm.grid(row=0, column=0, sticky="nsew")
        else:
            self.win.grid_rowconfigure(0, weight=1)
            self.win.grid_rowconfigure(2, weight=1)
            self.win.grid_columnconfigure(0, weight=1)
            self.win.grid_columnconfigure(2, weight=1)
            frm.grid(row=1, column=1, sticky="nsew")

        ttk.Label(frm, text="Host").grid(row=0, column=0, sticky="w")
        ttk.Entry(frm, textvariable=self.host_var).grid(row=0, column=1, sticky="ew")
        ttk.Label(frm, text="Port").grid(row=0, column=2, sticky="w", padx=(10,0))
        ttk.Entry(frm, textvariable=self.port_var, width=8).grid(row=0, column=3, sticky="w")

        ttk.Label(frm, text="Database").grid(row=1, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(frm, textvariable=self.database_var).grid(row=1, column=1, columnspan=3, sticky="ew", pady=(10, 0))

        ttk.Label(frm, text="Username").grid(row=2, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(frm, textvariable=self.username_var).grid(row=2, column=1, columnspan=3, sticky="ew", pady=(10, 0))

        ttk.Label(frm, text="Password").grid(row=3, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(frm, textvariable=self.password_var, show="*").grid(row=3, column=1, columnspan=3, sticky="ew", pady=(10, 0))

        btn_frame = ttk.Frame(frm)
        btn_frame.grid(row=4, column=0, columnspan=4, pady=(15, 0))
        ttk.Button(btn_frame, text="Connect", command=self.on_connect).grid(row=0, column=0, padx=6)
        ttk.Button(btn_frame, text="Cancel", command=self.on_cancel).grid(row=0, column=1, padx=6)

        frm.columnconfigure(1, weight=1)

        # allow pressing Enter/Escape
        self.win.bind('<Return>', lambda e: self.on_connect())
        self.win.bind('<Escape>', lambda e: self.on_cancel())

    def on_connect(self):
        try:
            conn = MariaDBConnection(
                host=self.host_var.get(),
                port=int(self.port_var.get()),
                user=self.username_var.get(),
                password=self.password_var.get(),
                database=self.database_var.get() or None
            )
            ok = conn._connect()
            if not ok:
                raise RuntimeError("Failed to connect to MariaDB.")
            self.connection = conn
            self._result = conn
            # close window
            try:
                self.win.destroy()
            except Exception:
                pass
            return True
        except Exception as e:
            messagebox.showerror("Error", f"{e}")
            self._result = None
            return False

    def on_cancel(self):
        self._result = None
        try:
            self.win.destroy()
        except Exception:
            pass

    def run(self):
        """Show the window and return MariaDBConnection on success, else None.

        If running as Toplevel (modal), grabs focus and waits for window close.
        If running as Tk root (standalone), enters mainloop until the user closes.
        """
        if self._is_root:
            # Running as main window
            self.win.mainloop()
        else:
            # Modal dialog
            self.win.transient(self.win.master)
            self.win.grab_set()
            self.win.wait_window()
        return self._result    
    