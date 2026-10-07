"""
gui_app.py
----------
Antarmuka Grafis (GUI) berbasis Tkinter untuk Sistem Distributed & Serial
Merge Sort (TCP/IP Socket Programming).

GUI ini memakai ulang seluruh backend yang sudah ada:
- `network_utils.py` : framing TCP + auto-discovery UDP.
- `fastsort.py`      : akselerasi NumPy.
- `master.py`        : MasterServer, serial / distributed / local parallel sorting.

Fitur:
- Tab MASTER NODE  : jalankan server, lihat worker terhubung, generate data,
                     serial sort, distributed sort, hapus berkas.
- Tab WORKER NODE  : auto-scan / hubungkan manual, terima & urutkan chunk.
- Tab LOCAL PARALLEL: satu komputer, seluruh thread CPU (offline).
- Konsol berwarna (ANSI) + progress bar real-time.

Jalankan:  python gui_app.py
"""

import io
import os
import re
import sys
import time
import random
import queue
import socket
import threading
import builtins
import tkinter as tk
import tkinter.font as tkfont
from tkinter import ttk, messagebox, simpledialog

import cli_ui
import master
import worker
import fastsort
from network_utils import (
    WorkerScanner, get_local_ip, get_all_local_ips,
    DEFAULT_MASTER_PORT, send_packet, recv_packet, tune_socket,
)

DEFAULT_DATA_SIZE = master.DEFAULT_DATA_SIZE
FILE_UNSORTED = master.FILE_UNSORTED
FILE_SORTED = master.FILE_SORTED

UI_FONT_SIZE = 12
CONSOLE_FONT_SIZE = 12
CONSOLE_FONT_FAMILY = "Consolas"

_OUT_QUEUE: "queue.Queue" = queue.Queue()

_ANSI_RE = re.compile(r"\x1b\[([0-9;]*)([A-Za-z])")

_ANSI_FG = {
    "30": "#3b4252", "31": "#e05561", "32": "#8cc265", "33": "#d5a04a",
    "34": "#4aa5f0", "35": "#c162de", "36": "#42b3c2", "37": "#d7dae0",
    "90": "#5c6370", "91": "#ff616e", "92": "#a5e075", "93": "#f0c674",
    "94": "#61afef", "95": "#de98f6", "96": "#56b6c2", "97": "#ffffff",
}
_ANSI_STYLE = {"1": "bold", "2": "dim", "3": "italic", "4": "underline"}


class AnsiConsole(tk.Text):
    """Text widget yang memahami kode warna ANSI, '\\r' (overwrite baris), dan '\\n'."""

    def __init__(self, parent, max_lines: int = 5000, **kw):
        super().__init__(
            parent, wrap="none", bg="#0d1117", fg="#c9d1d9",
            insertbackground="#c9d1d9", font=(CONSOLE_FONT_FAMILY, CONSOLE_FONT_SIZE),
            relief="flat", padx=8, pady=6, state="disabled", **kw,
        )
        self._max_lines = max_lines
        self._active: list = []
        self._fg_tags = []
        for code, color in _ANSI_FG.items():
            tag = f"fg_{code}"
            self.tag_configure(tag, foreground=color)
            self._fg_tags.append(tag)
        base = (CONSOLE_FONT_FAMILY, CONSOLE_FONT_SIZE)
        self.tag_configure("bold", font=(base[0], base[1], "bold"))
        self.tag_configure("italic", font=(base[0], base[1], "italic"))
        self.tag_configure("underline", underline=True)
        self.tag_configure("dim", foreground="#6e7681")

    def append(self, text: str):
        if not text:
            return
        self.configure(state="normal")
        pos = 0
        for match in _ANSI_RE.finditer(text):
            self._insert(text[pos:match.start()])
            if match.group(2) == "m":
                self._apply_sgr(match.group(1))
            pos = match.end()
        self._insert(text[pos:])
        total = int(self.index("end-1c").split(".")[0])
        if total > self._max_lines:
            self.delete("1.0", f"{total - self._max_lines + 1}.0")
        self.configure(state="disabled")
        self.see("end")

    def clear(self):
        self.configure(state="normal")
        self.delete("1.0", "end")
        self.configure(state="disabled")

    def _insert(self, text: str):
        if not text:
            return
        tags = tuple(self._active)
        for part in re.split(r"(\r\n|\r|\n)", text):
            if part == "":
                continue
            if part in ("\n", "\r\n"):
                self.insert("end", "\n", tags)
            elif part == "\r":
                try:
                    self.delete("end-1c linestart", "end-1c")
                except tk.TclError:
                    pass
            else:
                self.insert("end", part, tags)

    def _apply_sgr(self, params: str):
        codes = [c for c in (params.split(";") if params else ["0"]) if c != ""] or ["0"]
        for code in codes:
            if code == "0":
                self._active = []
            elif code in _ANSI_FG:
                self._active = [t for t in self._active if t not in self._fg_tags]
                self._active.append(f"fg_{code}")
            elif code in ("1", "2"):
                self._active = [t for t in self._active if t not in ("bold", "dim")]
                self._active.append(_ANSI_STYLE[code])
            elif code in ("3", "4"):
                tag = _ANSI_STYLE[code]
                if tag not in self._active:
                    self._active.append(tag)
            elif code in ("22", "23", "24"):
                drop = {"22": ("bold", "dim"), "23": ("italic",), "24": ("underline",)}[code]
                self._active = [t for t in self._active if t not in drop]


class QueueWriter(io.TextIOBase):
    """Stream io yang mengirim teks ke antrean GUI (thread-safe)."""

    def __init__(self, q: "queue.Queue"):
        self._q = q

    def write(self, s):
        if s:
            self._q.put(("text", s))
        return len(s)

    def flush(self):
        pass

    def isatty(self):
        return False

    @property
    def encoding(self):
        return "utf-8"


def gui_print_progress_bar(current, total, prefix="", suffix="", length=30, fill="#"):
    if total <= 0:
        total = 1
    _OUT_QUEUE.put(("progress", min(current, total), total, prefix, suffix))


def plain_preview(data, max_items: int = 8) -> str:
    if not data:
        return "[Belum ada data]"
    if len(data) <= max_items:
        return str(list(data))
    half = max_items // 2
    front = ", ".join(str(x) for x in data[:half])
    back = ", ".join(str(x) for x in data[-half:])
    return f"[{front}, ... ({len(data):,} angka) ..., {back}]"


class GuiWorker:
    """Worker Node yang ramah GUI (tanpa input() blocking, bisa dihentikan)."""

    def __init__(self, host: str, port: int, name: str, on_status):
        self.host = host
        self.port = port
        self.name = name
        self.on_status = on_status
        self.sock: "socket.socket | None" = None
        self.stop_flag = False

    def stop(self):
        self.stop_flag = True
        if self.sock:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                self.sock.close()
            except Exception:
                pass

    def run(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tune_socket(sock)
            print(f"{cli_ui.Colors.BRIGHT_YELLOW}[Worker] Menghubungkan ke {self.host}:{self.port} ...{cli_ui.Colors.RESET}")
            sock.settimeout(5.0)
            sock.connect((self.host, self.port))
            sock.settimeout(None)
            self.sock = sock
            self.on_status("TERHUBUNG")
            print(f"{cli_ui.Colors.BRIGHT_GREEN}[Worker] Berhasil terhubung ke Master!{cli_ui.Colors.RESET}")

            threads = os.cpu_count() or 1
            register = {
                "cmd": "REGISTER",
                "name": self.name,
                "hostname": socket.gethostname(),
                "threads": threads,
            }
            if not send_packet(sock, register):
                print("[Worker] Gagal mengirim registrasi.")
                return
            print(f"[Worker] Siap (STANDBY) dengan {threads} thread. Menunggu tugas...\n")

            while not self.stop_flag:
                packet = recv_packet(sock, progress_callback=worker.stream_progress("Terima Stream TCP"))
                if packet is None:
                    if not self.stop_flag:
                        print("[Worker] Koneksi terputus dari Master.")
                    break

                cmd = packet.get("cmd")
                if cmd == "SORT":
                    self._handle_sort(packet, threads)
                elif cmd == "GENERATE_UNSORTED":
                    self._handle_generate(packet)
                elif cmd == "SORT_ON_FLY":
                    self._handle_on_fly(packet, threads)
                elif cmd == "PING":
                    send_packet(sock, {"status": "PONG", "worker_name": self.name})
                elif cmd == "SHUTDOWN":
                    print("[Worker] Menerima instruksi SHUTDOWN dari Master.")
                    break
                elif cmd == "SUMMARY":
                    print("\n" + "=" * 62)
                    print(packet.get("title", "RINGKASAN DARI MASTER NODE"))
                    for line in packet.get("lines", []):
                        print(line)
                    print("=" * 62 + "\n")
                else:
                    print(f"[Worker] Perintah tidak dikenali: {cmd}")
        except Exception as err:
            if not self.stop_flag:
                print(f"{cli_ui.Colors.BRIGHT_RED}[Worker] Error: {err}{cli_ui.Colors.RESET}")
        finally:
            try:
                if self.sock:
                    self.sock.close()
            except Exception:
                pass
            self.on_status("TERPUTUS")
            print("[Worker] Worker berhenti.")

    def _handle_sort(self, packet, threads):
        data = packet.get("data", [])
        chunk_id = packet.get("chunk_id", 0)
        seed = packet.get("seed")
        session_seed = packet.get("session_seed")
        n_items = len(data)
        print(f"[Worker] Chunk #{chunk_id}: mengurutkan {n_items:,} data "
              f"(seed={seed}, sesi={session_seed}) ...")
        t0 = time.perf_counter()
        data = fastsort.sort_values(data, n_threads=threads)
        duration = time.perf_counter() - t0
        response = {
            "status": "OK", "chunk_id": chunk_id, "worker_name": self.name,
            "threads": threads, "seed": seed, "session_seed": session_seed,
            "sort_time": duration, "data": fastsort.to_int32(data),
        }
        if send_packet(self.sock, response):
            print(f"[Worker] Chunk #{chunk_id} selesai ({duration:.4f}s), dikirim balik ke Master.")
        else:
            self.stop_flag = True

    def _handle_generate(self, packet):
        n_items = packet.get("count", 500_000)
        chunk_id = packet.get("chunk_id", 0)
        seed = packet.get("seed")
        print(f"[Worker] Generate #{chunk_id}: membangkitkan {n_items:,} angka acak ...")
        t0 = time.perf_counter()
        rng = random.Random(seed) if seed is not None else random.Random()
        data = [rng.randint(1, 10_000_000) for _ in range(n_items)]
        duration = time.perf_counter() - t0
        response = {
            "status": "OK", "chunk_id": chunk_id, "worker_name": self.name,
            "gen_time": duration, "data": fastsort.to_int32(data),
        }
        if send_packet(self.sock, response, progress_callback=worker.stream_progress("Kirim Stream TCP")):
            print(f"[Worker] Generate #{chunk_id} ({n_items:,} data) selesai ({duration:.4f}s), dikirim ke Master.")
        else:
            self.stop_flag = True

    def _handle_on_fly(self, packet, threads):
        n_items = packet.get("count", 500_000)
        chunk_id = packet.get("chunk_id", 0)
        seed = packet.get("seed")
        print(f"[Worker] On-the-fly #{chunk_id}: generate + sort {n_items:,} data ...")
        t0 = time.perf_counter()
        rng = random.Random(seed) if seed is not None else random.Random()
        data = [rng.randint(1, 10_000_000) for _ in range(n_items)]
        data = fastsort.sort_values(data, n_threads=threads)
        duration = time.perf_counter() - t0
        response = {
            "status": "OK", "chunk_id": chunk_id, "worker_name": self.name,
            "threads": threads, "sort_time": duration, "data": fastsort.to_int32(data),
        }
        if send_packet(self.sock, response, progress_callback=worker.stream_progress("Kirim Stream TCP")):
            print(f"[Worker] On-the-fly #{chunk_id} selesai ({duration:.4f}s).")
        else:
            self.stop_flag = True


class MergeSortGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Distributed & Serial Merge Sort - GUI")
        self.root.geometry("1280x860")
        self.root.minsize(1080, 740)
        self._configure_appearance()

        self.busy = False
        self.server: "master.MasterServer | None" = None
        self.current_data = None
        self.waktu_unsort = None
        self.last_serial_time = None
        self.last_dist_time = None
        self.last_nodes_count = 1

        self.scanner: "WorkerScanner | None" = None
        self.worker_client: "GuiWorker | None" = None
        self.worker_running = False
        self._scan_snapshot = None
        self._master_snapshot = None

        self.action_buttons = []
        self._build_ui()

        sys.stdout = QueueWriter(_OUT_QUEUE)
        sys.stderr = QueueWriter(_OUT_QUEUE)

        self._print_intro()
        self.root.after(40, self._drain_queue)
        self.root.after(1000, self._refresh_master_status)
        self.root.after(900, self._refresh_worker_scan)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _configure_appearance(self):
        try:
            self.root.call("tk", "scaling", 1.2)
        except tk.TclError:
            pass
        for name in ("TkDefaultFont", "TkTextFont", "TkMenuFont", "TkHeadingFont", "TkTooltipFont"):
            try:
                tkfont.nametofont(name).configure(size=UI_FONT_SIZE)
            except tk.TclError:
                pass
        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass
        style.configure("TButton", font=("Segoe UI", UI_FONT_SIZE, "bold"), padding=(12, 8))
        style.configure("TLabel", font=("Segoe UI", UI_FONT_SIZE))
        style.configure("TLabelframe.Label", font=("Segoe UI", UI_FONT_SIZE, "bold"))
        style.configure("TNotebook.Tab", font=("Segoe UI", UI_FONT_SIZE + 1, "bold"), padding=(24, 10))
        style.map(
            "TNotebook.Tab",
            padding=[("selected", (24, 10)), ("!selected", (24, 10))],
            expand=[("selected", (0, 0, 0, 0)), ("!selected", (0, 0, 0, 0))],
            background=[("selected", "#ffffff"), ("active", "#e9edf2"), ("!selected", "#d8dee6")],
            foreground=[("selected", "#1f6feb"), ("!selected", "#333333")],
        )
        style.configure("TEntry", padding=4)
        style.configure("Treeview", font=("Segoe UI", UI_FONT_SIZE), rowheight=26)
        style.configure("Horizontal.TProgressbar", thickness=22)

    # ------------------------------------------------------------------ UI --

    def _build_ui(self):
        header = tk.Frame(self.root, bg="#1f6feb", height=56)
        header.pack(fill="x")
        tk.Label(
            header, text="  SISTEM DISTRIBUTED & SERIAL MERGE SORT",
            bg="#1f6feb", fg="white", font=("Segoe UI", 17, "bold"),
        ).pack(side="left", padx=12, pady=10)
        tk.Label(
            header, text="Socket Programming TCP/IP  |  Tkinter GUI  ",
            bg="#1f6feb", fg="#cfe1ff", font=("Segoe UI", 11),
        ).pack(side="right", padx=12)

        self.nb = ttk.Notebook(self.root)
        self.nb.pack(fill="both", expand=True, padx=8, pady=6)

        self.tab_master = ttk.Frame(self.nb, padding=10)
        self.tab_worker = ttk.Frame(self.nb, padding=10)
        self.tab_local = ttk.Frame(self.nb, padding=10)
        self.nb.add(self.tab_master, text="  Master Node  ")
        self.nb.add(self.tab_worker, text="  Worker Node  ")
        self.nb.add(self.tab_local, text="  Local Parallel  ")

        self._build_master_tab()
        self._build_worker_tab()
        self._build_local_tab()
        self._build_console()

    def _build_master_tab(self):
        info = ttk.LabelFrame(self.tab_master, text="Status Master", padding=10)
        info.pack(fill="x")

        self.lbl_master_ip = ttk.Label(info, text="-")
        self.lbl_workers = ttk.Label(info, text="0 node (server belum aktif)")
        self.lbl_unsorted = ttk.Label(info, text="-")
        self.lbl_sorted = ttk.Label(info, text="-")
        self.lbl_preview = ttk.Label(info, text="-", wraplength=980, justify="left")

        rows = [
            ("Alamat IP Master", self.lbl_master_ip),
            ("Worker Terhubung", self.lbl_workers),
            ("unsorted.txt", self.lbl_unsorted),
            ("sorted.txt", self.lbl_sorted),
            ("Pratinjau Data", self.lbl_preview),
        ]
        for i, (label, widget) in enumerate(rows):
            ttk.Label(info, text=f"{label} :", font=("Segoe UI", UI_FONT_SIZE, "bold")).grid(
                row=i, column=0, sticky="nw", padx=(0, 8), pady=2)
            widget.grid(row=i, column=1, sticky="w", pady=2)

        ctrl = ttk.LabelFrame(self.tab_master, text="Kontrol Master", padding=10)
        ctrl.pack(fill="x", pady=(10, 0))

        self.btn_server = ttk.Button(ctrl, text="Mulai Server Master", command=self.toggle_master_server)
        self.btn_gen = ttk.Button(ctrl, text="Generate Data", command=self.master_generate)
        self.btn_serial = ttk.Button(ctrl, text="Serial Sorting", command=self.master_serial)
        self.btn_dist = ttk.Button(ctrl, text="Distributed Sorting", command=self.master_distributed)
        self.btn_del = ttk.Button(ctrl, text="Hapus File .txt", command=self.delete_files)

        self.btn_server.grid(row=0, column=0, padx=4, pady=4, sticky="ew")
        self.btn_gen.grid(row=0, column=1, padx=4, pady=4, sticky="ew")
        self.btn_serial.grid(row=0, column=2, padx=4, pady=4, sticky="ew")
        self.btn_dist.grid(row=0, column=3, padx=4, pady=4, sticky="ew")
        self.btn_del.grid(row=0, column=4, padx=4, pady=4, sticky="ew")
        for c in range(5):
            ctrl.columnconfigure(c, weight=1)

        self.action_buttons.extend([self.btn_gen, self.btn_serial, self.btn_dist, self.btn_del])

        wl = ttk.LabelFrame(self.tab_master, text="Daftar Worker Aktif", padding=8)
        wl.pack(fill="both", expand=True, pady=(10, 0))
        self.worker_listbox = tk.Listbox(wl, height=6, font=(CONSOLE_FONT_FAMILY, UI_FONT_SIZE), activestyle="none")
        self.worker_listbox.pack(fill="both", expand=True)

    def _build_worker_tab(self):
        top = ttk.LabelFrame(self.tab_worker, text="Koneksi ke Master", padding=10)
        top.pack(fill="x")

        ttk.Label(top, text="IP Master:").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        self.ent_host = ttk.Entry(top, width=20)
        self.ent_host.grid(row=0, column=1, sticky="w", padx=4, pady=4)
        ttk.Label(top, text="Port:").grid(row=0, column=2, sticky="w", padx=4, pady=4)
        self.ent_port = ttk.Entry(top, width=8)
        self.ent_port.insert(0, str(DEFAULT_MASTER_PORT))
        self.ent_port.grid(row=0, column=3, sticky="w", padx=4, pady=4)

        self.btn_scan = ttk.Button(top, text="Mulai Auto-Scan", command=self.toggle_scan)
        self.btn_scan.grid(row=0, column=4, sticky="ew", padx=4, pady=4)
        self.btn_worker_connect = ttk.Button(top, text="Hubungkan Manual", command=self.connect_worker_manual)
        self.btn_worker_connect.grid(row=0, column=5, sticky="ew", padx=4, pady=4)
        self.btn_worker_disconnect = ttk.Button(top, text="Putuskan", command=self.disconnect_worker, state="disabled")
        self.btn_worker_disconnect.grid(row=0, column=6, sticky="ew", padx=4, pady=4)
        for c in range(7):
            top.columnconfigure(c, weight=1)

        self.lbl_worker_status = ttk.Label(
            top, text="Status: TERPUTUS", font=("Segoe UI", UI_FONT_SIZE + 1, "bold"), foreground="#c0392b")
        self.lbl_worker_status.grid(row=1, column=0, columnspan=7, sticky="w", padx=4, pady=(8, 0))

        scan = ttk.LabelFrame(self.tab_worker, text="Master Terdeteksi (Auto-Discovery UDP)", padding=8)
        scan.pack(fill="both", expand=True, pady=(10, 0))
        self.master_listbox = tk.Listbox(scan, height=7, font=(CONSOLE_FONT_FAMILY, UI_FONT_SIZE), activestyle="none")
        self.master_listbox.pack(fill="both", expand=True)
        self.btn_connect_selected = ttk.Button(scan, text="Hubungkan ke Master Terpilih", command=self.connect_worker_selected)
        self.btn_connect_selected.pack(fill="x", pady=(8, 0))
        self.action_buttons.append(self.btn_connect_selected)

    def _build_local_tab(self):
        info = ttk.LabelFrame(self.tab_local, text="Local Parallel (1 Komputer, Tanpa Jaringan)", padding=10)
        info.pack(fill="x")
        threads = os.cpu_count() or 1
        ttk.Label(info, text=f"CPU Lokal Terdeteksi : {threads} thread", font=("Segoe UI", UI_FONT_SIZE, "bold")).pack(anchor="w")
        ttk.Label(info, text="Mode ini tidak membutuhkan Wi-Fi/LAN maupun perangkat lain.").pack(anchor="w")

        self.lbl_local_unsorted = ttk.Label(info, text="unsorted.txt : -")
        self.lbl_local_sorted = ttk.Label(info, text="sorted.txt : -")
        self.lbl_local_preview = ttk.Label(info, text="Pratinjau : -", wraplength=980, justify="left")
        self.lbl_local_unsorted.pack(anchor="w", pady=(6, 0))
        self.lbl_local_sorted.pack(anchor="w")
        self.lbl_local_preview.pack(anchor="w")

        ctrl = ttk.LabelFrame(self.tab_local, text="Kontrol Local Parallel", padding=10)
        ctrl.pack(fill="x", pady=(10, 0))
        self.btn_l_gen = ttk.Button(ctrl, text="Generate Data", command=self.local_generate)
        self.btn_l_serial = ttk.Button(ctrl, text="Serial Sorting (Baseline)", command=self.local_serial)
        self.btn_l_par = ttk.Button(ctrl, text="Local Parallel Sorting", command=self.local_parallel)
        self.btn_l_del = ttk.Button(ctrl, text="Hapus File .txt", command=self.delete_files)
        for i, b in enumerate([self.btn_l_gen, self.btn_l_serial, self.btn_l_par, self.btn_l_del]):
            b.grid(row=0, column=i, padx=4, pady=4, sticky="ew")
            ctrl.columnconfigure(i, weight=1)
        self.action_buttons.extend([self.btn_l_gen, self.btn_l_serial, self.btn_l_par, self.btn_l_del])

    def _build_console(self):
        outer = ttk.LabelFrame(self.root, text="Konsol & Progres", padding=6)
        outer.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        pr = ttk.Frame(outer)
        pr.pack(fill="x", pady=(0, 4))
        self.pbar = ttk.Progressbar(pr, mode="determinate", maximum=100)
        self.pbar.pack(fill="x", side="left", expand=True)
        self.lbl_progress = ttk.Label(pr, text="", width=52, anchor="e")
        self.lbl_progress.pack(side="right", padx=(8, 0))

        text_frame = ttk.Frame(outer)
        text_frame.pack(fill="both", expand=True)
        self.console = AnsiConsole(text_frame)
        yscroll = ttk.Scrollbar(text_frame, orient="vertical", command=self.console.yview)
        xscroll = ttk.Scrollbar(text_frame, orient="horizontal", command=self.console.xview)
        self.console.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)
        self.console.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")
        text_frame.rowconfigure(0, weight=1)
        text_frame.columnconfigure(0, weight=1)

        bar = ttk.Frame(outer)
        bar.pack(fill="x", pady=(4, 0))
        ttk.Button(bar, text="Bersihkan Log", command=self.console.clear).pack(side="right")

    def _print_intro(self):
        print("Distributed & Serial Merge Sort - GUI siap digunakan.")
        print("Pilih tab: Master Node, Worker Node, atau Local Parallel.\n")

    # ------------------------------------------------------------- Helpers --

    def _push_status(self, text, color="#c0392b"):
        self.root.after(0, lambda: self.lbl_worker_status.config(text=f"Status: {text}", foreground=color))

    def _set_controls_enabled(self, enabled: bool):
        state = "normal" if enabled else "disabled"
        for btn in self.action_buttons:
            try:
                btn.config(state=state)
            except tk.TclError:
                pass

    def _run_task(self, fn):
        if self.busy:
            messagebox.showinfo("Sedang sibuk", "Tunggu hingga proses yang berjalan selesai.")
            return
        self.busy = True
        self._set_controls_enabled(False)

        def wrapper():
            try:
                fn()
            except Exception as err:
                import traceback
                print(f"\n[GUI ERROR] {err}")
                traceback.print_exc()
            finally:
                self.root.after(0, self._task_finished)

        threading.Thread(target=wrapper, daemon=True).start()

    def _task_finished(self):
        self.busy = False
        self._set_controls_enabled(True)
        self._refresh_master_status_once()

    def _ensure_data(self):
        if self.current_data:
            return self.current_data
        loaded = master.load_from_file(FILE_UNSORTED)
        if loaded:
            self.current_data = loaded
            return loaded
        messagebox.showwarning("Data kosong", "Belum ada data. Silakan lakukan Generate Data terlebih dahulu.")
        return None

    def _ask_n(self, title="Jumlah Data"):
        return simpledialog.askinteger(
            title, "Jumlah angka acak N:", initialvalue=DEFAULT_DATA_SIZE,
            minvalue=1, parent=self.root,
        )

    # --------------------------------------------------------- Queue drain --

    def _drain_queue(self):
        processed = 0
        try:
            while processed < 500:
                item = _OUT_QUEUE.get_nowait()
                processed += 1
                if item[0] == "text":
                    self.console.append(item[1])
                elif item[0] == "progress":
                    _, cur, tot, prefix, suffix = item
                    self._update_progress(cur, tot, prefix, suffix)
        except queue.Empty:
            pass
        self.root.after(40, self._drain_queue)

    def _update_progress(self, cur, tot, prefix, suffix):
        self.pbar.config(maximum=tot, value=cur)
        pct = (cur / tot * 100) if tot else 0
        self.lbl_progress.config(text=f"{prefix} {cur:,}/{tot:,} ({pct:5.1f}%) {suffix}")

    # ---------------------------------------------------------- Master tab --

    def toggle_master_server(self):
        if self.busy:
            messagebox.showinfo("Sedang sibuk", "Tunggu proses selesai untuk mengubah server.")
            return
        if self.server is None:
            try:
                srv = master.MasterServer(host="0.0.0.0", port=DEFAULT_MASTER_PORT)
                srv.start_server()
                self.server = srv
                self.btn_server.config(text="Stop Server Master")
                print(f"{cli_ui.Colors.BRIGHT_GREEN}[GUI] Master server aktif pada port {DEFAULT_MASTER_PORT}.{cli_ui.Colors.RESET}")
            except Exception as err:
                messagebox.showerror("Gagal", f"Tidak dapat memulai server pada port {DEFAULT_MASTER_PORT}:\n{err}")
        else:
            self.server.shutdown()
            self.server = None
            self.btn_server.config(text="Mulai Server Master")
            print("[GUI] Master server dihentikan.")

    def master_generate(self):
        n = self._ask_n()
        if not n:
            return

        def task():
            data, waktu = master.generate_random_data(n, server=self.server)
            self.current_data = data
            self.waktu_unsort = waktu

        self._run_task(task)

    def master_serial(self):
        data = self._ensure_data()
        if data is None:
            return

        def task():
            total, ok, result = master.run_serial_sorting(data, waktu_unsort=self.waktu_unsort)
            self.last_serial_time = total
            self.current_data = result

        self._run_task(task)

    def master_distributed(self):
        if self.server is None:
            messagebox.showwarning("Server belum aktif", "Jalankan Server Master terlebih dahulu.")
            return
        if len(self.server.get_active_workers()) == 0:
            messagebox.showwarning("Tanpa Worker", "Belum ada worker terhubung. Jalankan Worker Node terlebih dahulu.")
            return
        data = self._ensure_data()
        if data is None:
            return

        def task():
            result = master.run_distributed_sorting(self.server, data, waktu_unsort=self.waktu_unsort)
            if result:
                total, ok, sorted_data, nodes, threads = result
                self.last_dist_time = total
                self.last_nodes_count = nodes
                self.current_data = sorted_data
                if self.last_serial_time is not None:
                    master.print_comparison_metrics(
                        self.last_serial_time, total,
                        total_computers=nodes,
                        num_workers=len(self.server.get_active_workers()),
                    )

        self._run_task(task)

    def delete_files(self):
        ok, msg = master.delete_txt_files()
        self.current_data = None
        if ok:
            print(f"{cli_ui.Colors.BRIGHT_GREEN}[OK] {msg}{cli_ui.Colors.RESET}")
        else:
            print(f"{cli_ui.Colors.BRIGHT_YELLOW}[!] {msg}{cli_ui.Colors.RESET}")
        self._refresh_master_status_once()

    def _refresh_master_status(self):
        self._refresh_master_status_once()
        self.root.after(1200, self._refresh_master_status)

    def _refresh_master_status_once(self):
        try:
            ips = get_all_local_ips()
            self.lbl_master_ip.config(
                text=" | ".join(f"{ip}:{DEFAULT_MASTER_PORT}" for ip in ips[:2]) or "-")

            if self.server is not None:
                workers = self.server.get_active_workers()
                names = ", ".join(w["name"] for w in workers)
                self.lbl_workers.config(
                    text=f"{len(workers)} node" + (f" ({names})" if names else " (menunggu worker...)"))
                snapshot = tuple(w["name"] for w in workers)
                if snapshot != self._master_snapshot:
                    self._master_snapshot = snapshot
                    self.worker_listbox.delete(0, "end")
                    for w in workers:
                        self.worker_listbox.insert(
                            "end", f"{w['name']}  -  {w['addr'][0]}:{w['addr'][1]}  [{w['threads']} thread]")
            else:
                self.lbl_workers.config(text="0 node (server belum aktif)")

            data = self.current_data
            if data is None and os.path.exists(FILE_UNSORTED):
                data = None
            unsorted_txt = (f"ADA ({len(self.current_data):,} data di memori)"
                            if self.current_data else "Belum ada / belum dimuat")
            self.lbl_unsorted.config(text=unsorted_txt)
            self.lbl_sorted.config(text="ADA" if os.path.exists(FILE_SORTED) else "Belum ada")
            preview = plain_preview(self.current_data) if self.current_data else "-"
            self.lbl_preview.config(text=preview)

            self.lbl_local_unsorted.config(text=f"unsorted.txt : {unsorted_txt}")
            self.lbl_local_sorted.config(text=f"sorted.txt : {'ADA' if os.path.exists(FILE_SORTED) else 'Belum ada'}")
            self.lbl_local_preview.config(text=f"Pratinjau : {preview}")
        except Exception:
            pass

    # ---------------------------------------------------------- Worker tab --

    def toggle_scan(self):
        if self.scanner is None:
            self.scanner = WorkerScanner()
            self.scanner.start()
            self.btn_scan.config(text="Stop Auto-Scan")
            print("[GUI] Memulai auto-scan Master (UDP)...")
        else:
            self.scanner.stop()
            self.scanner = None
            self._scan_snapshot = None
            self.master_listbox.delete(0, "end")
            self.btn_scan.config(text="Mulai Auto-Scan")
            print("[GUI] Auto-scan dihentikan.")

    def _refresh_worker_scan(self):
        try:
            if self.scanner is not None:
                masters = self.scanner.get_masters()
                snapshot = tuple(f"{m.get('name')}:{m['ip']}:{m['port']}" for m in masters)
                if snapshot != self._scan_snapshot:
                    self._scan_snapshot = snapshot
                    self.master_listbox.delete(0, "end")
                    for m in masters:
                        self.master_listbox.insert(
                            "end", f"{m.get('name', 'Master-Node'):<22} {m['ip']}:{m['port']}")
                    self.masters_cache = masters
        except Exception:
            pass
        self.root.after(900, self._refresh_worker_scan)

    def connect_worker_selected(self):
        masters = self.scanner.get_masters() if self.scanner else []
        sel = self.master_listbox.curselection()
        if not sel or sel[0] >= len(masters):
            messagebox.showinfo("Pilih Master", "Pilih salah satu Master dari daftar terlebih dahulu.")
            return
        m = masters[sel[0]]
        self._start_worker(m["ip"], m["port"])

    def connect_worker_manual(self):
        host = self.ent_host.get().strip()
        if not host:
            messagebox.showinfo("IP kosong", "Masukkan alamat IP Master terlebih dahulu.")
            return
        try:
            port = int(self.ent_port.get().strip() or DEFAULT_MASTER_PORT)
        except ValueError:
            port = DEFAULT_MASTER_PORT
        self._start_worker(host, port)

    def _start_worker(self, host, port):
        if self.worker_running:
            messagebox.showinfo("Worker aktif", "Worker sedang terhubung. Putuskan dahulu.")
            return
        self.worker_running = True
        self.btn_worker_connect.config(state="disabled")
        self.btn_connect_selected.config(state="disabled")
        self.btn_worker_disconnect.config(state="normal")
        name = f"{socket.gethostname()}-GUI"
        self.worker_client = GuiWorker(host, port, name, self._push_status)

        def run():
            try:
                self.worker_client.run()
            finally:
                self.root.after(0, self._worker_finished)

        threading.Thread(target=run, daemon=True).start()

    def _worker_finished(self):
        self.worker_running = False
        self.btn_worker_connect.config(state="normal")
        self.btn_connect_selected.config(state="normal")
        self.btn_worker_disconnect.config(state="disabled")
        self.lbl_worker_status.config(text="Status: TERPUTUS", foreground="#c0392b")

    def disconnect_worker(self):
        if self.worker_client:
            print("[GUI] Memutuskan koneksi worker...")
            self.worker_client.stop()

    # ----------------------------------------------------------- Local tab --

    def local_generate(self):
        n = self._ask_n()
        if not n:
            return

        def task():
            data, waktu = master.generate_random_data(n, server=None)
            self.current_data = data
            self.waktu_unsort = waktu

        self._run_task(task)

    def local_serial(self):
        data = self._ensure_data()
        if data is None:
            return

        def task():
            total, ok, result = master.run_serial_sorting(data, waktu_unsort=self.waktu_unsort)
            self.last_serial_time = total
            self.current_data = result

        self._run_task(task)

    def local_parallel(self):
        data = self._ensure_data()
        if data is None:
            return
        max_threads = os.cpu_count() or 1
        n_threads = simpledialog.askinteger(
            "Jumlah Thread", f"Jumlah thread CPU (maks {max_threads}):",
            initialvalue=max_threads, minvalue=1, maxvalue=max_threads, parent=self.root)
        if not n_threads:
            return

        def task():
            total, ok, result = master.run_local_parallel_sorting(
                data, waktu_unsort=self.waktu_unsort, n_threads=n_threads)
            self.current_data = result
            if self.last_serial_time is not None:
                master.print_comparison_metrics(self.last_serial_time, total, total_computers=1)

        self._run_task(task)

    # ---------------------------------------------------------- Lifecycle ---

    def _on_close(self):
        try:
            if self.worker_client:
                self.worker_client.stop()
            if self.scanner:
                self.scanner.stop()
            if self.server:
                self.server.shutdown()
        except Exception:
            pass
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        self.root.destroy()


def main():
    cli_ui.print_progress_bar = gui_print_progress_bar
    master.print_progress_bar = gui_print_progress_bar
    worker.print_progress_bar = gui_print_progress_bar

    root = tk.Tk()
    try:
        root.call("tk", "scaling", 1.1)
    except Exception:
        pass
    MergeSortGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
