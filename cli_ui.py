"""
cli_ui.py
---------
Utilitas tampilan CLI interaktif berwarna (ANSI Escape Codes)
Kompatibel dengan Windows Terminal, PowerShell, CMD, dan Linux/macOS.
Dilengkapi penanganan encoding UTF-8 untuk mencegah UnicodeEncodeError pada Windows cp1252.
"""

import os
import sys

# Mengaktifkan encoding UTF-8 pada stdout & stderr Windows untuk mendukung karakter grafis
if sys.platform.startswith("win"):
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass
    os.system("")  # Aktifkan dukungan ANSI Virtual Terminal di Windows


class Colors:
    """Kode warna ANSI untuk styling teks terminal."""
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    ITALIC = "\033[3m"
    UNDERLINE = "\033[4m"

    # Foreground colors
    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"

    # Bright foreground colors
    BRIGHT_BLACK = "\033[90m"
    BRIGHT_RED = "\033[91m"
    BRIGHT_GREEN = "\033[92m"
    BRIGHT_YELLOW = "\033[93m"
    BRIGHT_BLUE = "\033[94m"
    BRIGHT_MAGENTA = "\033[95m"
    BRIGHT_CYAN = "\033[96m"
    BRIGHT_WHITE = "\033[97m"


def clear_screen():
    """Membersihkan layar terminal."""
    os.system("cls" if os.name == "nt" else "clear")


def banner():
    """Menampilkan banner sistem."""
    line = "=" * 65
    print(f"{Colors.BRIGHT_CYAN}{line}{Colors.RESET}")
    print(f"   {Colors.BOLD}{Colors.BRIGHT_WHITE}SISTEM DISTRIBUTED & SERIAL MERGE SORT{Colors.RESET}")
    print(f"   {Colors.BRIGHT_YELLOW}Socket Programming TCP/IP (Pure Python standard library){Colors.RESET}")
    print(f"{Colors.BRIGHT_CYAN}{line}{Colors.RESET}")


def print_header(title: str, subtitle: str = ""):
    """Mencetak header box yang rapi dan aman di semua terminal Windows."""
    width = 65
    print(f"\n{Colors.BRIGHT_BLUE}+{'-' * (width - 2)}+{Colors.RESET}")
    print(f"{Colors.BRIGHT_BLUE}|{Colors.BOLD}{Colors.BRIGHT_WHITE}{title.center(width - 2)}{Colors.RESET}{Colors.BRIGHT_BLUE}|{Colors.RESET}")
    if subtitle:
        print(f"{Colors.BRIGHT_BLUE}|{Colors.DIM}{Colors.CYAN}{subtitle.center(width - 2)}{Colors.RESET}{Colors.BRIGHT_BLUE}|{Colors.RESET}")
    print(f"{Colors.BRIGHT_BLUE}+{'-' * (width - 2)}+{Colors.RESET}")


def print_success(msg: str):
    print(f"{Colors.BRIGHT_GREEN}[OK] {msg}{Colors.RESET}")


def print_info(msg: str):
    print(f"{Colors.BRIGHT_CYAN}[i] {msg}{Colors.RESET}")


def print_warning(msg: str):
    print(f"{Colors.BRIGHT_YELLOW}[!] {msg}{Colors.RESET}")


def print_error(msg: str):
    print(f"{Colors.BRIGHT_RED}[X] {msg}{Colors.RESET}")


def print_task(msg: str):
    print(f"{Colors.BRIGHT_MAGENTA}[*] {msg}{Colors.RESET}")


def print_table_row(col1: str, col2: str, width1: int = 36, width2: int = 24, is_header: bool = False):
    """Mencetak baris tabel bergaya box aman encoding."""
    if is_header:
        border_top = f"{Colors.BRIGHT_CYAN}+{'-' * (width1 + 2)}+{'-' * (width2 + 2)}+{Colors.RESET}"
        content = f"{Colors.BRIGHT_CYAN}| {Colors.BOLD}{Colors.BRIGHT_WHITE}{col1:<{width1}}{Colors.RESET} {Colors.BRIGHT_CYAN}| {Colors.BOLD}{Colors.BRIGHT_WHITE}{col2:<{width2}}{Colors.RESET} {Colors.BRIGHT_CYAN}|{Colors.RESET}"
        border_mid = f"{Colors.BRIGHT_CYAN}+{'-' * (width1 + 2)}+{'-' * (width2 + 2)}+{Colors.RESET}"
        print(border_top)
        print(content)
        print(border_mid)
    else:
        content = f"{Colors.BRIGHT_CYAN}|{Colors.RESET} {col1:<{width1}} {Colors.BRIGHT_CYAN}|{Colors.RESET} {Colors.BOLD}{col2:<{width2}}{Colors.RESET} {Colors.BRIGHT_CYAN}|{Colors.RESET}"
        print(content)


def print_table_footer(width1: int = 36, width2: int = 24):
    border_bot = f"{Colors.BRIGHT_CYAN}+{'-' * (width1 + 2)}+{'-' * (width2 + 2)}+{Colors.RESET}"
    print(border_bot)


def print_progress_bar(current: int, total: int, prefix: str = "", suffix: str = "", length: int = 30, fill: str = "#"):
    """
    Menampilkan progress bar di terminal:
    Format:   Prefix                   [####################----------] 100.0% Suffix
    """
    if total <= 0:
        total = 1
    current = min(current, total)
    percent = (current / total) * 100
    filled_len = int(length * current // total)
    bar = fill * filled_len + "-" * (length - filled_len)

    sys.stdout.write(f"\r  {Colors.CYAN}{prefix:<24}{Colors.RESET} [{Colors.BRIGHT_GREEN}{bar}{Colors.RESET}] {Colors.BOLD}{Colors.BRIGHT_YELLOW}{percent:5.1f}%{Colors.RESET} {Colors.DIM}{suffix}{Colors.RESET}")
    sys.stdout.flush()
    if current >= total:
        sys.stdout.write("\n")
        sys.stdout.flush()

