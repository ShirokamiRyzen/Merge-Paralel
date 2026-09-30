"""
main.py
-------
Pusat Kendali Sistem Distributed & Serial Sorting.
Pilih apakah komputer ini akan bertindak sebagai:
  [1] MASTER (Server Node - Input data, koordinasi, & evaluasi sorting)
  [2] WORKER (Client Node - Membantu proses pengurutan data via TCP)
"""

import sys
import socket
from cli_ui import Colors, banner, clear_screen, print_header, print_info, print_warning
import master
import worker

DEFAULT_PORT = 5000


def get_local_ip() -> str:
    """Mendeteksi IP LAN lokal perangkat saat ini."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip


def main():
    while True:
        clear_screen()
        banner()
        local_ip = get_local_ip()

        print(f"  {Colors.DIM}Perangkat: {socket.gethostname()} | IP Lokal: {local_ip}{Colors.RESET}\n")
        print(f" {Colors.BRIGHT_BLUE}+-------------------------------------------------------------------+{Colors.RESET}")
        print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET}                     {Colors.BOLD}{Colors.BRIGHT_WHITE}PILIH PERAN KOMPUTER INI{Colors.RESET}                      {Colors.BRIGHT_BLUE}|{Colors.RESET}")
        print(f" {Colors.BRIGHT_BLUE}+-------------------------------------------------------------------+{Colors.RESET}")
        print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET}  {Colors.BRIGHT_CYAN}[1]{Colors.RESET} {Colors.BOLD}MASTER NODE (Server){Colors.RESET} - Bangkitkan Data, Koordinasi & Sorting {Colors.BRIGHT_BLUE}|{Colors.RESET}")
        print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET}  {Colors.BRIGHT_CYAN}[2]{Colors.RESET} {Colors.BOLD}WORKER NODE (Client){Colors.RESET} - Hubungkan ke Master & Urutkan Data   {Colors.BRIGHT_BLUE}|{Colors.RESET}")
        print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET}  {Colors.BRIGHT_RED}[0]{Colors.RESET} Keluar Program                                                {Colors.BRIGHT_BLUE}|{Colors.RESET}")
        print(f" {Colors.BRIGHT_BLUE}+-------------------------------------------------------------------+{Colors.RESET}\n")

        try:
            choice = input(f"{Colors.BRIGHT_YELLOW}Pilih opsi [1, 2, 0]: {Colors.RESET}").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n")
            break

        if choice == "1":
            master.run_master_cli(port=DEFAULT_PORT)

        elif choice == "2":
            clear_screen()
            banner()
            worker.run_worker_cli()
            input(f"\n{Colors.DIM}Tekan [Enter] untuk kembali ke menu utama...{Colors.RESET}")

        elif choice == "0":
            print(f"\n{Colors.BRIGHT_GREEN}Program ditutup. Sampai jumpa!{Colors.RESET}\n")
            sys.exit(0)

        else:
            print_warning("Pilihan tidak valid. Silakan pilih 1, 2, atau 0.")


if __name__ == "__main__":
    main()
