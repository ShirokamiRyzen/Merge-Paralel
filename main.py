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
from cli_ui import (
    Colors, banner, clear_screen, print_header, print_info, print_warning, 
    print_success, box_border, box_title, box_line
)
from network_utils import get_local_ip, DEFAULT_MASTER_PORT
import master
import worker

DEFAULT_PORT = DEFAULT_MASTER_PORT


def main():
    while True:
        clear_screen()
        banner()
        local_ip = get_local_ip()

        print(f"  {Colors.DIM}Perangkat: {socket.gethostname()} | IP Lokal: {local_ip}{Colors.RESET}\n")
        print(box_border())
        print(box_title(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}PILIH PERAN KOMPUTER INI{Colors.RESET}"))
        print(box_border())
        print(box_line(f" {Colors.BRIGHT_CYAN}[1]{Colors.RESET} {Colors.BOLD}MASTER NODE (Server){Colors.RESET} - Pusat Kontrol & Koordinasi Sorting"))
        print(box_line(f" {Colors.BRIGHT_CYAN}[2]{Colors.RESET} {Colors.BOLD}WORKER NODE (Client){Colors.RESET} - Infinity Scan Master & Urutkan Data"))
        print(box_line(f" {Colors.BRIGHT_CYAN}[3]{Colors.RESET} {Colors.BOLD}LOCAL PARALLEL{Colors.RESET} - 1 Komputer, Semua Thread CPU (Tanpa Wi-Fi)"))
        print(box_line(f" {Colors.BRIGHT_CYAN}[4]{Colors.RESET} {Colors.BOLD}Hapus File .txt{Colors.RESET} (unsorted.txt & sorted.txt)"))
        print(box_line(f" {Colors.BRIGHT_RED}[0]{Colors.RESET} Keluar Program"))
        print(box_border())

        try:
            choice = input(f"{Colors.BRIGHT_YELLOW}Pilih opsi [1-4, 0]: {Colors.RESET}").strip()
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

        elif choice == "3":
            clear_screen()
            banner()
            master.run_local_parallel_cli()
            input(f"\n{Colors.DIM}Tekan [Enter] untuk kembali ke menu utama...{Colors.RESET}")

        elif choice == "4":
            success, msg = master.delete_txt_files()
            if success:
                print_success(msg)
            else:
                print_warning(msg)
            input(f"\n{Colors.DIM}Tekan [Enter] untuk kembali ke menu utama...{Colors.RESET}")

        elif choice == "0":
            print(f"\n{Colors.BRIGHT_GREEN}Program ditutup. Sampai jumpa!{Colors.RESET}\n")
            sys.exit(0)

        else:
            print_warning("Pilihan tidak valid. Silakan pilih 1, 2, 3, 4, atau 0.")


if __name__ == "__main__":
    main()
