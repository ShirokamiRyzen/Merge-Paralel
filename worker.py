"""
worker.py
---------
Client Node (Worker) untuk Sistem Distributed Merge Sort.
- Auto-Discovery: Otomatis mendeteksi Master di jaringan Wi-Fi/LAN tanpa perlu ketik IP manual.
- Menerima chunk data via TCP Socket, mengurutkannya dengan Timsort lokal (RAM).
- Mengembalikan hasil data terurut ke Master.
- Dilengkapi progress bar di setiap tahap komputasi.
"""

import sys
import time
import socket
import argparse
from typing import Optional, Tuple
from network_utils import send_packet, recv_packet, discover_masters, DEFAULT_MASTER_PORT
from cli_ui import (
    Colors, banner, print_header, print_success, print_info, 
    print_warning, print_error, print_task, print_progress_bar
)


def select_or_discover_master() -> Optional[Tuple[str, int]]:
    """
    Mendeteksi Master Server secara otomatis di jaringan Wi-Fi/LAN via UDP broadcast.
    Worker tinggal memilih dari daftar Master yang terdeteksi tanpa input manual IP.
    """
    while True:
        print_header("PENCARIAN MASTER SERVER OTOMATIS", "Memindai Master di jaringan Wi-Fi/LAN...")
        print_progress_bar(1, 2, prefix="Memindai Jaringan LAN", suffix="Mengirim UDP Probe...")
        masters = discover_masters(timeout=1.5)
        print_progress_bar(2, 2, prefix="Memindai Jaringan LAN", suffix="Selesai (100%)")

        if masters:
            print_success(f"Ditemukan {len(masters)} Master Server aktif di jaringan:\n")
            for idx, m in enumerate(masters, 1):
                print(f"  {Colors.BRIGHT_CYAN}[{idx}]{Colors.RESET} {Colors.BOLD}{m['name']}{Colors.RESET} ({Colors.BRIGHT_GREEN}{m['ip']}:{m['port']}{Colors.RESET})")
            print(f"  {Colors.BRIGHT_YELLOW}[M]{Colors.RESET} Masukkan IP Master secara manual\n")

            default_choice = "1"
            prompt = f"{Colors.BRIGHT_YELLOW}Pilih Master untuk dihubungkan [1-{len(masters)}, default: {default_choice}]: {Colors.RESET}"
            try:
                choice = input(prompt).strip()
            except (KeyboardInterrupt, EOFError):
                return None

            if not choice:
                choice = default_choice

            if choice.upper() == "M":
                try:
                    manual_ip = input(f"{Colors.BRIGHT_YELLOW}Masukkan IP Master: {Colors.RESET}").strip()
                    manual_port_str = input(f"{Colors.BRIGHT_YELLOW}Masukkan Port [{DEFAULT_MASTER_PORT}]: {Colors.RESET}").strip()
                    manual_port = int(manual_port_str) if manual_port_str else DEFAULT_MASTER_PORT
                    return manual_ip, manual_port
                except Exception:
                    return None

            try:
                sel_idx = int(choice) - 1
                if 0 <= sel_idx < len(masters):
                    chosen = masters[sel_idx]
                    return chosen["ip"], chosen["port"]
                else:
                    print_warning("Nomor pilihan tidak valid.")
            except ValueError:
                print_warning("Pilihan tidak valid.")
        else:
            print_warning("Tidak ada Master yang terdeteksi secara otomatis.")
            print(f"  {Colors.CYAN}[R]{Colors.RESET} Pindai Ulang (Scan Again)")
            print(f"  {Colors.CYAN}[M]{Colors.RESET} Masukkan IP Manual")
            print(f"  {Colors.RED}[0]{Colors.RESET} Batal / Kembali\n")
            try:
                fallback_choice = input(f"{Colors.BRIGHT_YELLOW}Pilih opsi [R/M/0, default: R]: {Colors.RESET}").strip().upper()
            except (KeyboardInterrupt, EOFError):
                return None

            if not fallback_choice or fallback_choice == "R":
                continue
            elif fallback_choice == "M":
                try:
                    manual_ip = input(f"{Colors.BRIGHT_YELLOW}Masukkan IP Master: {Colors.RESET}").strip()
                    manual_port_str = input(f"{Colors.BRIGHT_YELLOW}Masukkan Port [{DEFAULT_MASTER_PORT}]: {Colors.RESET}").strip()
                    manual_port = int(manual_port_str) if manual_port_str else DEFAULT_MASTER_PORT
                    return manual_ip, manual_port
                except Exception:
                    return None
            else:
                return None


def run_worker(host: str, port: int, worker_name: Optional[str] = None):
    """
    Fungsi utama worker untuk menghubungkan diri ke Master dan
    menjalankan instruksi sorting secara terdistribusi.
    """
    if not worker_name:
        worker_name = socket.gethostname()

    print_header("WORKER NODE - DISTRIBUTED CLIENT", f"Node: {worker_name} | Target Master: {host}:{port}")

    # Inisialisasi socket TCP
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    try:
        print_task(f"Menghubungkan ke Master pada {Colors.BOLD}{host}:{port}{Colors.RESET} ...")
        sock.connect((host, port))
        print_success("Berhasil terhubung ke Master Server!")
    except Exception as err:
        print_error(f"Gagal terhubung ke Master ({host}:{port}): {err}")
        print_info("Pastikan master.py sudah berjalan dan firewall tidak memblokir port.")
        return

    # Kirim pesan registrasi awal ke Master
    handshake_payload = {
        "cmd": "REGISTER",
        "name": worker_name,
        "hostname": socket.gethostname(),
    }
    if not send_packet(sock, handshake_payload):
        print_error("Gagal mengirim pesan registrasi ke Master.")
        sock.close()
        return

    print_info(f"Worker siap ({Colors.BRIGHT_GREEN}STANDBY{Colors.RESET}). Menunggu tugas sorting dari Master...\n")

    try:
        while True:
            packet = recv_packet(sock)
            if packet is None:
                print_warning("Koneksi terputus dari Master. Worker berhenti.")
                break

            cmd = packet.get("cmd")

            if cmd == "SORT":
                data_chunk = packet.get("data", [])
                chunk_id = packet.get("chunk_id", 0)
                n_items = len(data_chunk)

                print_header(f"TUGAS KOMPUTASI: CHUNK #{chunk_id}", f"Jumlah Data: {n_items:,} integer")
                print_progress_bar(1, 3, prefix="Penerimaan Paket TCP", suffix="Selesai (1/3)")

                t_start = time.perf_counter()
                data_chunk.sort()
                t_end = time.perf_counter()
                sort_duration = t_end - t_start

                print_progress_bar(2, 3, prefix="Timsort Lokal RAM", suffix=f"{sort_duration:.4f} dtk (2/3)")

                response = {
                    "status": "OK",
                    "chunk_id": chunk_id,
                    "worker_name": worker_name,
                    "sort_time": sort_duration,
                    "data": data_chunk,
                }
                t_send_start = time.perf_counter()
                if send_packet(sock, response):
                    t_send_end = time.perf_counter()
                    print_progress_bar(3, 3, prefix="Pengiriman Balik TCP", suffix=f"{t_send_end - t_send_start:.4f} dtk (3/3)")
                    print_success(f"Chunk #{chunk_id} berhasil diproses & dikembalikan ke Master!\n")
                else:
                    print_error("Gagal mengirim data kembali ke Master.")
                    break

            elif cmd == "PING":
                send_packet(sock, {"status": "PONG", "worker_name": worker_name})

            elif cmd == "SHUTDOWN":
                print_info("Menerima instruksi SHUTDOWN dari Master. Menutup koneksi...")
                break

            else:
                print_warning(f"Perintah tidak dikenali: {cmd}")

    except KeyboardInterrupt:
        print_warning("\nWorker dihentikan secara manual (Ctrl+C).")
    except Exception as err:
        print_error(f"\nTerjadi error pada worker: {err}")
    finally:
        try:
            sock.close()
        except Exception:
            pass
        print_info("Socket ditutup. Worker selesai.")


def run_worker_cli():
    """CLI interaktif untuk Worker: auto-discovery Master tanpa ketik IP manual."""
    target = select_or_discover_master()
    if not target:
        return
    host, port = target
    run_worker(host=host, port=port)


if __name__ == "__main__":
    banner()
    parser = argparse.ArgumentParser(description="Worker Node untuk Distributed Merge Sort")
    parser.add_argument(
        "--host",
        type=str,
        default=None,
        help="IP Address Master Server (opsional, jika tidak diisi akan dicari otomatis)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_MASTER_PORT,
        help=f"Port Master Server (default: {DEFAULT_MASTER_PORT})",
    )
    parser.add_argument(
        "--name",
        type=str,
        default=None,
        help="Nama identitas Worker (opsional)",
    )

    args = parser.parse_args()

    if args.host:
        run_worker(host=args.host, port=args.port, worker_name=args.name)
    else:
        run_worker_cli()
