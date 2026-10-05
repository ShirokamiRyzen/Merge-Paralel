"""
worker.py
---------
Client Node (Worker) untuk Sistem Distributed Merge Sort.
- Auto-Discovery: Otomatis mendeteksi Master di jaringan Wi-Fi/LAN tanpa perlu ketik IP manual.
- Menerima chunk data via TCP Socket, mengurutkannya dengan Timsort lokal (RAM).
- Mengembalikan hasil data terurut ke Master.
- Dilengkapi progress bar di setiap tahap komputasi.
"""

import os
import sys
import time
import socket
import argparse
import concurrent.futures
from typing import Optional, Tuple, List
from network_utils import send_packet, recv_packet, discover_masters, WorkerScanner, DEFAULT_MASTER_PORT, get_local_ip, tune_socket
from cli_ui import (
    Colors, banner, clear_screen, print_header, print_success, print_info, 
    print_warning, print_error, print_task, print_progress_bar,
    box_border, box_line, box_title
)


def parallel_sort_data(arr: List[int], n_threads: Optional[int] = None) -> List[int]:
    """
    Mengurutkan array angka menggunakan seluruh thread CPU yang tersedia.
    Membagi array menjadi sub-chunk yang diproses secara simultan via ThreadPoolExecutor,
    lalu digabungkan secara cepat dengan run-merge linear Timsort.
    """
    if n_threads is None:
        n_threads = os.cpu_count() or 1
    n = len(arr)
    if n_threads <= 1 or n < 20_000:
        arr.sort()
        return arr

    sub_chunk_size = n // n_threads
    sub_chunks = []
    for t in range(n_threads):
        start = t * sub_chunk_size
        end = n if t == n_threads - 1 else (t + 1) * sub_chunk_size
        sub_chunks.append(arr[start:end])

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
        list(executor.map(lambda c: c.sort(), sub_chunks))

    merged = []
    for sc in sub_chunks:
        merged.extend(sc)
    merged.sort()
    return merged


def select_or_discover_master() -> Optional[Tuple[str, int]]:
    """
    Infinity Scan: Memindai Master Server secara terus-menerus di jaringan Wi-Fi/LAN.
    Daftar Master diperbarui secara real-time di layar.
    Pengguna cukup menekan [Enter] untuk langsung menghubungkan ke Master yang terdeteksi.
    """
    scanner = WorkerScanner()
    scanner.start()

    has_msvcrt = False
    try:
        import msvcrt
        has_msvcrt = True
    except ImportError:
        has_msvcrt = False

    spinner_chars = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    spinner_idx = 0
    buffer = ""
    last_rendered_keys = None
    start_time = time.time()
    local_worker_ip = get_local_ip()

    clear_screen()
    banner()

    try:
        while True:
            masters = scanner.get_masters()
            current_keys = [f"{m['ip']}:{m['port']}" for m in masters]

            # Redraw layar jika ada master baru yang terdeteksi atau berubah status
            if current_keys != last_rendered_keys:
                last_rendered_keys = current_keys
                clear_screen()
                banner()
                print_header("INFINITY AUTO-SCAN MASTER SERVER", "Pencarian Master di Jaringan Wi-Fi/LAN Secara Real-Time")
                print(f" {Colors.CYAN}•{Colors.RESET} Status Pemindai : {Colors.BRIGHT_GREEN}AKTIF (Continuous Infinity Scan){Colors.RESET}")
                print(f" {Colors.CYAN}•{Colors.RESET} IP Worker Lokal : {Colors.BOLD}{local_worker_ip}{Colors.RESET}")
                print(f" {Colors.CYAN}•{Colors.RESET} Port Discovery  : {Colors.DIM}UDP 5002{Colors.RESET}")

                if masters:
                    print(f"\n{Colors.BRIGHT_WHITE}{Colors.BOLD}Daftar Master Aktif Terdeteksi ({len(masters)} server ditemukan):{Colors.RESET}")
                    print(box_border())
                    for idx, m in enumerate(masters, 1):
                        name_str = m.get("name", "Master-Node")
                        ip_port_str = f"{m['ip']}:{m['port']}"
                        row_str = f" {Colors.BRIGHT_CYAN}[{idx}]{Colors.RESET} {Colors.BOLD}{name_str:<20}{Colors.RESET} {Colors.BRIGHT_GREEN}{ip_port_str:<22}{Colors.RESET} {Colors.BRIGHT_GREEN}[ONLINE]{Colors.RESET}"
                        print(box_line(row_str))
                    print(box_border())
                    print(f"  {Colors.BRIGHT_YELLOW}[M]{Colors.RESET} Masukkan IP Master secara manual")
                    print(f"  {Colors.BRIGHT_RED}[0]{Colors.RESET} Batal / Kembali ke Menu Utama")
                    print(f"\n{Colors.DIM}-------------------------------------------------------------------{Colors.RESET}")
                    print(f"{Colors.BRIGHT_GREEN}>>> Tekan [Enter] langsung untuk menghubungkan ke Master [1] <<<{Colors.RESET}")
                    if len(masters) > 1:
                        print(f"{Colors.DIM}-> Atau ketik nomor [1-{len(masters)}] lalu tekan [Enter]{Colors.RESET}")
                    print()
                else:
                    print(f"\n{Colors.BRIGHT_YELLOW}[*] Sedang memindai jaringan... Menunggu Master Server aktif.{Colors.RESET}")
                    print(f"    {Colors.DIM}Pastikan opsi [1] Master Node sudah dijalankan di komputer Master.{Colors.RESET}")
                    print(f"\n    {Colors.BRIGHT_YELLOW}[M]{Colors.RESET} Masukkan IP Manual  |  {Colors.BRIGHT_RED}[0]{Colors.RESET} Batal / Kembali ke Menu\n")

            # Indikator spinner dan status pencarian live
            spinner = spinner_chars[spinner_idx % len(spinner_chars)]
            spinner_idx += 1
            elapsed = int(time.time() - start_time)

            if not masters:
                sys.stdout.write(f"\r  {Colors.BRIGHT_CYAN}{spinner}{Colors.RESET} {Colors.DIM}Memindai LAN/Wi-Fi ({elapsed}s)... {Colors.RESET}{Colors.BRIGHT_YELLOW}{buffer}{Colors.RESET}   ")
                sys.stdout.flush()
            else:
                prompt_label = f"Pilih Master [default: 1]: {buffer}"
                sys.stdout.write(f"\r  {Colors.BRIGHT_YELLOW}{prompt_label}{Colors.RESET}   ")
                sys.stdout.flush()

            # Non-blocking input polling
            if has_msvcrt:
                if msvcrt.kbhit():
                    ch = msvcrt.getch()
                    # Tombol Enter ditekan
                    if ch in (b"\r", b"\n"):
                        sys.stdout.write("\n")
                        choice = buffer.strip()
                        buffer = ""
                        if not choice:
                            choice = "1" if masters else ""

                        if choice == "0":
                            scanner.stop()
                            return None
                        elif choice.upper() == "M":
                            scanner.stop()
                            try:
                                manual_ip = input(f"\n{Colors.BRIGHT_YELLOW}Masukkan IP Master: {Colors.RESET}").strip()
                                manual_port_str = input(f"{Colors.BRIGHT_YELLOW}Masukkan Port [{DEFAULT_MASTER_PORT}]: {Colors.RESET}").strip()
                                manual_port = int(manual_port_str) if manual_port_str else DEFAULT_MASTER_PORT
                                return manual_ip, manual_port
                            except Exception:
                                return None
                        # Jika pengguna langsung mengetik format IP (misal: 192.168.43.1)
                        elif "." in choice and len(choice.split(".")) == 4:
                            scanner.stop()
                            ip_parts = choice.split(":")
                            target_ip = ip_parts[0].strip()
                            target_port = int(ip_parts[1].strip()) if len(ip_parts) > 1 else DEFAULT_MASTER_PORT
                            return target_ip, target_port
                        else:
                            try:
                                sel_idx = int(choice) - 1
                                if 0 <= sel_idx < len(masters):
                                    chosen = masters[sel_idx]
                                    scanner.stop()
                                    return chosen["ip"], chosen["port"]
                            except ValueError:
                                pass
                    # Backspace ditekan
                    elif ch in (b"\x08", b"\x7f"):
                        if buffer:
                            buffer = buffer[:-1]
                            sys.stdout.write("\r" + " " * 70 + "\r")
                    # Ctrl+C ditekan
                    elif ch == b"\x03":
                        scanner.stop()
                        return None
                    else:
                        try:
                            char_str = ch.decode("utf-8")
                            if char_str.isprintable():
                                buffer += char_str
                        except Exception:
                            pass
                time.sleep(0.08)
            else:
                # Dukungan untuk HP Android (Termux) / Linux / macOS
                try:
                    import select
                    rlist, _, _ = select.select([sys.stdin], [], [], 0.15)
                    if rlist:
                        raw_in = sys.stdin.readline().strip()
                        if not raw_in and masters:
                            raw_in = "1"

                        if raw_in == "0":
                            scanner.stop()
                            return None
                        elif raw_in.upper() == "M":
                            scanner.stop()
                            manual_ip = input(f"\n{Colors.BRIGHT_YELLOW}Masukkan IP Master: {Colors.RESET}").strip()
                            manual_port_str = input(f"{Colors.BRIGHT_YELLOW}Masukkan Port [{DEFAULT_MASTER_PORT}]: {Colors.RESET}").strip()
                            manual_port = int(manual_port_str) if manual_port_str else DEFAULT_MASTER_PORT
                            return manual_ip, manual_port
                        elif "." in raw_in and len(raw_in.split(".")) == 4:
                            scanner.stop()
                            ip_parts = raw_in.split(":")
                            target_ip = ip_parts[0].strip()
                            target_port = int(ip_parts[1].strip()) if len(ip_parts) > 1 else DEFAULT_MASTER_PORT
                            return target_ip, target_port
                        else:
                            try:
                                sel_idx = int(raw_in) - 1
                                if 0 <= sel_idx < len(masters):
                                    chosen = masters[sel_idx]
                                    scanner.stop()
                                    return chosen["ip"], chosen["port"]
                            except ValueError:
                                pass
                except Exception:
                    time.sleep(0.2)

    except KeyboardInterrupt:
        scanner.stop()
        return None
    finally:
        scanner.stop()



def run_worker(host: str, port: int, worker_name: Optional[str] = None):
    """
    Fungsi utama worker untuk menghubungkan diri ke Master dan
    menjalankan instruksi sorting secara terdistribusi.
    """
    if not worker_name:
        worker_name = f"{socket.gethostname()}-Tab{os.getpid() % 1000}"

    print_header("WORKER NODE - DISTRIBUTED CLIENT", f"Node: {worker_name} | Target Master: {host}:{port}")

    # Inisialisasi socket TCP
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tune_socket(sock)

    try:
        print_task(f"Menghubungkan ke Master pada {Colors.BOLD}{host}:{port}{Colors.RESET} ...")
        sock.settimeout(5.0)  # Timeout 5 detik agar tidak freeze lama jika IP/firewall bermasalah
        sock.connect((host, port))
        sock.settimeout(None)  # Kembalikan ke mode blocking untuk transfer data
        print_success("Berhasil terhubung ke Master Server!")
    except Exception as err:
        print_error(f"Gagal terhubung ke Master ({host}:{port}): {err}")
        print_info(f"Petunjuk:")
        print(f"  1. Pastikan kedua perangkat berada di jaringan Wi-Fi / Hotspot yang sama.")
        print(f"  2. Jika Laptop sebagai Master: Periksa Windows Firewall (izinkan port 5000 TCP).")
        print(f"  3. Pastikan alamat IP Master ({host}) sudah benar dan opsi [1] Master aktif.")
        return

    worker_threads = os.cpu_count() or 1
    # Kirim pesan registrasi awal ke Master
    handshake_payload = {
        "cmd": "REGISTER",
        "name": worker_name,
        "hostname": socket.gethostname(),
        "threads": worker_threads,
    }
    if not send_packet(sock, handshake_payload):
        print_error("Gagal mengirim pesan registrasi ke Master.")
        sock.close()
        return

    print_info(f"Worker siap ({Colors.BRIGHT_GREEN}STANDBY{Colors.RESET}). Menunggu tugas sorting...\n")

    try:
        while True:
            # Menerima paket streaming secara kontinu
            packet = recv_packet(
                sock,
                progress_callback=lambda cur, tot: print_progress_bar(cur, tot, prefix="Menerima Stream TCP", suffix=f"{cur/(1024*1024):.1f}/{tot/(1024*1024):.1f} MB")
            )
            if packet is None:
                print_warning("Koneksi terputus dari Master. Worker berhenti.")
                break

            cmd = packet.get("cmd")

            if cmd == "SORT_ON_FLY":
                n_items = packet.get("count", 500_000)
                chunk_id = packet.get("chunk_id", 0)
                seed = packet.get("seed", None)

                print_header(f"TUGAS REALTIME ON-THE-FLY: CHUNK #{chunk_id}", f"Jumlah Data: {n_items:,} integer | {worker_name}")
                print_progress_bar(1, 3, prefix="Generate On-The-Fly", suffix=f"{n_items:,} data dibangkitkan lokal RAM (1/3)")

                t_start = time.perf_counter()
                import random
                rng = random.Random(seed) if seed is not None else random.Random()
                data_chunk = [rng.randint(1, 10_000_000) for _ in range(n_items)]

                print_progress_bar(2, 3, prefix="Proses Sorting", suffix="Mengurutkan data di RAM (2/3)")
                data_chunk = parallel_sort_data(data_chunk, n_threads=worker_threads)
                t_end = time.perf_counter()
                sort_duration = t_end - t_start

                response = {
                    "status": "OK",
                    "chunk_id": chunk_id,
                    "worker_name": worker_name,
                    "threads": worker_threads,
                    "sort_time": sort_duration,
                    "data": data_chunk,
                }
                t_send_start = time.perf_counter()
                if send_packet(
                    sock, 
                    response,
                    progress_callback=lambda cur, tot: print_progress_bar(cur, tot, prefix="Mengirim Balik TCP", suffix=f"{cur/(1024*1024):.1f}/{tot/(1024*1024):.1f} MB")
                ):
                    t_send_end = time.perf_counter()
                    print_progress_bar(3, 3, prefix="Pengiriman Balik TCP", suffix=f"{t_send_end - t_send_start:.4f} dtk (3/3)")
                    print_success(f"Chunk #{chunk_id} ({n_items:,} data) berhasil diproses On-The-Fly & dikembalikan ke Master!")
                    print_info(f"Worker kembali STANDBY. Siap menerima tugas sorting berikutnya...\n")
                else:
                    print_error("Gagal mengirim data kembali ke Master.")
                    break

            elif cmd == "GENERATE_UNSORTED":
                n_items = packet.get("count", 500_000)
                chunk_id = packet.get("chunk_id", 0)
                seed = packet.get("seed", None)

                print_header(f"TUGAS PEMBANGKITAN DATA (UNSORTED): CHUNK #{chunk_id}", f"Jumlah Data: {n_items:,} integer acak | {worker_name}")
                print_progress_bar(1, 2, prefix="Generate On-The-Fly", suffix=f"Membangkitkan {n_items:,} data di RAM (1/2)")

                t_start = time.perf_counter()
                import random
                rng = random.Random(seed) if seed is not None else random.Random()
                data_chunk = [rng.randint(1, 10_000_000) for _ in range(n_items)]
                t_end = time.perf_counter()
                gen_duration = t_end - t_start

                print_progress_bar(2, 2, prefix="Generate On-The-Fly", suffix=f"Selesai ({gen_duration:.4f} dtk) (2/2)")

                response = {
                    "status": "OK",
                    "chunk_id": chunk_id,
                    "worker_name": worker_name,
                    "gen_time": gen_duration,
                    "data": data_chunk,
                }
                t_send_start = time.perf_counter()
                if send_packet(
                    sock, 
                    response,
                    progress_callback=lambda cur, tot: print_progress_bar(cur, tot, prefix="Mengirim Stream TCP", suffix=f"{cur/(1024*1024):.1f}/{tot/(1024*1024):.1f} MB")
                ):
                    t_send_end = time.perf_counter()
                    print_success(f"Chunk #{chunk_id} ({n_items:,} data acak) berhasil dibangkitkan & dikirim ke Master!")
                    print_info(f"Worker kembali STANDBY. Menunggu tugas berikutnya...\n")
                else:
                    print_error("Gagal mengirim data acak ke Master.")
                    break

            elif cmd == "SORT":
                data_chunk = packet.get("data", [])
                chunk_id = packet.get("chunk_id", 0)
                n_items = len(data_chunk)

                print_header(f"TUGAS KOMPUTASI PARALEL: CHUNK #{chunk_id}", f"Jumlah Data: {n_items:,} integer | {worker_name}")
                print_progress_bar(1, 3, prefix="Penerimaan Stream TCP", suffix=f"{n_items:,} data diterima (1/3)")

                t_start = time.perf_counter()
                data_chunk = parallel_sort_data(data_chunk, n_threads=worker_threads)
                t_end = time.perf_counter()
                sort_duration = t_end - t_start

                print_progress_bar(2, 3, prefix="Proses Sorting", suffix=f"Selesai ({sort_duration:.4f} dtk) (2/3)")

                response = {
                    "status": "OK",
                    "chunk_id": chunk_id,
                    "worker_name": worker_name,
                    "threads": worker_threads,
                    "sort_time": sort_duration,
                    "data": data_chunk,
                }
                t_send_start = time.perf_counter()
                # Mengirim balik hasil terurut dengan streaming kontinu
                if send_packet(
                    sock, 
                    response,
                    progress_callback=lambda cur, tot: print_progress_bar(cur, tot, prefix="Mengirim Balik TCP", suffix=f"{cur/(1024*1024):.1f}/{tot/(1024*1024):.1f} MB")
                ):
                    t_send_end = time.perf_counter()
                    print_progress_bar(3, 3, prefix="Pengiriman Balik TCP", suffix=f"{t_send_end - t_send_start:.4f} dtk (3/3)")
                    print_success(f"Chunk #{chunk_id} ({n_items:,} data) berhasil diurutkan & dikembalikan ke Master!")
                    print_info(f"Worker kembali STANDBY. Siap menerima tugas sorting berikutnya...\n")
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
    while True:
        target = select_or_discover_master()
        if not target:
            break
        host, port = target
        run_worker(host=host, port=port)
        print(f"\n{Colors.DIM}Kembali ke mode auto-scan dalam 2 detik (Ctrl+C untuk keluar)...{Colors.RESET}")
        try:
            time.sleep(2)
        except KeyboardInterrupt:
            break


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
