"""
master.py
---------
Master Node (Server) untuk Sistem Distributed Merge Sort.
- Mengelola koneksi worker TCP socket.
- Membangkitkan N data integer acak positif dan menyimpannya ke unsorted.txt (dengan progress bar).
- Mode Serial Sorting vs Distributed Sorting (K-Way Merge dengan heapq.merge).
- Dilengkapi PROGRESS BAR di seluruh proses komputasi serial & paralel.
- Menyimpan hasil data terurut ke sorted.txt.
- Menghitung metrik performa: Waktu, Speedup, Efisiensi, dan Validasi data.
"""

import os
import sys
import time
import random
import socket
import heapq
import threading
import concurrent.futures
from typing import List, Dict, Any, Optional, Tuple

from network_utils import send_packet, recv_packet, MasterBeacon, DEFAULT_MASTER_PORT, get_local_ip, get_all_local_ips
from cli_ui import (
    Colors, banner, clear_screen, print_header, print_success, print_info, 
    print_warning, print_error, print_task, print_table_row, print_table_footer,
    print_progress_bar
)

DEFAULT_PORT = 5000
DEFAULT_DATA_SIZE = 1_000_000
FILE_UNSORTED = "unsorted.txt"
FILE_SORTED = "sorted.txt"


def is_sorted(arr: List[int]) -> bool:
    """Validasi apakah list sudah terurut secara non-decreasing."""
    return all(arr[i] <= arr[i + 1] for i in range(len(arr) - 1))


def format_data_preview(arr: Optional[List[int]], max_items: int = 8) -> str:
    """Membuat teks pratinjau data angka agar rapi di layar."""
    if not arr:
        return f"{Colors.DIM}[Belum ada data]{Colors.RESET}"
    if len(arr) <= max_items:
        return f"{Colors.CYAN}{arr}{Colors.RESET}"
    half = max_items // 2
    front = ", ".join(str(x) for x in arr[:half])
    back = ", ".join(str(x) for x in arr[-half:])
    return f"{Colors.CYAN}[{front}, ... ({len(arr):,} angka) ..., {back}]{Colors.RESET}"


def save_to_file(data: List[int], filename: str):
    """Menyimpan list angka ke file teks (satu angka per baris)."""
    with open(filename, "w", encoding="utf-8") as f:
        f.write("\n".join(map(str, data)) + "\n")


def load_from_file(filename: str) -> Optional[List[int]]:
    """Membaca list angka dari file teks."""
    if not os.path.exists(filename):
        return None
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return [int(line.strip()) for line in f if line.strip()]
    except Exception:
        return None


def generate_random_data(n: int) -> Tuple[List[int], float]:
    """
    Membangkitkan N angka integer acak POSITIF (tidak ada angka minus)
    dengan PROGRESS BAR bertahap dan menyimpannya langsung ke 'unsorted.txt'.
    Menampilkan rincian dan TOTAL WAKTU PROSES seperti pada proses akhir sorted.
    """
    print_header("PROSES PEMBANGKITAN DATA (UNSORTED)", f"Membangkitkan {n:,} Angka Acak Positif")
    print(f" {Colors.CYAN}•{Colors.RESET} Target Jumlah Data : {Colors.BOLD}{n:,} elemen{Colors.RESET}")
    print(f" {Colors.CYAN}•{Colors.RESET} Target Berkas      : {Colors.BOLD}{FILE_UNSORTED}{Colors.RESET}")

    t_total_start = time.perf_counter()

    # 1. Pembangkitan Angka dengan Progress Bar
    t_gen_start = time.perf_counter()
    data = []
    num_batches = 10
    batch_size = n // num_batches

    for b in range(num_batches):
        cur_batch = n - len(data) if b == num_batches - 1 else batch_size
        data.extend([random.randint(1, 10_000_000) for _ in range(cur_batch)])
        print_progress_bar(b + 1, num_batches, prefix="Generate Angka", suffix=f"{len(data):,}/{n:,}")

    t_gen_end = time.perf_counter()
    time_generate = t_gen_end - t_gen_start

    # 2. Penyimpanan ke File unsorted.txt dengan Progress Bar
    t_save_start = time.perf_counter()
    save_to_file(data, FILE_UNSORTED)
    t_save_end = time.perf_counter()
    time_save = t_save_end - t_save_start

    print_progress_bar(1, 1, prefix="Simpan unsorted.txt", suffix="Selesai (100%)")

    t_total_end = time.perf_counter()
    total_time = t_total_end - t_total_start

    print_success("Pembangkitan Data Unsorted Selesai!")
    print(f"\n{Colors.BRIGHT_WHITE}{Colors.BOLD}Rincian Waktu Proses Unsorted:{Colors.RESET}")
    print(f" {Colors.CYAN}•{Colors.RESET} Pratinjau Data        : {format_data_preview(data)}")
    print(f" {Colors.CYAN}•{Colors.RESET} Waktu Pembangkitan    : {time_generate:.6f} detik")
    print(f" {Colors.CYAN}•{Colors.RESET} Waktu Simpan ke File  : {time_save:.6f} detik")
    print(f" {Colors.CYAN}•{Colors.RESET} TOTAL WAKTU PROSES    : {Colors.BOLD}{Colors.BRIGHT_YELLOW}{total_time:.6f} detik{Colors.RESET}")
    print(f" {Colors.CYAN}•{Colors.RESET} Lokasi Berkas         : {Colors.BOLD}{FILE_UNSORTED}{Colors.RESET}")

    return data, total_time


class MasterServer:
    def __init__(self, host: str = "0.0.0.0", port: int = DEFAULT_PORT):
        self.host = host
        self.port = port
        self.local_ip = get_local_ip()
        self.server_sock: Optional[socket.socket] = None
        self.workers: List[Dict[str, Any]] = []
        self.workers_lock = threading.Lock()
        self.is_running = True
        self.accept_thread: Optional[threading.Thread] = None
        self.beacon = MasterBeacon(
            master_name=socket.gethostname(),
            master_ip=self.local_ip,
            tcp_port=self.port
        )

    def start_server(self):
        """Memulai TCP socket server, background worker listener, dan UDP auto-discovery beacon."""
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind((self.host, self.port))
        self.server_sock.listen(10)

        self.accept_thread = threading.Thread(target=self._listen_for_workers, daemon=True)
        self.accept_thread.start()

        # Mulai menyiarkan beacon auto-discovery ke LAN
        self.beacon.start()

    def _listen_for_workers(self):
        """Loop di latar belakang untuk menerima koneksi worker baru."""
        worker_counter = 1
        while self.is_running:
            try:
                client_sock, client_addr = self.server_sock.accept()
                client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

                reg_data = recv_packet(client_sock)
                raw_name = reg_data.get("name") if isinstance(reg_data, dict) and reg_data.get("name") else f"Node"
                # Beri nama unik dengan ID dan nama perangkat agar tidak rancu saat multi-tab lokal
                worker_name = f"Worker-{worker_counter} ({raw_name})"

                worker_info = {
                    "id": worker_counter,
                    "sock": client_sock,
                    "addr": client_addr,
                    "name": worker_name,
                }

                with self.workers_lock:
                    self.workers.append(worker_info)
                    active_count = len(self.workers)

                # Notifikasi real-time di console Master saat worker terdeteksi & terhubung
                sys.stdout.write(f"\r\n{Colors.BRIGHT_GREEN}[✓] WORKER BARU TERDETEKSI & TERHUBUNG: {worker_name} ({client_addr[0]}:{client_addr[1]}) [Total: {active_count} Worker Online]{Colors.RESET}\n")
                sys.stdout.write(f"{Colors.BRIGHT_YELLOW}Pilih menu [1-3, 0]: {Colors.RESET}")
                sys.stdout.flush()

                worker_counter += 1
            except Exception:
                if not self.is_running:
                    break

    def get_active_workers(self) -> List[Dict[str, Any]]:
        """Mengembalikan salinan daftar worker yang terdaftar."""
        with self.workers_lock:
            return list(self.workers)

    def get_live_workers(self) -> List[Dict[str, Any]]:
        """
        Memeriksa koneksi seluruh worker secara aktif (health check PING).
        Menghapus worker yang sudah terputus sebelum komputasi dimulai.
        """
        with self.workers_lock:
            live = []
            for w in list(self.workers):
                try:
                    w["sock"].settimeout(0.3)
                    if send_packet(w["sock"], {"cmd": "PING"}):
                        resp = recv_packet(w["sock"])
                        if resp and resp.get("status") == "PONG":
                            w["sock"].settimeout(None)
                            live.append(w)
                            continue
                except Exception:
                    pass
                try:
                    w["sock"].close()
                except Exception:
                    pass
            self.workers = live
            return list(self.workers)

    def remove_dead_worker(self, worker_info: Dict[str, Any]):
        """Menghapus worker yang terputus."""
        with self.workers_lock:
            if worker_info in self.workers:
                try:
                    worker_info["sock"].close()
                except Exception:
                    pass
                self.workers.remove(worker_info)

    def shutdown(self):
        """Mematikan server, menghentikan beacon, dan menutup seluruh koneksi worker."""
        self.is_running = False
        self.beacon.stop()
        with self.workers_lock:
            for w in self.workers:
                try:
                    send_packet(w["sock"], {"cmd": "SHUTDOWN"})
                    w["sock"].close()
                except Exception:
                    pass
            self.workers.clear()

        if self.server_sock:
            try:
                self.server_sock.close()
            except Exception:
                pass


def run_serial_sorting(data: List[int]) -> Tuple[float, bool, List[int]]:
    """
    Mode Serial: Mengurutkan data langsung di CPU master dengan PROGRESS BAR bertahap.
    """
    print_header("KOMPUTASI SERIAL SORTING", f"Memproses {len(data):,} Elemen di Master")
    print(f" {Colors.CYAN}•{Colors.RESET} Jumlah Data     : {Colors.BOLD}{len(data):,} elemen{Colors.RESET}")
    print(f" {Colors.CYAN}•{Colors.RESET} Pratinjau Asli  : {format_data_preview(data)}")

    t_start = time.perf_counter()

    # 1. Timsort Blok Serial dengan Progress Bar
    num_blocks = 10
    block_size = len(data) // num_blocks
    sorted_blocks = []

    for i in range(num_blocks):
        start = i * block_size
        end = len(data) if i == num_blocks - 1 else (i + 1) * block_size
        block = data[start:end]
        block.sort()
        sorted_blocks.append(block)
        print_progress_bar(i + 1, num_blocks, prefix="Timsort Serial Blok", suffix=f"Blok {i+1}/{num_blocks}")

    # 2. K-Way Merge Serial dengan Progress Bar
    total_len = len(data)
    step = max(total_len // 10, 1)
    sorted_data = []
    count = 0
    for val in heapq.merge(*sorted_blocks):
        sorted_data.append(val)
        count += 1
        if count % step == 0:
            print_progress_bar(count, total_len, prefix="Merge Serial CPU", suffix=f"{count:,}/{total_len:,}")
    if count > 0:
        print_progress_bar(total_len, total_len, prefix="Merge Serial CPU", suffix=f"{total_len:,}/{total_len:,}")

    t_end = time.perf_counter()
    duration = t_end - t_start

    print_success(f"Pengurutan Serial Selesai dalam {Colors.BOLD}{Colors.BRIGHT_YELLOW}{duration:.6f} detik{Colors.RESET}!")
    print(f" {Colors.CYAN}•{Colors.RESET} Hasil Terurut   : {format_data_preview(sorted_data)}")

    # 3. Validasi dengan Progress Bar
    is_valid = is_sorted(sorted_data) and (len(sorted_data) == len(data))
    print_progress_bar(1, 1, prefix="Validasi Urutan", suffix="Selesai (100%)")
    if is_valid:
        print_success("Validasi Urutan : BERHASIL (Data Terurut Sempurna)")
    else:
        print_error("Validasi Urutan : GAGAL (Data Tidak Terurut)")

    # 4. Simpan ke sorted.txt dengan Progress Bar
    save_to_file(sorted_data, FILE_SORTED)
    print_progress_bar(1, 1, prefix="Simpan sorted.txt", suffix="Selesai (100%)")
    print_success(f"Hasil terurut berhasil disimpan ke '{FILE_SORTED}'!")

    return duration, is_valid, sorted_data


def _worker_sort_task(worker_info: Dict[str, Any], chunk: List[int], chunk_id: int) -> Dict[str, Any]:
    """Mengirim chunk ke satu worker via TCP dan menerima hasil terurut."""
    sock = worker_info["sock"]
    name = worker_info["name"]
    
    t_dispatch_start = time.perf_counter()
    payload = {
        "cmd": "SORT",
        "chunk_id": chunk_id,
        "data": chunk,
    }
    if not send_packet(sock, payload):
        raise ConnectionError(f"Gagal mengirim data ke worker {name}")

    response = recv_packet(sock)
    t_dispatch_end = time.perf_counter()
    
    if not response or response.get("status") != "OK":
        raise ConnectionError(f"Respon tidak valid dari worker {name}")

    return {
        "worker_name": name,
        "chunk_id": chunk_id,
        "sorted_chunk": response.get("data", []),
        "worker_sort_time": response.get("sort_time", 0.0),
        "roundtrip_time": t_dispatch_end - t_dispatch_start,
    }


def run_distributed_sorting(server: MasterServer, data: List[int]) -> Optional[Tuple[float, bool, List[int]]]:
    """
    Mode Terdistribusi:
    Dilengkapi PROGRESS BAR di setiap tahapan (partisi, transmisi, komputasi worker, merge).
    """
    # 1. Bersihkan worker yang terputus dan ambil hanya worker yang benar-benar aktif
    active_workers = server.get_live_workers()
    k = len(active_workers)

    print_header("KOMPUTASI PARALEL / DISTRIBUTED SORTING", f"Distribusi ke {k} Worker Node via TCP/IP")
    print(f" {Colors.CYAN}•{Colors.RESET} Jumlah Data        : {Colors.BOLD}{len(data):,} elemen{Colors.RESET}")
    print(f" {Colors.CYAN}•{Colors.RESET} Pratinjau Asli      : {format_data_preview(data)}")
    print(f" {Colors.CYAN}•{Colors.RESET} Jumlah Worker (K)  : {Colors.BOLD}{Colors.BRIGHT_GREEN}{k} node aktif{Colors.RESET}")

    if k == 0:
        print_error("Belum ada worker aktif yang terhubung ke Master!")
        print_info(f"Hubungkan worker: python main.py (pilih [2])")
        return None

    # 2. Partisi data ke K worker dengan Progress Bar
    chunk_size = len(data) // k
    remainder = len(data) % k
    chunks = []
    start_idx = 0
    for i in range(k):
        end_idx = start_idx + chunk_size + (1 if i < remainder else 0)
        chunks.append(data[start_idx:end_idx])
        start_idx = end_idx
        print_progress_bar(i + 1, k, prefix="Partisi Chunk TCP", suffix=f"Chunk #{i+1} ({len(chunks[-1]):,} data)")

    t_dist_total_start = time.perf_counter()

    # 3. Distribusi & Komputasi Paralel ke Seluruh Worker Secara BERSAMAAN
    results = []
    failed_workers = []
    completed_count = 0

    print(f"\n{Colors.BRIGHT_WHITE}{Colors.BOLD}Memulai Pengiriman Paralel ke {k} Worker Node Bersamaan:{Colors.RESET}")
    for i in range(k):
        print(f"  {Colors.BRIGHT_CYAN}➔{Colors.RESET} Mengirim {len(chunks[i]):,} data (Chunk #{i+1}) ke {Colors.BOLD}{active_workers[i]['name']}{Colors.RESET}...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=k) as executor:
        future_to_worker = {
            executor.submit(_worker_sort_task, active_workers[i], chunks[i], i + 1): active_workers[i]
            for i in range(k)
        }

        for future in concurrent.futures.as_completed(future_to_worker):
            worker_info = future_to_worker[future]
            try:
                res = future.result()
                results.append(res)
                completed_count += 1
                print_progress_bar(completed_count, k, prefix="Komputasi TCP Worker", suffix=f"{res['worker_name']} Selesai ({completed_count}/{k})")
            except Exception as err:
                print_error(f"Gagal pada {worker_info['name']}: {err}")
                failed_workers.append(worker_info)

    for dead_w in failed_workers:
        server.remove_dead_worker(dead_w)

    if len(results) != k:
        print_error("Pengurutan terdistribusi gagal karena ada worker yang terputus.")
        return None

    # Urutkan results berdasarkan chunk_id agar terstruktur
    results.sort(key=lambda x: x["chunk_id"])

    t_network_done = time.perf_counter()
    network_and_sort_time = t_network_done - t_dist_total_start

    # Tabel Rincian Eksekusi Paralel Tiap Worker
    print(f"\n{Colors.BRIGHT_WHITE}{Colors.BOLD}Rincian Eksekusi Paralel Seluruh Worker:{Colors.RESET}")
    print(f" {Colors.BRIGHT_BLUE}+-------------------------------------------------------------------------------+{Colors.RESET}")
    print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET} {'Chunk':<8} | {'Nama Worker Node':<28} | {'Jumlah Data':<15} | {'Sort RAM':<10} | {'Roundtrip':<10} {Colors.BRIGHT_BLUE}|{Colors.RESET}")
    print(f" {Colors.BRIGHT_BLUE}+-------------------------------------------------------------------------------+{Colors.RESET}")
    for r in results:
        c_tag = f"#{r['chunk_id']}"
        w_name = r["worker_name"][:28]
        c_len = f"{len(r['sorted_chunk']):,} data"
        s_time = f"{r['worker_sort_time']:.4f}s"
        r_time = f"{r['roundtrip_time']:.4f}s"
        print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET} {c_tag:<8} | {w_name:<28} | {c_len:<15} | {s_time:<10} | {r_time:<10} {Colors.BRIGHT_BLUE}|{Colors.RESET}")
    print(f" {Colors.BRIGHT_BLUE}+-------------------------------------------------------------------------------+{Colors.RESET}")

    # 4. K-Way Merge di Master dengan Progress Bar
    t_merge_start = time.perf_counter()
    sorted_chunks = [r["sorted_chunk"] for r in results]

    total_len = len(data)
    step = max(total_len // 10, 1)
    final_sorted_data = []
    count = 0
    for val in heapq.merge(*sorted_chunks):
        final_sorted_data.append(val)
        count += 1
        if count % step == 0:
            print_progress_bar(count, total_len, prefix="K-Way Merge Master", suffix=f"{count:,}/{total_len:,}")
    if count > 0:
        print_progress_bar(total_len, total_len, prefix="K-Way Merge Master", suffix=f"{total_len:,}/{total_len:,}")

    t_merge_end = time.perf_counter()
    merge_time = t_merge_end - t_merge_start
    total_distributed_time = t_merge_end - t_dist_total_start

    print_success("Penggabungan K-Way Merge Selesai!")
    print(f" {Colors.CYAN}•{Colors.RESET} Hasil Terurut       : {format_data_preview(final_sorted_data)}")
    print(f" {Colors.CYAN}•{Colors.RESET} Waktu Jaringan+Sort : {network_and_sort_time:.6f} detik")
    print(f" {Colors.CYAN}•{Colors.RESET} Waktu K-Way Merge   : {merge_time:.6f} detik")
    print(f" {Colors.CYAN}•{Colors.RESET} TOTAL WAKTU DIST    : {Colors.BOLD}{Colors.BRIGHT_YELLOW}{total_distributed_time:.6f} detik{Colors.RESET}")

    # 4. Validasi dengan Progress Bar
    is_valid = (len(final_sorted_data) == len(data)) and is_sorted(final_sorted_data)
    print_progress_bar(1, 1, prefix="Validasi Urutan", suffix="Selesai (100%)")
    if is_valid:
        print_success("Validasi Urutan     : BERHASIL (Data Terurut Sempurna)")
    else:
        print_error("Validasi Urutan     : GAGAL (Data Rusak/Tidak Terurut)")

    # 5. Simpan ke sorted.txt dengan Progress Bar
    save_to_file(final_sorted_data, FILE_SORTED)
    print_progress_bar(1, 1, prefix="Simpan sorted.txt", suffix="Selesai (100%)")
    print_success(f"Hasil terurut berhasil disimpan ke '{FILE_SORTED}'!")

    return total_distributed_time, is_valid, final_sorted_data


def print_comparison_metrics(t_serial: float, t_dist: float, num_workers: int):
    """Menampilkan tabel perbandingan, Speedup, dan Efisiensi."""
    speedup = t_serial / t_dist if t_dist > 0 else 0
    efficiency = (speedup / num_workers * 100) if num_workers > 0 else 0

    print_header("METRIK EVALUASI: SERIAL VS DISTRIBUTED", "Perhitungan Speedup dan Efisiensi Komputasi")
    print_table_row("Parameter Evaluasi", "Nilai / Hasil", is_header=True)
    print_table_row("Waktu Serial (T_serial)", f"{t_serial:.6f} detik")
    print_table_row("Waktu Terdistribusi (T_dist)", f"{t_dist:.6f} detik")
    print_table_row("Jumlah Worker (K)", f"{num_workers} node")
    
    color_speedup = Colors.BRIGHT_GREEN if speedup >= 1.0 else Colors.BRIGHT_YELLOW
    print_table_row("Speedup (S = T_serial / T_dist)", f"{color_speedup}{speedup:.2f}x{Colors.RESET}")
    print_table_row("Efisiensi (E = S / K)", f"{efficiency:.2f}%")
    print_table_footer()

    print(f"\n{Colors.BRIGHT_CYAN}[Analisis Komputasi Terdistribusi]:{Colors.RESET}")
    if speedup > 1.0:
        print(f" {Colors.BRIGHT_GREEN}✔ Distributed Sorting LEBIH CEPAT dibanding mode Serial.{Colors.RESET}")
    else:
        print(f" {Colors.BRIGHT_YELLOW}ℹ Mode Serial lebih cepat atau seimbang.{Colors.RESET}")
        print("   Catatan: Overhead jaringan TCP & serialisasi data mempengaruhi waktu total.")
        print("   Untuk data besar (>= 1.000.000 data), akselerasi paralel multi-node akan lebih optimal.")


def run_master_cli(port: int = DEFAULT_PORT):
    """Menu CLI interaktif untuk Master Node."""
    server = MasterServer(host="0.0.0.0", port=port)
    try:
        server.start_server()
    except Exception as e:
        print_error(f"Gagal memulai server pada port {port}: {e}")
        return

    # Jika file unsorted.txt sudah ada di disk, muat secara otomatis
    current_data = load_from_file(FILE_UNSORTED)
    last_serial_time: Optional[float] = None
    last_dist_time: Optional[float] = None
    notification: Optional[str] = None
    is_notif_warning: bool = False

    if current_data:
        notification = f"Ditemukan '{FILE_UNSORTED}' ({len(current_data):,} data siap digunakan)!"

    while True:
        clear_screen()
        banner()

        workers = server.get_active_workers()
        data_count = len(current_data) if current_data is not None else 0
        preview_str = format_data_preview(current_data, max_items=8)

        unsorted_status = f"{Colors.BRIGHT_GREEN}ADA ({data_count:,} data){Colors.RESET}" if os.path.exists(FILE_UNSORTED) else f"{Colors.DIM}Belum ada{Colors.RESET}"
        sorted_status = f"{Colors.BRIGHT_GREEN}ADA{Colors.RESET}" if os.path.exists(FILE_SORTED) else f"{Colors.DIM}Belum ada{Colors.RESET}"

        print(f" {Colors.BRIGHT_BLUE}+-------------------------------------------------------------------+{Colors.RESET}")
        print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET}                    {Colors.BOLD}{Colors.BRIGHT_WHITE}PANEL KONTROL MASTER (SERVER){Colors.RESET}                    {Colors.BRIGHT_BLUE}|{Colors.RESET}")
        worker_text = f"{len(workers)} node"
        if workers:
            w_names = ", ".join(w.get("name", "Worker") for w in workers[:2])
            if len(workers) > 2:
                w_names += f" +{len(workers)-2}"
            worker_text += f" ({w_names})"

        server.all_ips = get_all_local_ips()
        ip_list_str = " | ".join(f"{ip}:{server.port}" for ip in server.all_ips[:3])

        print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET} Alamat IP Master  : {Colors.BOLD}{Colors.BRIGHT_GREEN}{ip_list_str}{Colors.RESET}")
        print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET} Auto-Discovery    : {Colors.BRIGHT_GREEN}AKTIF (UDP 5002 - Beacon & Subnet Sweep){Colors.RESET}")
        print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET} Worker Terhubung  : {Colors.BOLD}{Colors.BRIGHT_GREEN if workers else Colors.BRIGHT_WHITE}{worker_text}{Colors.RESET}")
        print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET} File unsorted.txt : {unsorted_status}")
        print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET} File sorted.txt   : {sorted_status}")
        print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET} Pratinjau Data    : {preview_str}")
        print(f" {Colors.BRIGHT_BLUE}+-------------------------------------------------------------------+{Colors.RESET}")
        print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET}  {Colors.BRIGHT_CYAN}[1]{Colors.RESET} {Colors.BOLD}Bangkitkan Data Acak{Colors.RESET} (Simpan ke {FILE_UNSORTED})               {Colors.BRIGHT_BLUE}|{Colors.RESET}")
        print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET}  {Colors.BRIGHT_CYAN}[2]{Colors.RESET} {Colors.BOLD}Jalankan Serial Sorting{Colors.RESET} (Simpan ke {FILE_SORTED})            {Colors.BRIGHT_BLUE}|{Colors.RESET}")
        print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET}  {Colors.BRIGHT_CYAN}[3]{Colors.RESET} {Colors.BOLD}Jalankan Distributed Sorting{Colors.RESET} (Simpan ke {FILE_SORTED})       {Colors.BRIGHT_BLUE}|{Colors.RESET}")
        print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET}  {Colors.BRIGHT_RED}[0]{Colors.RESET} Keluar / Matikan Master                                          {Colors.BRIGHT_BLUE}|{Colors.RESET}")
        print(f" {Colors.BRIGHT_BLUE}+-------------------------------------------------------------------+{Colors.RESET}")
        print(f" {Colors.DIM}💡 Tips HP Hotspot: Di Worker, Anda bisa langsung ketik IP Master di atas lalu [Enter]{Colors.RESET}")

        if notification:
            if is_notif_warning:
                print_warning(notification)
            else:
                print_success(notification)
            notification = None
            is_notif_warning = False

        try:
            choice = input(f"\n{Colors.BRIGHT_YELLOW}Pilih menu [1-3, 0]: {Colors.RESET}").strip()
        except (KeyboardInterrupt, EOFError):
            break

        if choice == "1":
            try:
                raw_n = input(f"{Colors.BRIGHT_YELLOW}Jumlah angka N acak positif [default 1,000,000]: {Colors.RESET}").strip()
                n_items = int(raw_n.replace(".", "").replace(",", "")) if raw_n else DEFAULT_DATA_SIZE
                if n_items <= 0:
                    n_items = DEFAULT_DATA_SIZE
            except ValueError:
                n_items = DEFAULT_DATA_SIZE

            current_data, total_unsorted_time = generate_random_data(n_items)
            last_serial_time = None
            last_dist_time = None
            notification = f"{len(current_data):,} angka positif berhasil dibangkitkan dan disimpan ke '{FILE_UNSORTED}' (Total Waktu: {total_unsorted_time:.4f} dtk)!"
            is_notif_warning = False
            input(f"\n{Colors.DIM}Tekan [Enter] untuk kembali ke panel kontrol...{Colors.RESET}")

        elif choice == "2":
            if current_data is None:
                current_data = load_from_file(FILE_UNSORTED)
            if current_data is None:
                notification = f"Data belum ada! Silakan bangkitkan data terlebih dahulu melalui menu [1]."
                is_notif_warning = True
                continue

            t_serial, _, _ = run_serial_sorting(current_data)
            last_serial_time = t_serial
            if last_dist_time is not None:
                print_comparison_metrics(last_serial_time, last_dist_time, len(server.get_active_workers()))
            input(f"\n{Colors.DIM}Tekan [Enter] untuk kembali ke panel kontrol...{Colors.RESET}")

        elif choice == "3":
            if current_data is None:
                current_data = load_from_file(FILE_UNSORTED)
            if current_data is None:
                notification = f"Data belum ada! Silakan bangkitkan data terlebih dahulu melalui menu [1]."
                is_notif_warning = True
                continue

            if len(server.get_active_workers()) == 0:
                notification = "Belum ada worker yang terhubung! Hubungkan worker terlebih dahulu."
                is_notif_warning = True
                continue

            result = run_distributed_sorting(server, current_data)
            if result:
                last_dist_time, _, _ = result
                if last_serial_time is not None:
                    print_comparison_metrics(last_serial_time, last_dist_time, len(server.get_active_workers()))
            input(f"\n{Colors.DIM}Tekan [Enter] untuk kembali ke panel kontrol...{Colors.RESET}")

        elif choice == "0":
            break
        else:
            notification = "Pilihan tidak valid. Silakan pilih 1, 2, 3, atau 0."
            is_notif_warning = True

    server.shutdown()
    clear_screen()
    print_info("Master Server telah dinonaktifkan.")


if __name__ == "__main__":
    banner()
    run_master_cli()
