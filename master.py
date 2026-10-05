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

from network_utils import (
    send_packet, recv_packet, MasterBeacon, DEFAULT_MASTER_PORT, 
    get_local_ip, get_all_local_ips, tune_socket, is_socket_alive
)
from cli_ui import (
    Colors, banner, clear_screen, print_header, print_success, print_info, 
    print_warning, print_error, print_task, print_table_row, print_table_footer,
    print_progress_bar, box_border, box_line, box_title, pad_ansi, visible_length
)

DEFAULT_PORT = 5000
DEFAULT_DATA_SIZE = 1_000_000
FILE_UNSORTED = "unsorted.txt"
FILE_SORTED = "sorted.txt"


def parallel_sort_data(arr: List[int], n_threads: Optional[int] = None) -> List[int]:
    """
    Mengurutkan array angka menggunakan seluruh thread CPU yang dialokasikan.
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
    print(f"\n{Colors.BRIGHT_WHITE}{Colors.BOLD}Rincian Waktu Pembangkitan Data Unsorted:{Colors.RESET}")
    print(f" {Colors.CYAN}•{Colors.RESET} Pratinjau Data        : {format_data_preview(data)}")
    print(f" {Colors.CYAN}•{Colors.RESET} Waktu Pembangkitan    : {time_generate:.6f} detik")
    print(f" {Colors.CYAN}•{Colors.RESET} Waktu Simpan ke File  : {time_save:.6f} detik")
    print(f" {Colors.CYAN}•{Colors.RESET} Waktu Unsort Selesai  : {Colors.BOLD}{Colors.BRIGHT_YELLOW}{total_time:.6f} detik{Colors.RESET}")
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
        tune_socket(self.server_sock)
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
                tune_socket(client_sock)

                reg_data = recv_packet(client_sock)
                raw_name = reg_data.get("name") if isinstance(reg_data, dict) and reg_data.get("name") else f"Node"
                worker_threads = reg_data.get("threads", 1) if isinstance(reg_data, dict) else 1
                # Beri nama unik dengan ID dan nama perangkat agar tidak rancu saat multi-tab lokal
                worker_name = f"Worker-{worker_counter} ({raw_name})"

                worker_info = {
                    "id": worker_counter,
                    "sock": client_sock,
                    "addr": client_addr,
                    "name": worker_name,
                    "threads": worker_threads,
                }

                with self.workers_lock:
                    self.workers.append(worker_info)
                    active_count = len(self.workers)
                    total_worker_th = sum(w.get("threads", 1) for w in self.workers)

                # Notifikasi real-time di console Master saat worker terdeteksi & terhubung
                sys.stdout.write(f"\r\n{Colors.BRIGHT_GREEN}[✓] WORKER BARU TERHUBUNG: {worker_name} ({client_addr[0]}:{client_addr[1]}) [Total: {active_count} Worker]{Colors.RESET}\n")
                sys.stdout.write(f"{Colors.BRIGHT_YELLOW}Pilih menu [1-2, 0]: {Colors.RESET}")
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
        Memeriksa koneksi seluruh worker secara instan (non-blocking) tanpa jeda PING.
        Menghapus worker yang sudah terputus sebelum komputasi dimulai.
        """
        with self.workers_lock:
            live = []
            for w in list(self.workers):
                if is_socket_alive(w["sock"]):
                    live.append(w)
                else:
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


def run_serial_sorting(data: List[int], waktu_unsort: Optional[float] = None) -> Tuple[float, bool, List[int]]:
    """
    Mode Serial: Mengurutkan data langsung di CPU master dengan PROGRESS BAR bertahap.
    Total waktu dan ringkasan ditampilkan di akhir proses (setelah simpan ke sorted.txt).
    """
    print_header("KOMPUTASI SERIAL SORTING", f"Memproses {len(data):,} Elemen di Master")
    print(f" {Colors.CYAN}•{Colors.RESET} Jumlah Data     : {Colors.BOLD}{len(data):,} elemen{Colors.RESET}")
    print(f" {Colors.CYAN}•{Colors.RESET} Pratinjau Asli  : {format_data_preview(data)}")

    t_overall_start = time.perf_counter()

    # 1. Timsort Blok Serial dengan Progress Bar
    t_sort_start = time.perf_counter()
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

    t_sort_end = time.perf_counter()
    sort_time = t_sort_end - t_sort_start

    print_success("Pengurutan Serial Selesai!")
    print(f" {Colors.CYAN}•{Colors.RESET} Hasil Terurut   : {format_data_preview(sorted_data)}")

    # 3. Validasi dengan Progress Bar
    is_valid = is_sorted(sorted_data) and (len(sorted_data) == len(data))
    print_progress_bar(1, 1, prefix="Validasi Urutan", suffix="Selesai (100%)")
    if is_valid:
        print_success("Validasi Urutan : BERHASIL (Data Terurut Sempurna)")
    else:
        print_error("Validasi Urutan : GAGAL (Data Tidak Terurut)")

    # 4. Simpan ke sorted.txt dengan Progress Bar
    t_save_start = time.perf_counter()
    save_to_file(sorted_data, FILE_SORTED)
    t_save_end = time.perf_counter()
    save_time = t_save_end - t_save_start

    print_progress_bar(1, 1, prefix="Simpan sorted.txt", suffix="Selesai (100%)")
    print_success(f"Hasil terurut berhasil disimpan ke '{FILE_SORTED}'!")

    t_overall_end = time.perf_counter()
    total_overall_time = t_overall_end - t_overall_start

    # Tampilkan ringkasan total waktu DI AKHIR PROSES
    print(f"\n{Colors.BRIGHT_WHITE}{Colors.BOLD}RINGKASAN WAKTU SERIAL SORTING:{Colors.RESET}")
    print(box_border())
    if waktu_unsort is not None:
        print(box_line(f"Waktu Unsort (Pembangkitan) : {Colors.BOLD}{waktu_unsort:.6f} detik{Colors.RESET}"))
    print(box_line(f"Waktu Pengurutan (Sorting)  : {Colors.BOLD}{sort_time:.6f} detik{Colors.RESET}"))
    print(box_line(f"Waktu Simpan sorted.txt     : {Colors.BOLD}{save_time:.6f} detik{Colors.RESET}"))
    print(box_line(f"TOTAL WAKTU KESELURUHAN     : {Colors.BOLD}{Colors.BRIGHT_YELLOW}{total_overall_time:.6f} detik{Colors.RESET} {Colors.DIM}(dari sorting s/d simpan){Colors.RESET}"))
    print(box_border())

    return total_overall_time, is_valid, sorted_data


def _worker_sort_task(worker_info: Dict[str, Any], chunk: List[int], chunk_id: int) -> Dict[str, Any]:
    """Mengirim chunk ke satu worker via TCP secara streaming kontinu dan menerima hasil terurut."""
    sock = worker_info["sock"]
    name = worker_info["name"]
    assigned_threads = worker_info.get("threads", 1)
    
    t_dispatch_start = time.perf_counter()
    payload = {
        "cmd": "SORT",
        "chunk_id": chunk_id,
        "data": chunk,
    }
    
    # Kirim paket via TCP secara streaming kontinu
    if not send_packet(
        sock, 
        payload, 
        progress_callback=lambda cur, tot: print_progress_bar(cur, tot, prefix=f"Kirim TCP #{chunk_id}", suffix=f"{cur/(1024*1024):.1f}/{tot/(1024*1024):.1f} MB")
    ):
        raise ConnectionError(f"Gagal mengirim data ke worker {name}")

    # Terima paket respon via TCP secara streaming kontinu
    response = recv_packet(
        sock, 
        progress_callback=lambda cur, tot: print_progress_bar(cur, tot, prefix=f"Terima TCP #{chunk_id}", suffix=f"{cur/(1024*1024):.1f}/{tot/(1024*1024):.1f} MB")
    )
    t_dispatch_end = time.perf_counter()
    
    if not response or response.get("status") != "OK":
        raise ConnectionError(f"Respon tidak valid dari worker {name}")

    actual_threads = response.get("threads", assigned_threads)

    return {
        "worker_name": name,
        "chunk_id": chunk_id,
        "threads": actual_threads,
        "sorted_chunk": response.get("data", []),
        "worker_sort_time": response.get("sort_time", 0.0),
        "roundtrip_time": t_dispatch_end - t_dispatch_start,
    }


def _master_local_sort_task(chunk: List[int], chunk_id: int, n_threads: int) -> Dict[str, Any]:
    """Memproses partisi data langsung di CPU Master Node menggunakan seluruh thread CPU Master."""
    t_start = time.perf_counter()
    chunk = parallel_sort_data(chunk, n_threads=n_threads)
    t_end = time.perf_counter()
    duration = t_end - t_start
    return {
        "worker_name": "Master Node (Lokal)",
        "chunk_id": chunk_id,
        "threads": n_threads,
        "sorted_chunk": chunk,
        "worker_sort_time": duration,
        "roundtrip_time": duration,
    }


def _worker_on_fly_task(worker_info: Dict[str, Any], n_items: int, chunk_id: int, seed: Optional[int] = None) -> Dict[str, Any]:
    """Mengirim instruksi ke worker untuk membangkitkan dan menyortir data on-the-fly di RAM lokalnya."""
    sock = worker_info["sock"]
    name = worker_info["name"]
    assigned_threads = worker_info.get("threads", 1)

    t_dispatch_start = time.perf_counter()
    payload = {
        "cmd": "SORT_ON_FLY",
        "chunk_id": chunk_id,
        "count": n_items,
        "seed": seed,
    }

    # Kirim paket instruksi (hanya 50 byte, instan 0.0001 detik tanpa kirim raw data)
    if not send_packet(sock, payload):
        raise ConnectionError(f"Gagal mengirim instruksi ke worker {name}")

    # Terima paket respon via TCP secara streaming kontinu
    response = recv_packet(
        sock, 
        progress_callback=lambda cur, tot: print_progress_bar(cur, tot, prefix=f"Terima Hasil #{chunk_id}", suffix=f"{cur/(1024*1024):.1f}/{tot/(1024*1024):.1f} MB")
    )
    t_dispatch_end = time.perf_counter()

    if not response or response.get("status") != "OK":
        raise ConnectionError(f"Respon tidak valid dari worker {name}")

    actual_threads = response.get("threads", assigned_threads)

    return {
        "worker_name": name,
        "chunk_id": chunk_id,
        "threads": actual_threads,
        "sorted_chunk": response.get("data", []),
        "worker_sort_time": response.get("sort_time", 0.0),
        "roundtrip_time": t_dispatch_end - t_dispatch_start,
    }


def _master_local_on_fly_task(n_items: int, chunk_id: int, n_threads: int, seed: Optional[int] = None) -> Dict[str, Any]:
    """Membangkitkan data dan mengurutkannya secara langsung on-the-fly di RAM lokal Master."""
    t_start = time.perf_counter()
    rng = random.Random(seed) if seed is not None else random.Random()
    local_data = [rng.randint(1, 10_000_000) for _ in range(n_items)]
    local_data = parallel_sort_data(local_data, n_threads=n_threads)
    t_end = time.perf_counter()
    duration = t_end - t_start
    return {
        "worker_name": "Master Node (Lokal)",
        "chunk_id": chunk_id,
        "threads": n_threads,
        "sorted_chunk": local_data,
        "worker_sort_time": duration,
        "roundtrip_time": duration,
    }


def run_distributed_sorting_on_fly(server: MasterServer, n_total: int = DEFAULT_DATA_SIZE, waktu_unsort: Optional[float] = None) -> Optional[Tuple[float, bool, List[int], int]]:
    """
    Mode Distributed Sorting Realtime On-The-Fly:
    - Master TIDAK mengirim data mentah (raw data) ke worker/slave (menghilangkan bottleneck transfer jaringan).
    - Setiap komputer (Master & seluruh Worker) membangkitkan dan mengurutkan porsi datanya
      secara langsung di RAM lokal secara simultan.
    - Master hanya menerima aliran data terurut dari worker dan menggabungkannya (K-Way Merge) secara linear.
    - Total waktu ditampilkan di akhir proses (setelah selesai simpan ke sorted.txt).
    """
    active_workers = server.get_live_workers()
    k_workers = len(active_workers)

    if k_workers == 0:
        print_error("Belum ada worker aktif yang terhubung ke Master!")
        print_info("Buka terminal baru lalu jalankan: python main.py (pilih [2])")
        return None

    master_threads = os.cpu_count() or 1
    worker_threads = sum(w.get("threads", 1) for w in active_workers)
    total_cluster_threads = master_threads + worker_threads
    total_computers = k_workers + 1

    print_header(
        "KOMPUTASI DISTRIBUTED SORTING (REALTIME ON-THE-FLY)", 
        f"{n_total:,} Data | {total_computers} Komputer (1 Master + {k_workers} Worker) | Realtime RAM Processing"
    )
    print(f" {Colors.CYAN}•{Colors.RESET} Total Target Data    : {Colors.BOLD}{n_total:,} elemen{Colors.RESET}")
    print(f" {Colors.CYAN}•{Colors.RESET} Mode Pemrosesan      : {Colors.BOLD}{Colors.BRIGHT_GREEN}Realtime On-The-Fly (Tanpa Kirim Raw Data ke Slave){Colors.RESET}")
    print(f" {Colors.CYAN}•{Colors.RESET} Komputer Terlibat    : {Colors.BOLD}{Colors.BRIGHT_CYAN}{total_computers} node{Colors.RESET} (1 Master + {k_workers} Worker)")

    # Alokasi jumlah data tiap node proporsional kapasitas
    nodes_info = [{
        "name": "Master Node (Lokal)",
        "threads": master_threads,
        "is_local": True,
        "worker_info": None,
    }]
    for w in active_workers:
        nodes_info.append({
            "name": w["name"],
            "threads": w.get("threads", 1),
            "is_local": False,
            "worker_info": w,
        })

    counts = []
    allocated = 0
    for idx, node in enumerate(nodes_info):
        if idx == len(nodes_info) - 1:
            cnt = n_total - allocated
        else:
            cnt = round(n_total * (node["threads"] / total_cluster_threads))
            allocated += cnt
        counts.append(cnt)
        node_lbl = "Master Node" if node["is_local"] else f"Worker #{idx}"
        print(f" {Colors.CYAN}•{Colors.RESET} Alokasi {node_lbl:<16}: {Colors.BOLD}{cnt:,} data{Colors.RESET} (Generated & Sorted On-The-Fly)")

    t_dist_total_start = time.perf_counter()

    if hasattr(server, "beacon") and server.beacon:
        server.beacon.pause()

    try:
        results = []
        failed_workers = []
        completed_count = 0

        print(f"\n{Colors.BRIGHT_WHITE}{Colors.BOLD}Memulai Eksekusi Realtime On-The-Fly ke Seluruh Komputer:{Colors.RESET}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=total_computers) as executor:
            future_map = {}

            # Master memproses porsinya sendiri di RAM lokal
            print(f"  {Colors.BRIGHT_GREEN}➔{Colors.RESET} Eksekusi {counts[0]:,} data (Chunk #1) langsung di {Colors.BOLD}Master RAM{Colors.RESET}...")
            fut_master = executor.submit(_master_local_on_fly_task, counts[0], 1, master_threads, random.randint(1, 1_000_000))
            future_map[fut_master] = nodes_info[0]

            # Worker menerima instruksi eksekusi on-the-fly tanpa kirim data mentah
            for w_idx, w in enumerate(active_workers):
                c_num = w_idx + 2
                node_meta = nodes_info[w_idx + 1]
                w_count = counts[w_idx + 1]
                print(f"  {Colors.BRIGHT_CYAN}➔{Colors.RESET} Menginstruksikan {w['name']} memproses {w_count:,} data (Chunk #{c_num}) On-The-Fly...")
                fut_worker = executor.submit(_worker_on_fly_task, w, w_count, c_num, random.randint(1, 1_000_000))
                future_map[fut_worker] = node_meta

            for future in concurrent.futures.as_completed(future_map):
                meta = future_map[future]
                try:
                    res = future.result()
                    results.append(res)
                    completed_count += 1
                    print_progress_bar(completed_count, total_computers, prefix="Realtime Komputasi", suffix=f"{res['worker_name']} Selesai ({completed_count}/{total_computers})")
                except Exception as err:
                    print_error(f"Gagal pada {meta['name']}: {err}")
                    if not meta.get("is_local"):
                        failed_workers.append(meta["worker_info"])

        for dead_w in failed_workers:
            server.remove_dead_worker(dead_w)

        if len(results) != total_computers:
            print_error("Pengurutan terdistribusi gagal karena ada worker yang terputus.")
            return None

        results.sort(key=lambda x: x["chunk_id"])

        t_network_done = time.perf_counter()
        process_time = t_network_done - t_dist_total_start

        # Tabel Rincian Eksekusi (tanpa kolom Thread)
        print(f"\n{Colors.BRIGHT_WHITE}{Colors.BOLD}Rincian Eksekusi Realtime On-The-Fly:{Colors.RESET}")
        t_border = f" {Colors.BRIGHT_BLUE}+{'-' * 8}+{'-' * 30}+{'-' * 18}+{'-' * 13}+{'-' * 13}+{Colors.RESET}"
        print(t_border)
        h_chunk = pad_ansi(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}Chunk{Colors.RESET}", 6)
        h_node = pad_ansi(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}Node Komputer{Colors.RESET}", 28)
        h_len = pad_ansi(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}Jumlah Data{Colors.RESET}", 16)
        h_sort = pad_ansi(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}Sort Time{Colors.RESET}", 11)
        h_rt = pad_ansi(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}Roundtrip{Colors.RESET}", 11)
        print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET} {h_chunk} {Colors.BRIGHT_BLUE}|{Colors.RESET} {h_node} {Colors.BRIGHT_BLUE}|{Colors.RESET} {h_len} {Colors.BRIGHT_BLUE}|{Colors.RESET} {h_sort} {Colors.BRIGHT_BLUE}|{Colors.RESET} {h_rt} {Colors.BRIGHT_BLUE}|{Colors.RESET}")
        print(t_border)
        for r in results:
            c_tag = pad_ansi(f"#{r['chunk_id']}", 6)
            w_name = pad_ansi(r["worker_name"][:28], 28)
            c_len = pad_ansi(f"{len(r['sorted_chunk']):,} data", 16)
            s_time = pad_ansi(f"{r['worker_sort_time']:.4f}s", 11)
            r_time = pad_ansi(f"{r['roundtrip_time']:.4f}s", 11)
            print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET} {c_tag} {Colors.BRIGHT_BLUE}|{Colors.RESET} {w_name} {Colors.BRIGHT_BLUE}|{Colors.RESET} {c_len} {Colors.BRIGHT_BLUE}|{Colors.RESET} {s_time} {Colors.BRIGHT_BLUE}|{Colors.RESET} {r_time} {Colors.BRIGHT_BLUE}|{Colors.RESET}")
        print(t_border)

        # K-Way Merge terakselerasi
        t_merge_start = time.perf_counter()
        sorted_chunks = [r["sorted_chunk"] for r in results]
        print_progress_bar(1, 2, prefix="K-Way Merge Master", suffix="Menggabungkan aliran data...")

        if len(sorted_chunks) == 1:
            final_sorted_data = sorted_chunks[0]
        else:
            final_sorted_data = []
            for chunk in sorted_chunks:
                final_sorted_data.extend(chunk)
            final_sorted_data.sort()

        print_progress_bar(2, 2, prefix="K-Way Merge Master", suffix="Selesai (100%)")
        t_merge_end = time.perf_counter()
        merge_time = t_merge_end - t_merge_start

        print_success("Penggabungan Realtime Selesai!")
        print(f" {Colors.CYAN}•{Colors.RESET} Hasil Terurut       : {format_data_preview(final_sorted_data)}")

        # Validasi
        is_valid = (len(final_sorted_data) == n_total) and is_sorted(final_sorted_data)
        print_progress_bar(1, 1, prefix="Validasi Urutan", suffix="Selesai (100%)")
        if is_valid:
            print_success("Validasi Urutan     : BERHASIL (Data Terurut Sempurna)")
        else:
            print_error("Validasi Urutan     : GAGAL (Data Rusak/Tidak Terurut)")

        # Simpan ke sorted.txt
        t_save_start = time.perf_counter()
        save_to_file(final_sorted_data, FILE_SORTED)
        t_save_end = time.perf_counter()
        save_time = t_save_end - t_save_start
        print_progress_bar(1, 1, prefix="Simpan sorted.txt", suffix="Selesai (100%)")
        print_success(f"Hasil terurut berhasil disimpan ke '{FILE_SORTED}'!")

        t_dist_total_end = time.perf_counter()
        total_distributed_time = t_dist_total_end - t_dist_total_start

        # Tampilkan TOTAL WAKTU di AKHIR PROSES!
        print(f"\n{Colors.BRIGHT_WHITE}{Colors.BOLD}RINGKASAN WAKTU DISTRIBUTED SORTING (ON-THE-FLY):{Colors.RESET}")
        print(box_border())
        if waktu_unsort is not None:
            print(box_line(f"Waktu Unsort (Pembangkitan) : {Colors.BOLD}{waktu_unsort:.6f} detik{Colors.RESET}"))
        print(box_line(f"Waktu Komputasi Paralel     : {Colors.BOLD}{process_time:.6f} detik{Colors.RESET}"))
        print(box_line(f"Waktu K-Way Merge           : {Colors.BOLD}{merge_time:.6f} detik{Colors.RESET}"))
        print(box_line(f"Waktu Simpan sorted.txt     : {Colors.BOLD}{save_time:.6f} detik{Colors.RESET}"))
        print(box_line(f"TOTAL WAKTU KESELURUHAN     : {Colors.BOLD}{Colors.BRIGHT_YELLOW}{total_distributed_time:.6f} detik{Colors.RESET} {Colors.DIM}(dari sorting s/d simpan){Colors.RESET}"))
        print(box_border())

        return total_distributed_time, is_valid, final_sorted_data, total_computers
    finally:
        if hasattr(server, "beacon") and server.beacon:
            server.beacon.resume()


def run_distributed_sorting(server: MasterServer, data: List[int]) -> Optional[Tuple[float, bool, List[int], int, int]]:
    """
    Mode Terdistribusi Multi-Komputer & Multi-Thread:
    Memanfaatkan 100% thread prosesor dari seluruh komputer (Master + Seluruh Worker).
    Data dipartisi proporsional berdasarkan kapasitas thread masing-masing komputer,
    kemudian setiap komputer menyortir partisi secara paralel dengan seluruh thread CPU lokalnya.
    """
    active_workers = server.get_live_workers()
    k_workers = len(active_workers)

    if k_workers == 0:
        print_error("Belum ada worker aktif yang terhubung ke Master!")
        print_info("Buka terminal lain atau komputer lain lalu jalankan: python main.py (pilih [2])")
        return None

    master_threads = os.cpu_count() or 1
    worker_threads = sum(w.get("threads", 1) for w in active_workers)
    total_cluster_threads = master_threads + worker_threads
    total_computers = k_workers + 1

    print_header(
        "KOMPUTASI PARALEL / DISTRIBUTED SORTING", 
        f"Kluster: {total_computers} Komputer (1 Master + {k_workers} Worker) | {total_cluster_threads} Total Thread CPU"
    )
    print(f" {Colors.CYAN}•{Colors.RESET} Jumlah Data         : {Colors.BOLD}{len(data):,} elemen{Colors.RESET}")
    print(f" {Colors.CYAN}•{Colors.RESET} Pratinjau Asli       : {format_data_preview(data)}")
    print(f" {Colors.CYAN}•{Colors.RESET} Total Komputer       : {Colors.BOLD}{Colors.BRIGHT_GREEN}{total_computers} node{Colors.RESET} (1 Master + {k_workers} Worker)")
    print(f" {Colors.CYAN}•{Colors.RESET} Total Thread Kluster : {Colors.BOLD}{Colors.BRIGHT_CYAN}{total_cluster_threads} Thread CPU{Colors.RESET} (Master: {master_threads} Th, Worker: {worker_threads} Th)")

    # 1. Menyiapkan daftar node komputasi
    nodes_info = [{
        "name": "Master Node (Lokal CPU)",
        "threads": master_threads,
        "is_local": True,
        "worker_info": None,
    }]
    for w in active_workers:
        nodes_info.append({
            "name": w["name"],
            "threads": w.get("threads", 1),
            "is_local": False,
            "worker_info": w,
        })

    # 2. Partisi data proporsional berdasarkan jumlah thread prosesor tiap komputer
    n_total = len(data)
    chunks = []
    start_idx = 0
    for idx, node in enumerate(nodes_info):
        if idx == len(nodes_info) - 1:
            end_idx = n_total
        else:
            portion = round(n_total * (node["threads"] / total_cluster_threads))
            end_idx = min(n_total, start_idx + portion)
        c_data = data[start_idx:end_idx]
        chunks.append(c_data)
        start_idx = end_idx
        node_label = f"Master ({node['threads']} Th)" if node["is_local"] else f"Worker #{idx} ({node['threads']} Th)"
        print_progress_bar(idx + 1, total_computers, prefix="Partisi Proporsional", suffix=f"{node_label}: {len(c_data):,} data")

    t_dist_total_start = time.perf_counter()

    # Jeda siaran beacon UDP sementara agar bandwidth Wi-Fi 100% dialokasikan ke koneksi TCP kontinu
    if hasattr(server, "beacon") and server.beacon:
        server.beacon.pause()

    try:
        results = []
        failed_workers = []
        completed_count = 0

        print(f"\n{Colors.BRIGHT_WHITE}{Colors.BOLD}Memulai Eksekusi Paralel ke {total_computers} Komputer ({total_cluster_threads} Thread):{Colors.RESET}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=total_computers) as executor:
            future_map = {}

            # Kirim tugas ke Master Node (Lokal)
            print(f"  {Colors.BRIGHT_GREEN}➔{Colors.RESET} Menugaskan {len(chunks[0]):,} data (Chunk #1) ke {Colors.BOLD}Master Node{Colors.RESET} ({master_threads} Thread CPU)...")
            fut_master = executor.submit(_master_local_sort_task, chunks[0], 1, master_threads)
            future_map[fut_master] = nodes_info[0]

            # Kirim tugas ke Worker Nodes via TCP
            for w_idx, w in enumerate(active_workers):
                c_num = w_idx + 2
                node_meta = nodes_info[w_idx + 1]
                w_th = node_meta["threads"]
                print(f"  {Colors.BRIGHT_CYAN}➔{Colors.RESET} Mengirim {len(chunks[w_idx + 1]):,} data (Chunk #{c_num}) ke {Colors.BOLD}{w['name']}{Colors.RESET} ({w_th} Thread CPU)...")
                fut_worker = executor.submit(_worker_sort_task, w, chunks[w_idx + 1], c_num)
                future_map[fut_worker] = node_meta

            for future in concurrent.futures.as_completed(future_map):
                meta = future_map[future]
                try:
                    res = future.result()
                    results.append(res)
                    completed_count += 1
                    print_progress_bar(completed_count, total_computers, prefix="Komputasi Paralel", suffix=f"{res['worker_name']} Selesai ({completed_count}/{total_computers})")
                except Exception as err:
                    print_error(f"Gagal pada {meta['name']}: {err}")
                    if not meta.get("is_local"):
                        failed_workers.append(meta["worker_info"])

        for dead_w in failed_workers:
            server.remove_dead_worker(dead_w)

        if len(results) != total_computers:
            print_error("Pengurutan terdistribusi gagal karena ada worker yang terputus.")
            return None

        # Urutkan results berdasarkan chunk_id agar terstruktur
        results.sort(key=lambda x: x["chunk_id"])

        t_network_done = time.perf_counter()
        network_and_sort_time = t_network_done - t_dist_total_start

        # Tabel Rincian Eksekusi Paralel Tiap Node
        print(f"\n{Colors.BRIGHT_WHITE}{Colors.BOLD}Rincian Eksekusi Tiap Node (Semua Thread Aktif):{Colors.RESET}")
        t_border = f" {Colors.BRIGHT_BLUE}+{'-' * 8}+{'-' * 30}+{'-' * 12}+{'-' * 18}+{'-' * 13}+{'-' * 13}+{Colors.RESET}"
        print(t_border)
        h_chunk = pad_ansi(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}Chunk{Colors.RESET}", 6)
        h_node = pad_ansi(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}Node Komputer{Colors.RESET}", 28)
        h_th = pad_ansi(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}Thread{Colors.RESET}", 10)
        h_len = pad_ansi(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}Jumlah Data{Colors.RESET}", 16)
        h_sort = pad_ansi(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}Sort Time{Colors.RESET}", 11)
        h_rt = pad_ansi(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}Roundtrip{Colors.RESET}", 11)
        print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET} {h_chunk} {Colors.BRIGHT_BLUE}|{Colors.RESET} {h_node} {Colors.BRIGHT_BLUE}|{Colors.RESET} {h_th} {Colors.BRIGHT_BLUE}|{Colors.RESET} {h_len} {Colors.BRIGHT_BLUE}|{Colors.RESET} {h_sort} {Colors.BRIGHT_BLUE}|{Colors.RESET} {h_rt} {Colors.BRIGHT_BLUE}|{Colors.RESET}")
        print(t_border)
        for r in results:
            c_tag = pad_ansi(f"#{r['chunk_id']}", 6)
            w_name = pad_ansi(r["worker_name"][:28], 28)
            r_th = pad_ansi(f"{r.get('threads', 1)} Th", 10)
            c_len = pad_ansi(f"{len(r['sorted_chunk']):,} data", 16)
            s_time = pad_ansi(f"{r['worker_sort_time']:.4f}s", 11)
            r_time = pad_ansi(f"{r['roundtrip_time']:.4f}s", 11)
            print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET} {c_tag} {Colors.BRIGHT_BLUE}|{Colors.RESET} {w_name} {Colors.BRIGHT_BLUE}|{Colors.RESET} {r_th} {Colors.BRIGHT_BLUE}|{Colors.RESET} {c_len} {Colors.BRIGHT_BLUE}|{Colors.RESET} {s_time} {Colors.BRIGHT_BLUE}|{Colors.RESET} {r_time} {Colors.BRIGHT_BLUE}|{Colors.RESET}")
        print(t_border)

        # 4. K-Way Merge di Master Terakselerasi
        t_merge_start = time.perf_counter()
        sorted_chunks = [r["sorted_chunk"] for r in results]
        print_progress_bar(1, 2, prefix="K-Way Merge Master", suffix="Menggabungkan potongan terurut...")

        if len(sorted_chunks) == 1:
            final_sorted_data = sorted_chunks[0]
        else:
            final_sorted_data = []
            for chunk in sorted_chunks:
                final_sorted_data.extend(chunk)
            final_sorted_data.sort()

        print_progress_bar(2, 2, prefix="K-Way Merge Master", suffix="Selesai (100%)")
        t_merge_end = time.perf_counter()
        merge_time = t_merge_end - t_merge_start
        total_distributed_time = t_merge_end - t_dist_total_start

        print_success("Penggabungan K-Way Merge Selesai!")
        print(f" {Colors.CYAN}•{Colors.RESET} Hasil Terurut       : {format_data_preview(final_sorted_data)}")
        print(f" {Colors.CYAN}•{Colors.RESET} Waktu Paralel/TCP   : {network_and_sort_time:.6f} detik")
        print(f" {Colors.CYAN}•{Colors.RESET} Waktu K-Way Merge   : {merge_time:.6f} detik")
        print(f" {Colors.CYAN}•{Colors.RESET} TOTAL WAKTU DIST    : {Colors.BOLD}{Colors.BRIGHT_YELLOW}{total_distributed_time:.6f} detik{Colors.RESET}")

        # 5. Validasi dengan Progress Bar
        is_valid = (len(final_sorted_data) == len(data)) and is_sorted(final_sorted_data)
        print_progress_bar(1, 1, prefix="Validasi Urutan", suffix="Selesai (100%)")
        if is_valid:
            print_success("Validasi Urutan     : BERHASIL (Data Terurut Sempurna)")
        else:
            print_error("Validasi Urutan     : GAGAL (Data Rusak/Tidak Terurut)")

        # 6. Simpan ke sorted.txt dengan Progress Bar
        save_to_file(final_sorted_data, FILE_SORTED)
        print_progress_bar(1, 1, prefix="Simpan sorted.txt", suffix="Selesai (100%)")
        print_success(f"Hasil terurut berhasil disimpan ke '{FILE_SORTED}'!")

        return total_distributed_time, is_valid, final_sorted_data, total_computers, total_cluster_threads
    finally:
        if hasattr(server, "beacon") and server.beacon:
            server.beacon.resume()


def print_comparison_metrics(t_serial: float, t_dist: float, total_computers: int, num_workers: int = 1):
    """Menampilkan tabel perbandingan, Speedup, dan Efisiensi."""
    speedup = t_serial / t_dist if t_dist > 0 else 0
    efficiency = (speedup / total_computers * 100) if total_computers > 0 else 0

    print_header("METRIK EVALUASI: SERIAL VS DISTRIBUTED", "Perhitungan Speedup dan Efisiensi Komputasi Kluster")
    print_table_row("Parameter Evaluasi", "Nilai / Hasil", is_header=True)
    print_table_row("Waktu Serial (T_serial)", f"{t_serial:.6f} detik")
    print_table_row("Waktu Terdistribusi (T_dist)", f"{t_dist:.6f} detik")
    print_table_row("Komputer Terlibat", f"{total_computers} node (1 Master + {num_workers} Worker)")
    
    color_speedup = Colors.BRIGHT_GREEN if speedup >= 1.0 else Colors.BRIGHT_YELLOW
    print_table_row("Speedup (S = T_serial / T_dist)", f"{color_speedup}{speedup:.2f}x{Colors.RESET}")
    print_table_row("Efisiensi (E = S / K)", f"{efficiency:.2f}%")
    print_table_footer()

    print(f"\n{Colors.BRIGHT_CYAN}[Analisis Komputasi Terdistribusi]:{Colors.RESET}")
    if speedup > 1.0:
        print(f" {Colors.BRIGHT_GREEN}✔ Distributed Sorting LEBIH CEPAT ({speedup:.2f}x akselerasi) dibanding mode Serial!{Colors.RESET}")
        print(f"   Memanfaatkan {total_computers} komputer secara simultan.")
    else:
        print(f" {Colors.BRIGHT_YELLOW}ℹ Mode Serial lebih cepat atau seimbang.{Colors.RESET}")
        print("   Catatan: Gunakan lebih banyak worker atau data >= 1.000.000 untuk efisiensi puncak.")


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
    last_nodes_count: int = 1
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

        server.all_ips = get_all_local_ips()
        ip_list_str = " | ".join(f"{ip}:{server.port}" for ip in server.all_ips[:2])

        if workers:
            w_names = ", ".join(w.get("name", "Worker") for w in workers[:2])
            if len(workers) > 2:
                w_names += f" +{len(workers)-2}"
            worker_text = f"{len(workers)} node ({w_names})"
        else:
            worker_text = f"{Colors.DIM}0 node (Menunggu worker terhubung...){Colors.RESET}"

        print(box_border())
        print(box_title(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}PANEL KONTROL MASTER (SERVER){Colors.RESET}"))
        print(box_border())
        print(box_line(f"Alamat IP Master  : {Colors.BOLD}{Colors.BRIGHT_GREEN}{ip_list_str}{Colors.RESET}"))
        print(box_line(f"Auto-Discovery    : {Colors.BRIGHT_GREEN}AKTIF (UDP 5002 - Beacon & Subnet Sweep){Colors.RESET}"))
        print(box_line(f"Worker Terhubung  : {Colors.BOLD}{Colors.BRIGHT_GREEN if workers else Colors.BRIGHT_WHITE}{worker_text}{Colors.RESET}"))
        print(box_line(f"File unsorted.txt : {unsorted_status}"))
        print(box_line(f"File sorted.txt   : {sorted_status}"))
        print(box_line(f"Pratinjau Data    : {preview_str}"))
        print(box_border())
        print(box_line(f" {Colors.BRIGHT_CYAN}[1]{Colors.RESET} {Colors.BOLD}Jalankan Serial Sorting{Colors.RESET} (Simpan ke {FILE_SORTED})"))
        print(box_line(f" {Colors.BRIGHT_CYAN}[2]{Colors.RESET} {Colors.BOLD}Jalankan Distributed Sorting{Colors.RESET} (Realtime On-The-Fly)"))
        print(box_line(f" {Colors.BRIGHT_RED}[0]{Colors.RESET} Keluar / Matikan Master"))
        print(box_border())
        print(f" {Colors.DIM}💡 Tips HP Hotspot: Di Worker, Anda bisa langsung ketik IP Master di atas lalu [Enter]{Colors.RESET}")

        if notification:
            if is_notif_warning:
                print_warning(notification)
            else:
                print_success(notification)
            notification = None
            is_notif_warning = False

        try:
            choice = input(f"\n{Colors.BRIGHT_YELLOW}Pilih menu [1-2, 0]: {Colors.RESET}").strip()
        except (KeyboardInterrupt, EOFError):
            break

        if choice == "1":
            try:
                raw_n = input(f"\n{Colors.BRIGHT_YELLOW}Jumlah angka N acak positif [default 1,000,000]: {Colors.RESET}").strip()
                n_items = int(raw_n.replace(".", "").replace(",", "")) if raw_n else DEFAULT_DATA_SIZE
                if n_items <= 0:
                    n_items = DEFAULT_DATA_SIZE
            except ValueError:
                n_items = DEFAULT_DATA_SIZE

            data, waktu_unsort = generate_random_data(n_items)
            current_data = data

            try:
                lanjut = input(f"\n{Colors.BRIGHT_YELLOW}Ingin melanjutkan ke pengurutan (Sorting)? [Y/n]: {Colors.RESET}").strip().lower()
            except (KeyboardInterrupt, EOFError):
                lanjut = "n"

            if lanjut in ["n", "no", "tidak", "t"]:
                notification = f"{len(current_data):,} data tersimpan di '{FILE_UNSORTED}'. Pengurutan dibatalkan."
                is_notif_warning = False
                continue

            t_serial, _, sorted_res = run_serial_sorting(current_data, waktu_unsort=waktu_unsort)
            last_serial_time = t_serial
            current_data = sorted_res
            if last_dist_time is not None:
                print_comparison_metrics(last_serial_time, last_dist_time, total_computers=last_nodes_count, num_workers=len(server.get_active_workers()))
            input(f"\n{Colors.DIM}Tekan [Enter] untuk kembali ke panel kontrol...{Colors.RESET}")

        elif choice == "2":
            if len(server.get_active_workers()) == 0:
                notification = "Belum ada worker aktif yang terhubung! Hubungkan worker terlebih dahulu."
                is_notif_warning = True
                continue

            try:
                raw_n = input(f"\n{Colors.BRIGHT_YELLOW}Jumlah angka N acak [default 1,000,000]: {Colors.RESET}").strip()
                n_items = int(raw_n.replace(".", "").replace(",", "")) if raw_n else DEFAULT_DATA_SIZE
                if n_items <= 0:
                    n_items = DEFAULT_DATA_SIZE
            except ValueError:
                n_items = DEFAULT_DATA_SIZE

            data, waktu_unsort = generate_random_data(n_items)
            current_data = data

            try:
                lanjut = input(f"\n{Colors.BRIGHT_YELLOW}Ingin melanjutkan ke pengurutan (Sorting)? [Y/n]: {Colors.RESET}").strip().lower()
            except (KeyboardInterrupt, EOFError):
                lanjut = "n"

            if lanjut in ["n", "no", "tidak", "t"]:
                notification = f"{len(current_data):,} data tersimpan di '{FILE_UNSORTED}'. Pengurutan dibatalkan."
                is_notif_warning = False
                continue

            result = run_distributed_sorting_on_fly(server, n_total=n_items, waktu_unsort=waktu_unsort)
            if result:
                last_dist_time, _, sorted_res, last_nodes_count = result
                current_data = sorted_res
                if last_serial_time is not None:
                    print_comparison_metrics(last_serial_time, last_dist_time, total_computers=last_nodes_count, num_workers=len(server.get_active_workers()))
            input(f"\n{Colors.DIM}Tekan [Enter] untuk kembali ke panel kontrol...{Colors.RESET}")

        elif choice == "0":
            break
        else:
            notification = "Pilihan tidak valid. Silakan pilih 1, 2, atau 0."
            is_notif_warning = True

    server.shutdown()
    clear_screen()
    print_info("Master Server telah dinonaktifkan.")


if __name__ == "__main__":
    banner()
    run_master_cli()
