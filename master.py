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
import queue
import concurrent.futures
from typing import List, Dict, Any, Optional, Tuple

from network_utils import (
    send_packet, recv_packet, MasterBeacon, DEFAULT_MASTER_PORT,
    get_local_ip, get_all_local_ips, tune_socket, is_socket_alive
)
import fastsort
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
    Mengurutkan array angka menggunakan rutin vektor native NumPy (bila tersedia).
    Jauh lebih cepat dari Timsort Python + ThreadPoolExecutor yang terhambat GIL.
    Fallback ke `list.sort` murni bila NumPy tidak terpasang.
    """
    return fastsort.sort_values(arr, n_threads=n_threads)


def is_sorted(arr: List[int]) -> bool:
    """Validasi apakah list sudah terurut secara non-decreasing (vektor NumPy)."""
    return fastsort.is_sorted_values(arr)


def format_data_preview(arr: Optional[List[int]], max_items: int = 8) -> str:
    """Membuat teks pratinjau data angka agar rapi di layar."""
    if arr is None or len(arr) == 0:
        return f"{Colors.DIM}[Belum ada data]{Colors.RESET}"
    if len(arr) <= max_items:
        return f"{Colors.CYAN}{arr}{Colors.RESET}"
    half = max_items // 2
    front = ", ".join(str(x) for x in arr[:half])
    back = ", ".join(str(x) for x in arr[-half:])
    return f"{Colors.CYAN}[{front}, ... ({len(arr):,} angka) ..., {back}]{Colors.RESET}"


def save_to_file(data: List[int], filename: str):
    """Menyimpan list angka ke file teks (satu angka per baris).

    Ditulis per-chunk dengan jeda GIL singkat agar event loop GUI tetap
    responsif ketika data berjumlah ratusan juta.
    """
    n = len(data)
    chunk = 500_000
    with open(filename, "w", encoding="utf-8") as f:
        if n == 0:
            f.write("\n")
            return
        for start in range(0, n, chunk):
            block = data[start:start + chunk]
            f.write("\n".join(map(str, block)))
            f.write("\n")
            time.sleep(0.001)


def load_from_file(filename: str) -> Optional[List[int]]:
    """Membaca list angka dari file teks (per-batch, melepas GIL berkala)."""
    if not os.path.exists(filename):
        return None
    try:
        data: List[int] = []
        with open(filename, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    data.append(int(line))
                    if len(data) % 1_000_000 == 0:
                        time.sleep(0.001)
        return data
    except Exception:
        return None


def delete_txt_files(file_unsorted: str = FILE_UNSORTED, file_sorted: str = FILE_SORTED) -> Tuple[bool, str]:
    """Menghapus berkas unsorted.txt dan sorted.txt jika ada."""
    deleted = []
    for fname in [file_unsorted, file_sorted]:
        if os.path.exists(fname):
            try:
                os.remove(fname)
                deleted.append(fname)
            except Exception as e:
                return False, f"Gagal menghapus '{fname}': {e}"
    if deleted:
        names_str = ", ".join(f"'{f}'" for f in deleted)
        return True, f"Berkas {names_str} berhasil dihapus!"
    return True, "Tidak ada berkas 'unsorted.txt' atau 'sorted.txt' yang ditemukan."


def generate_random_data(n: int, server: Optional[Any] = None) -> Tuple[List[int], float]:
    """
    Membangkitkan N angka integer acak POSITIF (tidak ada angka minus).
    Jika ada worker/slave yang terhubung ke server, proses pembangkitan data
    dilakukan secara terdistribusi (paralel) oleh Master dan seluruh Worker On-The-Fly,
    lalu disimpan ke 'unsorted.txt'.
    """
    active_workers = server.get_live_workers() if server else []
    k_workers = len(active_workers)
    total_computers = k_workers + 1

    t_total_start = time.perf_counter()

    if k_workers > 0:
        # Pembangkitan Paralel Terdistribusi On-The-Fly bersama Slave
        print_header(
            "PROSES PEMBANGKITAN DATA (UNSORTED) ON-THE-FLY", 
            f"Membangkitkan {n:,} Angka Acak | {total_computers} Komputer"
        )
        print(f" {Colors.CYAN}•{Colors.RESET} Target Jumlah Data : {Colors.BOLD}{n:,} elemen{Colors.RESET}")
        print(f" {Colors.CYAN}•{Colors.RESET} Mode Pembangkitan  : {Colors.BOLD}{Colors.BRIGHT_GREEN}Paralel On-The-Fly (Bantuan Komputer Slave){Colors.RESET}")
        print(f" {Colors.CYAN}•{Colors.RESET} Target Berkas      : {Colors.BOLD}{FILE_UNSORTED}{Colors.RESET}")

        # Pembagian porsi
        counts = []
        allocated = 0
        for idx in range(total_computers):
            if idx == total_computers - 1:
                cnt = n - allocated
            else:
                cnt = n // total_computers
                allocated += cnt
            counts.append(cnt)
            node_lbl = "Master Node" if idx == 0 else f"Worker #{idx}"
            print(f" {Colors.CYAN}•{Colors.RESET} Alokasi {node_lbl:<16}: {Colors.BOLD}{cnt:,} data acak{Colors.RESET} (Generated On-The-Fly)")

        t_gen_start = time.perf_counter()

        def _master_gen_task(count: int, seed: int) -> List[int]:
            rng = random.Random(seed)
            return [rng.randint(1, 10_000_000) for _ in range(count)]

        def _worker_gen_task(w_info: Dict[str, Any], count: int, c_id: int, seed: int) -> Tuple[int, List[int]]:
            payload = {
                "cmd": "GENERATE_UNSORTED",
                "chunk_id": c_id,
                "count": count,
                "seed": seed,
            }
            if not send_packet(w_info["sock"], payload):
                raise ConnectionError(f"Gagal mengirim instruksi generate ke worker {w_info['name']}")
            resp = recv_packet(
                w_info["sock"],
                progress_callback=lambda cur, tot: print_progress_bar(cur, tot, prefix=f"Terima Data #{c_id}", suffix=f"{cur/(1024*1024):.1f}/{tot/(1024*1024):.1f} MB")
            )
            if not resp or resp.get("status") != "OK":
                raise ConnectionError(f"Respon tidak valid dari worker {w_info['name']}")
            return c_id, resp.get("data", [])

        results: Dict[int, List[int]] = {}
        completed_count = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=total_computers) as executor:
            future_map = {}
            # Master local task
            fut_master = executor.submit(_master_gen_task, counts[0], random.randint(1, 1_000_000))
            future_map[fut_master] = (0, "Master Node")

            # Worker tasks
            for w_idx, w in enumerate(active_workers):
                c_num = w_idx + 1
                fut_w = executor.submit(_worker_gen_task, w, counts[c_num], c_num, random.randint(1, 1_000_000))
                future_map[fut_w] = (c_num, w["name"])

            for fut in concurrent.futures.as_completed(future_map):
                c_id, node_name = future_map[fut]
                try:
                    res_val = fut.result()
                    if c_id == 0:
                        results[0] = res_val
                    else:
                        _, arr = res_val
                        results[c_id] = arr
                    completed_count += 1
                    print_progress_bar(completed_count, total_computers, prefix="Pembangkitan On-The-Fly", suffix=f"{node_name} Selesai ({completed_count}/{total_computers})")
                except Exception as err:
                    print_warning(f"Worker {node_name} gagal generate data: {err}. Master membangkitkan porsi ini.")
                    # Fallback jika worker gagal
                    rng = random.Random()
                    results[c_id] = [rng.randint(1, 10_000_000) for _ in range(counts[c_id])]
                    completed_count += 1
                    print_progress_bar(completed_count, total_computers, prefix="Pembangkitan On-The-Fly", suffix=f"{node_name} (Lokal) ({completed_count}/{total_computers})")

        # Gabungkan data
        data = []
        for i in range(total_computers):
            data.extend(results.get(i, []))

        t_gen_end = time.perf_counter()
        time_generate = t_gen_end - t_gen_start

    else:
        # Pembangkitan Lokal (Mode Serial / tanpa worker)
        print_header("PROSES PEMBANGKITAN DATA (UNSORTED)", f"Membangkitkan {n:,} Angka Acak Positif")
        print(f" {Colors.CYAN}•{Colors.RESET} Target Jumlah Data : {Colors.BOLD}{n:,} elemen{Colors.RESET}")
        print(f" {Colors.CYAN}•{Colors.RESET} Mode Pembangkitan  : {Colors.BOLD}Lokal Master Node{Colors.RESET}")
        print(f" {Colors.CYAN}•{Colors.RESET} Target Berkas      : {Colors.BOLD}{FILE_UNSORTED}{Colors.RESET}")

        t_gen_start = time.perf_counter()
        data = []
        num_batches = 10
        batch_size = n // num_batches

        for b in range(num_batches):
            cur_batch = n - len(data) if b == num_batches - 1 else batch_size
            data.extend([random.randint(1, 10_000_000) for _ in range(cur_batch)])
            print_progress_bar(b + 1, num_batches, prefix="Generate Angka", suffix=f"{len(data):,}/{n:,}")
            time.sleep(0.001)

        t_gen_end = time.perf_counter()
        time_generate = t_gen_end - t_gen_start

    # Simpan ke unsorted.txt
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
    # Blok dibuat lebih kecil untuk data raksasa agar setiap `list.sort()`
    # (Timsort C yang menahan GIL) singkat, sehingga event loop GUI tetap
    # responsif. Untuk data kecil perilakunya tetap ~10 blok seperti semula.
    t_sort_start = time.perf_counter()
    if len(data) <= 5_000_000:
        block_size = max(1, len(data) // 10)
    else:
        block_size = 500_000
    num_blocks = max(1, (len(data) + block_size - 1) // block_size)
    sorted_blocks = []

    for i in range(num_blocks):
        start = i * block_size
        end = len(data) if i == num_blocks - 1 else (i + 1) * block_size
        block = list(data[start:end])
        block.sort()
        sorted_blocks.append(block)
        print_progress_bar(i + 1, num_blocks, prefix="Timsort Serial Blok", suffix=f"Blok {i+1}/{num_blocks}")
        time.sleep(0.001)

    # 2. K-Way Merge Serial dengan Progress Bar
    total_len = len(data)
    step = max(total_len // 10, 1)
    sorted_data = []
    count = 0
    for val in heapq.merge(*sorted_blocks):
        sorted_data.append(val)
        count += 1
        if count % 200_000 == 0:
            time.sleep(0.001)
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
        "data": fastsort.to_int32(chunk),
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


def broadcast_summary(server: MasterServer, title: str, lines: List[str]) -> None:
    """Mengirim ringkasan hasil ke seluruh Worker yang masih terhubung,
    agar Worker dapat menampilkan laporan akhir yang sama seperti Master."""
    payload = {"cmd": "SUMMARY", "title": title, "lines": lines}
    for w in server.get_live_workers():
        try:
            send_packet(w["sock"], payload)
        except Exception:
            pass


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
        f"{n_total:,} Data | {total_computers} Komputer | Realtime RAM Processing"
    )
    print(f" {Colors.CYAN}•{Colors.RESET} Total Target Data    : {Colors.BOLD}{n_total:,} elemen{Colors.RESET}")
    print(f" {Colors.CYAN}•{Colors.RESET} Mode Pemrosesan      : {Colors.BOLD}{Colors.BRIGHT_GREEN}Realtime On-The-Fly (Tanpa Kirim Raw Data ke Slave){Colors.RESET}")
    print(f" {Colors.CYAN}•{Colors.RESET} Komputer Terlibat    : {Colors.BOLD}{Colors.BRIGHT_CYAN}{total_computers} node{Colors.RESET}")

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

        # K-Way Merge terakselerasi (vektor NumPy)
        t_merge_start = time.perf_counter()
        sorted_chunks = [r["sorted_chunk"] for r in results]
        print_progress_bar(1, 2, prefix="K-Way Merge Master", suffix="Menggabungkan aliran data...")

        final_sorted_data = fastsort.merge_sorted_chunks(sorted_chunks)

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
        print(box_line(f"Waktu Komputasi : {Colors.BOLD}{process_time:.6f} detik{Colors.RESET}"))
        print(box_line(f"Waktu K-Way Merge           : {Colors.BOLD}{merge_time:.6f} detik{Colors.RESET}"))
        print(box_line(f"Waktu Simpan sorted.txt     : {Colors.BOLD}{save_time:.6f} detik{Colors.RESET}"))
        print(box_line(f"TOTAL WAKTU KESELURUHAN     : {Colors.BOLD}{Colors.BRIGHT_YELLOW}{total_distributed_time:.6f} detik{Colors.RESET} {Colors.DIM}(dari sorting s/d simpan){Colors.RESET}"))
        print(box_border())

        return total_distributed_time, is_valid, final_sorted_data, total_computers
    finally:
        if hasattr(server, "beacon") and server.beacon:
            server.beacon.resume()


def run_distributed_sorting(server: MasterServer, data: List[int], waktu_unsort: Optional[float] = None) -> Optional[Tuple[float, bool, List[int], int, int]]:
    """
    Mode Terdistribusi Multi-Komputer (Pipelined Stream Concurrent):
    - Mengeliminasi bottleneck transfer data masif dengan membagi data menjadi batch stream kecil.
    - Master dan seluruh Worker Slave bekerja secara simultan (bersama-sama) sejak detik pertama.
    - Master mengeksekusi porsi di RAM Master, sementara Worker mengeksekusi porsi di RAM Worker.
    - Dilengkapi Dynamic Work-Stealing jika salah satu komputer selesai lebih awal.
    - Hasil potongan terurut digabungkan secara linear (K-Way Merge) di Master.
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

    # -- SEED SESI KLUSTER -------------------------------------------------
    # Dibuat sekali di awal, dikirim ke setiap Worker, lalu ditampilkan ulang
    # di awal dan di akhir proses pada SEMUA node. Jika Master dan Worker
    # menampilkan seed yang SAMA, terbukti keduanya benar-benar berjalan pada
    # satu sesi komputasi paralel yang sama.
    session_seed = random.randrange(100_000, 1_000_000_000)

    def _chunk_seed(cid: int) -> int:
        """Seed unik & deterministik untuk sebuah chunk (turunan dari session_seed)."""
        return (session_seed * 1_000_003 + cid * 2_654_435_761) % 1_000_000_000

    print_header(
        "DISTRIBUTED SORTING (PIPELINED STREAM)",
        f"Kluster: {total_computers} Komputer | {total_cluster_threads} Total Thread CPU"
    )
    print(f" {Colors.CYAN}•{Colors.RESET} Jumlah Data : {Colors.BOLD}{len(data):,} elemen{Colors.RESET}")
    print(f" {Colors.CYAN}•{Colors.RESET} Pratinjau Asli : {format_data_preview(data)}")
    print(f" {Colors.CYAN}•{Colors.RESET} Total Komputer : {Colors.BOLD}{Colors.BRIGHT_GREEN}{total_computers} node{Colors.RESET}")
    print(f" {Colors.CYAN}•{Colors.RESET} Total Thread Kluster : {Colors.BOLD}{Colors.BRIGHT_CYAN}{total_cluster_threads} Thread CPU{Colors.RESET} (Master: {master_threads} Th, Worker: {worker_threads} Th)")
    print(f" {Colors.CYAN}•{Colors.RESET} Mode Eksekusi : {Colors.BOLD}{Colors.BRIGHT_GREEN}Pipelined Stream{Colors.RESET}")
    print(f" {Colors.CYAN}•{Colors.RESET} Seed Sesi Awal : {Colors.BOLD}{Colors.BRIGHT_YELLOW}{session_seed}{Colors.RESET}")

    n_total = len(data)
    # Tentukan ukuran chunk streaming: cukup kecil agar transfer ringan dan pipelining
    # berjalan, tetapi tidak terlalu banyak agar tidak membanjiri jaringan/terminal.
    # Jumlah chunk ditargetkan sekitar `CHUNKS_PER_NODE` per node (lebih tinggi =
    # granularitas work-stealing lebih halus & beban lebih merata).
    CHUNKS_PER_NODE = 32
    target_chunks = max(total_computers * CHUNKS_PER_NODE, 1)
    chunk_size = max(10_000, (n_total + target_chunks - 1) // target_chunks)
    # Batasi besar tiap transfer TCP agar tetap ringan & tidak membanjiri buffer Wi-Fi
    chunk_size = min(chunk_size, 500_000)

    raw_chunks = [data[i:i + chunk_size] for i in range(0, n_total, chunk_size)]
    num_chunks = len(raw_chunks)

    print(f" {Colors.CYAN}•{Colors.RESET} Partisi Pipelining : {Colors.BOLD}{num_chunks} chunk stream{Colors.RESET} (~{chunk_size:,} data per chunk)")

    # Siapkan antrean tugas untuk Master dan seluruh Worker
    # Antrean tugas BERSAMA (Shared Task Queue) untuk Dynamic Work-Stealing:
    # setiap node (Master & Worker) mengambil chunk berikutnya begitu ia selesai,
    # sehingga node tercepat otomatis mengerjakan lebih banyak dan node lambat
    # tidak lagi menjadi "straggler" yang menahan seluruh kluster. Ini mengatasi
    # ketidakseimbangan beban (load imbalance) pada perangkat heterogen.
    task_queue: queue.Queue = queue.Queue()
    for idx, c_data in enumerate(raw_chunks):
        task_queue.put((idx + 1, c_data))

    t_dist_total_start = time.perf_counter()

    # Jeda beacon UDP agar bandwidth LAN/Wi-Fi 100% untuk TCP stream
    if hasattr(server, "beacon") and server.beacon:
        server.beacon.pause()

    try:
        results = []
        results_lock = threading.Lock()
        progress_lock = threading.Lock()
        completed_chunks = 0
        failed_workers = []

        node_stats = {
            "Master Node (Lokal CPU)": {"chunks": 0, "items": 0, "sort_time": 0.0, "threads": master_threads}
        }
        for w in active_workers:
            node_stats[w["name"]] = {"chunks": 0, "items": 0, "sort_time": 0.0, "threads": w.get("threads", 1)}

        print(f"\n{Colors.BRIGHT_WHITE}{Colors.BOLD}Memulai Eksekusi ke Seluruh Komputer:{Colors.RESET}")

        # Thread dispatcher untuk masing-masing worker eksternal
        def _worker_dispatcher_task(w_info: Dict[str, Any]):
            w_sock = w_info["sock"]
            w_name = w_info["name"]
            assigned_th = w_info.get("threads", 1)

            while True:
                try:
                    c_id, c_data = task_queue.get_nowait()
                except queue.Empty:
                    break

                t_dispatch = time.perf_counter()
                c_seed = _chunk_seed(c_id)
                payload = {
                    "cmd": "SORT",
                    "chunk_id": c_id,
                    "data": fastsort.to_int32(c_data),
                    "seed": c_seed,
                    "session_seed": session_seed,
                }
                if not send_packet(w_sock, payload):
                    print_error(f"Gagal mengirim chunk #{c_id} ke {w_name}")
                    if w_info not in failed_workers:
                        failed_workers.append(w_info)
                    task_queue.put((c_id, c_data))
                    break

                resp = recv_packet(w_sock)
                t_return = time.perf_counter()
                if not resp or resp.get("status") != "OK":
                    print_error(f"Respon tidak valid dari {w_name} untuk chunk #{c_id}")
                    if w_info not in failed_workers:
                        failed_workers.append(w_info)
                    task_queue.put((c_id, c_data))
                    break

                # Verifikasi SEED SESI: bukti Worker memproses tugas dari sesi paralel yang sama.
                if resp.get("session_seed") != session_seed:
                    print_warning(f"Seed sesi berbeda dari {w_name} pada chunk #{c_id}.")

                s_time = resp.get("sort_time", t_return - t_dispatch)
                sorted_chunk = resp.get("data", [])
                actual_th = resp.get("threads", assigned_th)

                with results_lock:
                    results.append({
                        "chunk_id": c_id,
                        "worker_name": w_name,
                        "threads": actual_th,
                        "seed": c_seed,
                        "session_seed": resp.get("session_seed", session_seed),
                        "sorted_chunk": sorted_chunk,
                        "worker_sort_time": s_time,
                        "roundtrip_time": t_return - t_dispatch,
                    })
                    node_stats[w_name]["chunks"] += 1
                    node_stats[w_name]["items"] += len(sorted_chunk)
                    node_stats[w_name]["sort_time"] += s_time

                with progress_lock:
                    nonlocal completed_chunks
                    completed_chunks += 1
                    print_progress_bar(
                        completed_chunks,
                        num_chunks,
                        prefix="Komputasi",
                        suffix=f"{w_name}: Chunk #{c_id} (seed {c_seed}) Selesai ({completed_chunks}/{num_chunks})"
                    )

        # Thread pekerja lokal Master Node (mengambil dari antrean tugas bersama)
        def _master_local_worker_task():
            while True:
                try:
                    c_id, c_data = task_queue.get_nowait()
                except queue.Empty:
                    break

                t_m_start = time.perf_counter()
                sorted_chunk = parallel_sort_data(c_data, n_threads=master_threads)
                t_m_end = time.perf_counter()
                m_duration = t_m_end - t_m_start
                c_seed = _chunk_seed(c_id)

                with results_lock:
                    results.append({
                        "chunk_id": c_id,
                        "worker_name": "Master Node (Lokal CPU)",
                        "threads": master_threads,
                        "seed": c_seed,
                        "session_seed": session_seed,
                        "sorted_chunk": sorted_chunk,
                        "worker_sort_time": m_duration,
                        "roundtrip_time": m_duration,
                    })
                    node_stats["Master Node (Lokal CPU)"]["chunks"] += 1
                    node_stats["Master Node (Lokal CPU)"]["items"] += len(sorted_chunk)
                    node_stats["Master Node (Lokal CPU)"]["sort_time"] += m_duration

                with progress_lock:
                    nonlocal completed_chunks
                    completed_chunks += 1
                    print_progress_bar(
                        completed_chunks,
                        num_chunks,
                        prefix="Komputasi",
                        suffix=f"Master: Chunk #{c_id} (seed {c_seed}) Selesai ({completed_chunks}/{num_chunks})"
                    )

        # Jalankan Master worker dan Worker dispatchers secara BERSAMAAN di latar belakang
        workers_threads_list = []
        for w in active_workers:
            t = threading.Thread(target=_worker_dispatcher_task, args=(w,))
            workers_threads_list.append(t)
            t.start()

        t_master = threading.Thread(target=_master_local_worker_task)
        workers_threads_list.append(t_master)
        t_master.start()

        for t in workers_threads_list:
            t.join()

        # Fallback: proses sisa chunk yang tertinggal (mis. akibat worker terputus) di Master
        leftover = []
        while True:
            try:
                leftover.append(task_queue.get_nowait())
            except queue.Empty:
                break
        for c_id, c_data in leftover:
            t_m_start = time.perf_counter()
            sorted_chunk = parallel_sort_data(c_data, n_threads=master_threads)
            t_m_end = time.perf_counter()
            m_duration = t_m_end - t_m_start
            with results_lock:
                results.append({
                    "chunk_id": c_id,
                    "worker_name": "Master Node (Lokal CPU)",
                    "threads": master_threads,
                    "seed": _chunk_seed(c_id),
                    "session_seed": session_seed,
                    "sorted_chunk": sorted_chunk,
                    "worker_sort_time": m_duration,
                    "roundtrip_time": m_duration,
                })
                node_stats["Master Node (Lokal CPU)"]["chunks"] += 1
                node_stats["Master Node (Lokal CPU)"]["items"] += len(sorted_chunk)
                node_stats["Master Node (Lokal CPU)"]["sort_time"] += m_duration
        if leftover:
            with progress_lock:
                completed_chunks += len(leftover)
            print_progress_bar(completed_chunks, num_chunks, prefix="Komputasi", suffix=f"Fallback Master: {len(leftover)} chunk")

        for dead_w in failed_workers:
            server.remove_dead_worker(dead_w)

        if len(results) != num_chunks:
            print_error("Pengurutan terdistribusi gagal karena ada potongan yang tidak lengkap.")
            return None

        # Urutkan results berdasarkan chunk_id asli
        results.sort(key=lambda x: x["chunk_id"])

        # -- BUKTI SEED SESI -----------------------------------------------
        # Hitung berapa chunk Worker yang mengembalikan seed sesi yang SAMA.
        # Bila cocok, terbukti Worker memproses tugas dari sesi paralel yang sama.
        worker_results = [r for r in results if r["worker_name"] != "Master Node (Lokal CPU)"]
        session_verified = sum(1 for r in worker_results if r.get("session_seed") == session_seed)

        t_compute_done = time.perf_counter()
        concurrent_compute_time = t_compute_done - t_dist_total_start

        # Tampilkan tabel kontribusi komputasi bersama tiap node
        print(f"\n{Colors.BRIGHT_WHITE}{Colors.BOLD}Rincian Kontribusi Tiap Node:{Colors.RESET}")
        t_border = f" {Colors.BRIGHT_BLUE}+{'-' * 30}+{'-' * 10}+{'-' * 14}+{'-' * 18}+{'-' * 14}+{'-' * 14}+{Colors.RESET}"
        print(t_border)
        h_node = pad_ansi(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}Node Komputer{Colors.RESET}", 28)
        h_th = pad_ansi(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}Thread{Colors.RESET}", 8)
        h_chk = pad_ansi(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}Chunk{Colors.RESET}", 12)
        h_len = pad_ansi(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}Total Data{Colors.RESET}", 16)
        h_sort = pad_ansi(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}Waktu Sort{Colors.RESET}", 12)
        h_status = pad_ansi(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}Status{Colors.RESET}", 12)
        print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET} {h_node} {Colors.BRIGHT_BLUE}|{Colors.RESET} {h_th} {Colors.BRIGHT_BLUE}|{Colors.RESET} {h_chk} {Colors.BRIGHT_BLUE}|{Colors.RESET} {h_len} {Colors.BRIGHT_BLUE}|{Colors.RESET} {h_sort} {Colors.BRIGHT_BLUE}|{Colors.RESET} {h_status} {Colors.BRIGHT_BLUE}|{Colors.RESET}")
        print(t_border)
        for n_name, st in node_stats.items():
            col_node = pad_ansi(n_name[:28], 28)
            col_th = pad_ansi(f"{st['threads']} Th", 8)
            col_chk = pad_ansi(f"{st['chunks']} chunk", 12)
            col_len = pad_ansi(f"{st['items']:,} data", 16)
            col_sort = pad_ansi(f"{st['sort_time']:.4f}s", 12)
            col_stat = pad_ansi(f"{Colors.BRIGHT_GREEN}Selesai{Colors.RESET}", 12)
        print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET} {col_node} {Colors.BRIGHT_BLUE}|{Colors.RESET} {col_th} {Colors.BRIGHT_BLUE}|{Colors.RESET} {col_chk} {Colors.BRIGHT_BLUE}|{Colors.RESET} {col_len} {Colors.BRIGHT_BLUE}|{Colors.RESET} {col_sort} {Colors.BRIGHT_BLUE}|{Colors.RESET} {col_stat} {Colors.BRIGHT_BLUE}|{Colors.RESET}")
        print(t_border)

        # Tabel bukti seed tiap chunk: menunjukkan Master & Worker mengerjakan
        # potongan dari satu sesi paralel yang sama (seed sesi identik).
        print(f"\n{Colors.BRIGHT_WHITE}{Colors.BOLD}Seed Tiap Chunk (Sesi #{session_seed}):{Colors.RESET}")
        proof_border = f" {Colors.BRIGHT_BLUE}+{'-' * 8}+{'-' * 30}+{'-' * 14}+{'-' * 14}+{Colors.RESET}"
        print(proof_border)
        hp_chunk = pad_ansi(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}Chunk{Colors.RESET}", 6)
        hp_node = pad_ansi(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}Node Komputer{Colors.RESET}", 28)
        hp_seed = pad_ansi(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}Seed Chunk{Colors.RESET}", 12)
        hp_ss = pad_ansi(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}Seed Sesi{Colors.RESET}", 12)
        print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET} {hp_chunk} {Colors.BRIGHT_BLUE}|{Colors.RESET} {hp_node} {Colors.BRIGHT_BLUE}|{Colors.RESET} {hp_seed} {Colors.BRIGHT_BLUE}|{Colors.RESET} {hp_ss} {Colors.BRIGHT_BLUE}|{Colors.RESET}")
        print(proof_border)
        shown_results = results if len(results) <= 40 else (results[:20] + results[-20:])
        for r in shown_results:
            p_chunk = pad_ansi(f"#{r['chunk_id']}", 6)
            p_node = pad_ansi(r["worker_name"][:28], 28)
            p_seed = pad_ansi(f"{r.get('seed', '-')}", 12)
            if r.get("session_seed") == session_seed:
                p_ss = pad_ansi(f"{Colors.BRIGHT_GREEN}{r.get('session_seed', '-')}{Colors.RESET}", 12)
            else:
                p_ss = pad_ansi(f"{Colors.BRIGHT_RED}{r.get('session_seed', '-')}{Colors.RESET}", 12)
            print(f" {Colors.BRIGHT_BLUE}|{Colors.RESET} {p_chunk} {Colors.BRIGHT_BLUE}|{Colors.RESET} {p_node} {Colors.BRIGHT_BLUE}|{Colors.RESET} {p_seed} {Colors.BRIGHT_BLUE}|{Colors.RESET} {p_ss} {Colors.BRIGHT_BLUE}|{Colors.RESET}")
        if len(results) > 40:
            print(f" {Colors.DIM}... {len(results) - 40} chunk lainnya disembunyikan (seed tetap diverifikasi) ...{Colors.RESET}")
        print(proof_border)

        # 4. K-Way Merge Linear di Master (vektor NumPy)
        t_merge_start = time.perf_counter()
        sorted_chunks = [r["sorted_chunk"] for r in results]
        print_progress_bar(1, 2, prefix="K-Way Merge Master", suffix="Menggabungkan aliran potongan terurut...")

        final_sorted_data = fastsort.merge_sorted_chunks(sorted_chunks)

        print_progress_bar(2, 2, prefix="K-Way Merge Master", suffix="Selesai (100%)")
        t_merge_end = time.perf_counter()
        merge_time = t_merge_end - t_merge_start

        # 5. Validasi
        is_valid = (len(final_sorted_data) == len(data)) and is_sorted(final_sorted_data)
        print_progress_bar(1, 1, prefix="Validasi Urutan", suffix="Selesai (100%)")
        if is_valid:
            print_success("Validasi Urutan     : BERHASIL (Data Terurut Sempurna)")
        else:
            print_error("Validasi Urutan     : GAGAL (Data Rusak/Tidak Terurut)")

        # 6. Simpan ke sorted.txt
        t_save_start = time.perf_counter()
        save_to_file(final_sorted_data, FILE_SORTED)
        t_save_end = time.perf_counter()
        save_time = t_save_end - t_save_start
        print_progress_bar(1, 1, prefix="Simpan sorted.txt", suffix="Selesai (100%)")
        print_success(f"Hasil terurut berhasil disimpan ke '{FILE_SORTED}'!")

        t_dist_total_end = time.perf_counter()
        total_distributed_time = t_dist_total_end - t_dist_total_start

        # Ringkasan Waktu Keseluruhan (ditampilkan juga di sisi Worker Client)
        print(f"\n{Colors.BRIGHT_WHITE}{Colors.BOLD}RINGKASAN WAKTU DISTRIBUTED SORTING:{Colors.RESET}")
        summary_lines = []
        summary_lines.append(box_border())
        summary_lines.append(box_line(f"Total Data Terurut : {Colors.BOLD}{len(final_sorted_data):,} data{Colors.RESET}"))
        summary_lines.append(box_line(f"Total Komputer : {Colors.BOLD}{total_computers} node{Colors.RESET}"))
        summary_lines.append(box_line(f"Seed Sesi Awal : {Colors.BOLD}{Colors.BRIGHT_YELLOW}{session_seed}{Colors.RESET}"))
        summary_lines.append(box_line(f"Seed Sesi Akhir : {Colors.BOLD}{Colors.BRIGHT_YELLOW}{session_seed}{Colors.RESET}"))
        if worker_results:
            proof_color = Colors.BRIGHT_GREEN if session_verified == len(worker_results) else Colors.BRIGHT_RED
            summary_lines.append(box_line(f"Seed Sesi Cocok : {proof_color}{session_verified}/{len(worker_results)} chunk Worker pakai seed sesi sama{Colors.RESET}"))
        else:
            summary_lines.append(box_line(f"Seed Sesi Cocok : {Colors.DIM}tidak ada chunk yang diproses Worker (kluster tanpa Worker aktif){Colors.RESET}"))
        if is_valid:
            summary_lines.append(box_line(f"Status Validasi : {Colors.BRIGHT_GREEN}BERHASIL (Data Terurut Sempurna){Colors.RESET}"))
        else:
            summary_lines.append(box_line(f"Status Validasi : {Colors.BRIGHT_RED}GAGAL (Data Rusak/Tidak Terurut){Colors.RESET}"))
        if waktu_unsort is not None:
            summary_lines.append(box_line(f"Waktu Unsort (Pembangkitan) : {Colors.BOLD}{waktu_unsort:.6f} detik{Colors.RESET}"))
        summary_lines.append(box_line(f"Waktu Komputasi : {Colors.BOLD}{concurrent_compute_time:.6f} detik{Colors.RESET}"))
        summary_lines.append(box_line(f"Waktu K-Way Merge : {Colors.BOLD}{merge_time:.6f} detik{Colors.RESET}"))
        summary_lines.append(box_line(f"Waktu Simpan sorted.txt : {Colors.BOLD}{save_time:.6f} detik{Colors.RESET}"))
        summary_lines.append(box_line(f"TOTAL WAKTU KESELURUHAN : {Colors.BOLD}{Colors.BRIGHT_YELLOW}{total_distributed_time:.6f} detik{Colors.RESET}"))
        summary_lines.append(box_border())
        for line in summary_lines:
            print(line)

        # Tampilkan ringkasan yang sama di sisi Worker Client
        broadcast_summary(server, "RINGKASAN WAKTU DISTRIBUTED SORTING", summary_lines)

        return total_distributed_time, is_valid, final_sorted_data, total_computers, total_cluster_threads
    finally:
        if hasattr(server, "beacon") and server.beacon:
            server.beacon.resume()


def run_local_parallel_sorting(data: List[int], waktu_unsort: Optional[float] = None,
                               n_threads: Optional[int] = None) -> Tuple[float, bool, List[int]]:
    """
    Mode Local Parallel Sorting: memanfaatkan SELURUH thread CPU pada SATU komputer
    tanpa membutuhkan perangkat lain maupun koneksi Wi-Fi/LAN.

    Strategi:
    - Data dipartisi menjadi banyak chunk kecil (beberapa chunk per thread) agar
      beban merata ke semua core.
    - Setiap chunk disortir secara paralel oleh ThreadPoolExecutor. NumPy melepas
      GIL saat `np.sort`, sehingga thread benar-benar berjalan bersamaan di core
      yang berbeda (bukan sekadar concurrency).
    - Seluruh chunk terurut digabung kembali secara linear (K-Way Merge vektor).
    """
    n_total = len(data)
    max_threads = os.cpu_count() or 1
    if n_threads is None:
        n_threads = max_threads
    n_threads = max(1, min(int(n_threads), max_threads))

    print_header(
        "KOMPUTASI LOCAL PARALLEL SORTING",
        f"{n_total:,} Elemen | {n_threads} Thread CPU Lokal (Tanpa Jaringan)"
    )
    print(f" {Colors.CYAN}•{Colors.RESET} Jumlah Data : {Colors.BOLD}{n_total:,} elemen{Colors.RESET}")
    print(f" {Colors.CYAN}•{Colors.RESET} Pratinjau Asli : {format_data_preview(data)}")
    print(f" {Colors.CYAN}•{Colors.RESET} Thread CPU Dipakai : {Colors.BOLD}{Colors.BRIGHT_GREEN}{n_threads} / {max_threads} thread{Colors.RESET}")
    print(f" {Colors.CYAN}•{Colors.RESET} Mode Eksekusi : {Colors.BOLD}{Colors.BRIGHT_GREEN}Parallel Lokal (Offline){Colors.RESET}")

    t_overall_start = time.perf_counter()

    # Partisi adaptif: beberapa chunk per thread agar distribusi beban lebih merata.
    CHUNKS_PER_THREAD = 8
    target_chunks = max(n_threads * CHUNKS_PER_THREAD, 1)
    chunk_size = max(10_000, (n_total + target_chunks - 1) // target_chunks)
    chunk_size = min(chunk_size, 500_000)
    raw_chunks = [data[i:i + chunk_size] for i in range(0, n_total, chunk_size)]
    num_chunks = len(raw_chunks)

    print(f" {Colors.CYAN}•{Colors.RESET} Partisi Data : {Colors.BOLD}{num_chunks} chunk{Colors.RESET} (~{chunk_size:,} data per chunk)")

    results: List[Optional[List[int]]] = [None] * num_chunks
    progress_lock = threading.Lock()
    completed = 0

    t_sort_start = time.perf_counter()

    def _sort_chunk(idx: int, chunk: List[int]):
        nonlocal completed
        sorted_chunk = parallel_sort_data(chunk, n_threads=1)
        results[idx] = sorted_chunk
        with progress_lock:
            completed += 1
            print_progress_bar(
                completed,
                num_chunks,
                prefix="Sort Paralel Lokal",
                suffix=f"Chunk {completed}/{num_chunks} Selesai"
            )

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
        futures = [executor.submit(_sort_chunk, i, c) for i, c in enumerate(raw_chunks)]
        for fut in concurrent.futures.as_completed(futures):
            fut.result()

    t_sort_end = time.perf_counter()
    sort_time = t_sort_end - t_sort_start

    # K-Way Merge linear (vektor NumPy)
    t_merge_start = time.perf_counter()
    print_progress_bar(1, 2, prefix="K-Way Merge Lokal", suffix="Menggabungkan chunk terurut...")
    final_sorted_data = fastsort.merge_sorted_chunks([r for r in results if r is not None])
    print_progress_bar(2, 2, prefix="K-Way Merge Lokal", suffix="Selesai (100%)")
    t_merge_end = time.perf_counter()
    merge_time = t_merge_end - t_merge_start

    print_success("Pengurutan Local Parallel Selesai!")
    print(f" {Colors.CYAN}•{Colors.RESET} Hasil Terurut : {format_data_preview(final_sorted_data)}")

    # Validasi
    is_valid = (len(final_sorted_data) == n_total) and is_sorted(final_sorted_data)
    print_progress_bar(1, 1, prefix="Validasi Urutan", suffix="Selesai (100%)")
    if is_valid:
        print_success("Validasi Urutan : BERHASIL (Data Terurut Sempurna)")
    else:
        print_error("Validasi Urutan : GAGAL (Data Rusak/Tidak Terurut)")

    # Simpan ke sorted.txt
    t_save_start = time.perf_counter()
    save_to_file(final_sorted_data, FILE_SORTED)
    t_save_end = time.perf_counter()
    save_time = t_save_end - t_save_start
    print_progress_bar(1, 1, prefix="Simpan sorted.txt", suffix="Selesai (100%)")
    print_success(f"Hasil terurut berhasil disimpan ke '{FILE_SORTED}'!")

    t_overall_end = time.perf_counter()
    total_overall_time = t_overall_end - t_overall_start

    print(f"\n{Colors.BRIGHT_WHITE}{Colors.BOLD}RINGKASAN WAKTU LOCAL PARALLEL SORTING:{Colors.RESET}")
    print(box_border())
    print(box_line(f"Thread CPU Dipakai : {Colors.BOLD}{n_threads} thread{Colors.RESET}"))
    print(box_line(f"Jumlah Chunk : {Colors.BOLD}{num_chunks} chunk{Colors.RESET}"))
    if waktu_unsort is not None:
        print(box_line(f"Waktu Unsort (Pembangkitan) : {Colors.BOLD}{waktu_unsort:.6f} detik{Colors.RESET}"))
    print(box_line(f"Waktu Sort Paralel : {Colors.BOLD}{sort_time:.6f} detik{Colors.RESET}"))
    print(box_line(f"Waktu K-Way Merge : {Colors.BOLD}{merge_time:.6f} detik{Colors.RESET}"))
    print(box_line(f"Waktu Simpan sorted.txt : {Colors.BOLD}{save_time:.6f} detik{Colors.RESET}"))
    print(box_line(f"TOTAL WAKTU KESELURUHAN : {Colors.BOLD}{Colors.BRIGHT_YELLOW}{total_overall_time:.6f} detik{Colors.RESET} {Colors.DIM}(dari sorting s/d simpan){Colors.RESET}"))
    print(box_border())

    return total_overall_time, is_valid, final_sorted_data


def run_local_parallel_cli():
    """
    Menu CLI interaktif mode Local Parallel:
    hanya 1 komputer, memanfaatkan seluruh thread CPU, tanpa perangkat lain
    dan tanpa koneksi Wi-Fi/LAN. Menyediakan baseline Serial untuk perbandingan.
    """
    current_data = load_from_file(FILE_UNSORTED)
    last_serial_time: Optional[float] = None
    last_local_time: Optional[float] = None
    last_local_threads: int = os.cpu_count() or 1
    notification: Optional[str] = None
    is_notif_warning: bool = False

    if current_data is not None and len(current_data) > 0:
        notification = f"Ditemukan '{FILE_UNSORTED}' ({len(current_data):,} data siap digunakan)!"

    def _prepare_data() -> Optional[Tuple[List[int], Optional[float]]]:
        """Memilih sumber data (pakai berkas / bangkitkan baru) + konfirmasi sorting."""
        nonlocal current_data, notification, is_notif_warning
        use_existing = False
        if current_data is not None and len(current_data) > 0:
            try:
                pilih_data = input(f"\n{Colors.BRIGHT_YELLOW}Gunakan data yang ada ({len(current_data):,} data di {FILE_UNSORTED})? [Y/n]: {Colors.RESET}").strip().lower()
            except (KeyboardInterrupt, EOFError):
                pilih_data = "y"
            if pilih_data not in ["n", "no", "tidak", "t"]:
                use_existing = True

        waktu_unsort: Optional[float] = None
        if not use_existing:
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
            return None
        return current_data, waktu_unsort

    while True:
        clear_screen()
        banner()

        max_threads = os.cpu_count() or 1
        data_count = len(current_data) if current_data is not None else 0
        preview_str = format_data_preview(current_data, max_items=8)

        unsorted_status = f"{Colors.BRIGHT_GREEN}ADA ({data_count:,} data){Colors.RESET}" if os.path.exists(FILE_UNSORTED) else f"{Colors.DIM}Belum ada{Colors.RESET}"
        sorted_status = f"{Colors.BRIGHT_GREEN}ADA{Colors.RESET}" if os.path.exists(FILE_SORTED) else f"{Colors.DIM}Belum ada{Colors.RESET}"

        print(box_border())
        print(box_title(f"{Colors.BOLD}{Colors.BRIGHT_WHITE}PANEL LOCAL PARALLEL (SATU KOMPUTER){Colors.RESET}"))
        print(box_border())
        print(box_line(f"CPU Lokal Terdeteksi : {Colors.BOLD}{Colors.BRIGHT_GREEN}{max_threads} Thread{Colors.RESET}"))
        print(box_line(f"Koneksi Jaringan : {Colors.BRIGHT_GREEN}TIDAK DIPERLUKAN (Offline){Colors.RESET}"))
        print(box_line(f"File unsorted.txt : {unsorted_status}"))
        print(box_line(f"File sorted.txt : {sorted_status}"))
        print(box_line(f"Pratinjau Data : {preview_str}"))
        print(box_border())
        print(box_line(f" {Colors.BRIGHT_CYAN}[1]{Colors.RESET} {Colors.BOLD}Jalankan Local Parallel Sorting{Colors.RESET} (Semua Thread CPU)"))
        print(box_line(f" {Colors.BRIGHT_CYAN}[2]{Colors.RESET} {Colors.BOLD}Jalankan Serial Sorting{Colors.RESET} (Baseline 1 CPU)"))
        print(box_line(f" {Colors.BRIGHT_CYAN}[3]{Colors.RESET} {Colors.BOLD}Hapus File .txt{Colors.RESET} ({FILE_UNSORTED} & {FILE_SORTED})"))
        print(box_line(f" {Colors.BRIGHT_RED}[0]{Colors.RESET} Kembali ke Menu Utama"))
        print(box_border())
        print(f" {Colors.DIM}Mode ini hanya butuh 1 komputer: seluruh core CPU dipakai, tanpa Wi-Fi/LAN.{Colors.RESET}")

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
            prepared = _prepare_data()
            if not prepared:
                continue
            prepared_data, waktu_unsort = prepared

            try:
                raw_th = input(f"\n{Colors.BRIGHT_YELLOW}Jumlah thread CPU [default {max_threads}, maks {max_threads}]: {Colors.RESET}").strip()
                n_threads = int(raw_th) if raw_th else max_threads
            except ValueError:
                n_threads = max_threads
            n_threads = max(1, min(n_threads, max_threads))

            t_local, _, sorted_res = run_local_parallel_sorting(prepared_data, waktu_unsort=waktu_unsort, n_threads=n_threads)
            last_local_time = t_local
            last_local_threads = n_threads
            current_data = sorted_res

            if last_serial_time is not None:
                print_comparison_metrics(last_serial_time, last_local_time, total_computers=1)
                print_info(f"Speedup murni dari paralelisme lokal: {n_threads} thread CPU pada 1 komputer.")
            input(f"\n{Colors.DIM}Tekan [Enter] untuk kembali ke panel kontrol...{Colors.RESET}")

        elif choice == "2":
            prepared = _prepare_data()
            if not prepared:
                continue
            prepared_data, waktu_unsort = prepared

            t_serial, _, sorted_res = run_serial_sorting(prepared_data, waktu_unsort=waktu_unsort)
            last_serial_time = t_serial
            current_data = sorted_res

            if last_local_time is not None:
                print_comparison_metrics(last_serial_time, last_local_time, total_computers=1)
            input(f"\n{Colors.DIM}Tekan [Enter] untuk kembali ke panel kontrol...{Colors.RESET}")

        elif choice == "3":
            success, msg = delete_txt_files(FILE_UNSORTED, FILE_SORTED)
            current_data = None
            notification = msg
            is_notif_warning = not success
            continue

        elif choice == "0":
            break
        else:
            notification = "Pilihan tidak valid. Silakan pilih 1, 2, 3, atau 0."
            is_notif_warning = True


def print_comparison_metrics(t_serial: float, t_dist: float, total_computers: int, num_workers: int = 1):
    """Menampilkan tabel perbandingan, Speedup, dan Efisiensi."""
    speedup = t_serial / t_dist if t_dist > 0 else 0
    efficiency = (speedup / total_computers * 100) if total_computers > 0 else 0

    print_header("METRIK EVALUASI: SERIAL VS DISTRIBUTED", "Perhitungan Speedup dan Efisiensi Komputasi Kluster")
    print_table_row("Parameter Evaluasi", "Nilai / Hasil", is_header=True)
    print_table_row("Waktu Serial (T_serial)", f"{t_serial:.6f} detik")
    print_table_row("Waktu Terdistribusi (T_dist)", f"{t_dist:.6f} detik")
    print_table_row("Komputer Terlibat", f"{total_computers} node")
    
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

    if current_data is not None and len(current_data) > 0:
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
        print(box_line(f" {Colors.BRIGHT_CYAN}[2]{Colors.RESET} {Colors.BOLD}Jalankan Distributed Sorting{Colors.RESET} (Pipelined Stream)"))
        print(box_line(f" {Colors.BRIGHT_CYAN}[3]{Colors.RESET} {Colors.BOLD}Hapus File .txt{Colors.RESET} ({FILE_UNSORTED} & {FILE_SORTED})"))
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
            choice = input(f"\n{Colors.BRIGHT_YELLOW}Pilih menu [1-3, 0]: {Colors.RESET}").strip()
        except (KeyboardInterrupt, EOFError):
            break

        if choice == "1":
            use_existing = False
            if current_data is not None and len(current_data) > 0:
                try:
                    pilih_data = input(f"\n{Colors.BRIGHT_YELLOW}Gunakan data yang ada ({len(current_data):,} data di {FILE_UNSORTED})? [Y/n]: {Colors.RESET}").strip().lower()
                except (KeyboardInterrupt, EOFError):
                    pilih_data = "y"
                if pilih_data not in ["n", "no", "tidak", "t"]:
                    use_existing = True

            if not use_existing:
                try:
                    raw_n = input(f"\n{Colors.BRIGHT_YELLOW}Jumlah angka N acak positif [default 1,000,000]: {Colors.RESET}").strip()
                    n_items = int(raw_n.replace(".", "").replace(",", "")) if raw_n else DEFAULT_DATA_SIZE
                    if n_items <= 0:
                        n_items = DEFAULT_DATA_SIZE
                except ValueError:
                    n_items = DEFAULT_DATA_SIZE

                # Mode Serial murni lokal di Master (tanpa melibatkan slave/worker)
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
            else:
                waktu_unsort = None

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

            use_existing = False
            if current_data is not None and len(current_data) > 0:
                try:
                    pilih_data = input(f"\n{Colors.BRIGHT_YELLOW}Gunakan data yang ada ({len(current_data):,} data di {FILE_UNSORTED})? [Y/n]: {Colors.RESET}").strip().lower()
                except (KeyboardInterrupt, EOFError):
                    pilih_data = "y"
                if pilih_data not in ["n", "no", "tidak", "t"]:
                    use_existing = True

            if not use_existing:
                try:
                    raw_n = input(f"\n{Colors.BRIGHT_YELLOW}Jumlah angka N acak [default 1,000,000]: {Colors.RESET}").strip()
                    n_items = int(raw_n.replace(".", "").replace(",", "")) if raw_n else DEFAULT_DATA_SIZE
                    if n_items <= 0:
                        n_items = DEFAULT_DATA_SIZE
                except ValueError:
                    n_items = DEFAULT_DATA_SIZE

                data, waktu_unsort = generate_random_data(n_items, server=server)
                current_data = data

                try:
                    lanjut = input(f"\n{Colors.BRIGHT_YELLOW}Ingin melanjutkan ke pengurutan (Sorting)? [Y/n]: {Colors.RESET}").strip().lower()
                except (KeyboardInterrupt, EOFError):
                    lanjut = "n"

                if lanjut in ["n", "no", "tidak", "t"]:
                    notification = f"{len(current_data):,} data tersimpan di '{FILE_UNSORTED}'. Pengurutan dibatalkan."
                    is_notif_warning = False
                    continue
            else:
                waktu_unsort = None

            result = run_distributed_sorting(server, current_data, waktu_unsort=waktu_unsort)
            if result:
                last_dist_time, _, sorted_res, last_nodes_count, _ = result
                current_data = sorted_res
                if last_serial_time is not None:
                    print_comparison_metrics(last_serial_time, last_dist_time, total_computers=last_nodes_count, num_workers=len(server.get_active_workers()))
            input(f"\n{Colors.DIM}Tekan [Enter] untuk kembali ke panel kontrol...{Colors.RESET}")

        elif choice == "3":
            success, msg = delete_txt_files(FILE_UNSORTED, FILE_SORTED)
            current_data = None
            notification = msg
            is_notif_warning = not success
            continue

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
