# Dokumentasi Teknis — Sistem Distributed & Serial Merge Sort

Dokumen ini menjelaskan arsitektur, protokol jaringan, algoritma, dan detail implementasi
sistem **Distributed & Serial Merge Sort** berbasis TCP/IP socket murni (standard library Python).

> Untuk panduan pemakaian singkat, lihat [README.md](README.md).

---

## 1. Ikhtisar

Sistem mengurutkan sejumlah besar angka integer positif dengan membagi beban ke beberapa
komputer (**Master + Worker**) melalui dua mode:

| Mode | Deskripsi | Fungsi utama |
| ---- | --------- | ------------ |
| **Serial** | 100% lokal di CPU Master (baseline) | `master.run_serial_sorting` |
| **Local Parallel** | Satu komputer saja (tanpa jaringan/perangkat lain), seluruh thread/core CPU dipakai untuk menyortir chunk secara paralel, lalu K-Way Merge | `master.run_local_parallel_sorting` |
| **Distributed (Pipelined Stream)** | Master & Worker menyortir porsi masing-masing secara simultan, hasil digabung di Master | `master.run_distributed_sorting` |

Tujuan: membandingkan **waktu serial** vs **waktu terdistribusi**, lalu menghitung **Speedup** & **Efisiensi**.

---

## 2. Arsitektur & Topologi

```text
        UDP 5002 (Broadcast / Sweep / Beacon)        UDP 5002
   ┌───────────────────────────────────────────┐  ┌─────────────┐
   │              Jaringan Wi-Fi / LAN          │  │   Worker-2  │
   └───────────────────────────────────────────┘  └──────┬──────┘
                 ▲              ▲                         │ TCP 5000
                 │              │                         │
        ┌────────┴───────┐   ┌──┴──────────┐         ┌────┴────────┐
        │  MASTER NODE   │◄──┤  Worker-1   │◄────────┤   Worker-N  │
        │  TCP Server    │   │  TCP Client │  TCP    │  TCP Client │
        │  UDP Beacon    │   └─────────────┘         └─────────────┘
        └────────────────┘
```

- **Master** menjalankan TCP server (`0.0.0.0:5000`), UDP beacon (`5002`), dan mengoordinasi komputasi.
- **Worker** melakukan auto-discovery lewat UDP, lalu terhubung ke TCP Master dan menunggu perintah.
- Koneksi Worker bersifat **persisten** (satu koneksi TCP untuk banyak chunk).

---

## 3. Struktur Modul

| Berkas | Tanggung jawab |
| ------ | -------------- |
| `main.py` | Menu utama: pilih peran (Master/Worker/Local Parallel), hapus berkas |
| `gui_app.py` | Antarmuka grafis Tkinter: tab Master/Worker/Local Parallel, konsol ANSI, progress bar (versi GUI dari `main.py`) |
| `master.py` | MasterServer, serial & distributed sorting, metrik, CLI Master |
| `worker.py` | Client: discovery, loop perintah, sorting lokal, kirim balik hasil |
| `network_utils.py` | Framing TCP (pickle + zstd/zlib), progress callback, UDP discovery |
| `fastsort.py` | Akselerasi NumPy: sort vektor, packing `int32`, validasi & merge |
| `cli_ui.py` | Warna ANSI, box, header, tabel, progress bar (aman Windows/Linux) |
| `requirements.txt` | Daftar dependensi eksternal (`numpy`, `zstandard`) |

### 3.1 `master.py` — Fungsi & Kelas Kunci

| Simbol | Deskripsi |
| ------ | --------- |
| `parallel_sort_data(arr, n_threads)` | Sort vektor via `fastsort.sort_values` (NumPy `np.sort`, fallback `list.sort`) — bebas GIL dan jauh lebih cepat dari thread Python |
| `is_sorted(arr)` | Validasi urutan non-decreasing secara vektor (`fastsort.is_sorted_values`) |
| `format_data_preview(arr, max_items)` | Pratinjau angka untuk UI |
| `save_to_file` / `load_from_file` | Baca/tulis `unsorted.txt` & `sorted.txt` (satu angka per baris) |
| `delete_txt_files` | Hapus kedua berkas data |
| `generate_random_data(n, server)` | Bangkitkan $N$ angka acak (terdistribusi bila ada Worker), simpan ke `unsorted.txt` |
| `MasterServer` | TCP server: `start_server`, `get_live_workers`, `remove_dead_worker`, `shutdown` |
| `run_serial_sorting(data, waktu_unsort)` | Mode serial murni lokal |
| `run_local_parallel_sorting(data, waktu_unsort, n_threads)` | Mode paralel lokal: seluruh thread CPU, tanpa jaringan |
| `run_local_parallel_cli()` | Menu interaktif Local Parallel (1 komputer, offline) |
| `run_distributed_sorting(server, data, ...)` | Mode distributed pipelined stream |
| `broadcast_summary(server, title, lines)` | Kirim ringkasan akhir ke seluruh Worker |
| `print_comparison_metrics(...)` | Tabel Speedup & Efisiensi |
| `run_master_cli(port)` | Menu interaktif Master |

> Catatan: `run_distributed_sorting_on_fly` juga tersedia (Worker membangkitkan+menyortir data di RAM-nya sendiri, tanpa mengirim raw data). Saat ini belum diekspos ke menu CLI dan disediakan sebagai alternatif/eksperimen.

### 3.2 `worker.py` — Fungsi & Kelas Kunci

| Simbol | Deskripsi |
| ------ | --------- |
| `parallel_sort_data(arr, n_threads)` | Sama dengan Master: sort multi-thread lokal |
| `stream_progress(prefix)` | Callback progres yang hanya tampil untuk transfer ≥ 8 MB |
| `select_or_discover_master()` | Layar *Infinity Scan* + input non-blocking (Enter/pilih IP manual) |
| `run_worker(host, port, name)` | Konek ke Master, kirim REGISTER, lalu loop perintah (SORT, SUMMARY, dll.) |
| `run_worker_cli()` | Discovery berulang + prompt Enter sebelum kembali scan |

### 3.3 `network_utils.py` — Fungsi & Kelas Kunci

| Simbol | Deskripsi |
| ------ | --------- |
| `tune_socket(sock)` | Aktifkan `TCP_NODELAY` + buffer 4 MB |
| `recv_exact(sock, n)` | Terima tepat `n` byte (loop) |
| `is_socket_alive(sock)` | Cek koneksi non-blocking via `select` + `MSG_PEEK` |
| `send_packet(sock, obj, cb)` | Framing + kompresi + kirim streaming (progress throttled) |
| `recv_packet(sock, cb)` | Terima + dekompresi + unpickle (progress throttled) |
| `MasterBeacon` | UDP beacon/responder sisi Master |
| `WorkerScanner` | UDP scanner berkelanjutan sisi Worker |
| `get_all_local_ips` / `get_local_ip` | Resolusi IP lokal (multi-antarmuka) |
| `get_broadcast_ip` / `get_gateway_ip` | Alamat broadcast & gateway subnet |

### 3.4 `cli_ui.py` — Utilitas UI

`Colors`, `banner`, `clear_screen`, `print_header`, `print_success/info/warning/error/task`,
`box_border/line/title`, `print_table_row/footer`, `print_progress_bar`.

`print_progress_bar` memakai `\r` + ANSI `ESC[K` untuk menghapus sisa baris lama
(mencegah tampilan "nyampah" saat suffix berubah panjang).

### 3.5 Dependensi & Library

**Library eksternal (opsional, disarankan)** - didefinisikan di `requirements.txt`:

| Library | Versi | Fungsi |
| ------- | ----- | ------ |
| `numpy` | `>=2.0` | Sorting vektor native, packing `int32`, validasi urutan, dan K-Way merge (melepas GIL) |
| `zstandard` | `>=0.22` | Kompresi payload TCP cepat (zstd level 1) untuk menekan bottleneck Wireless/LAN |

Jika paket di atas tidak terpasang, sistem tetap berjalan dengan **fallback ke *standard library*** (`list.sort`, `zlib`) - hanya lebih lambat.

**Modul *standard library* Python yang dipakai** (tanpa install tambahan):

| Modul | Kegunaan |
| ----- | -------- |
| `socket`, `struct` | Framing TCP & UDP auto-discovery |
| `pickle` | Serialisasi objek paket |
| `zlib` | Kompresi fallback bila `zstandard` tidak tersedia |
| `threading`, `queue`, `concurrent.futures` | Eksekusi simultan, antrean tugas (work-stealing), pembangkitan data terdistribusi |
| `heapq`, `random`, `time`, `os`, `sys` | Merge/utility, RNG & seed, pengukuran waktu, I/O |
| `argparse`, `select`, `msvcrt`, `json` | CLI Worker, input non-blocking, beacon UDP |

Memasang dependensi eksternal:

```bash
pip install -r requirements.txt
```

### 3.6 `gui_app.py` — Antarmuka Grafis (GUI)

GUI berbasis **Tkinter** (standard library, tanpa dependensi tambahan) yang membungkus
seluruh alur CLI. Dijalankan dengan `python gui_app.py`.

| Simbol | Deskripsi |
| ------ | --------- |
| `AnsiConsole(tk.Text)` | Text widget yang memahami kode ANSI (warna/bold), `\r` (overwrite baris, untuk progress) dan `\n`; escape non-warna (mis. `ESC[K`) diabaikan |
| `QueueWriter(io.TextIOBase)` | Stream `sys.stdout`/`sys.stderr` yang meneruskan teks ke antrean thread-safe agar aman dari thread latar |
| `gui_print_progress_bar(...)` | Pengganti `print_progress_bar` di `cli_ui`/`master`/`worker`, mengirim event progress ke progress bar GUI |
| `GuiWorker` | Worker Node ramah-GUI: konek, `REGISTER`, loop perintah (`SORT`, `GENERATE_UNSORTED`, `SORT_ON_FLY`, `SUMMARY`, `PING`, `SHUTDOWN`), bisa dihentikan (tanpa `input()` blocking) |
| `MergeSortGUI` | Kelas utama: 3 tab (Master/Worker/Local Parallel), konsol, progress bar, auto-refresh status |
| `main()` | Entry point: patch `print_progress_bar` lalu `root.mainloop()` |

Cara kerja integrasi tanpa mengubah backend:

1. `master.py`/`network_utils.py`/`fastsort.py` dipakai apa adanya.
2. `sys.stdout`/`sys.stderr` dialihkan ke `QueueWriter`; thread utama GUI (via `after`) men-drain antrean ke `AnsiConsole` dan progress bar.
3. `print_progress_bar` pada modul `cli_ui`, `master`, dan `worker` di-*monkeypatch* ke versi GUI (`gui_print_progress_bar`).
4. Operasi berat (generate, sort serial/distributed/local) berjalan di *worker thread*; tombol dikunci (`_set_controls_enabled`) selama proses agar UI tetap responsif.
5. Worker memakai `GuiWorker` (bukan `worker.run_worker`) agar ringkasan dari Master cukup ditampilkan di konsol tanpa menunggu `[Enter]`.

Tampilan (font, tombol, tab) diatur terpusat di `MergeSortGUI._configure_appearance()`
(font default 12, tema `clam`, padding seragam untuk tab aktif/non-aktif, dsb.).

---

## 4. Protokol Komunikasi TCP

### 4.1 Framing

Setiap paket = **header 8 byte** + **body**.

```text
+----------------+--------------------+----------------------------+
| Header (8 B) | Flag codec | Body (pickle, opsional terkompresi) |
| big-endian u64 | bit63=zlib, bit62=zstd | payload serialized |
+----------------+--------------------+----------------------------+
```

- `HEADER_STRUCT = ">Q"` (unsigned 64-bit big-endian), `HEADER_SIZE = 8`.
- `FLAG_ZLIB = 1 << 63`, `FLAG_ZSTD = 1 << 62`. Panjang asli = `raw_length & ~(FLAG_ZLIB | FLAG_ZSTD)`.
- **Kompresi adaptif**: bila payload > 4096 byte, dicoba `zstandard` level 1 (dipakai bila menghemat ≥ 8%); jika tidak menguntungkan, fallback `zlib` level 1 (≥ 10%); jika semua tidak hemat, payload dikirim mentah. zstd jauh lebih cepat dari zlib sehingga cocok untuk bottleneck Wi-Fi.
- **Representasi data**: integer besar dikirim sebagai `ndarray[int32]` (~4 MB/juta angka) alih-alih `list` pickle (~5 MB/juta) — lebih ringkas dan praktis nol-overhead saat serialisasi.
- Transfer dipecah per 256 KB; callback progres dipanggil maksimum tiap `0.12` detik (selalu di akhir).

### 4.2 Objek yang Dipertukarkan

Semua objek di-`pickle`. Umumnya `dict` dengan kunci `cmd`.

### 4.3 Daftar Perintah

| `cmd` | Arah | Field | Fungsi |
| ----- | ---- | ----- | ------ |
| `REGISTER` | Worker → Master | `name`, `hostname`, `threads` | Registrasi awal Worker |
| `SORT` | Master → Worker | `chunk_id`, `data`, `seed`, `session_seed` | Kirim chunk mentah untuk disortir (dengan seed chunk & seed sesi kluster) |
| `SORT_ON_FLY` | Master → Worker | `chunk_id`, `count`, `seed` | Worker bangkitkan + sortir di RAM (tanpa kirim data) |
| `GENERATE_UNSORTED` | Master → Worker | `chunk_id`, `count`, `seed` | Worker bangkitkan data acak & kirim ke Master |
| `SUMMARY` | Master → Worker | `title`, `lines` | Laporan akhir agar tampil di sisi Worker (termasuk `session_seed`) |
| `PING` | Master → Worker | — | Health-check; Worker balas `PONG` |
| `SHUTDOWN` | Master → Worker | — | Instruksi menutup koneksi |

**Respons Worker** (untuk `SORT`/`SORT_ON_FLY`/`GENERATE_UNSORTED`):

```python
{
"status": "OK",
"chunk_id": <int>,
"worker_name": <str>,
"threads": <int>,
"seed": <int>, # seed chunk (dikembalikan untuk verifikasi, khusus SORT)
"session_seed": <int>, # seed sesi kluster
"sort_time": <float>, # detik (khusus SORT / SORT_ON_FLY)
"gen_time": <float>, # detik (khusus GENERATE_UNSORTED)
"data": <list[int]>
}
```

**Seed sesi (`session_seed`)**: Master membuat satu `session_seed` di awal sesi
distributed sorting, mengirimkannya pada setiap `SORT`, dan menampilkannya di awal & akhir
proses. Worker menampilkan seed yang sama saat tugas mulai dan pada ringkasan akhir, serta
mengembalikannya pada setiap respons. Master memverifikasi kecocokan seed dan menandai bila
ada node yang tidak sinkron. Seed identik di kedua sisi = bukti keduanya berjalan pada satu
sesi paralel yang sama.

---

## 5. Auto-Discovery UDP (Port 5002)

### 5.1 Pesan

- **Probe (Worker → broadcast/gateway/subnet)**: `b"DISCOVER_MERGE_SORT_MASTER"`
- **Announce (Master → Worker)**: JSON

```json
{ "type": "MASTER_ANNOUNCE", "name": "<hostname>", "ip": "<ip>", "port": 5000 }
```

### 5.2 Sisi Master (`MasterBeacon`)

- Menyiarkan `MASTER_ANNOUNCE` tiap **1 detik** ke: `255.255.255.255`, broadcast subnet, **gateway** (mis. `192.168.43.1` hotspot Android), dan `127.0.0.1`.
- Menjawab probe `DISCOVER_...` dengan memilih IP Master **satu subnet** dengan Worker (`_get_matching_ip`).
- `pause()` / `resume()` untuk membebaskan bandwidth saat transfer TCP intensif.

### 5.3 Sisi Worker (`WorkerScanner`)

- **Probe broadcast + gateway** tiap **0.8 detik**.
- **Subnet Unicast Sweep** tiap **2 detik**: kirim probe ke `prefix.1`–`prefix.254` (menembus *AP Isolation*).
- Menyimpan Master aktif dan menghapus entri yang tidak terlihat > **4 detik**.
- Deteksi mesin lokal → gunakan `127.0.0.1` (mendukung beberapa tab pada satu komputer).

---

## 6. Algoritma

### 6.1 Sort Lokal (`parallel_sort_data` / `fastsort.sort_values`)

```text
if NumPy tersedia dan len(arr) >= 20_000:
    return np.sort(np.asarray(arr, dtype=int32))   # native C, melepas GIL
else:
    arr.sort()                                     # fallback Timsort Python
    return arr
```

> Catatan: paralelisme berbasis thread (`ThreadPoolExecutor`) tidak dipakai lagi karena
> terhambat GIL dan tidak menambah kecepatan untuk beban CPU. Paralelisme nyata kini
> bersifat **antar-mesin** (distributed). NumPy melepas GIL dan memakai rutin C vektor.

### 6.2 Distributed Sorting — Pipelined Stream (`run_distributed_sorting`)

**1) Ukuran chunk adaptif**

```text
CHUNKS_PER_NODE = 32
target_chunks = max(total_computers * 32, 1)
chunk_size = max(10_000, ceil(n_total / target_chunks))
chunk_size = min(chunk_size, 500_000) # batas transfer TCP agar buffer Wi-Fi tidak banjir
```

**2) Dynamic Work-Stealing**

Seluruh chunk dimasukkan ke **satu antrean bersama** (`task_queue`, thread-safe).
Setiap node (Master lokal + tiap Worker) mengambil chunk berikutnya begitu ia selesai:

```text
task_queue = Queue()
for idx, chunk in enumerate(raw_chunks): task_queue.put((idx + 1, chunk))
# tiap node loop:  c_id, c_data = task_queue.get_nowait()  -> proses -> ulangi
```

Dengan cara ini node tercepat otomatis mengerjakan lebih banyak, sehingga node/perangkat
lambat **tidak lagi menjadi straggler** yang menahan seluruh kluster (mengatasi load imbalance).

**3) Eksekusi simultan**

- Master: thread lokal mengambil dari `task_queue` dan menyortir dengan NumPy.
- Tiap Worker: satu *dispatcher thread* mengambil dari `task_queue`, mengirim chunk via TCP secara streaming, menunggu hasil, mencatat statistik.
- Keduanya berjalan bersamaan; **work-stealing** memastikan beban mengikuti kecepatan node.

**4) Fallback**: setelah semua thread selesai, sisa chunk (mis. akibat Worker terputus) diproses Master.

**5) K-Way Merge & finalisasi**

```text
hasil_chunks = seluruh chunk terurut dari semua node
final = fastsort.merge_sorted_chunks(hasil_chunks) # concatenate + np.sort (vektor)
validasi: len(final) == n_total dan is_sorted(final)
simpan  : sorted.txt
```

**6) Ringkasan**: dihitung lalu dicetak di Master **dan** dikirim ke Worker via `SUMMARY`.

### 6.3 Kompleksitas

| Tahap | Kompleksitas |
| ----- | ------------ |
| Sort per chunk (Timsort) | $O(m \log m)$, `m` = ukuran chunk |
| Gabung akhir (Timsort atas data hampir/kembali gabungan) | $O(n \log n)$ |
| Distribusi & merge TKO | Bergantung jaringan (didominasi I/O + sort lokal) |

---

## 7. Alur Data End-to-End (Distributed)

```text
Master CLI [2]
   │
   ├─ (opsional) generate_random_data → unsorted.txt
├─ Load data
├─ Bagi chunk + isi satu `task_queue` (dynamic work-stealing)
├─ Beacon.pause()
├─ ThreadPool (tiap node mengambil dari `task_queue`):
│ ├─ Master local thread ──► parallel_sort_data (NumPy) ──► results[]
│ └─ Worker dispatcher ──► TCP SORT (int32+zstd) ──► Worker sort ──► TCP hasil ──► results[]
   ├─ Beacon.resume()
   ├─ K-Way Merge + validasi + simpan sorted.txt
   ├─ Cetak ringkasan  ──► broadcast_summary() ──► Worker menampilkan ringkasan + [Enter]
   └─ print_comparison_metrics (Speedup/Efisiensi)
```

---

## 8. Konstanta & Konfigurasi

| Konstanta | Nilai | Lokasi |
| --------- | ----- | ------ |
| `DEFAULT_MASTER_PORT` | `5000` | `network_utils.py` |
| `DEFAULT_DISCOVERY_PORT` | `5002` | `network_utils.py` |
| `DEFAULT_DATA_SIZE` | `1_000_000` | `master.py` |
| Range angka acak | `1 .. 10_000_000` | `master.py`, `worker.py` |
| Header TCP | `>Q` (8 byte) | `network_utils.py` |
| Ambang kompresi | `> 4096` byte; zstd ≥ 8% / zlib ≥ 10% | `network_utils.py` |
| Level kompresi zstd | `1` (throughput tinggi) | `network_utils.py` |
| Throttle progress | `0.12` detik | `network_utils.py` |
| Ambang progress Worker | `8 MB` | `worker.py` |
| Chunk per node | `32` | `master.py` |
| Batas ukuran chunk | `10_000 .. 500_000` | `master.py` |
| Ambang NumPy | `20_000` elemen | `fastsort.py` |
| Buffer socket | 4 MB (`SO_RCVBUF`/`SO_SNDBUF`) | `network_utils.py` |

Argumen CLI Worker:

```bash
python worker.py --host 192.168.1.10 --port 5000 --name "Laptop-2"
```

- `--host` (opsional): jika kosong → auto-discovery.
- `--port` (default 5000), `--name` (opsional).

---

## 9. Menjalankan & Skenario Pengujian

### 9.1 Menjalankan

```bash
# Semua perangkat
python main.py
# pilih [1] Master di komputer 1, [2] Worker di komputer lain
```

### 9.2 Skenario

1. **Baseline**: jalankan mode Serial pada data $N$ → catat `T_serial`.
2. **Distributed**: jalankan mode Distributed pada data yang sama → catat `T_dist`.
3. Bandingkan Speedup/Efisiensi yang muncul otomatis.
4. Variasikan jumlah Worker dan ukuran $N$ (disarankan ≥ 1.000.000 data).

### 9.3 Uji cepat tanpa jaringan penuh

- Satu komputer, dua tab: jalankan Master lalu Worker (loopback `127.0.0.1`).
- Gunakan $N$ kecil (mis. 200.000) untuk verifikasi fungsional.

---

## 10. Troubleshooting

| Gejala | Penyebab umum | Solusi |
| ------ | ------------- | ------ |
| Worker tidak menemukan Master | Firewall / beda jaringan / AP isolation | Izinkan port 5000/TCP & 5002/UDP; pastikan satu jaringan; ketik IP manual (`[M]`) |
| Master tidak melihat Worker | Port TCP 5000 diblokir | Buat rule firewall TCP inbound 5000 |
| Worker error saat menerima data | Master berhenti / koneksi terputus | Pastikan Master tetap berjalan; Worker kembali ke auto-scan |
| Output terminal "nyampah" | Progress bar lama | Sudah ditangani: throttle + `ESC[K` + gate transfer < 8 MB |
| Distribusi tidak seimbang | Kapasitas thread berbeda | Normal: alokasi proporsional terhadap jumlah thread tiap node |
| `sorted.txt` tidak sesuai harapan | Data/bekas uji | Hapus via menu `[3]` lalu jalankan ulang |

---

## 11. Catatan Desain & Keterbatasan

- **Konsol & UI**: seluruh program menggunakan `print`/ANSI sederhana. Dependensi eksternal bersifat **opsional** (lihat `requirements.txt`): `numpy` & `zstandard`. Bila tidak terpasang, sistem otomatis memakai fallback standard library (lebih lambat).
- **Hosting aman**: `eval` tidak dipakai; deserialisasi memakai `pickle` standar (asumsi jaringan terpercaya/LAN).
- **Kompresi**: adaptif dan hanya dipakai bila menguntungkan. Untuk angka acak murni, zstd masih menghemat ~15%; untuk data terurut bisa ~30%.
- **Akselerasi NumPy**: `np.sort`/`np.all`/`np.concatenate` untuk sort, validasi, dan merge; data int dikirim sebagai `int32` (hemat bandwidth) dan bebas GIL.
- **Mode On-The-Fly** (`SORT_ON_FLY` / `run_distributed_sorting_on_fly`): tersedia di modul, berguna ketika data dibangkitkan secara lokal di tiap node untuk menghindari transfer raw besar.
- **Keadilan vs makespan**: kini dipilih **dynamic work-stealing** (satu antrean bersama) agar node cepat mengerjakan lebih banyak dan node lambat tidak menjadi straggler; ini mengoptimalkan *makespan* (waktu total) ketimbang kontribusi yang identik antar node.

- **Meta UI**: versi CLI memakai `main.py` (menu teks), versi grafis memakai `gui_app.py` (Tkinter). Keduanya berbagi backend yang sama; `gui_app.py` tidak mengubah modul lain selain mem-*patch* `print_progress_bar` saat runtime.

---

## 12. Antarmuka Grafis (GUI) & Build `.exe`

### 12.1 Menjalankan GUI

```bash
python gui_app.py
```

Dua instance GUI dapat dijalankan pada satu komputer (mis. Master di tab/instance 1, Worker di instance 2) karena Worker mendukung loopback `127.0.0.1`.

### 12.2 Build `.exe` (Windows, PyInstaller)

```bash
# sekali saja
pip install pyinstaller

# build ulang
python -m PyInstaller --noconfirm --clean --onefile --windowed --name MergeSortGUI gui_app.py
```

| Flag | Fungsi |
| ---- | ------ |
| `--onefile` | Menghasilkan satu berkas `.exe` |
| `--windowed` | Tidak membuka jendela konsol (khusus GUI) |
| `--name` | Nama berkas output |
| `--clean` | Membersihkan cache build sebelumnya |

Hasil: `dist\MergeSortGUI.exe`. Untuk startup lebih cepat, ganti `--onefile` menjadi `--onedir` (menghasilkan folder `dist\MergeSortGUI\`).

Catatan:

- `numpy` & `zstandard` otomatis dibundel bila terpasang, sehingga `.exe` memakai jalur cepat.
- Berkas data (`unsorted.txt`, `sorted.txt`) dibuat di *working directory* tempat `.exe` dijalankan — letakkan `.exe` di folder proyek agar datanya konsisten.
- Mode `--onefile` menjalankan 2 proses (bootloader + aplikasi); ini normal.
- Artefak `build/`, `dist/`, dan `*.spec` sudah dikecualikan lewat `.gitignore`.

---

Dokumen ini dapat diperbarui seiring perubahan kode.
