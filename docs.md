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
| `main.py` | Menu utama: pilih peran (Master/Worker), hapus berkas |
| `master.py` | MasterServer, serial & distributed sorting, metrik, CLI Master |
| `worker.py` | Client: discovery, loop perintah, sorting lokal, kirim balik hasil |
| `network_utils.py` | Framing TCP (pickle + zlib), progress callback, UDP discovery |
| `cli_ui.py` | Warna ANSI, box, header, tabel, progress bar (aman Windows/Linux) |

### 3.1 `master.py` — Fungsi & Kelas Kunci

| Simbol | Deskripsi |
| ------ | --------- |
| `parallel_sort_data(arr, n_threads)` | Sort paralel: pecah array menjadi `n_threads` bagian, sort tiap bagian via `ThreadPoolExecutor`, gabung lalu sort final (Timsort) |
| `is_sorted(arr)` | Validasi urutan non-decreasing |
| `format_data_preview(arr, max_items)` | Pratinjau angka untuk UI |
| `save_to_file` / `load_from_file` | Baca/tulis `unsorted.txt` & `sorted.txt` (satu angka per baris) |
| `delete_txt_files` | Hapus kedua berkas data |
| `generate_random_data(n, server)` | Bangkitkan $N$ angka acak (terdistribusi bila ada Worker), simpan ke `unsorted.txt` |
| `MasterServer` | TCP server: `start_server`, `get_live_workers`, `remove_dead_worker`, `shutdown` |
| `run_serial_sorting(data, waktu_unsort)` | Mode serial murni lokal |
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

---

## 4. Protokol Komunikasi TCP

### 4.1 Framing

Setiap paket = **header 8 byte** + **body**.

```text
+----------------+--------------------+----------------------------+
| Header (8 B)   | Flag (bit 63)      | Body (pickle, opsional zlib)|
| big-endian u64 | 1 = terkompresi    | payload serialized          |
+----------------+--------------------+----------------------------+
```

- `HEADER_STRUCT = ">Q"` (unsigned 64-bit big-endian), `HEADER_SIZE = 8`.
- `FLAG_COMPRESSED = 1 << 63`. Panjang asli = `raw_length & ~FLAG_COMPRESSED`.
- **Kompresi**: bila payload > 4096 byte, dicoba `zlib.compress(level=1)`; dipakai hanya jika menghemat ≥ 10% bandwidth.
- Transfer dipecah per 256 KB; callback progres dipanggil maksimum tiap `0.12` detik (selalu di akhir).

### 4.2 Objek yang Dipertukarkan

Semua objek di-`pickle`. Umumnya `dict` dengan kunci `cmd`.

### 4.3 Daftar Perintah

| `cmd` | Arah | Field | Fungsi |
| ----- | ---- | ----- | ------ |
| `REGISTER` | Worker → Master | `name`, `hostname`, `threads` | Registrasi awal Worker |
| `SORT` | Master → Worker | `chunk_id`, `data` | Kirim chunk mentah untuk disortir |
| `SORT_ON_FLY` | Master → Worker | `chunk_id`, `count`, `seed` | Worker bangkitkan + sortir di RAM (tanpa kirim data) |
| `GENERATE_UNSORTED` | Master → Worker | `chunk_id`, `count`, `seed` | Worker bangkitkan data acak & kirim ke Master |
| `SUMMARY` | Master → Worker | `title`, `lines` | Laporan akhir agar tampil di sisi Worker |
| `PING` | Master → Worker | — | Health-check; Worker balas `PONG` |
| `SHUTDOWN` | Master → Worker | — | Instruksi menutup koneksi |

**Respons Worker** (untuk `SORT`/`SORT_ON_FLY`/`GENERATE_UNSORTED`):

```python
{
  "status": "OK",
  "chunk_id": <int>,
  "worker_name": <str>,
  "threads": <int>,
  "sort_time": <float>,   # detik (khusus SORT / SORT_ON_FLY)
  "gen_time": <float>,    # detik (khusus GENERATE_UNSORTED)
  "data": <list[int]>
}
```

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

### 6.1 Sort Paralel Lokal (`parallel_sort_data`)

```text
if n_threads <= 1 or n < 20_000:
    arr.sort()                      # Timsort
    return arr

sub_chunks = bagi arr menjadi n_threads bagian ~sama besar
ThreadPoolExecutor(max_workers=n_threads):
    tiap thread -> sub_chunk.sort()
merged = gabung semua sub_chunk
merged.sort()                       # merge linear final (Timsort)
return merged
```

### 6.2 Distributed Sorting — Pipelined Stream (`run_distributed_sorting`)

**1) Ukuran chunk adaptif**

```text
CHUNKS_PER_NODE = 16
target_chunks   = max(total_computers * 16, 1)
chunk_size      = max(10_000, ceil(n_total / target_chunks))
chunk_size      = min(chunk_size, 1_000_000)   # batas transfer TCP
```

**2) Alokasi proporsional (weighted round-robin)**

Kapasitas tiap node = jumlah thread CPU-nya. Untuk chunk ke-`i`, pilih node dengan
*kekurangan porsi* (deficit) terbesar:

```text
capacities = [threads_master, threads_worker_1, ...]
deficit[i] = ((idx+1) * capacities[i] / sum(capacities)) - assigned[i]
node       = argmax(deficit)
```

Contoh: 20 chunk, kapasitas 12 vs 4 → Master 15 chunk, Worker 5 chunk.

**3) Eksekusi simultan**

- Master: thread lokal mengambil dari `master_queue` dan menyortir dengan seluruh thread CPU-nya.
- Tiap Worker: satu *dispatcher thread* mengirim chunk via TCP, menunggu hasil, mencatat statistik.
- Keduanya berjalan bersamaan; **tidak ada perampasan antrean** (tanpa work-stealing) agar distribusi adil.

**4) Fallback**: setelah semua thread selesai, sisa chunk (mis. akibat Worker terputus) diproses Master.

**5) K-Way Merge & finalisasi**

```text
hasil_chunks = seluruh chunk terurut dari semua node
final = gabung(hasil_chunks); final.sort()   # merge linear
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
   ├─ Bagi chunk + alokasi proporsional (weighted round-robin)
   ├─ Beacon.pause()
   ├─ ThreadPool:
   │     ├─ Master local thread  ──► parallel_sort_data ──► results[]
   │     └─ Worker dispatcher    ──► TCP SORT ──► Worker sort ──► TCP hasil ──► results[]
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
| Ambang kompresi | `> 4096` byte & hemat ≥ 10% | `network_utils.py` |
| Throttle progress | `0.12` detik | `network_utils.py` |
| Ambang progress Worker | `8 MB` | `worker.py` |
| Chunk per node | `16` | `master.py` |
| Batas ukuran chunk | `10_000 .. 1_000_000` | `master.py` |
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

- **Konsol & UI**: seluruh program menggunakan `print`/ANSI sederhana; tidak ada dependensi eksternal.
- **Hosting aman**: `eval` tidak dipakai; deserialisasi memakai `pickle` standar (asumsi jaringan terpercaya/LAN).
- **Kompresi**: hanya dipakai bila menguntungkan; angka acak besar umumnya tidak terkompresi banyak.
- **Mode On-The-Fly** (`SORT_ON_FLY` / `run_distributed_sorting_on_fly`): tersedia di modul, berguna ketika data dibangkitkan secara lokal di tiap node untuk menghindari transfer raw besar.
- **Keadilan vs makespan**: dipilih alokasi proporsional (adil, deterministik) alih-alih work-stealing dinamis agar kontribusi tiap node terlihat merata dan reprodusibel.

---

Dokumen ini dapat diperbarui seiring perubahan kode.
