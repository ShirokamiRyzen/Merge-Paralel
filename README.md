# Sistem Distributed & Serial Sorting (TCP/IP Socket Programming)

Proyek tugas besar mata kuliah **Komputasi Terdistribusi**.
Sistem ini mengimplementasikan algoritma **Distributed Merge Sort** menggunakan protokol jaringan **TCP/IP murni** (`socket` programming standar Python tanpa framework eksternal) dan membandingkan performanya secara langsung dengan **Serial Sorting**.

Dilengkapi **Auto-Discovery Jaringan (UDP)**: Worker dapat mendeteksi Master secara otomatis di Wi-Fi/LAN/hotspot tanpa mengetik alamat IP secara manual.

---

## 📌 1. Fitur Utama

### 1.1 Pipelined Stream Concurrent Sorting

- **Streaming Chunk Adaptif**: Data dipartisi menjadi potongan (*chunk*) berukuran dinamis. Jumlah chunk ditargetkan sekitar `32 × jumlah node` dengan batas maksimum `500.000` data per chunk, sehingga transfer tetap ringan dan granularitas *work-stealing* lebih halus.
- **Komputasi Bersamaan Sejak Detik Pertama**: Master langsung menyortir porsi lokalnya, sementara setiap Worker menyortir porsinya di CPU masing-masing tanpa saling menunggu.
- **Dynamic Work-Stealing (Anti-Straggler)**: Semua node mengambil tugas dari **satu antrean bersama**. Node tercepat otomatis mengerjakan lebih banyak, sedangkan node/perangkat lambat tidak lagi menahan seluruh kluster (mengatasi *load imbalance* pada perangkat heterogen).
- **Akselerasi NumPy**: Sorting menggunakan rutin vektor native NumPy (bebas GIL) alih-alih `ThreadPoolExecutor` yang terhambat GIL. Validasi urutan dan K-Way Merge juga dihitung secara vektor.
- **Transfer Ringkas via Int32 + Zstandard**: Angka dikirim sebagai `ndarray[int32]` (~4 MB/juta vs ~5 MB pickle list) dan dikompresi dengan `zstandard` level 1 yang jauh lebih gesit dari zlib — menekan **bottleneck Wireless/LAN**.
- **K-Way Merge Paralel (Range-Partitioned)**: Master menggabungkan seluruh potongan terurut. Pada mode Local Parallel, penggabungan dilakukan **paralel** dengan membagi rentang nilai (pivot/kuantil + `np.searchsorted`) sehingga ikut bertambah cepat saat thread ditambah (lihat [§5 Algoritma](#-5-algoritma-yang-digunakan)).
- **Ringkasan Terkirim ke Semua Node**: Setelah komputasi selesai, Master otomatis mengirim laporan ringkasan (total data, status validasi, rincian waktu) ke setiap Worker, lalu Worker menampilkan prompt **[Enter]** sebelum kembali STANDBY.
- **Seed Sesi Kluster**: Master membuat satu `session_seed` di awal, mengirimkannya ke semua Worker, dan menampilkannya di **awal & akhir** proses (plus tabel `seed` tiap chunk). Worker menampilkan `session_seed` yang sama saat menerima tugas dan pada ringkasan akhir — jika seed identik, kedua node berada pada sesi yang sama. Master juga memverifikasi balik seed tiap chunk dan menandai bila ada yang tidak sinkron.
- **Progress Bar Bersih**: Callback progres jaringan di-*throttle* (~8×/detik) dan transfer kecil (< 8 MB) tidak menampilkan bar, sehingga konsol tidak "nyampah".

### 1.2 Auto-Discovery Jaringan (UDP)

- **Infinity Scan** di sisi Worker: broadcast + subnet unicast sweep (1–254) + gateway hotspot + loopback, sehingga menembus *AP Isolation* pada hotspot HP (Android/iPhone) sekaligus mendukung **multi-tab lokal** (loopback).
- **Beacon Master** menjawab probe dan menyiarkan ANNOUNCE secara berkala; daftar Master yang tidak merespons > 4 detik otomatis dihapus.
- Beacon otomatis di-*pause* selama transfer TCP intensif agar bandwidth diutamakan untuk data.

### 1.3 Alur Pengujian Interaktif

1. **Pilih Sumber Data**: gunakan `unsorted.txt` yang ada, atau bangkitkan data baru berukuran $N$.
2. **Pembangkitan Unsorted** (opsional, terdistribusi bila ada Worker) disertai progress bar dan pencatatan `waktu_unsort`.
3. **Konfirmasi** lanjut ke pengurutan: `[Y/n]`.
4. **Pengurutan + Validasi** (*perfect non-decreasing order*) + simpan ke `sorted.csv`.
5. **Ringkasan Waktu** di akhir proses (ditampilkan juga di sisi Worker).

### 1.4 Pemisahan Mode Serial, Local Parallel, dan Distributed

- **Mode Serial (`[1]`)** — 100% lokal di Master, tanpa Worker. Menjadi *baseline*.
- **Mode Local Parallel (`[3]`)** — hanya 1 komputer, **tanpa perangkat lain & tanpa Wi-Fi/LAN**. Data dibagi **1 chunk per thread**, tiap chunk disortir `np.sort` (melepas GIL) secara serentak di core berbeda, lalu digabung **K-Way Merge paralel** (`fastsort.parallel_merge`). Cocok dijalankan *offline*.
- **Mode Distributed (`[2]`)** — pipelined stream terdistribusi penuh, menghasilkan metrik **Speedup $S$** dan **Efisiensi $E$**.

### 1.5 Manajemen Berkas

- Menu **Hapus File Data** untuk menghapus `unsorted.txt` & `sorted.csv`, tersedia di `main.py` dan panel Master.

---

## 📂 2. Struktur Berkas

```text
Merge-Paralel/
├── main.py # Pusat kendali CLI (Master / Worker / Local Parallel / Hapus File)
├── gui_app.py # GUI Tkinter (Master / Worker / Local Parallel) - versi grafis
├── master.py # Master Node (TCP Server + UDP Beacon Discovery)
├── worker.py # Worker Node (TCP Client + Infinity Scan Discovery)
├── network_utils.py # Framing TCP (pickle+zstd), progress, UDP Discovery
├── fastsort.py # Akselerasi NumPy: sort vektor, packing int32, validasi & merge
├── cli_ui.py # Antarmuka ANSI (box, warna, progress bar)
├── requirements.txt # Dependensi opsional (numpy, zstandard)
├── build.bat # Skrip build .exe sekali-klik (Windows)
├── unsorted.txt # Data mentah sebelum diurutkan (otomatis dibuat)
├── sorted.csv # Hasil akhir terurut — format CSV satu kolom (otomatis dibuat)
├── README.md # Dokumentasi singkat (berkas ini)
└── docs.md # Dokumentasi teknis lengkap
```

> **Format keluaran CSV**: hasil pengurutan disimpan ke **`sorted.csv`** — satu angka per baris
> (CSV satu kolom), sehingga dapat langsung dibuka di Excel/Google Sheets. Berkas masukan
> mentah (`unsorted.txt`) tetap berformat teks satu angka per baris.

> 📖 Dokumentasi teknis mendalam (arsitektur, protokol, algoritma, troubleshooting) ada di **[docs.md](docs.md)**.

---

## 🚀 3. Panduan Menjalankan (Multi-Device, Satu Wi-Fi / Hotspot)

> Pastikan semua perangkat terhubung ke jaringan yang sama.

### Versi GUI (Tkinter)

Tersedia versi grafis tanpa perlu ketik menu di terminal:

```bash
python gui_app.py
```

Jendela GUI memiliki 3 tab:
- **Master Node** — mulai/stop server, lihat daftar worker terhubung, Generate Data, Serial Sorting, Distributed Sorting, Hapus File.
- **Worker Node** — Auto-Scan (UDP) / hubungkan manual ke IP Master, lalu terima & urutkan chunk.
- **Local Parallel** — 1 komputer, semua thread CPU, tanpa jaringan.

Dilengkapi konsol berwarna (mendukung kode ANSI) dan progress bar real-time. Versi CLI (`main.py`) tetap tersedia dan berfungsi sama.

### Versi .EXE (Windows)

Sudah tersedia hasil build siap pakai: **`dist/MergeSortGUI.exe`** — cukup klik dua kali, tanpa perlu instal Python. Letakkan `.exe` di folder proyek bila ingin memakai berkas `unsorted.txt`/`sorted.csv` yang sama (berkas data dibuat di folder tempat `.exe` dijalankan).

**Build ulang `.exe` (setelah mengubah kode):**

Cara termudah — cukup **klik dua kali `build.bat`** (Windows). Skrip otomatis:
mencari Python, memasang PyInstaller & dependensi (bila perlu), lalu membangun `dist\MergeSortGUI.exe`.

Atau manual lewat terminal:

```bash
# 1. Pasang PyInstaller (sekali saja)
pip install pyinstaller

# 2. Bangun satu berkas .exe (GUI tanpa jendela konsol)
python -m PyInstaller --noconfirm --clean --onefile --windowed --name MergeSortGUI gui_app.py
```

Hasil: **`dist\MergeSortGUI.exe`** (±24 MB, onefile). Untuk startup lebih cepat dan folder distribusi, ganti `--onefile` dengan `--onedir`.

Prasyarat & catatan:

- Python 3.8+ (diuji 3.14) dan `tkinter` (sudah bawaan Python Windows).
- `numpy` & `zstandard` ikut dibundel bila terpasang, sehingga `.exe` memakai jalur cepat.
- Mode onefile menjalankan 2 proses (bootloader + aplikasi) — normal.
- `.spec`, `build/`, dan `dist/` sudah masuk `.gitignore`.

### Versi CLI

### Langkah 1 — Master Node (Komputer 1)

```bash
python main.py
```

Pilih **[1] MASTER NODE (Server)**. Panel Master tampil:

```text
+------------------------------------------------------------------------+
| PANEL KONTROL MASTER (SERVER)                                          |
+------------------------------------------------------------------------+
| Alamat IP Master : 192.168.1.10:5000                                   |
| Auto-Discovery   : AKTIF (UDP 5002 - Beacon & Subnet Sweep)           |
| Worker Terhubung : 1 node (Worker-1 (DESKTOP-L5EMIAD))                 |
| File unsorted.txt: ADA (10,000,000 data)                              |
| File sorted.csv  : Belum ada                                           |
| Pratinjau Data   : [1, 3, 7, ... (10,000,000 angka) ..., 9999998]      |
+------------------------------------------------------------------------+
| [1] Jalankan Serial Sorting (Simpan ke sorted.csv)                     |
| [2] Jalankan Distributed Sorting (Pipelined Stream) |
| [3] Hapus File Data (unsorted.txt & sorted.csv)                       |
| [0] Keluar / Matikan Master                                            |
+------------------------------------------------------------------------+
```

### Langkah 2 — Worker Node (Komputer 2 / tab lain)

```bash
python main.py
```

Pilih **[2] WORKER NODE (Client)**. Worker menjalankan *Infinity Auto-Scan*:

```text
+------------------------------------------------------------------------+
| INFINITY AUTO-SCAN MASTER SERVER                                       |
| Pencarian Master di Jaringan Wi-Fi/LAN Secara Real-Time                |
+------------------------------------------------------------------------+
 • Status Pemindai : AKTIF (Continuous Infinity Scan)
 • IP Worker Lokal : 192.168.1.15
 • Port Discovery  : UDP 5002

Daftar Master Aktif Terdeteksi (1 server ditemukan):
+------------------------------------------------------------------------+
| [1] LAPTOP-MASTER   192.168.1.10:5000   [ONLINE]                       |
+------------------------------------------------------------------------+
 [M] Masukkan IP Master secara manual
 [0] Batal / Kembali ke Menu Utama

>>> Tekan [Enter] langsung untuk menghubungkan ke Master [1] <<<
```

Tekan **[Enter]** untuk terhubung. Setelah selesai, Worker menampilkan ringkasan dari Master lalu menunggu **[Enter]** untuk kembali STANDBY.

### Langkah 3 — Jalankan Sorting di Master

- **[1] Serial Sorting** — menguji murni 1 komputer (baseline).
- **[2] Distributed Sorting (Pipelined Stream)** — paralel bersama semua Worker.

Contoh keluaran mode Distributed:

```text
Rincian Kontribusi Tiap Node:
+------------------------------+----------+--------------+------------------+--------------+--------------+
| Node Komputer | Thread | Chunk | Total Data | Waktu Sort | Status |
+------------------------------+----------+--------------+------------------+--------------+--------------+
| Master Node (Lokal CPU) | 12 Th | 15 chunk | 150,000,000 data | 17.1770s | Selesai |
| Worker-1 (DESKTOP-L5EMIAD) | 12 Th | 15 chunk | 150,000,000 data | 17.2061s | Selesai |
+------------------------------+----------+--------------+------------------+--------------+--------------+

RINGKASAN WAKTU DISTRIBUTED SORTING:
+------------------------------------------------------------------------+
| Total Data Terurut : 300,000,000 data |
| Total Komputer : 2 node |
| Status Validasi : BERHASIL (Data Terurut Sempurna) |
| Waktu Komputasi : 17.2061 detik |
| Waktu K-Way Merge : 3.4210 detik |
+------------------------------------------------------------------------+

METRIK EVALUASI: WAKTU KOMPUTASI SERIAL VS PARALEL
+--------------------------------------+--------------------------+
| Parameter Evaluasi | Nilai / Hasil |
+--------------------------------------+--------------------------+
| Waktu Komputasi Serial (T_serial) | 45.8123 detik |
| Waktu Komputasi Paralel (T_parallel) | 20.6271 detik |
| Komputer Terlibat | 2 node |
| Speedup (S = T_serial / T_parallel) | 2.22x |
| Efisiensi (E = S / K) | 111.05% |
+--------------------------------------+--------------------------+
```

> Semua ringkasan waktu kini memakai **waktu komputasi (sorting + merge)** saja —
> pembangkitan data dan penulisan berkas (`sorted.csv`) **tidak** dihitung, agar
> perbandingan serial vs paralel adil dan skalabilitas paralel terlihat.

---

## 📱 4. Hotspot HP & Konfigurasi Jaringan

1. **Hotspot Seluler** — sistem memakai **Subnet Sweep** & gateway ping untuk menembus *AP Isolation*. Jika masih gagal, ketik IP Master secara manual (opsi `[M]`) di Worker.
2. **Windows Defender Firewall** — izinkan port TCP `5000` dan UDP `5002`:

```powershell
New-NetFirewallRule -DisplayName "MergeSort TCP" -Direction Inbound -LocalPort 5000 -Protocol TCP -Action Allow
New-NetFirewallRule -DisplayName "MergeSort UDP" -Direction Inbound -LocalPort 5002 -Protocol UDP -Action Allow
```

---

## 🧠 5. Algoritma yang Digunakan

Daftar **seluruh algoritma** yang dipakai aplikasi, lokasi implementasinya, dan fungsi masing-masing.

### 5.1 Pengurutan (Sorting)

| # | Algoritma | Lokasi | Fungsi |
|---|-----------|--------|--------|
| 1 | **Distributed Merge Sort** | `master.run_distributed_sorting` | Algoritma utama *divide-and-conquer* terdistribusi: data dibagi jadi banyak chunk → tiap chunk disortir paralel di Master & Worker → digabung (K-Way Merge) di Master. |
| 2 | **Local Parallel Merge Sort** | `master.run_local_parallel_sorting` | Versi 1-komputer dari Merge Sort: 1 chunk per thread, tiap chunk disortir serentak di core berbeda, lalu digabung paralel. Tanpa jaringan. |
| 3 | **Vectorized Comparison Sort (NumPy Introsort)** | `fastsort.sort_values` | Mengurutkan tiap chunk memakai `np.sort` (rutin C NumPy, varian *introsort*: quicksort→heapsort→insertion). Ini **melepas GIL**, sehingga banyak thread benar-benar berjalan serentak. Kompleksitas $O(m \log m)$ per chunk. |
| 4 | **Timsort (Python `list.sort`)** | `fastsort.sort_values`, `run_serial_sorting` | Sort bawaan Python (hibrida merge+insertion, adaptif terhadap *run* terurut). Dipakai sebagai **baseline Serial** dan **fallback** bila NumPy tidak tersedia atau data < 20.000 elemen. |
| 5 | **K-Way Merge (Concat + Sort vektor)** | `fastsort.merge_sorted_chunks` | Menggabungkan semua chunk terurut menjadi satu kesatuan: `np.concatenate` lalu `np.sort` vektor (atau `extend` + `sort` pada fallback). Kompleksitas $O(n \log n)$. |
| 6 | **Parallel Range-Partitioned K-Way Merge** | `fastsort.parallel_merge` | Menggabungkan chunk terurut **secara paralel**: sampel kuantil dijadikan pivot → `np.searchsorted` membagi nilai tiap chunk menjadi `p` rentang → tiap rentang digabung di thread terpisah. Membuat fase merge ikut *scaling* dengan jumlah thread. |
| 7 | **Heap Merge (`heapq.merge`)** | `run_serial_sorting` | Penggabungan serial berbasis *min-heap* (lazy, hemat memori) untuk beberapa blok terurut pada mode Serial. Kompleksitas $O(n \log k)$ bila semua sumber tersedia. |

### 5.2 Paralelisme & Distribusi

| # | Algoritma | Lokasi | Fungsi |
|---|-----------|--------|--------|
| 8 | **Dynamic Work-Stealing** | `run_distributed_sorting` (shared `queue.Queue`) | Semua node mengambil tugas dari **satu antrean bersama** (pull-based). Node tercepat otomatis mengerjakan lebih banyak → node lambat tidak menjadi *straggler*, sehingga *makespan* (waktu total) minimal pada kluster heterogen. |
| 9 | **Pipelined Stream Concurrent** | `run_distributed_sorting` | Master dan seluruh Worker mulai menyortir **sejak detik pertama** tanpa saling menunggu; transfer data di-*stream* per chunk agar jaringan & CPU tumpang-tindih (*overlap*). |
| 10 | **Adaptive Chunking / Partitioning** | `run_distributed_sorting`, `run_local_parallel_sorting` | Menentukan ukuran chunk secara dinamis dari jumlah node/thread (mis. `32 × node` untuk distributed, `1 × thread` untuk lokal), lalu dibatasi (10.000–500.000) agar transfer ringan dan beban merata. Fungsi: menyeimbangkan *granularitas* vs *overhead*. |
| 11 | **Pembangkitan Data Terdistribusi (On-The-Fly)** | `master.generate_random_data`, `worker` (`GENERATE_UNSORTED`) | Membangkitkan data acak secara paralel oleh Master + seluruh Worker (masing-masing punya seed unik), lalu Master merapikan urutannya — mempercepat tahap `unsorted`. |
| 12 | **Deterministic Chunk Seed (Multiplicative Hashing)** | `run_distributed_sorting._chunk_seed` | Menurunkan seed unik & deterministik tiap chunk dari `session_seed`: `(session_seed × 1.000.003 + chunk_id × 2.654.435.761) mod 1e9` (konstanta Knuth/multiplicative hash). Fungsi: reproduksibilitas + bukti sesi paralel yang sama antar-node. |

### 5.3 Serialisasi & Jaringan

| # | Algoritma | Lokasi | Fungsi |
|---|-----------|--------|--------|
| 13 | **Length-Prefixed TCP Framing** | `network_utils.send_packet` / `recv_packet` | Membungkus tiap paket dengan header **8-byte big-endian (`>Q`)** berisi panjang + flag codec, sehingga penerima bisa membaca objek besar utuh tanpa masalah fragmentasi TCP. |
| 14 | **Adaptive Compression (zstd → zlib → raw)** | `network_utils` | Memilih codec terbaik otomatis: coba `zstandard` level 1 (dipakai bila hemat ≥ 8%), lalu `zlib` level 1 (≥ 10%), jika tidak menguntungkan kirim mentah. Fungsi: menekan *bottleneck* Wi-Fi/LAN. |
| 15 | **int32 Vector Packing** | `fastsort.to_int32` / `to_list` | Mengubah `list[int]` menjadi `ndarray[int32]` (~4 MB/juta vs ~5 MB list pickle). Fungsi: hemat bandwidth & serialisasi panjang-mendekati-nol. |
| 16 | **UDP Auto-Discovery (Broadcast + Subnet Sweep + Gateway + Loopback + Expiry)** | `MasterBeacon`, `WorkerScanner` | Worker menemukan Master otomatis tanpa ketik IP: probe broadcast + `prefix.1–254` (*menembus AP Isolation* hotspot) + gateway + `127.0.0.1`; Master membalas & menyiarkan *announce* berkala. Master yang tidak aktif > 4 detik dihapus. |
| 17 | **Non-Blocking Socket Liveness (`select` + `MSG_PEEK`)** | `network_utils.is_socket_alive` | Mendeteksi koneksi TCP terputus **tanpa latensi PING** (peek byte tanpa mengonsumsi), agar Master bisa menandai/membersihkan Worker mati. |

### 5.4 Validasi & Utilitas

| # | Algoritma | Lokasi | Fungsi |
|---|-----------|--------|--------|
| 18 | **Vectorized Sortedness Validation** | `fastsort.is_sorted_values` | Membuktikan hasil benar-benar tak-menurun secara vektor: `np.all(a[:-1] <= a[1:])` (per-chunk + jeda GIL untuk data raksasa). Kompleksitas $O(n)$. |
| 19 | **Mersenne Twister PRNG** | `random`, `random.Random(seed)` | Membangkitkan angka acak (dan mengulang deret yang sama dari sebuah seed) untuk data uji maupun pembangkitan on-the-fly yang deterministik. |
| 20 | **Progressive Chunked I/O (GIL-friendly)** | `save_to_file`, `load_from_file`, `to_int32`, `is_sorted_values` | Memproses I/O & konversi per-blok (mis. 500.000) diselingi `time.sleep(0.001)` singkat. Fungsi: menjaga event loop GUI tetap responsif pada ratusan juta data. |
| 21 | **Producer–Consumer stdout Queue (GUI)** | `gui_app.QueueWriter` / `AnsiConsole` | Mengarahkan `print` dari thread latar ke antrean thread-safe, lalu di-*drain* ke TextView oleh thread utama Tkinter (`after`). Fungsi: output aman & UI tidak *freeze*. |

> **Catatan skalabilitas (Amdahl's Law):** fase pengurutan benar-benar paralel dan `np.sort` sudah sangat cepat
> (~beberapa milidetik untuk 1 juta data), sehingga penambahan thread baru terasa jelas pada data besar
> (mis. ≥ 5–10 juta). Karena itu **metrik perbandingan memakai waktu komputasi (sorting + merge)**,
> tanpa I/O & pembangkitan data yang bersifat serial dan sama di semua mode.

---

## 🧮 6. Metrik Evaluasi

Metrik memakai **waktu komputasi** (sorting + merge) — pembangkitan data dan penulisan `sorted.csv` **tidak** dihitung, agar perbandingan serial vs paralel adil.

| Metrik | Rumus |
| ------ | ----- |
| Speedup | $S = T_{serial} / T_{parallel}$ |
| Efisiensi | $E = S / K \times 100\%$ , dengan $K$ = jumlah komputasi paralel (node untuk Distributed, thread untuk Local Parallel) |

---

## 📈 7. Hasil Pengukuran & Analisis Skalabilitas

Pengukuran pada **AMD Ryzen 5 7535HS (6 core fisik / 12 thread SMT)**, mode Local Parallel, $N = 10.000.000$ data. Waktu yang dicatat adalah **waktu komputasi (sort + merge)**, tanpa I/O.

### 7.1 Contoh hasil

```text
Serial              : 6.294077 detik   (Timsort Python, 1 CPU)
Paralel (4 thread)  : 0.059675 detik
Paralel (12 thread) : 0.055005 detik
```

Perhatikan: Paralel **114× lebih cepat dari Serial** (NumPy vs Timsort Python), tetapi **4 vs 12 thread hanya beda ~8%**.

### 7.2 Skalabilitas `np.sort` murni (N = 10 juta)

| Thread | Waktu | Speedup vs 1 thread |
| :----: | :---: | :-----------------: |
| 1 | 0.0693 s | 1.00× |
| 2 | 0.0396 s | 1.81× |
| 4 | 0.0256 s | **2.80×** |
| 6 | 0.0256 s | 2.80× |
| 8 | 0.0224 s | 3.20× |
| 12 | 0.0199 s | 3.60× |

Dari **4 → 12 thread hanya naik 1.28×**.

### 7.3 Rincian fase komputasi (12 thread, N = 10 juta)

| Fase | Waktu | Scaling |
| ---- | :---: | :-----: |
| Sort chunk (paralel, `np.sort`) | ~0.020 s | baik |
| K-Way Merge (paralel, range-partitioned) | ~0.040 s | terbatas (~1.5×) |

Fase **merge mendominasi** total dan bagian orkestrasinya sebagian tertahan GIL.

### 7.4 Mengapa beda 4 vs 12 thread sangat kecil?

1. **Memory-bandwidth bound** — menyortir `int32` adalah operasi intensif memori. Sekitar **4 core sudah hampir menjenuhkan bandwidth RAM** (~2.8× dari plafon ~3.6×), sehingga menambah core berikutnya nyaris tidak menambah throughput.
2. **SMT / Hyper-Threading** — CPU ini hanya punya **6 core fisik** (12 thread logis). Thread ke-7…12 berbagi core fisik yang sama dan hampir tidak menambah kinerja untuk beban memori.
3. **Fase merge dominan & tidak scaling penuh** — K-Way Merge lebih lama daripada sort (menyortir ulang seluruh data), dan slicing/`concatenate` di dalamnya sebagian tertahan GIL → hanya ~1.5× dari 4→12 thread.
4. **Hukum Amdahl** — $S_{\max} = \dfrac{1}{(1-P) + P/N}$. Karena sebagian pipeline serial (overhead chunk, merge, konversi) makin dominan saat N thread besar, penambahan thread menurun manfaatnya.

### 7.5 Rekomendasi

- Pada mesin 6-core, ***sweet spot* = 4–6 thread**; menaikkan ke 12 hampir tidak berguna.
- Untuk melihat perbedaan besar antar jumlah thread, pakai **CPU dengan lebih banyak core fisik**, atau **N lebih besar** (mis. 50–100 juta) agar fase paralel lebih dominan — namun **bandwidth RAM** tetap menjadi plafonnya.
- Perbandingan **Serial vs Paralel tetap sangat signifikan** (puluhan kali) karena NumPy melepas GIL dan memakai rutin C vektor.

---

## 🛠️ 8. Kebutuhan Sistem

- **Python 3.8+** (diuji pada 3.14).
- **Dependensi opsional** (disarankan dipasang untuk performa terbaik):

```bash
pip install -r requirements.txt
```

- `numpy` — sorting vektor native, packing `int32`, validasi & merge cepat.
- `zstandard` — kompresi payload TCP cepat untuk menekan bottleneck Wireless.
- Tanpa kedua paket di atas, sistem tetap berjalan dengan *standard library* (fallback otomatis, hanya lebih lambat).
- Jaringan Wi-Fi/LAN yang sama antar perangkat (atau beberapa tab pada satu komputer).

---

## 📄 Lisensi

Proyek akademik untuk keperluan pembelajaran **Komputasi Terdistribusi**.
