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
- **Linear K-Way Merge**: Master menggabungkan seluruh potongan terurut secara linear di akhir proses.
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
4. **Pengurutan + Validasi** (*perfect non-decreasing order*) + simpan ke `sorted.txt`.
5. **Ringkasan Waktu** di akhir proses (ditampilkan juga di sisi Worker).

### 1.4 Pemisahan Mode Serial dan Distributed

- **Mode Serial (`[1]`)** — 100% lokal di Master, tanpa Worker. Menjadi *baseline*.
- **Mode Distributed (`[2]`)** — pipelined stream terdistribusi penuh, menghasilkan metrik **Speedup $S$** dan **Efisiensi $E$**.

### 1.5 Manajemen Berkas

- Menu **Hapus File .txt** untuk menghapus `unsorted.txt` & `sorted.txt`, tersedia di `main.py` dan panel Master.

---

## 📂 2. Struktur Berkas

```text
Merge-Paralel/
├── main.py # Pusat kendali CLI (Master / Worker / Hapus File)
├── master.py # Master Node (TCP Server + UDP Beacon Discovery)
├── worker.py # Worker Node (TCP Client + Infinity Scan Discovery)
├── network_utils.py # Framing TCP (pickle+zstd), progress, UDP Discovery
├── fastsort.py # Akselerasi NumPy: sort vektor, packing int32, validasi & merge
├── cli_ui.py # Antarmuka ANSI (box, warna, progress bar)
├── requirements.txt # Dependensi opsional (numpy, zstandard)
├── unsorted.txt # Data mentah sebelum diurutkan (otomatis dibuat)
├── sorted.txt # Hasil akhir terurut (otomatis dibuat)
├── README.md # Dokumentasi singkat (berkas ini)
└── docs.md # Dokumentasi teknis lengkap
```

> 📖 Dokumentasi teknis mendalam (arsitektur, protokol, algoritma, troubleshooting) ada di **[docs.md](docs.md)**.

---

## 🚀 3. Panduan Menjalankan (Multi-Device, Satu Wi-Fi / Hotspot)

> Pastikan semua perangkat terhubung ke jaringan yang sama.

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
| File sorted.txt  : Belum ada                                           |
| Pratinjau Data   : [1, 3, 7, ... (10,000,000 angka) ..., 9999998]      |
+------------------------------------------------------------------------+
| [1] Jalankan Serial Sorting (Simpan ke sorted.txt)                     |
| [2] Jalankan Distributed Sorting (Pipelined Stream) |
| [3] Hapus File .txt (unsorted.txt & sorted.txt)                       |
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
 | Node Komputer                | Thread   | Chunk        | Total Data       | Waktu Sort   | Status       |
 +------------------------------+----------+--------------+------------------+--------------+--------------+
 | Master Node (Lokal CPU)      | 12 Th    | 15 chunk     | 150,000,000 data | 17.1770s     | Selesai     |
 | Worker-1 (DESKTOP-L5EMIAD)   | 12 Th    | 15 chunk     | 150,000,000 data | 17.2061s     | Selesai     |
 +------------------------------+----------+--------------+------------------+--------------+--------------+

RINGKASAN WAKTU DISTRIBUTED SORTING:
 +------------------------------------------------------------------------+
 | Total Data Terurut        : 300,000,000 data                           |
 | Total Komputer            : 2 node                |
 | Status Validasi           : BERHASIL (Data Terurut Sempurna)           |
 | Waktu Komputasi : 17.2061 detik                              |
 | Waktu K-Way Merge         : 3.4210 detik                               |
 | Waktu Simpan sorted.txt   : 4.1205 detik                               |
 | TOTAL WAKTU KESELURUHAN   : 24.7476 detik   |
 +------------------------------------------------------------------------+
```

---

## 📱 4. Hotspot HP & Konfigurasi Jaringan

1. **Hotspot Seluler** — sistem memakai **Subnet Sweep** & gateway ping untuk menembus *AP Isolation*. Jika masih gagal, ketik IP Master secara manual (opsi `[M]`) di Worker.
2. **Windows Defender Firewall** — izinkan port TCP `5000` dan UDP `5002`:

```powershell
New-NetFirewallRule -DisplayName "MergeSort TCP" -Direction Inbound -LocalPort 5000 -Protocol TCP -Action Allow
New-NetFirewallRule -DisplayName "MergeSort UDP" -Direction Inbound -LocalPort 5002 -Protocol UDP -Action Allow
```

---

## 🧮 5. Metrik Evaluasi

| Metrik | Rumus |
| ------ | ----- |
| Speedup | $S = T_{serial} / T_{distributed}$ |
| Efisiensi | $E = S / K \times 100\%$ , dengan $K$ = jumlah komputer |

---

## 🛠️ 6. Kebutuhan Sistem

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
