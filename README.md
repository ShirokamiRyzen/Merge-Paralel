# Sistem Distributed & Serial Sorting (TCP/IP Socket Programming)

Proyek tugas besar mata kuliah **Komputasi Terdistribusi**.  
Sistem ini mengimplementasikan algoritma **Distributed Merge Sort** menggunakan protokol jaringan TCP/IP murni (`socket` programming standar Python tanpa framework eksternal) dan membandingkan performanya secara langsung dengan **Serial Sorting**.

Dilengkapi fitur **Realtime On-The-Fly Processing** dan **Auto-Discovery Jaringan**: Worker dapat mendeteksi Master secara otomatis tanpa perlu mengetik alamat IP secara manual!

---

## 📌 1. Fitur Utama & Cara Kerja Sistem

### 1. Pipelined Stream Concurrent Sorting (Kedua Komputer Bekerja Bersamaan)
- **Eliminasi Bottleneck Jaringan**: Menghilangkan hambatan di mana Master mengirim separuh data berukuran raksasa sekaligus lalu diam menunggu Slave. Data dipartisi menjadi *streaming chunks* berukuran ringan (~50.000 data per batch).
- **Komputasi Simultan Sejak Detik Pertama**: Master langsung menyortir porsi lokalnya di CPU Master, sementara Worker Slave menyortir porsi di CPU Worker secara bersamaan tanpa saling menunggu.
- **Alokasi Proporsional per Node**: Setiap komputer langsung menerima porsi tetap sesuai kapasitas thread CPU-nya (weighted round-robin), sehingga beban kerja benar-benar terdistribusi merata dan tidak ada node yang mengambil jatah node lain.
- **Linear K-Way Merge**: Master menggabungkan seluruh potongan terurut dari Master dan Worker secara linear dengan algoritma merge terakselerasi.
- **Ringkasan Terkirim ke Semua Node**: Setelah komputasi selesai, Master otomatis mengirim laporan ringkasan (waktu, validasi, total data) ke setiap Worker, lalu Worker menampilkan prompt [Enter] sebelum kembali STANDBY.

### 2. Alur Pengujian Interaktif & Terstruktur
Baik pada mode Serial maupun Distributed:
1. **Pilihan Sumber Data**: Pengguna dapat menggunakan data yang sudah tersimpan di `unsorted.txt` secara langsung atau membangkitkan data baru berukuran $N$.
2. **Pembangkitan Unsorted**: Jika membuat data baru, angka acak dibangkitkan dan disimpan ke `unsorted.txt` disertai progress bar dan pencatatan waktu pembangkitan (`waktu_unsort`).
3. **Prompt Konfirmasi**: Ditampilkan konfirmasi apakah ingin melanjutkan ke proses pengurutan (*Sorting*): `[Y/n]`.
4. **Proses Pengurutan & Simpan**: Data diurutkan secara simultan, divalidasi kebenarannya (*perfect non-decreasing order*), lalu disimpan ke `sorted.txt`.
5. **Ringkasan Waktu di Akhir Proses**: Waktu eksekusi ditampilkan secara rapi di akhir proses:
   - Waktu Unsort (Pembangkitan)
   - Waktu Komputasi Simultan
   - Waktu K-Way Merge
   - Waktu Simpan berkas `sorted.txt`
   - TOTAL WAKTU KESELURUHAN (dari komputasi sampai berkas selesai ditulis)

### 3. Pemisahan Mode Serial dan Distributed
- **Mode Serial (`[1] Jalankan Serial Sorting`)**:
  - **100% Murni Lokal di Komputer Master** (slave node tidak dilibatkan).
  - Pembangkitan data dan pengurutan berjalan murni pada CPU lokal Master sebagai tolok ukur (*baseline*).
- **Mode Distributed (`[2] Jalankan Distributed Sorting (Pipelined Stream)`)**:
  - **Paralel Terdistribusi Penuh**: Master dan seluruh Worker memproses data secara simultan dan kontributif.
  - Menghasilkan tabel metrik evaluasi kecepatan (**Speedup $S$** dan **Efisiensi $E$**).

### 4. Manajemen Berkas Cepat
- Menu **`Hapus File .txt`**: Menghapus `unsorted.txt` dan `sorted.txt` secara instan, tersedia di menu utama `main.py` maupun panel Master `master.py`.

---

## 📂 2. Struktur Berkas

```text
Merge-Paralel/
├── main.py           # Pusat kendali CLI (Pilih Master / Worker / Hapus File)
├── master.py         # Program Master Node (Server TCP + UDP Beacon Discovery)
├── worker.py         # Program Worker Node (Client + Infinity Scan Auto-Discovery)
├── network_utils.py  # Modul framing TCP socket streaming & UDP Discovery
├── cli_ui.py         # Antarmuka ANSI aman terminal, box framing, & progress bar
├── unsorted.txt      # Berkas data angka mentah sebelum diurutkan (otomatis dibuat)
├── sorted.txt        # Berkas data angka hasil akhir terurut (otomatis dibuat)
└── README.md         # Dokumentasi proyek
```

---

## 🚀 3. Panduan Menjalankan Program (Multi-Device di Satu Wi-Fi / Hotspot)

> **Catatan**: Pastikan seluruh komputer/perangkat terhubung ke jaringan Wi-Fi atau Hotspot yang sama.

### Langkah 1: Jalankan Master Node (Komputer 1)
1. Buka terminal dan jalankan:
   ```bash
   python main.py
   ```
2. Pilih opsi **`[1] MASTER NODE (Server)`**.
3. Master Server dan layanan auto-discovery beacon (UDP 5002) langsung aktif.

Tampilan Panel Master:
```text
 +------------------------------------------------------------------------+
 |                     PANEL KONTROL MASTER (SERVER)                      |
 +------------------------------------------------------------------------+
 | Alamat IP Master  : 192.168.1.10:5000                                  |
 | Auto-Discovery    : AKTIF (UDP 5002 - Beacon & Subnet Sweep)           |
 | Worker Terhubung  : 1 node (Worker-1 (DESKTOP-L5EMIAD))                |
 | File unsorted.txt : Belum ada                                          |
 | File sorted.txt   : Belum ada                                          |
 | Pratinjau Data    : [Belum ada data]                                   |
 +------------------------------------------------------------------------+
 |  [1] Jalankan Serial Sorting (Simpan ke sorted.txt)                    |
 |  [2] Jalankan Distributed Sorting (Pipelined Stream)                   |
 |  [3] Hapus File .txt (unsorted.txt & sorted.txt)                       |
 |  [0] Keluar / Matikan Master                                           |
 +------------------------------------------------------------------------+
```

---

### Langkah 2: Jalankan Worker Node (Komputer 2 / Slave Lainnya)
1. Buka terminal di Komputer 2 (atau tab terminal baru) dan jalankan:
   ```bash
   python main.py
   ```
2. Pilih opsi **`[2] WORKER NODE (Client)`**.
3. Worker akan otomatis menjalankan **Infinity Auto-Scan** dan mendeteksi Master di jaringan:
   ```text
   +------------------------------------------------------------------------+
   |                    INFINITY AUTO-SCAN MASTER SERVER                    |
   |        Pencarian Master di Jaringan Wi-Fi/LAN Secara Real-Time         |
   +------------------------------------------------------------------------+
    • Status Pemindai : AKTIF (Continuous Infinity Scan)
    • IP Worker Lokal : 192.168.1.15
    • Port Discovery  : UDP 5002

   Daftar Master Aktif Terdeteksi (1 server ditemukan):
   +------------------------------------------------------------------------+
   |  [1] LAPTOP-MASTER        192.168.1.10:5000      [ONLINE]              |
   +------------------------------------------------------------------------+
     [M] Masukkan IP Master secara manual
     [0] Batal / Kembali ke Menu Utama

   >>> Tekan [Enter] langsung untuk menghubungkan ke Master [1] <<<
   ```
4. Tekan **`[Enter]`** (atau ketik nomor Master). Worker langsung terhubung ke Master!
5. Pada layar Master, notifikasi worker baru akan muncul secara *real-time*.

---

### Langkah 3: Eksekusi Sorting di Komputer Master
Pilih pengujian pada menu Master:
- **`[1] Jalankan Serial Sorting`**: Menguji pemrosesan murni 1 komputer lokal Master.
- **`[2] Jalankan Distributed Sorting (Realtime On-The-Fly)`**: Menguji pemrosesan terdistribusi bersama seluruh worker slave.

Contoh keluaran tabel dan ringkasan waktu akhir:
```text
Rincian Eksekusi Realtime On-The-Fly:
 +--------+------------------------------+------------------+-------------+-------------+
 | Chunk  | Node Komputer                | Jumlah Data      | Sort Time   | Roundtrip   |
 +--------+------------------------------+------------------+-------------+-------------+
 | #1     | Master Node (Lokal)          | 5,000,000 data   | 3.0158s     | 3.0158s     |
 | #2     | Worker-1 (DESKTOP-L5EMIAD)   | 5,000,000 data   | 3.0637s     | 3.9341s     |
 +--------+------------------------------+------------------+-------------+-------------+
  K-Way Merge Master       [##############################] 100.0% Selesai (100%)
[OK] Penggabungan Realtime Selesai!
 • Hasil Terurut       : [1, 2, 2, 3, ... (10,000,000 angka) ..., 9999995, 10000000]
  Validasi Urutan          [##############################] 100.0% Selesai (100%)
[OK] Validasi Urutan     : BERHASIL (Data Terurut Sempurna)
  Simpan sorted.txt        [##############################] 100.0% Selesai (100%)
[OK] Hasil terurut berhasil disimpan ke 'sorted.txt'!

RINGKASAN WAKTU DISTRIBUTED SORTING (ON-THE-FLY):
 +------------------------------------------------------------------------+
 | Waktu Unsort (Pembangkitan) : 1.954210 detik                           |
 | Waktu Komputasi Paralel     : 3.934120 detik                           |
 | Waktu K-Way Merge           : 0.447074 detik                           |
 | Waktu Simpan sorted.txt     : 1.120531 detik                           |
 | TOTAL WAKTU KESELURUHAN     : 5.501725 detik (dari sorting s/d simpan) |
 +------------------------------------------------------------------------+
```

---

## 📱 4. Panduan Hotspot HP & Konfigurasi Jaringan

1. **Jaringan Hotspot Seluler**:
   - Jika laptop dihubungkan ke Hotspot HP, sistem telah dilengkapi **Subnet Sweep** untuk menembus isolasi klien (*AP Isolation*).
   - Di sisi Worker, Anda juga bisa langsung mengetikkan IP Master yang tertera di layar Master jika discovery otomatis dibatasi oleh router/hotspot.

2. **Pengaturan Windows Defender Firewall**:
   - Jika Master berjalan di Windows dan Worker di laptop lain tidak dapat menemukan Master, pastikan port TCP `5000` dan UDP `5002` diizinkan:
   ```powershell
   New-NetFirewallRule -DisplayName "MergeSort TCP" -Direction Inbound -LocalPort 5000 -Protocol TCP -Action Allow
   New-NetFirewallRule -DisplayName "MergeSort UDP" -Direction Inbound -LocalPort 5002 -Protocol UDP -Action Allow
   ```
