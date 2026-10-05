# Sistem Distributed & Serial Sorting (TCP/IP Socket Programming)

Proyek tugas besar mata kuliah **Komputasi Terdistribusi**.  
Sistem ini mengimplementasikan algoritma **Distributed Merge Sort** menggunakan protokol jaringan TCP/IP murni (`socket` programming tanpa framework eksternal) dan membandingkan performanya secara langsung dengan **Serial Sorting**.

Dilengkapi fitur **Auto-Discovery Jaringan**: Worker dapat mendeteksi Master secara otomatis tanpa perlu mengetik alamat IP secara manual!

---

## 📌 1. Cara Kerja Sistem

1. **Master Node (Server)**:
   - Mengelola koneksi worker TCP socket (`0.0.0.0:5000`).
   - Menjalankan **UDP Beacon** untuk menyiarkan keberadaan Master ke subnet Wi-Fi/LAN lokal.
   - Otomatis mendeteksi client/worker yang bergabung secara *real-time*.
   - Membangkitkan $N$ data integer acak **positif** (tanpa angka minus) dan menyimpannya ke **`unsorted.txt`**.
   - **Mode Serial**: Mengurutkan data langsung di CPU master dan menyimpan hasil terurut ke **`sorted.txt`**.
   - **Mode Terdistribusi**: Membagi data ke seluruh node komputasi dengan dukungan 2 mode:
     * **Mode Hybrid (Master + Worker)**: Master CPU ikut memproses partisi secara lokal bersama Worker sehingga beban terbagi seimbang (tidak ada node idle) dan speedup optimal tercapai bahkan dengan 1 worker.
     * **Mode Dedicated (Worker Murni)**: Master hanya bertindak sebagai orkestrator/distributor ke worker eksternal.
   - **Optimasi Kecepatan Jaringan & Komputasi**:
     * **Adaptive Zlib Compression (Level 1)**: Mengurangi ukuran transmisi TCP hingga 70% (~1.5 MB untuk 1 juta data) hanya dalam ~20 ms.
     * **TCP Socket Buffer 4MB & TCP_NODELAY**: Menghilangkan window stall dan delay Nagle pada jaringan Wi-Fi/Hotspot.
     * **Zero-Copy Memoryview Framing**: Mencegah overhead alokasi memori berulang pada socket receive.
     * **C-Level Accelerated K-Way Merge**: Penggabungan potongan terurut langsung di level C (Timsort Run Merge) dalam < 0.05 detik.
   - Dilengkapi **Progress Bar** di seluruh proses komputasi.
   - Menghitung metrik performa (**Waktu Eksekusi**, **Speedup $S$**, **Efisiensi $E$**) dan validasi kebenaran urutan data.

2. **Worker Node (Client)**:
   - Memindai jaringan Wi-Fi/LAN lokal secara otomatis (*UDP Broadcast Discovery*).
   - Menampilkan daftar Master yang ditemukan; pengguna cukup memilih nomor Master (atau tekan `[Enter]`).
   - Menerima chunk data via TCP, mengurutkannya di RAM lokal, dan mengembalikan hasil ke Master (dengan progress bar).

---

## 📂 2. Struktur File

```
Merge-Paralel/
├── main.py           # Pusat kendali CLI (Pilih Master / Worker)
├── master.py         # Program Master Node (Server + UDP Beacon)
├── worker.py         # Program Worker Node (Client + Auto-Discovery)
├── network_utils.py  # Modul framing TCP socket & UDP Discovery
├── cli_ui.py         # Utilitas warna ANSI, format terminal, & progress bar
├── unsorted.txt      # Berkas data angka mentah sebelum diurutkan
├── sorted.txt        # Berkas data angka hasil akhir yang sudah terurut
└── README.md         # Dokumentasi proyek
```

---

## 🚀 3. Cara Menjalankan Program (2 Laptop di Satu Wi-Fi)

> **Catatan**: Pastikan kedua laptop terhubung ke jaringan Wi-Fi / Hotspot yang sama.

### Langkah 1: Di Laptop 1 (Master)
1. Buka terminal dan jalankan:
   ```bash
   python main.py
   ```
2. Pilih opsi **`[1] MASTER NODE`**.
3. Master Server dan layanan auto-discovery beacon langsung aktif.

---

### Langkah 2: Di Laptop 2 (Worker)
1. Buka terminal di Laptop 2 dan jalankan:
   ```bash
   python main.py
   ```
2. Pilih opsi **`[2] WORKER NODE`**.
3. Worker akan otomatis menjalankan **Continuous Infinity Scan** di jaringan dan menampilkan daftar Master secara *real-time*:
   ```text
   +-------------------------------------------------------------------+
   |                 INFINITY AUTO-SCAN MASTER SERVER                  |
   |      Pencarian Master di Jaringan Wi-Fi/LAN Secara Real-Time      |
   +-------------------------------------------------------------------+
    • Status Pemindai : AKTIF (Continuous Infinity Scan)
    • IP Worker Lokal : 192.168.1.15
    • Port Discovery  : UDP 5002

   Daftar Master Aktif Terdeteksi (1 server ditemukan):
    +-------------------------------------------------------------------+
    |  [1] LAPTOP-MASTER       192.168.1.4:5000      [ONLINE]           |
    +-------------------------------------------------------------------+
     [M] Masukkan IP Master secara manual
     [0] Batal / Kembali ke Menu Utama

   -------------------------------------------------------------------
   >>> Tekan [Enter] langsung untuk menghubungkan ke Master [1] <<<

     Pilih Master [default: 1]: 
   ```
4. Tekan **`[Enter]`** (atau ketik `1`).
5. Worker langsung terhubung ke Master **tanpa perlu mengetik alamat IP secara manual**!
6. Di layar Laptop 1 (Master), worker akan terdeteksi seketika dengan notifikasi real-time hijau cerah.

---

### Langkah 3: Melakukan Pengujian di Laptop 1 (Master)
Di panel kontrol Master Laptop 1:
- Pilih **`[1]`** untuk membangkitkan data acak (tersimpan ke `unsorted.txt`).
- Pilih **`[2]`** untuk menguji Serial Sorting (tersimpan ke `sorted.txt`).
- Pilih **`[3]`** untuk menguji Distributed Sorting bersama Worker (tersimpan ke `sorted.txt`).
- Pilih **`[4]`** untuk mengganti **Mode Partisi**: `Hybrid (Master + Worker)` *(default, akselerasi maksimal)* atau `Dedicated (Worker Murni)`.
- Tabel metrik evaluasi kecepatan (**Speedup** dan **Efisiensi**) akan langsung ditampilkan.

---

## 📱 4. Panduan Khusus Konektivitas HP (Android / Termux) & Laptop

Jika menggunakan **HP (Hotspot Tethering)** atau **HP sebagai Worker/Master (Termux)**:

1. **Jaringan Hotspot HP**:
   - Jika HP bertindak sebagai Hotspot Tethering, hubungkan Laptop ke Hotspot HP tersebut.
   - Sistem sudah dilengkapi **Subnet Unicast Sweep** sehingga mampu menembus batasan isolasi broadcast (*AP Isolation*) pada Hotspot Android secara otomatis.
   - Di sisi Worker, jika Master sudah terdeteksi di list, cukup tekan **`[Enter]`**.
   - Jika Master belum muncul otomatis (karena restriksi jaringan tertentu), **Anda bisa langsung mengetik alamat IP Master yang tertera di layar Master** (contoh: `192.168.43.1` atau `192.168.43.15`) lalu tekan **`[Enter]`**.

2. **Dukungan Terminal HP (Termux Android)**:
   - Program sudah 100% mendukung Termux Android (menggunakan pembacaan input non-blocking Unix `select`).

3. **Catatan Windows Defender Firewall (Jika Laptop sebagai Master)**:
   - Saat terhubung ke Hotspot HP, Windows sering menganggap jaringan sebagai *Public Network* dan memblokir port masuk.
   - Jika HP gagal terhubung ke Laptop:
     - Izinkan aplikasi Python di Windows Defender Firewall, atau
     - Buka PowerShell (Run as Administrator) dan jalankan:
       ```powershell
       New-NetFirewallRule -DisplayName "MergeSort TCP" -Direction Inbound -LocalPort 5000 -Protocol TCP -Action Allow
       New-NetFirewallRule -DisplayName "MergeSort UDP" -Direction Inbound -LocalPort 5002 -Protocol UDP -Action Allow
       ```

