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
   - **Mode Terdistribusi**: Membagi data ke $K$ worker secara paralel, menerima potongan terurut, lalu menggabungkannya dengan `heapq.merge`, serta menyimpan hasil akhir ke **`sorted.txt`**.
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
3. Worker akan otomatis memindai jaringan dan menampilkan Master yang terdeteksi:
   ```text
   +---------------------------------------------------------------+
   |               PENCARIAN MASTER SERVER OTOMATIS                |
   |           Memindai Master di jaringan Wi-Fi/LAN...            |
   +---------------------------------------------------------------+
     Memindai Jaringan LAN    [##############################] 100.0% Selesai (100%)

   [OK] Ditemukan 1 Master Server aktif di jaringan:

     [1] DESKTOP-L5EMIAD (192.168.1.4:5000)
     [M] Masukkan IP Master secara manual

   Pilih Master untuk dihubungkan [1, default: 1]: 
   ```
4. Tekan **`[Enter]`** (atau ketik `1`).
5. Worker langsung terhubung ke Master **tanpa mengetik alamat IP secara manual**!
6. Di layar Laptop 1 (Master), worker akan terdeteksi otomatis dan jumlah worker aktif langsung bertambah.

---

### Langkah 3: Melakukan Pengujian di Laptop 1 (Master)
Di panel kontrol Master Laptop 1:
- Pilih **`[1]`** untuk membangkitkan data acak (tersimpan ke `unsorted.txt`).
- Pilih **`[2]`** untuk menguji Serial Sorting (tersimpan ke `sorted.txt`).
- Pilih **`[3]`** untuk menguji Distributed Sorting bersama Worker (tersimpan ke `sorted.txt`).
- Tabel metrik evaluasi kecepatan (**Speedup** dan **Efisiensi**) akan langsung ditampilkan.
