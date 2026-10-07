"""
fastsort.py
-----------
Akselerasi numerik opsional berbasis NumPy & Zstandard (dengan fallback murni
standard library bila paket tidak tersedia).

Kenapa modul ini ada:
- Sorting Python (`list.sort`) hanya memakai 1 core dan lambat untuk data besar.
  `ThreadPoolExecutor` pada sort Python TIDAK menambah kecepatan karena terikat
  GIL. NumPy melepas GIL dan memakai rutin C vektor, sehingga jauh lebih cepat.
- Mengirim `list[int]` via pickle boros bandwidth (5 MB per 1 juta angka) dan
  lambat diserialisasi. Representasi `ndarray[int32]` hanya 4 MB dan praktis
  nol-overhead saat pickle, sangat membantu pada jaringan Wireless.
- Validasi urutan (`is_sorted`) dan penggabungan (K-Way Merge) dilakukan secara
  vektor, bukan loop Python, sehingga ratusan juta elemen tetap wajar.
"""

import os
import time
from typing import Any, List, Optional, Sequence

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:  # pragma: no cover - fallback
    np = None
    HAS_NUMPY = False

# Batas nilai yang aman disimpan sebagai int32 (cukup untuk 1..10.000.000).
INT32_MIN = -2_147_483_648
INT32_MAX = 2_147_483_647

# Ambang minimal jumlah elemen sebelum NumPy dipakai (menghindari overhead konversi).
NUMPY_THRESHOLD = 20_000


def to_int32(values: Sequence[int]) -> Any:
    """
    Mengubah list/iterable int menjadi `ndarray[int32]` bila menguntungkan
    (hemat ~20% bandwidth vs pickle list dan bebas kompresi yang boros).
    Jika NumPy tidak ada atau data kecil, nilai dikembalikan apa adanya.

    Konversi dilakukan bertahap (per-chunk) dengan jeda GIL singkat agar event
    loop GUI tidak membeku ("Not Responding") pada data berjumlah ratusan juta.
    """
    if not HAS_NUMPY:
        return values
    if isinstance(values, np.ndarray):
        return values if values.dtype == np.int32 else values.astype(np.int32, copy=False)
    try:
        n = len(values)
    except TypeError:
        return values
    if n < NUMPY_THRESHOLD:
        return values
    try:
        arr = np.empty(n, dtype=np.int32)
    except Exception:
        return np.asarray(values, dtype=np.int32)
    step = 1_000_000
    for i in range(0, n, step):
        arr[i:i + step] = values[i:i + step]
        time.sleep(0.001)
    return arr


def to_list(values: Any) -> List[int]:
    """Mengubah ndarray kembali menjadi list Python (untuk kode yang butuh list)."""
    if HAS_NUMPY and isinstance(values, np.ndarray):
        return values.tolist()
    return list(values)


def sort_values(values: Any, n_threads: Optional[int] = None) -> Any:
    """
    Mengurutkan nilai secara menaik.
    - NumPy: `np.sort` (native C, melepas GIL) untuk data >= NUMPY_THRESHOLD.
    - Fallback: `list.sort` (Timsort) untuk data kecil / tanpa NumPy.

    Catatan: parameter `n_threads` dipertahankan demi kompatibilitas tanda tangan,
    tetapi paralelisme kini bersifat antar-mesin (distributed). Percobaan paralel
    lokal berbasis thread tidak efektif untuk sort Python karena GIL.
    """
    if HAS_NUMPY:
        try:
            if len(values) >= NUMPY_THRESHOLD:
                return np.sort(to_int32(values))
        except TypeError:
            pass
    if isinstance(values, list):
        values.sort()
        return values
    return sorted(values)


def is_sorted_values(values: Any) -> bool:
    """
    Validasi urutan non-decreasing secara vektor (tanpa loop Python per elemen).

    Untuk data besar yang masih berupa `list`, validasi dilakukan per-chunk dengan
    jeda GIL singkat agar event loop GUI tidak membeku, sekaligus menghindari
    konversi seluruh list menjadi satu array raksasa.
    """
    try:
        n = len(values)
    except TypeError:
        return True
    if n < 2:
        return True
    if HAS_NUMPY and n >= NUMPY_THRESHOLD:
        try:
            if isinstance(values, np.ndarray):
                arr = values if values.dtype == np.int32 else values.astype(np.int32, copy=False)
                return bool(np.all(arr[:-1] <= arr[1:]))
            step = 500_000
            prev = None
            for i in range(0, n, step):
                chunk = np.asarray(values[i:i + step], dtype=np.int32)
                if chunk.size >= 2 and bool(np.any(chunk[:-1] > chunk[1:])):
                    return False
                if prev is not None and int(chunk[0]) < prev:
                    return False
                prev = int(chunk[-1])
                time.sleep(0.001)
            return True
        except TypeError:
            pass
    return all(values[i] <= values[i + 1] for i in range(n - 1))


def merge_sorted_chunks(chunks: List[Any]) -> Any:
    """
    Menggabungkan sejumlah potongan yang SUDAH terurut menjadi satu kesatuan terurut.
    - NumPy: `concatenate` + `sort` vektor (jauh lebih cepat dari `.sort()` list Python).
    - Fallback: extend + sort list.
    """
    chunks = [c for c in chunks if c is not None and len(c) > 0]
    if not chunks:
        return []
    if len(chunks) == 1:
        return chunks[0]
    if HAS_NUMPY:
        arrays = [to_int32(c) for c in chunks]
        merged = np.concatenate(arrays)
        merged.sort()
        return merged
    merged: List[int] = []
    for c in chunks:
        merged.extend(c)
    merged.sort()
    return merged
