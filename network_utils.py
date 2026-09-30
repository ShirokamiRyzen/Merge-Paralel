"""
network_utils.py
----------------
Modul utilitas komunikasi socket TCP/IP & UDP Auto-Discovery untuk Komputasi Terdistribusi.
1. TCP Framing: Berbasis length-prefix (8-byte big-endian) untuk data besar bebas fragmentasi.
2. UDP Auto-Discovery: Master menyiarkan beacon dan merespons pencarian worker,
   sehingga worker otomatis mendeteksi Master di jaringan Wi-Fi/LAN tanpa input manual IP.
"""

import socket
import struct
import pickle
import json
import time
import threading
from typing import Any, Optional, List, Dict

# Ukuran header TCP: 8 byte integer unsigned (64-bit Big-Endian / >Q)
HEADER_STRUCT = ">Q"
HEADER_SIZE = struct.calcsize(HEADER_STRUCT)

# Port default
DEFAULT_MASTER_PORT = 5000
DEFAULT_DISCOVERY_PORT = 5002


def recv_exact(sock: socket.socket, num_bytes: int) -> Optional[bytes]:
    """Menerima tepat num_bytes dari socket TCP stream."""
    buffer = bytearray()
    while len(buffer) < num_bytes:
        chunk = sock.recv(min(num_bytes - len(buffer), 65536))
        if not chunk:
            return None
        buffer.extend(chunk)
    return bytes(buffer)


def send_packet(sock: socket.socket, payload_obj: Any) -> bool:
    """Mengirim objek Python dengan 8-byte length prefix framing."""
    try:
        serialized_data = pickle.dumps(payload_obj, protocol=pickle.HIGHEST_PROTOCOL)
        header = struct.pack(HEADER_STRUCT, len(serialized_data))
        sock.sendall(header + serialized_data)
        return True
    except (socket.error, BrokenPipeError, ConnectionResetError) as err:
        return False


def recv_packet(sock: socket.socket) -> Optional[Any]:
    """Membaca satu paket utuh [Header 8-byte] + [Payload] dari socket TCP."""
    try:
        header_data = recv_exact(sock, HEADER_SIZE)
        if not header_data:
            return None
        (payload_length,) = struct.unpack(HEADER_STRUCT, header_data)
        payload_data = recv_exact(sock, payload_length)
        if not payload_data:
            return None
        return pickle.loads(payload_data)
    except (socket.error, pickle.PickleError, ConnectionResetError):
        return None


def get_all_local_ips() -> List[str]:
    """Mendapatkan semua alamat IP lokal non-loopback yang aktif pada mesin."""
    ips = set()
    # 1. Probe socket ke gateway/DNS umum tanpa mengirim data internet nyata
    test_destinations = [
        ("8.8.8.8", 80),
        ("1.1.1.1", 80),
        ("192.168.43.1", 80),  # IP default Hotspot Android
        ("172.20.10.1", 80),   # IP default Hotspot iPhone
        ("192.168.1.1", 80),
        ("192.168.0.1", 80)
    ]
    for host, port in test_destinations:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect((host, port))
            ip = s.getsockname()[0]
            if ip and not ip.startswith("127."):
                ips.add(ip)
            s.close()
        except Exception:
            pass

    # 2. Ambil dari getaddrinfo hostname
    try:
        hostname = socket.gethostname()
        for info in socket.getaddrinfo(hostname, None, socket.AF_INET):
            ip = info[4][0]
            if ip and not ip.startswith("127."):
                ips.add(ip)
    except Exception:
        pass

    # 3. Ambil dari gethostbyname_ex
    try:
        _, _, host_ips = socket.gethostbyname_ex(socket.gethostname())
        for ip in host_ips:
            if ip and not ip.startswith("127."):
                ips.add(ip)
    except Exception:
        pass

    result = list(ips)
    return result if result else ["127.0.0.1"]


def get_local_ip() -> str:
    """Mendapatkan alamat IP lokal LAN utama dari mesin."""
    all_ips = get_all_local_ips()
    # Prioritaskan IP Wi-Fi/Hotspot (192.168.x.x, 172.20.x.x, 10.x.x.x)
    for ip in all_ips:
        if ip.startswith("192.168.43."):  # Hotspot Android
            return ip
        if ip.startswith("172.20.10."):  # Hotspot iPhone
            return ip
    for ip in all_ips:
        if ip.startswith("192.168.") or ip.startswith("172.") or ip.startswith("10."):
            return ip
    return all_ips[0] if all_ips else "127.0.0.1"


def get_broadcast_ip(local_ip: str) -> str:
    """Mendapatkan alamat subnet directed broadcast (misal 192.168.1.255)."""
    try:
        parts = local_ip.split(".")
        if len(parts) == 4 and parts[0] != "127":
            return f"{parts[0]}.{parts[1]}.{parts[2]}.255"
    except Exception:
        pass
    return "255.255.255.255"


def get_gateway_ip(local_ip: str) -> str:
    """Mendapatkan perkiraan alamat IP Gateway (biasanya HP Hotspot di .1)."""
    try:
        parts = local_ip.split(".")
        if len(parts) == 4 and parts[0] != "127":
            return f"{parts[0]}.{parts[1]}.{parts[2]}.1"
    except Exception:
        pass
    return "127.0.0.1"


class MasterBeacon:
    """
    Layanan Auto-Discovery di sisi Master:
    - Menjawab probe pencarian worker melalui UDP.
    - Menyiarkan announce berkala ke broadcast, gateway (HP hotspot), dan subnet sweep.
    """
    def __init__(self, master_name: str, master_ip: str, tcp_port: int, discovery_port: int = DEFAULT_DISCOVERY_PORT):
        self.master_name = master_name
        self.master_ip = master_ip
        self.tcp_port = tcp_port
        self.discovery_port = discovery_port
        self.is_running = False
        self.thread: Optional[threading.Thread] = None
        self.udp_sock: Optional[socket.socket] = None

    def start(self):
        """Memulai thread background UDP responder & beacon."""
        self.is_running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _get_matching_ip(self, client_ip: str) -> str:
        """Memilih IP Master yang satu subnet dengan Worker."""
        if client_ip == "127.0.0.1":
            return "127.0.0.1"
        try:
            c_prefix = ".".join(client_ip.split(".")[:3])
            for my_ip in get_all_local_ips():
                if ".".join(my_ip.split(".")[:3]) == c_prefix:
                    return my_ip
        except Exception:
            pass
        return self.master_ip

    def _run(self):
        try:
            self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            self.udp_sock.bind(("0.0.0.0", self.discovery_port))
            self.udp_sock.settimeout(0.4)
        except Exception:
            return

        last_bcast = 0.0

        while self.is_running:
            now = time.time()

            # 1. Kirim periodic beacon announce setiap 1.0 detik
            if now - last_bcast >= 1.0:
                all_ips = get_all_local_ips()
                for cur_ip in all_ips:
                    payload = json.dumps({
                        "type": "MASTER_ANNOUNCE",
                        "name": self.master_name,
                        "ip": cur_ip,
                        "port": self.tcp_port
                    }).encode("utf-8")

                    targets = [
                        ("255.255.255.255", self.discovery_port),
                        ("<broadcast>", self.discovery_port),
                        (get_broadcast_ip(cur_ip), self.discovery_port),
                        (get_gateway_ip(cur_ip), self.discovery_port),  # Gateway Android Hotspot
                        ("127.0.0.1", self.discovery_port),
                    ]
                    for dest in targets:
                        try:
                            self.udp_sock.sendto(payload, dest)
                        except Exception:
                            pass
                last_bcast = now

            # 2. Dengarkan permintaan DISCOVER dari worker
            try:
                data, addr = self.udp_sock.recvfrom(2048)
                if b"DISCOVER_MERGE_SORT_MASTER" in data:
                    chosen_ip = self._get_matching_ip(addr[0])
                    reply_payload = json.dumps({
                        "type": "MASTER_ANNOUNCE",
                        "name": self.master_name,
                        "ip": chosen_ip,
                        "port": self.tcp_port
                    }).encode("utf-8")
                    self.udp_sock.sendto(reply_payload, addr)
            except (socket.timeout, ConnectionResetError, OSError):
                pass
            except Exception:
                if not self.is_running:
                    break

    def stop(self):
        """Menghentikan layanan beacon."""
        self.is_running = False
        if self.udp_sock:
            try:
                self.udp_sock.close()
            except Exception:
                pass


def discover_masters(timeout: float = 1.5, discovery_port: int = DEFAULT_DISCOVERY_PORT) -> List[Dict[str, Any]]:
    """
    Pencarian Master Otomatis (one-shot) oleh Worker Node.
    """
    scanner = WorkerScanner(discovery_port=discovery_port)
    scanner.start()
    time.sleep(timeout)
    masters = scanner.get_masters()
    scanner.stop()
    return masters


class WorkerScanner:
    """
    Pemindai Master Berkelanjutan (Infinity Scanner) untuk Worker Node:
    - Menggunakan UDP Broadcast + Subnet Unicast Sweep + Gateway Direct Ping.
    - Menembus AP Isolation / Hotspot Android / iPhone tethering secara 100%.
    - Menjaga daftar Master yang aktif dan otomatis menghapus Master yang offline (> 4 detik).
    """
    def __init__(self, discovery_port: int = DEFAULT_DISCOVERY_PORT):
        self.discovery_port = discovery_port
        self.masters: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock()
        self.is_running = False
        self.thread: Optional[threading.Thread] = None
        self.sock: Optional[socket.socket] = None

    def start(self):
        """Memulai background thread scanner."""
        self.is_running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            self.sock.bind(("0.0.0.0", 0))  # port acak untuk worker
            self.sock.settimeout(0.3)
        except Exception:
            return

        query = b"DISCOVER_MERGE_SORT_MASTER"
        last_probe = 0.0
        last_sweep = 0.0

        while self.is_running:
            now = time.time()
            all_ips = get_all_local_ips()

            # 1. Kirim probe broadcast & gateway setiap 0.8 detik
            if now - last_probe >= 0.8:
                for cur_ip in all_ips:
                    probe_destinations = [
                        ("255.255.255.255", self.discovery_port),
                        ("<broadcast>", self.discovery_port),
                        (get_broadcast_ip(cur_ip), self.discovery_port),
                        (get_gateway_ip(cur_ip), self.discovery_port),  # Gateway Android Hotspot (192.168.43.1)
                        ("127.0.0.1", self.discovery_port),
                    ]
                    for dest in probe_destinations:
                        try:
                            self.sock.sendto(query, dest)
                        except Exception:
                            pass
                last_probe = now

            # 2. Subnet Unicast Sweep setiap 2.0 detik (Tembus AP Isolation Hotspot HP)
            if now - last_sweep >= 2.0:
                for cur_ip in all_ips:
                    if not cur_ip.startswith("127."):
                        parts = cur_ip.split(".")
                        if len(parts) == 4:
                            prefix = f"{parts[0]}.{parts[1]}.{parts[2]}"
                            # Kirim unicast ke host 1-254 (hanya butuh ~5 milidetik)
                            for i in range(1, 255):
                                try:
                                    self.sock.sendto(query, (f"{prefix}.{i}", self.discovery_port))
                                except Exception:
                                    pass
                last_sweep = now

            # 3. Terima respons dari Master
            try:
                data, addr = self.sock.recvfrom(2048)
                msg = json.loads(data.decode("utf-8"))
                if msg.get("type") == "MASTER_ANNOUNCE" and "ip" in msg and "port" in msg:
                    reported_ip = msg["ip"]
                    m_name = msg.get("name", "Master-Node")
                    port = msg["port"]

                    # Jika dari mesin lokal yang sama (hostname sama atau loopback)
                    if m_name == socket.gethostname() or addr[0] == "127.0.0.1":
                        reported_ip = "127.0.0.1"
                    elif reported_ip == "127.0.0.1" and addr[0] != "127.0.0.1":
                        reported_ip = addr[0]

                    # Deduplikasi master berdasarkan hostname & port
                    key = f"{m_name}:{port}"
                    with self.lock:
                        # Jika sudah ada entri dengan key yang sama, pertahankan 127.0.0.1 jika lokal
                        if key in self.masters and self.masters[key]["ip"] == "127.0.0.1" and reported_ip != "127.0.0.1":
                            self.masters[key]["last_seen"] = now
                        else:
                            self.masters[key] = {
                                "name": m_name,
                                "ip": reported_ip,
                                "port": port,
                                "last_seen": now
                            }
            except (socket.timeout, json.JSONDecodeError, ConnectionResetError, OSError):
                pass
            except Exception:
                if not self.is_running:
                    break

            # Bersihkan master yang sudah tidak merespons > 4 detik
            with self.lock:
                expired = [k for k, v in self.masters.items() if now - v.get("last_seen", 0) > 4.0]
                for k in expired:
                    del self.masters[k]

    def get_masters(self) -> List[Dict[str, Any]]:
        """Mengembalikan salinan daftar Master yang saat ini aktif."""
        with self.lock:
            return list(self.masters.values())

    def stop(self):
        """Menghentikan background scanner."""
        self.is_running = False
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass


