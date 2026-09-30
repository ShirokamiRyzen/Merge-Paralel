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


def get_local_ip() -> str:
    """Mendapatkan alamat IP lokal LAN dari mesin."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip


def get_broadcast_ip(local_ip: str) -> str:
    """Mendapatkan alamat subnet directed broadcast (misal 192.168.1.255)."""
    try:
        parts = local_ip.split(".")
        if len(parts) == 4 and parts[0] != "127":
            return f"{parts[0]}.{parts[1]}.{parts[2]}.255"
    except Exception:
        pass
    return "255.255.255.255"


class MasterBeacon:
    """
    Layanan Auto-Discovery di sisi Master:
    - Menjawab probe pencarian worker melalui UDP.
    - Menyiarkan heartbeat announce setiap 1.0 detik ke subnet LAN.
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

    def _run(self):
        try:
            self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            self.udp_sock.bind(("0.0.0.0", self.discovery_port))
            self.udp_sock.settimeout(0.5)
        except Exception:
            return

        payload_dict = {
            "type": "MASTER_ANNOUNCE",
            "name": self.master_name,
            "ip": self.master_ip,
            "port": self.tcp_port
        }
        msg_bytes = json.dumps(payload_dict).encode("utf-8")

        bcast_destinations = [
            ("255.255.255.255", self.discovery_port),
            ("<broadcast>", self.discovery_port),
            (get_broadcast_ip(self.master_ip), self.discovery_port),
            ("127.0.0.1", self.discovery_port),
        ]

        last_bcast = 0.0

        while self.is_running:
            now = time.time()
            # 1. Kirim periodic broadcast hanya setiap 1.0 detik
            if now - last_bcast >= 1.0:
                for dest in bcast_destinations:
                    try:
                        self.udp_sock.sendto(msg_bytes, dest)
                    except Exception:
                        pass
                last_bcast = now

            # 2. Dengarkan permintaan DISCOVER dari worker
            try:
                data, addr = self.udp_sock.recvfrom(2048)
                if b"DISCOVER_MERGE_SORT_MASTER" in data:
                    self.udp_sock.sendto(msg_bytes, addr)
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
    - Terus memindai subnet LAN di background thread.
    - Menjaga daftar Master yang aktif dan otomatis menghapus Master yang offline (> 4 detik).
    """
    def __init__(self, discovery_port: int = DEFAULT_DISCOVERY_PORT):
        self.discovery_port = discovery_port
        self.masters: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock()
        self.is_running = False
        self.thread: Optional[threading.Thread] = None
        self.sock: Optional[socket.socket] = None
        self.local_ip = get_local_ip()

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
            self.sock.settimeout(0.4)
        except Exception:
            return

        query = b"DISCOVER_MERGE_SORT_MASTER"
        probe_destinations = [
            ("255.255.255.255", self.discovery_port),
            ("<broadcast>", self.discovery_port),
            (get_broadcast_ip(self.local_ip), self.discovery_port),
            ("127.0.0.1", self.discovery_port),
        ]
        last_probe = 0.0

        while self.is_running:
            now = time.time()
            # Kirim probe pencarian setiap 0.8 detik
            if now - last_probe >= 0.8:
                for dest in probe_destinations:
                    try:
                        self.sock.sendto(query, dest)
                    except Exception:
                        pass
                last_probe = now

            # Terima respons dari Master
            try:
                data, addr = self.sock.recvfrom(2048)
                msg = json.loads(data.decode("utf-8"))
                if msg.get("type") == "MASTER_ANNOUNCE" and "ip" in msg and "port" in msg:
                    key = f"{msg['ip']}:{msg['port']}"
                    with self.lock:
                        self.masters[key] = {
                            "name": msg.get("name", "Master-Node"),
                            "ip": msg["ip"],
                            "port": msg["port"],
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


