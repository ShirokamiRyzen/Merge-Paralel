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


class MasterBeacon:
    """
    Layanan Auto-Discovery di sisi Master:
    - Menjawab probe pencarian worker melalui UDP.
    - Menyiarkan heartbeat announce setiap 1.5 detik ke subnet LAN.
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
            self.udp_sock.settimeout(0.8)
        except Exception:
            return

        payload_dict = {
            "type": "MASTER_ANNOUNCE",
            "name": self.master_name,
            "ip": self.master_ip,
            "port": self.tcp_port
        }
        msg_bytes = json.dumps(payload_dict).encode("utf-8")

        while self.is_running:
            # 1. Kirim periodic broadcast
            try:
                self.udp_sock.sendto(msg_bytes, ("<broadcast>", self.discovery_port))
                self.udp_sock.sendto(msg_bytes, ("255.255.255.255", self.discovery_port))
            except Exception:
                pass

            # 2. Dengarkan permintaan DISCOVER dari worker
            try:
                data, addr = self.udp_sock.recvfrom(2048)
                if b"DISCOVER_MERGE_SORT_MASTER" in data:
                    self.udp_sock.sendto(msg_bytes, addr)
            except socket.timeout:
                pass
            except Exception:
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
    Pencarian Master Otomatis oleh Worker Node:
    Mengirimkan broadcast probe dan mengumpulkan respons dari semua Master aktif di LAN.
    Returns:
        List berisi dict: [{"name": str, "ip": str, "port": int}, ...]
    """
    masters_dict: Dict[str, Dict[str, Any]] = {}
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.settimeout(0.3)

    query = b"DISCOVER_MERGE_SORT_MASTER"
    start_time = time.time()

    # Kirim probe pencarian
    try:
        sock.sendto(query, ("<broadcast>", discovery_port))
        sock.sendto(query, ("255.255.255.255", discovery_port))
        sock.sendto(query, ("127.0.0.1", discovery_port))
    except Exception:
        pass

    while (time.time() - start_time) < timeout:
        try:
            data, addr = sock.recvfrom(2048)
            msg = json.loads(data.decode("utf-8"))
            if msg.get("type") == "MASTER_ANNOUNCE" and "ip" in msg and "port" in msg:
                key = f"{msg['ip']}:{msg['port']}"
                if key not in masters_dict:
                    masters_dict[key] = {
                        "name": msg.get("name", "Master-Node"),
                        "ip": msg["ip"],
                        "port": msg["port"]
                    }
        except (socket.timeout, json.JSONDecodeError):
            pass
        except Exception:
            break

    sock.close()
    return list(masters_dict.values())
