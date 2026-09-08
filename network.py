from __future__ import annotations
#librerie per creare il canale di comunicazione
import json
import socket
import threading
from typing import Callable, Dict, Tuple

from protocollo import per_il_wire, dal_wire

#creazione canale tcp
class Trasporto_Network:

    def __init__(self, node_id: str, host: str, port: int, peer_addresses: Dict[str, Tuple[str, int]],
                 broadcast_targets: list[str] | None = None):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peer_addresses = dict(peer_addresses)
        self.broadcast_targets = list(broadcast_targets) if broadcast_targets is not None \
            else list(self.peer_addresses.keys())
        self._server_socket: socket.socket | None = None
        self._on_message: Callable | None = None
        self._stop = False
        self._accept_thread: threading.Thread | None = None

    def start(self, on_message_callback: Callable) -> None:
        self._on_message = on_message_callback
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(32)
        self._stop = False
        self._accept_thread = threading.Thread(target=self._accetta_loop, daemon=True)
        self._accept_thread.start()

    def _accetta_loop(self) -> None:
        self._server_socket.settimeout(0.5)
        while not self._stop:
            try:
                conn, _ = self._server_socket.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._mantieni_connessione, args=(conn,), daemon=True).start()

    def _mantieni_connessione(self, conn: socket.socket) -> None:
        with conn:
            conn.settimeout(2.0)
            buf = b""
            try:
                while b"\n" not in buf:
                    chunk = conn.recv(65536)
                    if not chunk:
                        break
                    buf += chunk
            except socket.timeout:
                pass
            if not buf:
                return
            line = buf.split(b"\n", 1)[0]
            try:
                dati = json.loads(line.decode("utf-8"))
                mess = dal_wire(dati)
            except Exception:
                return 
            if self._on_message:
                self._on_message(mess)

    def _invia(self, host: str, port: int, messaggio) -> None:
        payload = (json.dumps(per_il_wire(messaggio)) + "\n").encode("utf-8")
        try:
            with socket.create_connection((host, port), timeout=2.0) as s:
                s.sendall(payload)
        except OSError:
            pass  #peer irraggiungibile e il pbft deve tollerare i messaggi persi

    def invia_a(self, entity_id: str, messaggio) -> None:
        addr = self.peer_addresses.get(entity_id)
        if addr is None:
            return
        host, port = addr
        threading.Thread(target=self._invia, args=(host, port, messaggio), daemon=True).start()

    def broadcast(self, messaggio) -> None:
        for entity_id in self.broadcast_targets:
            self.invia_a(entity_id, messaggio)

    def stop(self) -> None:
        self._stop = True
        if self._server_socket:
            try:
                self._server_socket.close()
            except OSError:
                pass