from __future__ import annotations

import itertools
import json
import threading
import time
from typing import Dict, Optional

from protocollo import Operation, Request, Reply, firma_messaggio, verifica_messaggio

class Client_kv:

    def __init__(self, client_id: str, k: int, node_ids: list[str],
                 transport, private_key: bytes, key_registry,
                 timeout: float = 3.0, retries: int = 4):
        self.client_id = client_id
        self.k = k
        self.node_ids = list(node_ids)
        self.transport = transport
        self.private_key = private_key
        self.key_registry = key_registry
        self.timeout = timeout
        self.retries = retries

        self._lock = threading.Lock()
        self._replies: Dict[int, Dict[str, Reply]] = {}
        self._timestamp_counter = itertools.count(1)
        self._known_primary_index = 0

    def start(self) -> None:
        self.transport.start(self.in_risposta)

    def stop(self) -> None:
        self.transport.stop()

    #operazioni pubbliche
    def put(self, key: str, value) -> Optional[dict]:
        return self._invia_richiesta(Operation("PUT", key, value))

    def get(self, key: str) -> Optional[dict]:
        return self._invia_richiesta(Operation("GET", key))

    def delete(self, key: str) -> Optional[dict]:
        return self._invia_richiesta(Operation("DELETE", key))

    #invio e attesa quorum
    def _invia_richiesta(self, operation: Operation) -> Optional[dict]:
        timestamp = next(self._timestamp_counter)
        request = Request(operation=operation, timestamp=timestamp, client_id=self.client_id)
        request.signature = firma_messaggio(request, self.private_key)

        with self._lock:
            self._replies[timestamp] = {}

        for attempt in range(self.retries):
            if attempt == 0:
                #primo tentativo solo al primario ipotizzato
                primary = self.node_ids[self._known_primary_index % len(self.node_ids)]
                self.transport.invia_a(primary, request)
            else:
                # tentativi successivi -> broadcast (primario forse cambiato o il primo messaggio è andato perso)
                self.transport.broadcast(request)

            result = self._attendi_quorum(timestamp, self.timeout)
            if result is not None:
                return result

        return None  #nessun quorum raggiunto entro i tentativi previsti

    def _attendi_quorum(self, timestamp: int, timeout: float) -> Optional[dict]:
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                bucket = self._replies.get(timestamp, {})
                conteggi: Dict[str, int] = {}
                for reply in bucket.values():
                    key = json.dumps(reply.result, sort_keys=True)
                    conteggi[key] = conteggi.get(key, 0) + 1
                for key, count in conteggi.items():
                    if count >= self.k + 1:
                        return json.loads(key)
            time.sleep(0.05)
        return None

    def in_risposta(self, msg: Reply) -> None:
        try:
            sender_pk = self.key_registry.get_chiave_pubblica(msg.sender_id)
        except KeyError:
            return
        if not verifica_messaggio(msg, sender_pk):
            return  #reply non firmata correttamente -> scartata

        with self._lock:
            bucket = self._replies.setdefault(msg.timestamp, {})
            bucket[msg.sender_id] = msg
            try:
                self._known_primary_index = self.node_ids.index(msg.sender_id)
            except ValueError:
                pass
