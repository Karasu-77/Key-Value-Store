from __future__ import annotations
#librerie varie
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from protocollo import (
    Operation, Request, PrePrepare, Prepare, Commit, Reply,
    ViewChange, NewView, digest_operazione, firma_messaggio, verifica_messaggio )


#lo store applicativo, eseguito solo dopo il commit pbft
class Store_chiave:

    def __init__(self):
        self._data: Dict[str, Any] = {}

    def apply(self, operation: Operation) -> dict:
        if operation.tipo_operazione == "PUT":
            self._data[operation.chiave] = operation.valore
            return {"status": "OK"}
        if operation.tipo_operazione == "GET":
            if operation.chiave in self._data:
                return {"status": "OK", "value": self._data[operation.chiave]}
            return {"status": "NOT_FOUND"}
        if operation.tipo_operazione == "DELETE":
            esisteva = operation.chiave in self._data
            self._data.pop(operation.chiave, None)
            return {"status": "OK" if esisteva else "NOT_FOUND"}
        raise ValueError(f"operazione sconosciuta: {operation.tipo_operazione}")


#stato del consenso per una singola operazione, indicizzato per timestamp
@dataclass
class Voce_log:
    view: int
    timestamp: int
    request: Optional[Request]
    pre_prepare: Optional[PrePrepare] = None
    prepares_received: Dict[str, Prepare] = field(default_factory=dict)
    commits_received: Dict[str, Commit] = field(default_factory=dict)
    prepared: bool = False
    committed: bool = False


class Nodo:

    def __init__(self, node_id: str, all_node_ids: list[str], k: int,
                 key_registry, private_key: bytes, request_timeout: float = 4.0):
        self.node_id = node_id
        self.all_node_ids = list(all_node_ids)
        self.n = len(self.all_node_ids)
        self.k = k
        self.view = 0
        self.store = Store_chiave()
        self.log: Dict[int, Voce_log] = {}
        self.key_registry = key_registry
        self.private_key = private_key
        self.transport = None  #assegnato con trasporto_bind()

        self.lock = threading.RLock()
        self.vc = Gestore_cambio_vista(self)

        self._request_timeout = request_timeout
        self._last_progress = time.time()
        self._stop = False
        self._watchdog_thread: Optional[threading.Thread] = None
        #richieste viste dal client ma non ancora arrivate a pre-prepare:
        #servono per rilevare un primario fermo e per farle riproporre
        #dal nuovo primario dopo un cambio vista
        self._pending_client_requests: Dict[int, tuple[Request, float]] = {}

    #ciclo di vita
    def trasporto_bind(self, transport) -> None:
        self.transport = transport

    def start(self) -> None:
        self._stop = False
        self._watchdog_thread = threading.Thread(target=self._watchdog_loop, daemon=True)
        self._watchdog_thread.start()

    def stop(self) -> None:
        self._stop = True

    def _watchdog_loop(self) -> None:
        while not self._stop:
            time.sleep(0.3)
            now = time.time()
            with self.lock:
                log_pending = any(not e.committed for e in self.log.values())
                log_overdue = log_pending and (now - self._last_progress > self._request_timeout)
                client_overdue = any(
                    (ts not in self.log or self.log[ts].pre_prepare is None)
                    and (now - since > self._request_timeout)
                    for ts, (_, since) in self._pending_client_requests.items()
                )
            if log_overdue or client_overdue:
                self.al_timeout()

    #ruolo nella view attuale
    def primario_attuale(self) -> str:
        #il primario e' noto deterministicamente: round-robin su view % N
        return self.all_node_ids[self.view % self.n]

    def primario(self) -> bool:
        return self.node_id == self.primario_attuale()

    #dispatch messaggi in ricezione
    def gestione_messaggio(self, messaggio) -> None:
        handlers = {
            Request: self.in_richiesta,
            PrePrepare: self.in_pre_prepare,
            Prepare: self.in_prepare,
            Commit: self.in_commit,
            ViewChange: self.vc.in_cambio_vista,
            NewView: self.vc.in_nuova_vista,
        }
        handler = handlers.get(type(messaggio))
        if handler:
            handler(messaggio)

    #fase 1: request -> pre-prepare
    def in_richiesta(self, request: Request) -> None:
        try:
            client_pk = self.key_registry.get_chiave_pubblica(request.client_id)
        except KeyError:
            return
        if not verifica_messaggio(request, client_pk):
            return

        with self.lock:
            self._pending_client_requests.setdefault(request.timestamp, (request, time.time()))

        if self.primario():
            self._proposta_primario(request)
        #se non è primario tiene solo traccia in _pending_client_requests
        #se non arriva un pre-prepare entro il timeout, il primario risulta guasto

    def _proposta_primario(self, request: Request) -> None:
        #crea e diffonde un pre-prepare per una richiesta
        with self.lock:
            if request.timestamp in self.log and self.log[request.timestamp].pre_prepare:
                return
            digest = digest_operazione(request.operation)
            pp = PrePrepare(view=self.view, timestamp=request.timestamp,
                             operation_digest=digest, request=request,
                             sender_id=self.node_id)
            pp.signature = firma_messaggio(pp, self.private_key)
            self.log[request.timestamp] = Voce_log(view=self.view, timestamp=request.timestamp,
                                                     request=request, pre_prepare=pp)
            self._last_progress = time.time()

        self.transport.broadcast(pp)
        self.in_pre_prepare(pp)  #il primario partecipa anche alle fasi successive

    #fase 2: pre-prepare -> prepare
    def in_pre_prepare(self, msg: PrePrepare) -> None:
        if msg.view != self.view:
            return
        primario = self.primario_attuale()
        if msg.sender_id != primario:
            return
        try:
            primary_pk = self.key_registry.get_chiave_pubblica(primario)
            client_pk = self.key_registry.get_chiave_pubblica(msg.request.client_id)
        except KeyError:
            return
        if not verifica_messaggio(msg, primary_pk):
            return
        if not verifica_messaggio(msg.request, client_pk):
            return
        if digest_operazione(msg.request.operation) != msg.operation_digest:
            return

        with self.lock:
            existing = self.log.get(msg.timestamp)
            if existing and existing.pre_prepare and \
                    existing.pre_prepare.operation_digest != msg.operation_digest:
                return  #già accettata un'altra operazione con lo stesso timestamp
            entry = self.log.setdefault(
                msg.timestamp, Voce_log(view=msg.view, timestamp=msg.timestamp, request=msg.request))
            entry.request = entry.request or msg.request
            entry.pre_prepare = msg
            self._last_progress = time.time()
            self._pending_client_requests.pop(msg.timestamp, None)

            prepare = Prepare(view=msg.view, timestamp=msg.timestamp,
                               operation_digest=msg.operation_digest, sender_id=self.node_id)
            prepare.signature = firma_messaggio(prepare, self.private_key)

        self.transport.broadcast(prepare)
        self.in_prepare(prepare)

    #fase 3: prepare -> commit
    def in_prepare(self, msg: Prepare) -> None:
        if msg.view != self.view:
            return
        try:
            sender_pk = self.key_registry.get_chiave_pubblica(msg.sender_id)
        except KeyError:
            return
        if not verifica_messaggio(msg, sender_pk):
            return

        commit_da_inviare = None
        with self.lock:
            entry = self.log.setdefault(
                msg.timestamp, Voce_log(view=msg.view, timestamp=msg.timestamp, request=None))
            if entry.pre_prepare and entry.pre_prepare.operation_digest != msg.operation_digest:
                return
            entry.prepares_received[msg.sender_id] = msg
            self._last_progress = time.time()

            coerenti = [p for p in entry.prepares_received.values()
                        if p.operation_digest == msg.operation_digest]
            #soglia: 2k prepare coerenti, incluso il proprio
            if not entry.prepared and len(coerenti) >= 2 * self.k:
                entry.prepared = True
                commit_da_inviare = Commit(view=self.view, timestamp=msg.timestamp,
                                            operation_digest=msg.operation_digest, sender_id=self.node_id)
                commit_da_inviare.signature = firma_messaggio(commit_da_inviare, self.private_key)

        if commit_da_inviare:
            self.transport.broadcast(commit_da_inviare)
            self.in_commit(commit_da_inviare)

    #fase 4: commit -> esecuzione + reply
    def in_commit(self, msg: Commit) -> None:
        if msg.view != self.view:
            return
        try:
            sender_pk = self.key_registry.get_chiave_pubblica(msg.sender_id)
        except KeyError:
            return
        if not verifica_messaggio(msg, sender_pk):
            return

        entry_da_eseguire = None
        with self.lock:
            entry = self.log.get(msg.timestamp)
            if entry is None:
                return
            if entry.pre_prepare and entry.pre_prepare.operation_digest != msg.operation_digest:
                return
            entry.commits_received[msg.sender_id] = msg
            self._last_progress = time.time()

            coerenti = [c for c in entry.commits_received.values()
                        if c.operation_digest == msg.operation_digest]
            altri = len(coerenti) - (1 if self.node_id in entry.commits_received else 0)
            # soglia: 2k commit coerenti, esclusi i propri
            if entry.prepared and not entry.committed and entry.request and altri >= 2 * self.k:
                entry.committed = True
                entry_da_eseguire = entry

        if entry_da_eseguire:
            self._esegui_e_rispondi(entry_da_eseguire)

    def _esegui_e_rispondi(self, entry: Voce_log) -> None:
        result = self.store.apply(entry.request.operation)
        reply = Reply(view=self.view, timestamp=entry.timestamp,
                       client_id=entry.request.client_id, result=result, sender_id=self.node_id)
        reply.signature = firma_messaggio(reply, self.private_key)
        self.transport.invia_a(entry.request.client_id, reply)

    #rilevamento guasti del primario
    def al_timeout(self) -> None:
        self.vc.avvia_cambio_vista()

#cambio vista: elezione di un nuovo primario quando quello attuale è down
class Gestore_cambio_vista:

    def __init__(self, node: Nodo):
        self.node = node
        self._pending: Dict[int, Dict[str, ViewChange]] = {}
        self._started_views: set[int] = set()

    def avvia_cambio_vista(self) -> None:
        node = self.node
        with node.lock:
            new_view = node.view + 1
            if new_view in self._started_views:
                return
            self._started_views.add(new_view)
            prepared_prepares = []
            for entry in node.log.values():
                if entry.prepared and not entry.committed:
                    prepared_prepares.extend(entry.prepares_received.values())
            vc = ViewChange(new_view=new_view, prepared_set=prepared_prepares, sender_id=node.node_id)
            vc.signature = firma_messaggio(vc, node.private_key)

        node.transport.broadcast(vc)
        self.in_cambio_vista(vc)

    def in_cambio_vista(self, msg: ViewChange) -> None:
        node = self.node
        try:
            sender_pk = node.key_registry.get_chiave_pubblica(msg.sender_id)
        except KeyError:
            return
        if not verifica_messaggio(msg, sender_pk):
            return

        nuova_view_msg = None
        with node.lock:
            bucket = self._pending.setdefault(msg.new_view, {})
            bucket[msg.sender_id] = msg

            primario_candidato = node.all_node_ids[msg.new_view % node.n]
            if node.node_id != primario_candidato:
                return
            if len(bucket) < 2 * node.k + 1:
                return 

            pre_prepares = []
            visti = set()
            for vc in bucket.values():
                for p in vc.prepared_set:
                    if p.timestamp in visti:
                        continue
                    entry = node.log.get(p.timestamp)
                    if entry and entry.pre_prepare:
                        pre_prepares.append(entry.pre_prepare)
                        visti.add(p.timestamp)

            nuova_view_msg = NewView(new_view=msg.new_view, view_change_proofs=list(bucket.values()),
                                      pre_prepares=pre_prepares, sender_id=node.node_id)
            nuova_view_msg.signature = firma_messaggio(nuova_view_msg, node.private_key)

        if nuova_view_msg:
            node.transport.broadcast(nuova_view_msg)
            self.in_nuova_vista(nuova_view_msg)

    def in_nuova_vista(self, msg: NewView) -> None:
        node = self.node
        primario_atteso = node.all_node_ids[msg.new_view % node.n]
        if msg.sender_id != primario_atteso:
            return
        try:
            primary_pk = node.key_registry.get_chiave_pubblica(primario_atteso)
        except KeyError:
            return
        if not verifica_messaggio(msg, primary_pk):
            return
        if len(msg.view_change_proofs) < 2 * node.k + 1:
            return
        for vc in msg.view_change_proofs:
            try:
                vc_pk = node.key_registry.get_chiave_pubblica(vc.sender_id)
            except KeyError:
                return
            if not verifica_messaggio(vc, vc_pk):
                return

        with node.lock:
            if msg.new_view <= node.view:
                return
            node.view = msg.new_view
            node._last_progress = time.time()
            da_riproporre = list(msg.pre_prepares)
            mai_proposte = [
                req for ts, (req, _) in node._pending_client_requests.items()
                if ts not in {pp.timestamp for pp in da_riproporre}
                and (ts not in node.log or node.log[ts].pre_prepare is None)
            ]

        #ripropone nella nuova view le operazioni già viste
        for old_pp in da_riproporre:
            new_pp = PrePrepare(view=node.view, timestamp=old_pp.timestamp,
                                 operation_digest=old_pp.operation_digest,
                                 request=old_pp.request, sender_id=node.node_id)
            new_pp.signature = firma_messaggio(new_pp, node.private_key)
            if node.node_id == node.primario_attuale():
                node.transport.broadcast(new_pp)
            node.in_pre_prepare(new_pp)

        #propone da zero quelle che il vecchio primario aveva ignorato del tutto
        if node.node_id == node.primario_attuale():
            for req in mai_proposte:
                node._proposta_primario(req)