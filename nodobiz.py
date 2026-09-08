from __future__ import annotations

from nodo import Nodo, Voce_log
from protocollo import Prepare, Reply, firma_messaggio


class Nodo_bizantino(Nodo):

    def __init__(self, *args, behavior: str = "silent", **kwargs):
        super().__init__(*args, **kwargs)
        self.behavior = behavior

    def in_richiesta(self, request) -> None:
        if self.behavior == "silent" and self.primario():
            return  #primario silenzioso: non propone nulla viene rilevato via timeout
        super().in_richiesta(request)

    def in_pre_prepare(self, msg) -> None:
        if self.behavior == "silent":
            return  #omissione pura

        if self.behavior == "equivocate":
            digest_falso = "0" * 64  #non corrisponde a nessuna operazione
            prepare_vero = Prepare(view=msg.view, timestamp=msg.timestamp,
                                    operation_digest=msg.operation_digest, sender_id=self.node_id)
            prepare_vero.signature = firma_messaggio(prepare_vero, self.private_key)
            prepare_falso = Prepare(view=msg.view, timestamp=msg.timestamp,
                                     operation_digest=digest_falso, sender_id=self.node_id)
            prepare_falso.signature = firma_messaggio(prepare_falso, self.private_key)

            for i, nid in enumerate(self.all_node_ids):
                if nid == self.node_id:
                    continue
                self.transport.invia_a(nid, prepare_vero if i % 2 == 0 else prepare_falso)
            return  #non passa dal normale in_pre_prepare/in_prepare

        super().in_pre_prepare(msg)

    def _esegui_e_rispondi(self, entry: Voce_log) -> None:
        if self.behavior == "get_falso" and entry.request.operation.tipo_operazione == "GET":
            risultato_falso = {"status": "OK", "value": "__VALORE_FALSO__"}
            reply = Reply(view=self.view, timestamp=entry.timestamp,
                           client_id=entry.request.client_id, result=risultato_falso,
                           sender_id=self.node_id)
            reply.signature = firma_messaggio(reply, self.private_key)
            self.transport.invia_a(entry.request.client_id, reply)
            return
        super()._esegui_e_rispondi(entry)