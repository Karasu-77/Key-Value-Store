from __future__ import annotations

import time

from protocollo import Registro_chiavi, genera_recupero_chiave
from nodo import Nodo
from nodobiz import Nodo_bizantino
from network import Trasporto_Network
from client import Client_kv

BASE_PORT = 9000

def costruisci_cluster(k: int, comportamenti_bizantini: dict[str, str] | None = None,
                        request_timeout: float = 2.0):
    comportamenti_bizantini = comportamenti_bizantini or {}
    n = 3 * k + 1
    nodo_id = [f"nodo{i}" for i in range(n)]
    client_id = "client0"

    key_registry = Registro_chiavi()
    addresses: dict[str, tuple[str, int]] = {}
    for i, nid in enumerate(nodo_id):
        addresses[nid] = ("127.0.0.1", BASE_PORT + i)
    addresses[client_id] = ("127.0.0.1", BASE_PORT + n)

    nodi: dict[str, Nodo] = {}
    for nid in nodo_id:
        priv, pub = genera_recupero_chiave()
        key_registry.registra(nid, pub)
        behavior = comportamenti_bizantini.get(nid)
        if behavior:
            node = Nodo_bizantino(nid, nodo_id, k, key_registry, priv,
                                   behavior=behavior, request_timeout=request_timeout)
        else:
            node = Nodo(nid, nodo_id, k, key_registry, priv, request_timeout=request_timeout)
        altri = [other for other in nodo_id if other != nid]
        transport = Trasporto_Network(nid, "127.0.0.1", addresses[nid][1], addresses,
                                       broadcast_targets=altri)
        node.trasporto_bind(transport)
        nodi[nid] = node

    client_priv, client_pub = genera_recupero_chiave()
    key_registry.registra(client_id, client_pub)
    client_transport = Trasporto_Network(client_id, "127.0.0.1", addresses[client_id][1], addresses,
                                          broadcast_targets=nodo_id)
    client = Client_kv(client_id, k, nodo_id, client_transport, client_priv, key_registry)

    return nodi, client, key_registry


def avvia_tutti(nodi: dict[str, Nodo], client: Client_kv) -> None:
    for node in nodi.values():
        node.transport.start(node.gestione_messaggio)
        node.start()
    client.start()
    time.sleep(0.3)  #tempo al server tcp per mettersi in ascolto


def ferma_tutti(nodi: dict[str, Nodo], client: Client_kv) -> None:
    for node in nodi.values():
        node.transport.stop()
        node.stop()
    client.stop()


def esegui_scenario(titolo: str, nodi: dict[str, Nodo], client: Client_kv) -> None:
    print(f"\n=== {titolo} ===")
    print("PUT foo=1234        ->", client.put("foo", 1234))
    print("GET foo            ->", client.get("foo"))
    print("DELETE foo         ->", client.delete("foo"))
    print("GET foo (dopo del) ->", client.get("foo"))


if __name__ == "__main__":
    #Scenario 1: tutti i nodi corretti, k=1 -> N=4
    nodi, client, _ = costruisci_cluster(k=1)
    avvia_tutti(nodi, client)
    esegui_scenario("Scenario 1: cluster onesto (N=4, k=1)", nodi, client)
    ferma_tutti(nodi, client)

    time.sleep(0.5)

    #Scenario 2: un nodo bizantino che mente sui GET
    nodi, client, _ = costruisci_cluster(k=1, comportamenti_bizantini={"nodo1": "get_falso"})
    avvia_tutti(nodi, client)
    esegui_scenario("Scenario 2: nodo1 mente sui GET (N=4, k=1)", nodi, client)
    ferma_tutti(nodi, client)

    time.sleep(0.5)

    #Scenario 3: nodo0 è completamente silenzioso -> i backup rilevano il timeout e fanno view change, nodo1 completa
    nodi, client, _ = costruisci_cluster(k=1, comportamenti_bizantini={"nodo0": "silent"},
                                          request_timeout=1.5)
    client.timeout = 2.5
    client.retries = 3
    avvia_tutti(nodi, client)
    esegui_scenario("Scenario 3: nodo0 (primario) è silenzioso -> view change", nodi, client)
    ferma_tutti(nodi, client)
