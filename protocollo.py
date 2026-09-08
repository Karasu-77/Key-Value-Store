#librerie varie per gestire codifica e hashing
from __future__ import annotations
import base64
import hashlib
import json
from dataclasses import dataclass, field, asdict
from typing import Any, Optional

#librerie per la crittografia e firma
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes, serialization

@dataclass
class Operation:
    tipo_operazione: str  # PUT GET DELETE
    chiave: str
    valore: Optional[Any] = None

#calcolo dell'hash in base ai tre campi dell'Operation
def digest_operazione(op: Operation) -> str:
    # hashing
    payload = json.dumps(
        {"tipo_operazione": op.tipo_operazione, "chiave": op.chiave, "valore": op.valore},
        sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()

#messaggi protocollo
@dataclass
class Request:
    #client -> primario
    operation: Operation
    timestamp: int
    client_id: str
    signature: Optional[bytes] = None


@dataclass
class PrePrepare:
    #primario -> tutti i backup
    view: int
    timestamp: int
    operation_digest: str
    request: Request
    sender_id: str
    signature: Optional[bytes] = None


@dataclass
class Prepare:
    #backup -> tutti, incluso il primario
    view: int
    timestamp: int
    operation_digest: str
    sender_id: str
    signature: Optional[bytes] = None


@dataclass
class Commit:
    #ogni nodo -> tutti
    view: int
    timestamp: int
    operation_digest: str
    sender_id: str
    signature: Optional[bytes] = None


@dataclass
class Reply:
    #nodo -> client
    view: int
    timestamp: int
    client_id: str
    result: Any
    sender_id: str
    signature: Optional[bytes] = None


@dataclass
class ViewChange:
    #backup -> nuovo primario, quando rileva il primario guasto
    new_view: int
    prepared_set: list = field(default_factory=list)  #i prepare già inviati
    sender_id: str = ""
    signature: Optional[bytes] = None


@dataclass
class NewView:
    #nuovo primario -> tutti per far ripartire la view
    new_view: int
    view_change_proofs: list = field(default_factory=list)  # >= 2k+1 viewchange
    pre_prepares: list = field(default_factory=list)  #operazioni da riproporre
    sender_id: str = ""
    signature: Optional[bytes] = None

# crittografia
def genera_recupero_chiave() -> tuple[bytes, bytes]:
    #ritorna (chiave_privata_pem, chiave_pubblica_pem)
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    priv_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    pub_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return priv_pem, pub_pem


_PSS_PADDING = lambda: padding.PSS(
    mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH
)


def firma_rsa(data: bytes, private_key_pem: bytes) -> bytes:
    private_key = serialization.load_pem_private_key(private_key_pem, password=None)
    return private_key.sign(data, _PSS_PADDING(), hashes.SHA256())


def verifica_rsa(data: bytes, signature: bytes, public_key_pem: bytes) -> bool:
    public_key = serialization.load_pem_public_key(public_key_pem)
    try:
        public_key.verify(signature, data, _PSS_PADDING(), hashes.SHA256())
        return True
    except Exception:
        return False


def firma_byte(messaggio) -> bytes:
    #rappresentazione del messaggio, esclusa la sua firma
    d = asdict(messaggio)
    d.pop("signature", None)
    d = _codifica_bytes(d)
    return json.dumps(d, sort_keys=True, separators=(",", ":")).encode("utf-8")


def firma_messaggio(messaggio, private_key_pem: bytes) -> bytes:
    return firma_rsa(firma_byte(messaggio), private_key_pem)


def verifica_messaggio(messaggio, public_key_pem: bytes) -> bool:
    if messaggio.signature is None:
        return False
    return verifica_rsa(firma_byte(messaggio), messaggio.signature, public_key_pem)


class Registro_chiavi:
    #chiavi pubbliche note a tutti i nodi, condivise all'avvio
    def __init__(self):
        self._chiavi_pubbliche: dict[str, bytes] = {}

    def registra(self, entity_id: str, public_key_pem: bytes) -> None:
        self._chiavi_pubbliche[entity_id] = public_key_pem

    def get_chiave_pubblica(self, entity_id: str) -> bytes:
        return self._chiavi_pubbliche[entity_id]


#serializzazione su json
def _codifica_bytes(oggetto):
    if isinstance(oggetto, bytes):
        return {"__b64__": base64.b64encode(oggetto).decode("ascii")}
    if isinstance(oggetto, dict):
        return {k: _codifica_bytes(v) for k, v in oggetto.items()}
    if isinstance(oggetto, list):
        return [_codifica_bytes(v) for v in oggetto]
    return oggetto


def _decodifica_bytes(oggetto):
    if isinstance(oggetto, dict) and set(oggetto.keys()) == {"__b64__"}:
        return base64.b64decode(oggetto["__b64__"])
    if isinstance(oggetto, dict):
        return {k: _decodifica_bytes(v) for k, v in oggetto.items()}
    if isinstance(oggetto, list):
        return [_decodifica_bytes(v) for v in oggetto]
    return oggetto


def _richiesta_dal_dizionario(d: dict) -> Request:
    d = dict(d)
    d["operation"] = Operation(**d["operation"])
    return Request(**d)


def _preparazione_dal_dizionario(d: dict) -> PrePrepare:
    d = dict(d)
    d["request"] = _richiesta_dal_dizionario(d["request"])
    return PrePrepare(**d)


def _cambio_view_dal_dizionario(d: dict) -> ViewChange:
    d = dict(d)
    d["prepared_set"] = [Prepare(**p) for p in d["prepared_set"]]
    return ViewChange(**d)


def _nuova_view_dal_dizionario(d: dict) -> NewView:
    d = dict(d)
    d["view_change_proofs"] = [_cambio_view_dal_dizionario(vc) for vc in d["view_change_proofs"]]
    d["pre_prepares"] = [_preparazione_dal_dizionario(pp) for pp in d["pre_prepares"]]
    return NewView(**d)


_RECONSTRUCTORS = {
    "Request": _richiesta_dal_dizionario,
    "PrePrepare": _preparazione_dal_dizionario,
    "Prepare": lambda d: Prepare(**d),
    "Commit": lambda d: Commit(**d),
    "Reply": lambda d: Reply(**d),
    "ViewChange": _cambio_view_dal_dizionario,
    "NewView": _nuova_view_dal_dizionario,
}


def per_il_wire(messaggio) -> dict:
    #messaggio (dataclass) -> dict pronto per json.dumps
    d = _codifica_bytes(asdict(messaggio))
    d["__type__"] = type(messaggio).__name__
    return d


def dal_wire(d: dict):
    #dict ricevuto da rete -> messaggio (dataclass) del tipo giusto
    d = dict(d)
    type_name = d.pop("__type__")
    d = _decodifica_bytes(d)
    return _RECONSTRUCTORS[type_name](d)
