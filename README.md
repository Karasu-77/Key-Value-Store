# Key value store distribuito con tolleranza ai guasti bizantini
Realizzazione di un key value store distribuito su più nodi, con tre operazioni: PUT key value, GET key, DELETE key.
L'attenzione del progetto è sulla possibilità che uno o più nodi siano compromessi o si comportino in modo arbitrario (guasti bizantini).
Come riferimento per sicurezza e fault tolerance è stato usato PBFT (Practical Byzantine Fault Tolerance).

# Modello del sistema
Rete asincrona ma anche sincrona all'occorrenza: i messaggi possono essere persi, rallentati o arrivare fuori ordine ma non corrotti tramite l'ausilio della firma digitale.
N = 3k + 1 nodi replica, di cui al massimo k possono essere bizantini (anche per guasti arbitrari e non solo per crash).
Ogni nodo ha un ruolo di primario o backup, il primario della vista corrente è noto deterministicamente a tutti tramite round-robin (view % N), senza bisogno di messaggi aggiuntivi.
Requisiti di accordo bizantino:
- tutti i nodi corretti memorizzano lo stesso valore.
- se il primario è corretto tutti i nodi corretti memorizzano esattamente ciò che lui ha inviato.
Le chiavi pubbliche di tutti i nodi e dei client sono note staticamente a tutti (niente CA).

# Protocollo
Ogni operazione attraversa quattro fasi prima di essere eseguita sullo store:
1. Request: il client invia l'operazione firmata al primario.
2. Pre-Prepare: il primario assegna un timestamp, calcola il digest dell'operazione e fa broadcast ai backup.
3. Prepare: ogni nodo, se accetta il pre-prepare, fa broadcast di un proprio PREPARE. Quando un nodo ne raccoglie 2k coerenti (incluso il proprio), si dichiara prepared.
4. Commit: ogni nodo fa broadcast di un COMMIT. Quando ne raccoglie 2k coerenti (esclusi i propri), esegue l'operazione sullo store locale e risponde al client con una REPLY firmata.
Il client accetta il risultato solo dopo aver ricevuto k+1 reply concordanti da nodi diversi: è questa soglia a proteggere il client da un nodo che mente sulla risposta finale.
### View change
Se un nodo non vede progressi entro un timeout (nessun pre-prepare, o un'operazione bloccata a metà), sospetta il primario guasto e avvia il cambio vista: broadcast di VIEW-CHANGE, il nuovo primario (noto anch'esso per round-robin) attende 2k+1 conferme e poi diffonde NEW-VIEW per far ripartire tutti nella nuova vista, con le operazioni pendenti riproposte.
### Sicurezza
Ogni messaggio del protocollo è firmato con RSA-PSS + SHA-256 (si firma il digest del messaggio con la chiave privata del mittente; chi riceve verifica ricalcolando il digest e controllando la firma con la chiave pubblica del mittente). Questo impedisce che un nodo bizantino possa spacciarsi per un altro nodo o per il client, anche se può comunque mentire nel contenuto di ciò che invia con la propria identità.

# Codice

- `protocollo.py`  Messaggi del protocollo (`Request`, `PrePrepare`, `Prepare`, `Commit`, `Reply`, `ViewChange`, `NewView`)
                   firma/verifica RSA, serializzazione JSON

- `nodo.py`        `Store_chiave` (lo store)
                   `Nodo` (le quattro fasi di consenso, soglie 2k/2k+1)
                   `Gestore_cambio_vista` (view change)                            

- `network.py`      Trasporto TCP: una connessione per messaggio
                    `broadcast`/`invia_a` asincroni su thread separati                                                 

- `client.py`       `Cliente_kv`: invia le richieste
                    attende k+1 reply prima di fidarsi del risultato                                                 

- `nodobiz.py`      `Nodo_bizantino`: tre comportamenti simulati 
                    (`silent`, `lie_on_get`, `equivocate`).                                                                

- `main.py`         Bootstrap di un cluster N=3k+1 su localhost
                    esecuzione degli scenari                                                               


# Scenari
- cluster ok: PUT/GET/DELETE completati correttamente da tutti i nodi.
- un nodo mente sui GET: nodo1 risponde ai GET con un valore falso; il client lo scarta perché non raggiunge la soglia di k+1 risposte concordanti e ottiene comunque il valore corretto dagli altri nodi.
- il primario è completamente silenzioso: nodo0 non propone nulla. I backup rilevano il timeout ed effettuano il cambio vista, nodo1 completa l'operazione.
