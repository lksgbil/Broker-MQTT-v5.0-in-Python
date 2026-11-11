# Documentatie Proiect: Broker MQTT v5.0 in Python

## Etapa 1: Arhitectura, Specificatii Tehnice si Planificare

## 1. Descrierea Protocolului, Scopul si Cerintele Functionale

### 1.1 Descrierea Protocolului

**MQTT (Message Queuing Telemetry Transport) v5.0** este un protocol de retea usor, bazat pe modelul **Publish/Subscribe**, conceput pentru transportul eficient al mesajelor pe conexiuni cu latime de banda redusa sau latenta ridicata (specific mediilor IoT / M2M).

Sistemul dezvoltat este un **Broker MQTT v5.0 custom**, scris de la zero in Python fara framework-uri terte pentru protocoale, avand ca scop intelegerea si manipularea directa a cadrelor binare la nivelul stivei TCP/IP.

### 1.2 Scopul Proiectului

* Implementarea unui broker capabil sa accepte conexiuni de la clienti MQTT standard (ex: `MQTTX`, `paho-mqtt`).

* Manipularea eficienta a memoriei de retea prin structuri `bytes` si `bytearray`.

* Decodificarea si codificarea dinamica a pachetelor de control conform specificatiei oficiale OASIS MQTT v5.0.

* Rutarea mesajelor intre publicatori si abonati cu suport pentru niveluri de calitate a serviciului (QoS 0/1).

### 1.3 Cerinte Functionale (CF)

* **CF-1 (Socket Server & Transport):** Ascultare pe portul TCP 1883 si gestionarea conexiunilor simultane.

* **CF-2 (Parsing Binar Fix & Variabil):** Identificarea tipului de pachet si decodificarea lungimii ramase (*Remaining Length*) folosind algoritmul *Variable Byte Integer* (Base-128).

* **CF-3 (Handshake & Identificare):** Validarea pachetului `CONNECT` (verificare versiune protocol v5.0, `Clean Start`, `Client ID`) si generarea pachetului `CONNACK` cu coduri de motiv (*Reason Codes*).

* **CF-4 (Gestionare Abonamente & Rutare):** Parsarea pachetelor `SUBSCRIBE`/`UNSUBSCRIBE`, mentinerea unui arbore de topic-uri si redirectionarea mesajelor `PUBLISH` catre clientii abonati.

* **CF-5 (Keep Alive & Disconnect):** Monitorizarea temporizarilor de pauza (interval `Keep Alive`) si trimiterea de raspunsuri `PINGRESP` la cererile `PINGREQ`.

## 2. Specificatii Tehnice

### 2.1 Structura Generala a Pachetelor (Binary Frame)

Fiecare pachet MQTT v5.0 respecta structura binara de mai jos:

```
+-----------------------+----------------------------------+
|      Byte 1           |           Byte 2..N              |
+-----------------------+----------------------------------+
| Packet Type | Flags   | Remaining Length (VBI: 1-4 bytes)|
|  (4 biti)   | (4 biti)| (Base-128, bit 7 = continuare)   |
+-----------------------+----------------------------------+
|                 VARIABLE HEADER                          |
|         (Packet ID, Properties, Reason Codes)            |
+----------------------------------------------------------+
|                     PAYLOAD                              |
|           (Client ID, Topic Name, Message Data)          |
+----------------------------------------------------------+
```

### 2.2 Formate de Mesaje Cheie

#### A. Algoritmul Variable Byte Integer (VBI)

Lungimea ramasa (*Remaining Length*) este stocata pe 1–4 octeti. Bitul 7 (MSB) arata daca mai urmeaza un octet (`1`) sau daca este ultimul (`0`), iar bitii 0..6 stocheaza datele utile:

$$\text{Valoare} = \sum_{i=0}^{N-1} (\text{octet}_i \text{ AND } 0\text{x}7\text{F}) \times 128^i$$

#### B. Pachetul `CONNECT` (Type 1)

* **Fixed Header:** `0x10` urmat de VBI.

* **Variable Header:** Protocol Name (`"MQTT"`), Version (`0x05`), Connect Flags (`Username`, `Password`, `Will Flag`, `Clean Start`), Keep Alive (`uint16` Big-Endian).

* **Payload:** Client Identifier, Will Properties/Topic/Payload, Username, Password.

#### C. Pachetul `CONNACK` (Type 2)

* **Byte 1:** `0x20` (Type = 2, Flags = 0).

* **Byte 2:** Remaining Length.

* **Byte 3:** Session Present Flag (Bit 0).

* **Byte 4:** Connect Reason Code (`0x00` Success, `0x80` Unspecified Error, `0x84` Unsupported Protocol Version).

### 2.3 Fluxuri de Comunicare (Sequence Diagrams)

#### Fluxul de Conectare (Handshake)

```
Client                                  Broker (Python)
  |                                            |
  | -------- TCP SYN / ESTABLISHED ----------> | (socket.accept)
  |                                            |
  | ------------ CONNECT (0x10) -------------> | parse_raw_packet()
  |                                            | parse_connect_flags()
  | <----------- CONNACK (0x20) -------------- | validate & send bytearray
  |                                            |
```

#### Fluxul Publish / Subscribe

```
Client A (Subscriber)        Broker MQTT           Client B (Publisher)
  |                              |                              |
  | --- SUBSCRIBE ("t/1") ---->  |                              |
  | <-- SUBACK (0x90) ---------  |                              |
  |                              |                              |
  |                              | <-- PUBLISH ("t/1", "val") - |
  | <-- PUBLISH ("t/1", "val") - |                              |
```

### 2.4 Masina de Stari a Conexiunii (Broker Session State)

```
   +-------------------+
   |   DISCONNECTED    |
   +-------------------+
             |
             | Receives TCP Connection
             v
   +-------------------+
   |    CONNECTING     | <--- Validare Header, Version (v5.0) & Flags
   +-------------------+
             |
   +---------+---------+
   | (Success)         | (Invalid Protocol / Reserved Bits)
   v                   v
+------------+   +-------------------+
| CONNECTED  |   | CLOSE CONNECTION  |
+------------+   +-------------------+
   |      |
   |      +---> Trimitere/Primire PINGREQ -> PINGRESP
   |
   +---> Receives DISCONNECT / Timeout (Keep Alive * 1.5)
   v
+-------------------+
|   DISCONNECTED    |
+-------------------+
```

### 2.5 Temporizari si Politici de Timeout

* **Keep Alive Timeout:** Daca un client declara un `Keep Alive` de $K$ secunde, brokerul acorda o toleranta de $1.5 \times K$ secunde. Daca nu primeste niciun pachet (sau `PINGREQ`) in acest interval, brokerul inchide conexiunea TCP.

* **Response Timeout:** Raspunsul `CONNACK` sau `SUBACK` este trimis sincron imediat dupa parsarea cererii.

## 3. Planificarea Activitatilor si Repartizarea Responsabilitatilor

Proiectul este structurat pe 5 module principale de dezvoltare, validate prin teste unitare automate:

| Etapa / Modul | Descriere Activitate | Livrabil Tehnic | Status | Responsabil | 
| ----- | ----- | ----- | ----- | ----- | 
| **Modulul 1** | Arhitectura de baza, I/O binar (`bytes`/`bytearray`), operatii pe biti. | Parsere flag-uri, bitmasking | **Completat** | Echipa | 
| **Modulul 2** | Encodare/Decodare VBI (Baza 128), operare `struct` Big-Endian, `parse_raw_packet`. | Algoritm VBI + Suita `unittest` | **Completat** | Echipa | 
| **Modulul 3** | Structura Pachete MQTT v5.0: Handshake (`CONNECT`, `CONNACK`), validare versiune & proprietati. | Parser `CONNECT` & builder `CONNACK` | **In lucru** | Echipa | 
| **Modulul 4** | Engine de abonamente, reprezentare arbore topic-uri, suport wildcard (`+`, `#`). | `SubscriptionTree` & logica de rutare | Programat | Echipa | 
| **Modulul 5** | Concurrenta (Multi-threading / Async I/O), integrare finala Socket Server, testare cu MQTTX. | Broker executabil pe portul 1883 | Programat | Echipa | 
