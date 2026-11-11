Documentatie Proiect: Broker MQTT v5.0 in PythonEtapa 1: Arhitectura, Specificatii Tehnice si Planificare1. Descrierea Protocolului, Scopul si Cerintele Functionale1.1 Descrierea ProtocoluluiMQTT (Message Queuing Telemetry Transport) v5.0 este un protocol de retea usor, bazat pe modelul Publish/Subscribe, conceput pentru transportul eficient al mesajelor pe conexiuni cu latime de banda redusa sau latenta ridicata (specific mediilor IoT / M2M).Sistemul dezvoltat este un Broker MQTT v5.0 custom, scris de la zero in Python fara framework-uri terte pentru protocoale, avand ca scop intelegerea si manipularea directa a cadrelor binare la nivelul stivei TCP/IP.1.2 Scopul ProiectuluiImplementarea unui broker capabil sa accepte conexiuni de la clienti MQTT standard (ex: MQTTX, paho-mqtt).Manipularea eficienta a memoriei de retea prin structuri bytes si bytearray.Decodificarea si codificarea dinamica a pachetelor de control conform specificatiei oficiale OASIS MQTT v5.0.Rutarea mesajelor intre publicatori si abonati cu suport pentru niveluri de calitate a serviciului (QoS 0/1).1.3 Cerinte Functionale (CF)CF-1 (Socket Server & Transport): Ascultare pe portul TCP 1883 si gestionarea conexiunilor simultane.CF-2 (Parsing Binar Fix & Variabil): Identificarea tipului de pachet si decodificarea lungimii ramase (Remaining Length) folosind algoritmul Variable Byte Integer (Base-128).CF-3 (Handshake & Identificare): Validarea pachetului CONNECT (verificare versiune protocol v5.0, Clean Start, Client ID) si generarea pachetului CONNACK cu coduri de motiv (Reason Codes).CF-4 (Gestionare Abonamente & Rutare): Parsarea pachetelor SUBSCRIBE/UNSUBSCRIBE, mentinerea unui arbore de topic-uri si redirectionarea mesajelor PUBLISH catre clientii abonati.CF-5 (Keep Alive & Disconnect): Monitorizarea temporizarilor de pauza (interval Keep Alive) si trimiterea de raspunsuri PINGRESP la cererile PINGREQ.2. Specificatii Tehnice2.1 Structura Generala a Pachetelor (Binary Frame)Fiecare pachet MQTT v5.0 respecta structura binara de mai jos:+-----------------------+----------------------------------+
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
2.2 Formate de Mesaje CheieA. Algoritmul Variable Byte Integer (VBI)Lungimea ramasa (Remaining Length) este stocata pe 1–4 octeti. Bitul 7 (MSB) arata daca mai urmeaza un octet (1) sau daca este ultimul (0), iar bitii 0..6 stocheaza datele utile:$$\text{Valoare} = \sum_{i=0}^{N-1} (\text{octet}_i \ \& \ \text{0x7F}) \times 128^i$$B. Pachetul CONNECT (Type 1)Fixed Header: 0x10 urmat de VBI.Variable Header: Protocol Name ("MQTT"), Version (0x05), Connect Flags (Username, Password, Will Flag, Clean Start), Keep Alive (uint16 Big-Endian).Payload: Client Identifier, Will Properties/Topic/Payload, Username, Password.C. Pachetul CONNACK (Type 2)Byte 1: 0x20 (Type = 2, Flags = 0).Byte 2: Remaining Length.Byte 3: Session Present Flag (Bit 0).Byte 4: Connect Reason Code (0x00 Success, 0x80 Unspecified Error, 0x84 Unsupported Protocol Version).2.3 Fluxuri de Comunicare (Sequence Diagrams)Fluxul de Conectare (Handshake)Client                                  Broker (Python)
  |                                            |
  | -------- TCP SYN / ESTABLISHED ----------> | (socket.accept)
  |                                            |
  | ------------ CONNECT (0x10) -------------> | parse_raw_packet()
  |                                            | parse_connect_flags()
  | <----------- CONNACK (0x20) -------------- | validate & send bytearray
  |                                            |
Fluxul Publish / SubscribeClient A (Subscriber)        Broker MQTT           Client B (Publisher)
  |                              |                              |
  | --- SUBSCRIBE ("t/1") ---->  |                              |
  | <-- SUBACK (0x90) ---------  |                              |
  |                              |                              |
  |                              | <-- PUBLISH ("t/1", "val") - |
  | <-- PUBLISH ("t/1", "val") - |                              |
2.4 Masina de Stari a Conexiunii (Broker Session State)   +-------------------+
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
2.5 Temporizari si Politici de TimeoutKeep Alive Timeout: Daca un client declara un Keep Alive de $K$ secunde, brokerul acorda o toleranta de $1.5 \times K$ secunde. Daca nu primeste niciun pachet (sau PINGREQ) in acest interval, brokerul inchide conexiunea TCP.Response Timeout: Raspunsul CONNACK sau SUBACK este trimis sincron imediat dupa parsarea cererii.3. Planificarea Activitatilor si Repartizarea ResponsabilitatilorProiectul este structurat pe 5 module principale de dezvoltare, validate prin teste unitare automate:Etapa / ModulDescriere ActivitateLivrabil TehnicStatusResponsabilModulul 1Arhitectura de baza, I/O binar (bytes/bytearray), operatii pe biti.Parsere flag-uri, bitmaskingCompletatEchipaModulul 2Encodare/Decodare VBI (Baza 128), operare struct Big-Endian, parse_raw_packet.Algoritm VBI + Suita unittestCompletatEchipaModulul 3Structura Pachete MQTT v5.0: Handshake (CONNECT, CONNACK), validare versiune & proprietati.Parser CONNECT & builder CONNACKIn lucruEchipaModulul 4Engine de abonamente, reprezentare arbore topic-uri, suport wildcard (+, #).SubscriptionTree & logica de rutareProgramatEchipaModulul 5Concurrenta (Multi-threading / Async I/O), integrare finala Socket Server, testare cu MQTTX.Broker executabil pe portul 1883ProgramatEchipa4. Ghid de Pregatire pentru Evaluarea Orala (Punctul O - 25%)La evaluarea orala trebuie sa justifici deciziile tehnice luate in codul scris pana acum:Q1: De ce folositi bytes si bytearray si nu string pentru citirea din socket?Raspuns: Protocolul MQTT este un protocol binar, nu de text. Octetii contin masti de biti (flag-uri) si valori numerice packed. bytes este imutabil si eficient pentru citire directa din retea, iar bytearray este mutabil, permitand alipirea (append) rapida a octetilor cand construim pachetele de raspuns binar (CONNACK).Q2: Cum functioneaza decodificarea Variable Byte Integer (VBI)?Raspuns: VBI foloseste sistemul Baza 128. Parcurgem octetii pe rand: extragem datele utile cu masca octet & 0x7F (bitii 0..6) si le inmultim cu un multiplicator care creste cu puterile lui 128 ($1, 128, 16384, \dots$). Ne oprim cand octet & 0x80 == 0, ceea ce inseamna ca bitul 7 (MSB) este zero si nu mai urmeaza alti octeti.Q3: De ce folositi struct.unpack('>H', ...)?Raspuns: In specificatia MQTT, toate numerele intregi pe 16 biti (cum ar fi lungimea sirurilor UTF-8 sau Packet Identifier) sunt transmise in ordinea Big-Endian (Network Byte Order). Formatul '>H' ii spune modulului struct sa interpreteze 2 octeti (H = unsigned short) cu cel mai semnificativ octet primul (>).Q4: Cum izolati flag-urile dintr-un singur octet?Raspuns: Combinam shiftarea pe biti (>>) cu masca binara &. De exemplu, pentru a extrage QoS_Level din primul octet de PUBLISH, shiftam la dreapta cu o pozitie (first_byte >> 1) pentru a aduce bitii de QoS la margine, apoi aplicam masca & 0x03 (0b11) pentru a izola doar cei 2 biti de interes.Q5: Cum ati testat codul pentru a preveni erorile la modificari ulterioare?Raspuns: Am creat o suita de teste automate folosind unittest. Am folosit testare de tip Roundtrip (verificam daca decode(encode(X)) == X), validarea cazurilor limita (la pragurile de 127/128 octeti) si self.subTest pentru a izola testarea pe seturi mari de date fara a intrerupe executarea testelor la prima eroare.
