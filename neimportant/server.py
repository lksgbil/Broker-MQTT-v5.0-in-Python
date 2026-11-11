import socket
import threading

clienti = []

def broadcast(data, client_socket, client_addr):
    destinatari = [c for c in clienti if c != client_socket]

    if not destinatari:
        client_socket.send(b"[SERVER] Esti singurul conectat la server asa ca nu am trimis mesajul!")
        return

    mesaj = (f"[{client_addr}]").encode('utf-8') + data

    for client in destinatari:
        client.send(mesaj)

def gestionare_client(client_socket, client_addr):
    client_socket.send(b"[SERVER] Te-ai conectat la server, acum poti scrie pe chat!") # mesaj la client
    print(f"[{client_addr}] s-a conectat la server!") # mesaj pentru server

    clienti.append(client_socket)

    while True:
        try:
            data = client_socket.recv(1024)

            broadcast(data, client_socket, client_addr)
        except ConnectionResetError:
            print(f"[{client_addr}] s-a deconectat de la server!")
            break
        

def pornire_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(('127.0.0.1', 1883))
    server_socket.listen()

    while True:
        conn, addr = server_socket.accept()
        threading.Thread(target=gestionare_client, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    pornire_server()