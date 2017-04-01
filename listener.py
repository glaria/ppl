#hasta ahora deja enviar mensajes entre clientes online
from multiprocessing.connection import Listener
from multiprocessing import Process, Manager
from multiprocessing.connection import Client

from time import time

def notify_new_client(id,clients,users,inbox): 
    for client, client_info in clients.items():
        if not client == id:
            print "sending new client to", client
            conn = Client(address=client_info[0], authkey=client_info[1])
            conn.send(("server_notify_go_online_user", id))
            conn.close()
	
	    print "checking inbox", client
    for registro, registro_info in inbox.items():#comprobar esto
	if registro==id:
		conn = Client(address=client_info[0], authkey=client_info[1])
		conn.send(("server_notify_inbox",registro_info))

def notify_quit_client(id,clients): 
    for client, client_info in clients.items():
            print "sending quit client to", client
            conn = Client(address=client_info[0], authkey=client_info[1])
            conn.send(("server_notify_quit_user", id))

def send_message(destino,mensaje,clients,users,inbox):
    if destino in clients: #comprueba si el destinatario esta conectado
           conn = Client(address=clients[destino][0], authkey=clients[destino][1])
           conn.send(("server_notify_chat",mensaje))
    elif destino in users: #comprueba si el destinatario esta registrado
	   inbox[destino]=mensaje
    else:
           conn = Client(address=clients[mensaje[0]][0], authkey=clients[mensaje[0]][1])
           conn.send("no existe el usuario")
def serve_client(conn, id, clients,users,inbox):
    connected = True
    while connected:
        try:
            m = conn.recv()
        except EOFError:
            print 'connection abruptly closed by client'
            connected = False
        if m[1] == "quit":    
            connected = False
            del clients[m[0][0]]                       
            notify_quit_client(m[0][0], clients)            
            print id, 'connection closed'
            conn.close() 
        elif m[1] == "go_online":
            client_info = m[2]
            clients[m[0][0]] = client_info
            notify_new_client(m[0][0], clients,users,inbox)
        elif m[1] == "chat":
            sendto = m[2][0]
            message = m[2][1]
            send_message(sendto,(m[0][0],message),clients,users,inbox)

if __name__ == '__main__':
    listener = Listener(address=('127.0.0.1', 6000), authkey='secret password server')
    print 'listener starting'

    m = Manager()
    clients = m.dict() #registro de los usuarios online
    users = m.dict() #registro de todos los usuarios existentes
    inbox = m.dict() #mensajes recibidos mientras estaba offline, {id,(from,message)}
    
    while True:
        print 'clients', clients #agregado
        print 'accepting conexions'
        conn = listener.accept()
        print 'connection accepted from', listener.last_accepted

        p = Process(target=serve_client, args=(conn,listener.last_accepted,clients,users,inbox))
        p.start()
    listener.close()
print 'end server'
