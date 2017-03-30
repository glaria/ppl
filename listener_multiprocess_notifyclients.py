from multiprocessing.connection import Listener
from multiprocessing import Process, Manager
from multiprocessing.connection import Client
from multiprocessing import Lock
from multiprocessing.connection import AuthenticationError

from time import time
#revisar los archivos originales a ver que pasa

def notify_new_client(id,clients): 
    for client, client_info in clients.items():
        if not client == id:
            print "sending new client to", client
            conn = Client(address=client_info[0], authkey=client_info[1])
            conn.send(("new client", id))
            conn.close()

def notify_quit_client(id,clients): 
    for client, client_info in clients.items():
            print "sending quit client to", client
            conn = Client(address=client_info[0], authkey=client_info[1])
            conn.send(("quit client", id))
def notify_message_client(id, message, clients):


def serve_client(conn, id, clients,registro, almacen, s,t,u):
    connected = True
    while connected:
        try:
            m = conn.recv()
            if m[1] == 'new_user':
		t.acquire()
	    	if m[0][0] in registro.keys(): #registro es un diccionario
			conn.send(['el usuario ya existe',False])
                else:
			registro.update({m[0]})
                        print 'registro actualizado', registro
                        conn.send(['nuevo usuario registrado', True])
            		t.release()
                	s.acquire()
                	clients.update({m})
                	s.release
   	    elif m[1] == 'chat':
  		
                remitente = m[0][0]
                sendto = m[2][0]
                mensaje = m[2][1]
                k = 0
                for client, client_info in clients.items():
                        print 'hey'
                	if sendto == client: #si esta online
                                print 'fine'
                                k = 1
	        		conn = Client(address=client_info[0], authkey=client_info[1])
                        	conn.send((remitente,mensaje))	
                if k == 0: #si esta offline guardamos el mensaje en el almacen
                	s.acquire()
			almacen[sendto]=(remitente,mensaje)
                	conn.send('notify_send')
                	s.release()
        except EOFError:
            print 'connection abruptly closed by client'
            connected = False
        print 'received message:', m, 'from', id
        
    del clients[id]                       
    notify_quit_client(id, clients)            
    print id, 'connection closed'


if __name__ == '__main__':
    manager = Manager()
    almacen = manager.dict() #guarda los mensajes si el usuario esta offline
    registro = manager.dict() #usuarios registrados
    clients = manager.dict() #usuarios online
    s = Lock() #semaforo almacen
    t = Lock() #semaforo registro
    u = Lock() #semaforo clients
    listener = Listener(address=('127.0.0.1', 6000), authkey='secret password server')
    print 'listener starting'
    
    while True:
        print 'accepting conexions'
        conn = listener.accept()
        print 'connection accepted from', listener.last_accepted
        #client_info = conn.recv()
        #clients[listener.last_accepted] = client_info
        print 'clientes', clients
        notify_new_client(listener.last_accepted, clients)

        p = Process(target=serve_client, args=(conn,listener.last_accepted,clients,registro,almacen,s,t,u))
        p.start()
    listener.close()
    print 'end server'
