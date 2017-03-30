from multiprocessing.connection import Listener
from multiprocessing import Process, Manager
from multiprocessing.connection import Client

from time import time

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

def serve_client(conn, id, clients):
    connected = True
    while connected:
        try:
            m = conn.recv()
        except EOFError:
            print 'connection abruptly closed by client'
            connected = False
        print 'received message:', m, 'from', id
        if m == "quit":    
            connected = False
            conn.close() 
    del clients[id]                       
    notify_quit_client(id, clients)            
    print id, 'connection closed'


if __name__ == '__main__':
    listener = Listener(address=('127.0.0.1', 6000), authkey='secret password server')
    print 'listener starting'

    m = Manager()
    clients = m.dict()
    
    while True:
        print 'clients', clients #agregado
        print 'accepting conexions'
        conn = listener.accept()
        print 'connection accepted from', listener.last_accepted
        client_info = conn.recv()
        clients[listener.last_accepted] = client_info
        notify_new_client(listener.last_accepted, clients)

        p = Process(target=serve_client, args=(conn,listener.last_accepted,clients))
        p.start()
    listener.close()
    print 'end server'
