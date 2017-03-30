# -*- coding: utf-8 -*-

from multiprocessing.connection import Listener
from multiprocessing import Process
from multiprocessing.connection import AuthenticationError
from time import time
from multiprocessing import Manager
from multiprocessing import Lock
from multiprocessing.connection import Client

from time import time

def serve_client(conn, id,registro,almacen,s,t):
    while True:
        try:
            
            m = conn.recv()
	    if m[1] == 'new_user':
		t.acquire()
	    	if m[0][0] in registro.keys(): #registro es un diccionario
			conn.send(['el usuario ya existe',False])
                else:
			registro.update({m[0]})
                        conn.send(['nuevo usuario registrado', True])
            	t.release()
   	    elif m[1] == 'send_mail':
  		s.acquire()
                remitente = m[0][0]
                destino = m[2][0]
                mensaje = m[2][1]
		registro.update({(destino,(remitente,mensaje))})
                conn.send('notify_send')
                s.release()
            elif m[1] == 'consulta':
                s.acquire()
                propio = m[0][0]
                conn.send(registro.get(propio))
                del registro[propio]
                s.release()
		     
        except EOFError:
            print 'No receive, connection abruptly closed by client'
            break
        print 'received message:', m, 'from', id
        
        answer = 'hola', m[1]
        try:
            conn.send(answer)
        except IOError:
            print 'No send, connection abruptly closed by client'
            break

    conn.close()
    print 'connection', id, 'closed'

if __name__ == '__main__':
    manager = Manager()
    almacen = manager.dict() #guarda los mensajes
    registro = manager.dict() #usuarios registrados
    s = Lock() #semaforo almacen
    t = Lock() #semaforo registro
    listener = Listener(address=('127.0.0.1', 6000), authkey='secret password')
    print 'listener starting'

    while True:
        print 'accepting conexions'
        try:
            conn = listener.accept()                
            print 'connection accepted from', listener.last_accepted
            p = Process(target=serve_client, args=(conn,listener.last_accepted,registro,almacen,s,t))
            p.start()
        except AuthenticationError:
            print 'Connection refused, incorrect password'

    listener.close()
    print 'end'
