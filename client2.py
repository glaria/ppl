from multiprocessing.connection import Client
from random import random
from time import sleep

from multiprocessing.connection import Listener
from multiprocessing import Process

local_listener = (('127.0.0.1', 5004),'secret client 2 password')

def client_listener():
    cl = Listener(address=local_listener[0], authkey=local_listener[1])
    print '...client listener starting' 
    print '...accepting conexions'
    while True:
        conn = cl.accept()    
        m = conn.recv()
        if m[0]=="server_notify_go_online_user":
        	message = "Se ha conectado el usuario", m[1]
        	print message
        elif m[0]=="server_notify_quit_user":
		message = "Se ha desconectado el usuario", m[1]
		print message
	elif m[0]=="server_notify_chat":
		message = "[",m[1][0],"] dice:",m[1][1]
		print message
	elif m[0]=="server_notify_inbox":
		message = "Mensajes recibidos mientras estabas desconectado",m[1]

		print message

if __name__ == '__main__':

    print 'trying to connect'
    conn = Client(address=('127.0.0.1', 6000), authkey='secret password server')
    
    cl = Process(target=client_listener, args=())
    cl.start()
    
    id = raw_input("user")
    password = raw_input("pass")
    conn.send([(id,password), "go_online", local_listener])
    connected = True
    while connected:
        sendto = raw_input("sendto?")
        message = raw_input("Message here")
        if message == 'quit':
            connected = False
            conn.send(([(id,password), "quit", []]))
        else:
            print "continue connected"
            conn.send(([(id,password), "chat", (sendto,message)]))
        
    print "last message"
    conn.close()
    cl.terminate()
print "end client"
