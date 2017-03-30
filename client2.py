from multiprocessing.connection import Client
from random import random
from time import sleep

from multiprocessing.connection import Listener
from multiprocessing import Process

local_listener = (('127.0.0.1', 5004),'secret client 2 password')

def client_listener():
    cl = Listener(address=local_listener[0], authkey=local_listener[1])
    print '.............client listener starting' 
    print '.............accepting conexions'
    while True:
        conn = cl.accept()
        print '.............connection accepted from', cl.last_accepted        
        m = conn.recv()
        print '.............message received from server', m 


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
