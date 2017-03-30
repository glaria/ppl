from multiprocessing.connection import Client
from random import random
from time import sleep

from multiprocessing.connection import Listener
from multiprocessing import Process

local_listener = (('127.0.0.1', 5002),'secret client 4 password')

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
    conn.send(local_listener)

    cl = Process(target=client_listener, args=())
    cl.start()
    
    connected = True
    while connected:
        value = raw_input("'C', stay connected. 'Q' quit connection")
        if value == 'Q':
            connected = False
        else:
            print "continue connected"
            conn.send("connected")
        
    print "last message"
    conn.send("quit")
    conn.close()
    cl.terminate()
    print "end client"
