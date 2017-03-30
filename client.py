# ppl
chat system
from multiprocessing.connection import Client
from random import random
from time import sleep

from multiprocessing.connection import Listener
from multiprocessing import Process

local_listener = (('127.0.0.1', 5001),'secret client password')

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

    user = raw_input('Name')
    password = raw_input('Password')
    conn.send([(user,password),"new_user",[],local_listener])
    cl = Process(target=client_listener, args=())
    cl.start()

    connected = True
    while connected:
        #modifico el protocolo, quit es para salir
        message = raw_input("Message here, 'Q' to quit connection")
        sendto = raw_input('send to?')
        if message == 'Q':
            connected = False
        else:
            print "continue connected"
        conn.send([(user,password),"chat",(sendto,message)])

        
    print "last message"
    conn.send([(user,password),"quit"])
    conn.close()
    cl.terminate()
    print "end client"
