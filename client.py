# -*- coding: utf-8 -*-
from multiprocessing.connection import Client
from random import random
from time import sleep
from multiprocessing.connection import Listener
from multiprocessing import Process
#falta import Tkinter
local_listener = (('127.0.0.1', 5001),'secret client 1 password')

def client_listener():
    cl = Listener(address=local_listener[0], authkey=local_listener[1])
    print '..client listener starting' 
    print '..accepting conexions'
    while True:
        conn = cl.accept()
        #print '..connection accepted from', cl.last_accepted        
        m = conn.recv()
        #print '..message received from server', m 
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
    conectado = True
    while conectado:
        inicio = raw_input("1.Register, else Login")
        if inicio == "1":
            typo = "new_user"
            usuario = raw_input("user")
            password = raw_input("pass")
            addressbook = []
        else:
            typo = "go_online"
            usuario = raw_input("user")
            password = raw_input("pass")
        if typo == "go_online":
            conn.send([(usuario,password), typo, local_listener])
        elif typo == "new_user":
            conn.send([(usuario,password), typo, addressbook])
        answer = conn.recv()
        print answer[0]
        if answer[0] == "notify_new_user":
            print "hola"
            if answer[1][0] == False:
                print answer[1][1]
                print "hi"
        elif answer[0] == "notify_go_online":
            if answer[1][0] == True:
                conectado = False #he hecho login, asi que salgo del bucle inicial
            else:
                print answer[1][1]
    #la gestion de los typo estaria en el tkinter
    
    connected = True
    while connected:
        options = raw_input("1.Chat, 2. Add addressbook, else quit")#esto serian botones del tkinter
        if options == "1":
            sendto = raw_input("sendto?")
            message = raw_input("Message here")
            conn.send([(usuario,password), "chat", (sendto,message)])
        elif options == "2":
            nuevo = raw_input("new contact?")
            conn.send([(usuario,password), "add_contact", nuevo])
        else:
            conn.send(([(usuario,password), "quit", []]))
        
        c = conn.recv()
        #aqui la gestion de las respuestas que se reciben desde el servidor
        clave = c[0]
        if clave == "quit":
            if c[1][0] == True:
                connected = False
            else:
                print c[1][1]
        elif clave == "notify_chat":
            if c[1][0] == False:
                print c[1][1]
        elif clave == "notify_add_contact":
            if c[1][0] == True:
                addressbook = c[2] #comprobar donde se va a meter el addressbook
            else:
                print c[1][1]
           
    print "last message"
    conn.close()
    cl.terminate()
print "end client"
