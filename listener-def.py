#hay que seguir cambiando cosas desde listener-t
#!/usr/bin/env python
# -*- coding: utf-8 -*-
#hasta ahora deja enviar mensajes entre clientes online
from multiprocessing.connection import Listener
from multiprocessing import Process, Manager
from multiprocessing.connection import Client

from time import time,sleep

def notify_new_client(id,clients): 
    for client, client_info in clients.items():
        if not client == id:
            print "sending new client to", client
            conn = Client(address=client_info[0], authkey=client_info[1])
            conn.send(("server_notify_go_online_user", id))
            conn.close()

def notify_quit_client(id,clients): 
    for client, client_info in clients.items():
            print "sending quit client to", client
            conn = Client(address=client_info[0], authkey=client_info[1])
            conn.send(("server_notify_quit_user", id))
            conn.close()
def check_inbox(usuario,inbox):
    print "checking inbox", usuario
    for registro, registro_info in inbox.items():#comprobar esto
	print registro
	if registro==usuario:
		return ("server_notify_inbox",registro_info)
		#conn = Client(address=registro[0], authkey=registro_info[1])
		#conn.send(("server_notify_inbox",registro_info))
		#conn.close()
def send_message(destino,mensaje,clients):
    print "hola", destino,mensaje
    if destino in clients:
           conn = Client(address=clients[destino][0], authkey=clients[destino][1])
           conn.send(("server_notify_chat", mensaje))
    else:
           inbox[destino]=mensaje
	   print "inbox", inbox
def serve_client(conn, clients,users,inbox):
    connected = True
    while connected:
        try:
            m = conn.recv()
        except EOFError:
            print 'connection abruptly closed by client'
            connected = False            
            print m[0][0], 'connection closed'
        nick = m[0][0]
        password = m[0][1]
        clave = m[1]
        if clave == "go_online": 
            if nick in users:
                if users[nick] == password:
                    client_info = m[2]
                    clients[nick] = client_info
                    notify_new_client(nick, clients)
                    conn.send(["notify_go_online",True])
                else:
                    conn.send(["notify_go_online",(False, "wrong password")])
            else:
                conn.send(["notify_go_online",(False,"el usuario no existe")])
#hasta aqui, mirar en listener-t.py       
            connected = False
            del clients[m[0][0]]                       
            notify_quit_client(m[0][0], clients)            
            print m[0][0], 'connection closed'
            conn.close() 
        elif m[1] == "go_online":
            print "por alla"
            if m[0][0] in users:
	    	if users[m[0][0]] == m[0][1]:	#si (id,pass) ok
                 	print "hola"
                	client_info = m[2]
                	clients[m[0][0]] = client_info
                	notify_new_client(m[0][0], clients)
                        conn = Client(address=client_info[0],authkey=client_info[1])
                        conn.send(check_inbox(m[0][0],inbox))
		else: #wrong pass
                	message = ["notify_connect", (False, "contraseña incorrecta")]
                        conn = Client(address=m[2][0], authkey=m[2][1])
                        conn.send(message)
            	client_info = m[2]
            	clients[m[0][0]] = client_info
            	notify_new_client(m[0][0], clients)
	    else: #registro nuevo usuario
		client_info = m[2]
                clients[m[0][0]] = client_info
                users[m[0][0]] = m[0][1] #registro del nuevo user
		print "users", users
		print "clients", clients
		notify_new_client(m[0][0], clients)
        elif m[1] == "chat":
	    print "por aqui"
            sendto = m[2][0]
            message = m[2][1]
            if sendto in users:
            	send_message(sendto,(m[0][0],message),clients)
            else:
		print "no existe el usuario", sendto

if __name__ == '__main__':
    listener = Listener(address=('127.0.0.1', 6000), authkey='secret password server')
    print 'listener starting'

    m = Manager()
    clients = m.dict()
    users = m.dict() #registro de todos los usuarios existentes
    inbox = m.dict() #mensajes recibidos mientras estaba offline, {id,(from,message)}
    
    while True:
        print 'accepting conexions'
        conn = listener.accept()
        print 'connection accepted from', listener.last_accepted

        p = Process(target=serve_client, args=(conn,clients,users,inbox))
        p.start()
    listener.close()
print 'end server'
