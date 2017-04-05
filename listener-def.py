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
            
        nick = m[0][0]
        password = m[0][1]
        clave = m[1]
        if clave == "go_online": 
            if nick in users:
                if users[nick] == password:
                    client_info = m[2]
                    clients[nick] = client_info
		    conn.send(["notify_go_online",True])
                    notify_new_client(nick, clients) #solo se le deberia pasar a la addressbook de nick y que a su vez lo tengan en la suya
		    check_inbox(nick,clients,inbox) #desde aqui se le envia el mensaje directamente al listener del cliente nick
                else:
                    conn.send(["notify_go_online",(False, "wrong password")])
            else:
                conn.send(["notify_go_online",(False,"el usuario no existe")])
        elif clave == "new_user":
	    agenda = m[2]
	    if nick not in users:
                users[nick] = password
		#check_addressbook(agenda)
		addressbook[nick] = agenda#=check_addressbook(agenda)
       		conn.send(["notify_new",(True,"message"),addressbook[nick]]) #envia la addressbook actualizada
	    else:
		conn.send(["notify_new", (False, "ya existe el usuario")])
        elif clave == "quit":
		connected = False
		conn.send(["notify_quit",True]) #el cliente debe esperar a recibir este True para desconectar
		notify_quit_client(nick,clients) #hay que cambiar notify_quit para que ignore a nick
		#verificar que el del clients[nick] se hace al final y fuera del while
	elif clave == "chat": #queda por poner las limitaciones referentes a la addressbook
		destino = m[2][0]
		mensaje = m[2][1]
		if destino in users: #da igual que este conectado  o no
		    conn.send(["notify_chat",(True,"message")])
		    send_message(destino, mensaje,clients)
		else:
		    conn.send(["notify_chat",(False,"el usuario no existe")])
	#elif clave == "add_contact"
    del clients[nick] #ojo, debe existir nick
		


if __name__ == '__main__':
    listener = Listener(address=('127.0.0.1', 6000), authkey='secret password server')
    print 'listener starting'

    m = Manager()
    clients = m.dict()
    users = m.dict() #registro de todos los usuarios existentes
    inbox = m.dict() #mensajes recibidos mientras estaba offline, {id,(from,message)}
    addressbook = m.dict()
    while True:
        print 'accepting conexions'
        conn = listener.accept()
        print 'connection accepted from', listener.last_accepted

        p = Process(target=serve_client, args=(conn,clients,users,inbox,addressbook))
        p.start()
    listener.close()
print 'end server'
