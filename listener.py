#!/usr/bin/env python
# -*- coding: utf-8 -*-
#hasta ahora deja enviar mensajes entre clientes online
from multiprocessing.connection import Listener
from multiprocessing import Process, Manager, Lock
from multiprocessing.connection import Client

from time import time,sleep

def notify_new_client(id,clients,addressbook): 
    for client, client_info in clients.items():
        if not client == id and client in addressbook[id]:
            print "sending new client to", client
            conn = Client(address=client_info[0], authkey=client_info[1])
            conn.send(("server_notify_go_online_user", id))
            conn.close()

def notify_quit_client(id,clients,addressbook): 
    for client, client_info in clients.items():
            if client in addressbook[id]:
                print "sending quit client to", client
                conn = Client(address=client_info[0], authkey=client_info[1])
                conn.send(("server_notify_quit_user", id))
                conn.close()
def check_inbox(usuario,clients,inbox):
    print "checking inbox", usuario
    for registro, registro_info in inbox.items():#comprobar esto
	    print registro
	    if registro==usuario:
	        if inbox[usuario] != []:
		        for i in registro_info:
		            conn = Client(address=clients[usuario][0], authkey=clients[usuario][1])
		            conn.send(("server_notify_inbox",i))
		        del inbox[usuario]
		        conn.close()
def send_message(usuario,destino,mensaje,clients,inbox):
    if destino in clients:
           conn = Client(address=clients[destino][0], authkey=clients[destino][1])
           conn.send(["server_notify_chat",(usuario,mensaje)])
    else:
        
        inbox[destino]+=[(usuario,mensaje)]
  
	print "inbox", inbox
    
def serve_client(conn, clients,users,inbox,addressbook):
    connected = True
    while connected:
        try:
            m = conn.recv()
            nick = m[0][0]
            password = m[0][1]
            clave = m[1]
            if clave == "go_online": 
                s.acquire()
                if nick in users:
                    if users[nick] == password:
                        client_info = m[2]
                        clients[nick] = client_info
		        conn.send(["notify_go_online",(True,"message")])
                        notify_new_client(nick, clients,addressbook) #solo se le deberia pasar a la addressbook de nick y que a su vez lo tengan en la suya (python set)
                        u.acquire()
		        check_inbox(nick,clients,inbox) #desde aqui se le envia el mensaje directamente al listener del cliente nick
                        u.release()
                    else:
                        conn.send(["notify_go_online",(False, "wrong password")])
                else:
                    conn.send(["notify_go_online",(False,"el usuario no existe")])
                s.release()
            elif clave == "new_user":
	            agenda = m[2]
	            if nick not in users:
                        users[nick] = password
		        #check_addressbook(agenda)
		        addressbook[nick] = agenda#=check_addressbook(agenda)
		        inbox[nick] = []
                    
       		        conn.send(["notify_new",(True,"message"),addressbook[nick]]) #envia la addressbook actualizada
	            else:
		        conn.send(["notify_new", (False, "ya existe el usuario")])
            elif clave == "quit":
		        connected = False
		        conn.send(["notify_quit",(True, "")]) #el cliente debe esperar a recibir este True para desconectar
                        del clients[nick]
		        notify_quit_client(nick,clients,addressbook) 
                
		        #verificar que el del clients[nick] se hace al final y fuera del while
	    elif clave == "chat": #queda por poner las limitaciones referentes a la addressbook
		destino = m[2][0]
	        mensaje = m[2][1]
                if destino in addressbook[nick]: #si lo tienes agregado
		    if destino in users: #da igual que este conectado  o no
		        conn.send(["notify_chat",(True,"message")])
		        send_message(nick,destino, mensaje,clients,inbox)
		else:
		    conn.send(["notify_chat",(False,"el usuario no esta en tu addressbook")])
	    elif clave == "add_contact":
                nuevo = m[2]
                if nuevo in users:
                    if nuevo in addressbook[nick]:
                        respuesta = (False, "ya existe en tu addressbook")
                    else:
                        addressbook[nick] += [nuevo]
                        print "adress", addressbook
                        respuesta = (True,"message")
                else:
                    respuesta = (False, "no existe el usuario")
		conn.send(["notify_add_contact", respuesta])

        except EOFError:
            print 'connection abruptly closed by client'
            connected = False            
            del clients[nick]
            notify_quit_client(nick,clients,addressbook)
        print "users", users
        print "clients", clients
    #del clients[nick] #ojo, debe existir nick
		


if __name__ == '__main__':
    listener = Listener(address=('127.0.0.1', 6000), authkey='server')
    print 'listener starting'

    m = Manager()
    clients = m.dict()
    users = m.dict() #registro de todos los usuarios existentes
    inbox = m.dict() #mensajes recibidos mientras estaba offline, {id,(from,message)}    
    addressbook = m.dict()
    #faltaria poner los semaforos
    s= Lock() #s-> users, t->clients, u->inbox, v->addressbook
    u = Lock()
    t = Lock()
    v = Lock()
    while True:
        print 'accepting conexions'
        conn = listener.accept()
        print 'connection accepted from', listener.last_accepted

        p = Process(target=serve_client, args=(conn,clients,users,inbox,addressbook))
        p.start()
    listener.close()
print 'end server'
