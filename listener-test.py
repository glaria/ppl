from multiprocessing.connection import Listener
from multiprocessing import Process, Manager, Lock
from multiprocessing.connection import Client

from time import time

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

def serve_client((conn,last_accepted,clients,users,inbox,s,t,u)):
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
					notify_new_client(nick, clients)
					conn.send(["notify_go_online",True])
				else:
					conn.send(["notify_go_online",(False, "wrong password")])
			else:
				conn.send(["notify_go_online",(False,"el usuario no existe")])
			#conn.send("notify_new", False)
		elif clave == "new_user":
			if nick not in users:
				users[nick] = password
				conn.send(["notify_new",True])
			else:
				conn.send(["notify_new", (False, "ya existe el usuario")])
		elif clave == "quit":
			connected = False
			conn.send(["notify_quit",True]) #el cliente debe esperar a recibir este True para desconectar
		#elif clave == "chat":
			 
	del clients[id]                       
	notify_quit_client(id, clients)            
	print id, 'connection closed'


if __name__ == '__main__':
	listener = Listener(address=('127.0.0.1', 6000), authkey='secret password server')
	print 'listener starting'

	m = Manager()
	clients = m.dict()
	users = m.dict()
	inbox = m.dict()
	s = Lock()
	t = Lock()
	u = Lock()
	while True:
		print 'accepting conexions'
		conn = listener.accept()
		print 'connection accepted from', listener.last_accepted
		p = Process(target=serve_client, args=(conn,listener.last_accepted,clients,users,inbox,s,t,u))
		p.start()
	listener.close()
	print 'end server'
