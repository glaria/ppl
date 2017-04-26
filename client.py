# -*- coding: utf-8 -*-
from multiprocessing.connection import Client
from random import random
from time import sleep
from multiprocessing.connection import Listener
from multiprocessing import Process, Queue
from Tkinter import *
#falta import Tkinter
local_listener = (('127.0.0.1', 5002),'secret client 2 password')

def client_listener():
    cl = Listener(address=local_listener[0], authkey=local_listener[1])
    print '..client listener starting' 
    print '..accepting conexions'
    while True:
        conn1 = cl.accept()
        #print '..connection accepted from', cl.last_accepted        
        m = conn1.recv()
        #print '..message received from server', m 
        if m[0]=="server_notify_go_online_user":
        	message = "Se ha conectado el usuario", m[1]
        	queue.put(["1",message])
        	print message
        elif m[0]=="server_notify_quit_user":
		message = "Se ha desconectado el usuario", m[1]
		queue.put(["1",message])
		print message
	elif m[0]=="server_notify_chat":
		message = "[",m[1][0],"] dice:",m[1][1]
		queue.put(["1",message])
		print message
	elif m[0]=="server_notify_inbox":
		message = "Mensajes recibidos mientras estabas desconectado",m[1]
		queue.put(["1",message])
		print message

if __name__ == '__main__':
    root = Tk()
    
    root.title("Message")
    root.resizable(0, 0)
    
    frame = Frame(root)    
    frame.pack()
    queue = Queue()
    w = 600
    h = 200
    canvas = Canvas(frame, width=w, height=h, bg="green")
    canvas.grid(row=0, column=0, columnspan=90)
    
    obj =[]
    obj.append(canvas.create_text(210,70,text="Mensajes desde otros clientes"))
    obj.append(canvas.create_text(150,105,text="Respuestas del servidor"))
    
    
    user_l = Label(frame,text="Usuario")
    user_l.grid(row=1, column=0)
    user_e = Entry(frame,width=10,textvariable="") 
    user_e.grid(row=1, column=1)

    pass_l = Label(frame,text="Pass")
    pass_l.grid(row=2, column=0)
    pass_e = Entry(frame,width=10,textvariable="")
    pass_e.grid(row=2, column=1)
    
    mens_l= Label(frame,text="Mensaje")
    mens_l.grid(row=3, column=0)
    mens_e = Entry(frame,width=10,textvariable="")
    mens_e.grid(row=3, column=1)

    print 'trying to connect'
    conn = Client(address=('127.0.0.1', 6000), authkey='server')
    
    cl = Process(target=client_listener, args=())
    cl.start()
    conectado = True
    connected = False
    def connect_r():
        typo = "new_user"
        usuario = user_e.get()
        password = pass_e.get()
        addressbook = []
        conn.send([(usuario,password), typo, addressbook])
        mens = conn.recv()
        queue.put(["2",mens])
    register=Button(frame, text="Register", command=connect_r, width=7)
    register.grid(row=1, column=3)
    
    def login():
        typo = "go_online"
        global usuario
        usuario = user_e.get()
        global password
        password = pass_e.get()
        conn.send([(usuario,password), typo, local_listener])
        mens = conn.recv()
        queue.put(["2",mens])
    login_b =Button(frame, text="Login", command=login, width=7)
    login_b.grid(row=2, column=3)
    
    def send_m():
        sendto = user_e.get()
        message = mens_e.get()
        conn.send([(usuario,password), "chat", (sendto,message)])
        mens = conn.recv()
        queue.put(["2",mens])
    send_b = Button(frame, text="Send", command=send_m, width=7)
    send_b.grid(row=3, column =3)
    
    def add_n():
        nuevo = user_e.get()
        conn.send([(usuario,password), "add_contact", nuevo])
        mens = conn.recv()
        queue.put(["2",mens])
    add_b = Button(frame, text="Add", command=add_n, width=7)
    add_b.grid(row=1, column =4)
    
    def quit_c():
        conn.send(([(usuario,password), "quit", []]))
        queue.put("bye")
    quit_b = Button(frame, text="Quit", command=quit_c, width=7)
    quit_b.grid(row=2, column =4)
    
    try:
        while True:
 
            if not queue.empty():
                s = queue.get()
                if s == "bye":
                    break
                else:
                    if s[0] == "1":
                        canvas.itemconfigure(1,text=str(s[1]))
                        sleep(2)
                    else:
                        canvas.itemconfigure(2,text=str(s[1]))
            root.update()

    except TclError:
        pass 

    print "last message"
    conn.close()
    cl.terminate()
    print "end client"
