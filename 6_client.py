# -*- coding: utf-8 -*-
from multiprocessing.connection import Client
from random import random
from time import sleep
from multiprocessing.connection import Listener
from multiprocessing import Process
''' 
envio array de 3 elementos, el primer elemento dice si es un login/register o un mensaje
si es un login/register los otros 2 elementos son nombre de usuario, contraseña
si es un mensaje son el destinatario y el texto del mensaje
'''
print 'trying to connect'
conn = Client(address=('127.0.0.1', 6000), authkey='secret password')
print 'connection accepted'
e = False #global?
cliente = raw_input('Name')
password = raw_input('Password')
conn.send([(cliente,password),"new_user",[]])
estado = conn.recv()
print estado[0]
e = estado[1]
while e: #bool, true 
    opciones = raw_input('Que quiere hacer?: 1->enviar mensaje, 2->consultar correo')
    if opciones == '1':
    	message = raw_input('Message to send? ')
    	sendto = raw_input('DESTINATARIO')
    	print 'sending message'
    	conn.send([(cliente,password),"send_mail",(sendto,message)])
    	answer = conn.recv() 
    	print 'received message', answer 
    else:
        conn.send([(cliente,password),"consulta",[]])
        print 'mis mensajes', conn.recv()

conn.close()

