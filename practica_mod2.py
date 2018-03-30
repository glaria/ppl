import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Circle, ConnectionPatch
import matplotlib.lines as mlines
from matplotlib.widgets import TextBox
from numpy import random #creo que no es necesario
from numpy.linalg import inv



#Verificar la posicion de los botones, del textbox 
#Por hacer, crear la funcion classify, annadir botones, annadir el metodo de fisher

#importante, para dos clases no funciona (quizas por eso es que piden que hagamos dos funciones distintas....)
#ni idea, pero para 2 clases si que me funciona, aunque la X salia vacia :(


def least_square(X,T): #importante controlar como entran X y T
	Z = np.zeros((X.shape[0]+1,X.shape[1]))

	X=X.transpose()
	Z=Z.transpose()

	for i in range(X.shape[0]):
		Z[i] = np.append(1,X[i])

	Z=Z.transpose()
	X=X.transpose()

	A= inv(np.dot(Z,Z.transpose()))
	B= np.dot(Z,T)

	return np.dot(A,B)

def hiperplanos(W,K):

	hiperplanos = K*(K-1)/2 #numero de hiperplanos

	m_hiperplanos = np.zeros((hiperplanos,3)) #matriz donde se guardn los hiperplanos

	count = 0
	for i in range(K):
	    for j in range(K):
	        if j > i:
	            m_hiperplanos[count] = W.T[i]- W.T[j] 
	            count += 1

	#vigilar si m_hiperplanos debe ir traspuesto o no, asi como en W (creo que si)

	
	return (m_hiperplanos.T)	

#sobre los colores: usaremos {'b', 'g', 'r', 'c', 'm', 'y', 'k'} si hay mas de 7 clases las siguientes seran escalas de grises
class CreatePoints(object):
    """Draw and drag points.

    Use left button to place points.
    Points are draggable. Use right button
    to disconnect and print and return
    the coordinates of the points.

    Args:
         fig: matplotlib figure
         ax: matplotlib axes
    """
    
    def __init__(self, fig, ax):
	self.class_list = dict() #diccionario con formato clase: color
	self.color_list = ['b', 'g', 'r', 'c', 'm', 'y', 'k']

        self.circle_list = [] #no debe ser un array de numpy, porque no sabemos que tamanno tendra a priori, y numpy es ineficiente para estos casos

        self.x0 = None
        self.y0 = None

        self.fig = fig
        self.ax = ax

	axbox = plt.axes([0.15, 0.02, 0.1, 0.07])
	self.class_box = TextBox(axbox, 'Clase Actual', initial='')
	self.class_box.on_submit(self.submit)

	axmbox = plt.axes([0.5, 0.02, 0.1, 0.07])
	self.method_box = TextBox(axmbox, 'Metodo', initial='lsq')
	self.method_box.on_submit(self.metodo)
	self.metodo_actual = None
	
        
	#axblda = plt.axes([0.6, 0.02, 0.2, 0.07])
	#blda = Button(axblda, 'Fisher LDA')

        self.cidpress = fig.canvas.mpl_connect(
            'button_press_event', self.on_press)
        self.cidrelease = fig.canvas.mpl_connect(
            'button_release_event', self.on_release)
        self.cidmove = fig.canvas.mpl_connect(
            'motion_notify_event', self.on_move)

        self.press_event = None
        self.current_circle = None
	self.moving_class = None

    def submit(self, text):
	self.current_clas = str(text)
	if self.current_clas not in self.class_list:
		if self.color_list: #si la lista de colores disponibles no esta vacia
			self.class_list[self.current_clas] = self.color_list.pop()
		else:
			self.class_list[self.current_clas] = str(random.random()) # le asignamos un color aleatorio de la escala de grises en caso que hayamos agotado el pool de colores
			
    def metodo(self,text):

	self.metodo_actual = (str(text))

    def drawing(self,hiperplanos_graf):
	for i in range(len(hiperplanos_graf.T)):
	    #xA coordenada x del punto A, yB coordenada y del punto B
	    xA = (-hiperplanos_graf[0,i] -hiperplanos_graf[2,i]*40)/hiperplanos_graf[1,i] #importante que esto sea distinto de cero
	    yB = (-hiperplanos_graf[0,i] -hiperplanos_graf[1,i]*40)/hiperplanos_graf[2,i] #importante que esto sea distinto de cero

	    xmin, xmax = ax.get_xbound()
	    p1 = [xA,40]
	    p2 = [40, yB]
	    if(p2[0] == p1[0]):
        	xmin = xmax = p1[0]
       		ymin, ymax = ax.get_ybound()
    	    else:
        	ymax = p1[1]+(p2[1]-p1[1])/(p2[0]-p1[0])*(xmax-p1[0])
        	ymin = p1[1]+(p2[1]-p1[1])/(p2[0]-p1[0])*(xmin-p1[0])

	    if self.colores:
		line_color = self.colores.pop()
	    else:
		line_color = str(random.random())

    	    l = mlines.Line2D([xmin,xmax], [ymin,ymax], color = line_color)
    	    self.ax.add_line(l)


	

    def on_press(self, event): #solo se debe activar bajo determinadas circunstancias, hay que annadirle un if

        x0, y0 = event.xdata, event.ydata
        if len(self.circle_list) > 1 and event.button == 3:

	    self.colores = ['b', 'g', 'r', 'c', 'm', 'y', 'k']
	    for i in range(len(self.ax.lines)-1, -1 , -1): #recorro las rectas almacenadas en ax, el orden debe ser inverso porque esta lista es dinamica ??

		self.ax.lines[i].remove()
	    	self.fig.canvas.draw()


    
	    class_list = set(i[0] for i in self.circle_list) #lista de clases
	    datos_clase = {key: [] for key in class_list} #se inicializa un diccionario vacio donde iran class: [dato1, dato2...]
	    for element in self.circle_list:
		datos_clase[element[0]].append(element[1].center)
	    
	    K = len(class_list) #numero de clases
	    N = len(self.circle_list)

	    
	    #inicializamos X y T

	    X = np.zeros((2,N))
	    T = np.zeros((K,N))
	    i = 0
	    k = 0
	    for clase in datos_clase: #construccion de T
	    	for j in range(len(datos_clase[clase])):
			T[i,k] = 1
			X[:,k] = datos_clase[clase][j]
			k += 1
		i += 1
	    if self.metodo_actual == 'lsq':
	    	W = least_square(X,T.T) #cambiar la funcion para no tener que trasponer aqui
	    elif self.metodo_actual == 'lda':
		print 'aqui ira el clasificador de fisher'

	    
	    try:
	        hiperplanos_graf = hiperplanos(W,K)	
	        self.drawing(hiperplanos_graf)
	        self.fig.canvas.draw()
	    except NameError:
		pass #se podria intentar hacer highlight en el textbox del metodo, investigar esto

            return

	if x0 > 10 and x0 < 40 and y0 > 10 and event.button == 1: #para no sacar puntos del text box

		class_color = self.class_list[self.current_clas]
		
	
	        for element in self.circle_list:
		    circle = element[1]
	            contains, attr = circle.contains(event)
	            if contains:
	                self.press_event = event
	                self.current_circle = circle
			self.moving_class = element[0]
	                self.x0, self.y0 = self.current_circle.center
	                return
	
	        c = Circle((x0, y0), 0.5, color = class_color)
	        self.ax.add_patch(c)
	        self.circle_list.append((self.current_clas,c))

	        self.current_circle = None
	        self.fig.canvas.draw()

    def on_release(self, event):
        self.press_event = None
        self.current_circle = None

    def on_move(self, event):
        if (self.press_event is None or
            event.inaxes != self.press_event.inaxes or
            self.current_circle == None):
            return
        
        dx = event.xdata - self.press_event.xdata
        dy = event.ydata - self.press_event.ydata
	try: #una vez el punto ha sido eliminado pueden haber errores
	    self.circle_list.remove((self.moving_class, self.current_circle))
	except:
	    pass
	if self.x0 + dx >= 40:
	    self.current_circle.center = self.x0 + dx + 10, self.y0 + dy #asi eliminamos puntos
	else:
            self.current_circle.center = self.x0 + dx, self.y0 + dy
	    self.circle_list.append((self.moving_class, self.current_circle))

        self.fig.canvas.draw()


if __name__ == '__main__':

    fig = plt.figure()
    ax = plt.subplot(111)
    ax.set_xlim(10, 41)
    ax.set_ylim(10, 40)
    ax.set_aspect('equal')
    fig.subplots_adjust(bottom=0.15)
    plt.axvspan(40, 41, color='red', alpha=0.5)
    
    start = CreatePoints(fig, ax)
    plt.show()
