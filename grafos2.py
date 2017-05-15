# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep

class Grafos_3_ciclos(MRJob):
    def mapper(self, _, line):
        if len(line) > 0:
            line = line.replace('"', '')
            w = line.split(",")
            if w[0] != w[1]:
                yield w[1], w[0]
                yield w[0], w[1]
    
    def reducer(self, clave, lista):
        t = []
        for nodo in lista:
            if nodo not in t:
                t.append(nodo)
                if clave < nodo:
                    yield (clave,nodo),list(set(t))
                else:
                    yield (nodo,clave),list(set(t))

    def filtering(self,clave,dato):
        dato1 = dato.next()
        dato2 = dato.next()
        if len(set(dato1)&set(dato2)) > 0:
            for i in set(dato1)&set(dato2):
                aux = sorted(list((clave[0],clave[1],i)))
                yield (aux[0],aux[1]),aux[2]

    def cleaning(self, arista, lista): #quito repetidos
        s = []
        for elemento in lista:
            if elemento not in s:
                s.append(elemento)
                yield (arista[0],arista[1],elemento), None
      
    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                    reducer = self.reducer),
            MRStep(reducer = self.filtering),
            MRStep(reducer = self.cleaning) 
        ]

if __name__ == '__main__':
    import sys
    sys.stderr = open('localerrorlog.txt','w')
    Grafos_3_ciclos.run()
