
   # -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep
import collections

class Grafos(MRJob):
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
        for nodo in t:
            if clave < nodo:
              
                yield (clave,nodo),(len(set(t)),None)
            else:

                yield (nodo,clave),(None,len(set(t)))
    def filtering(self,clave,dato):
        dato1 = dato.next()
        dato2 = dato.next()
        if dato1[0] == None:
            yield clave, [dato2[0],dato1[1]]
        else:
            yield clave, [dato1[0],dato2[1]]

    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                    reducer = self.reducer),
            MRStep(reducer = self.filtering) 
        ]

if __name__ == '__main__':
    import sys
    sys.stderr = open('localerrorlog.txt','w')
    Grafos.run()                     
