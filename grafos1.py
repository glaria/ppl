# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep
import collections

class Grafos(MRJob):
    def mapper(self, _, line):
        if len(line) > 0:
            line = line.replace('"', '')
            w = line.split()
            print w
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
                print (clave,nodo),(len(set(t)),None)
                yield (clave,nodo),(len(set(t)),None)
            else:
                print (nodo,clave),(None,len(set(t)))
                yield (nodo,clave),(None,len(set(t)))
    def filtering(self,clave,dato):
        l = [0,0]
        dato1 = dato.next()
        dato2 = dato.next()
        if dato1 == None:
            l[0] = dato2
        else:
            l[1] = dato1
        yield clave, l

    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                    reducer = self.reducer),
            MRStep(reducer = self.filtering) 
        ]

if __name__ == '__main__':

    Grafos.run()
                        
