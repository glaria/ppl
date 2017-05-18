# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env
import string

class tf_idf(MRJob):
    
    def mapper1(self,_,line):
        for x in string.punctuation:
            line = line.replace(x,' ')
        for word in line.split():
            yield (word.lower(), jobconf_from_env('map.input.file')), 1

    def reducer1(self,key,counter):
        k = sum(counter)
        yield key[1],(key[0],k) #devuelvo como key el nombre del doc

    def reducer11(self,key,lista):
        count = 0
        words = []
        k = []
        for j in lista:
            count += j[1]
            words.append(j[0])
            k.append(j[1])
       
        for i in range(len(k)):
            yield words[i], (key,k[i],count) #devuelve (word,doc)->(count(w in doc), total(doc))

    def mapper2(self,key,lista): #quiero sacar el numero de doc
        yield key, (lista[0],lista[1],lista[2],1)
  
    def reducer2(self,key,lista):
        documents = []
        

    def steps(self):
        return [
            MRStep(mapper = self.mapper1,
                    reducer = self.reducer1),
            MRStep(reducer = self.reducer11),
            #MRStep(mapper = self.mapper2,
                    #reducer = self.reducer2), 
        ]

            

if __name__ == '__main__':
    tf_idf.run()
