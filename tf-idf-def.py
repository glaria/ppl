# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env
import string

class tf_idf(MRJob):
    
    def mapper(self,_,line):
        for x in string.punctuation:
            line = line.replace(x,' ')
        for word in line.split():
            yield (word.lower(), jobconf_from_env('map.input.file')), 1

    def reducer(self,key,counter):
        k = sum(counter)
        yield ("doc",key[1]), k #numero de apariciones de key[0] en key[1] la suma de esto me dara el total
        yield key, k


    def filtering(self, key, total):
        if key[0] == "doc":
            yield (10,key[1]), sum(total) #total de palabras en el doc key[1]
        else:
            yield key, sum(total)

    def cleaning(self,key,total):    
        

    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                    reducer = self.reducer),
            MRStep(reducer = self.filtering) 
        ]

            

if __name__ == '__main__':
    tf_idf.run()
