# -*- coding: utf-8 -*-


#Funciona!!!!!
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env
import string

class tf_idf(MRJob):
    
    def mapper(self,_,line):
        for x in string.punctuation:
            line = line.replace(x,' ')
        yield "doc", jobconf_from_env('map.input.file')
        for word in line.split():
            yield (word.lower(), jobconf_from_env('map.input.file')), 1

    def reducer(self,key,counter):
        list_doc = []
        if key == "doc":
            for data in counter:
                if data not in list_doc:
                    list_doc.append(data)
        else:
            yield key[1],(key[0],sum(counter))  #key[0] aparece k veces en key[1]
        if len(list_doc) > 0:
            for doc in list_doc:
                yield doc,(".total*.", len(list_doc))

    def filtering(self,key,lista):
        first = lista.next()
        assert first[0] == ".total*."
        total = first[1]

        for info in lista:
            yield info[0],(key,info[1],total)

    def final(self,key,lista):
        docs = []
        freq = []
        for info in lista:
            docs.append(info[0])
            freq.append(info[1])
            total = info[2]
        for i in range(len(docs)):
            yield (key,docs[i]),(freq[i],len(docs),total)

    
    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                    reducer = self.reducer),
            MRStep(reducer = self.filtering),
            MRStep(reducer = self.final)
        ]

            

if __name__ == '__main__':
    tf_idf.run()
