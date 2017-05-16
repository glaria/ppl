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
        yield key, sum(counter)
 
 if __name__ == '__main__':
    tf_idf.run()
