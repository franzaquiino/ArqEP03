#Consolidar  cobranças por dia

from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime

class Cob(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]

    def mapper(self, _, line):
        c = line.split(',')
        
        if c[16]!='total_amount':
            dia = c[1].split(' ')
            valor = float(c[16])
            yield dia[0], valor


    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    Cob.run()
