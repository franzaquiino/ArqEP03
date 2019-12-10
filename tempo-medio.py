#media de corrida por dia em minutos
from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class temp_med(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]

    def mapper(self, _, line):
        f = '%Y-%m-%d %H:%M:%S'
        c = line.split(',')
       
        if c[16]!='total_amount':
            dif = (datetime.strptime(c[2], f) - datetime.strptime(c[1], f)).total_seconds()
            dia = c[1].split(' ')
            yield dia[0], dif / 60


    def reducer(self, key, values):
        i,conta,soma=0,0,0
        for i in values:
            conta += 1
            soma += i 
        yield key, soma/float(conta)

if __name__ == '__main__':
    temp_med.run()
