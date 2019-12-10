#Valor médio de corrida a cada 15 minutos
from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class AVG_VLR(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]

    def mapper(self, _, line):
        f = '%Y-%m-%d %H:%M:%S'
        c = line.split(',')
        
        if c[16]!='total_amount':
            data = datetime.strptime(c[1], f)
            minutos= (data.minute//15) * 15
            newdata = data.replace( minute=minutos,second=0).strftime("%Y-%m-%d %H:%M:%S")
            yield newdata, float(c[16])


    def reducer(self, key, values):
        i,conta,soma=0,0,0
        for i in values:
            conta += 1
            soma += i 
        yield key, soma/float(conta)

if __name__ == '__main__':
    AVG_VLR.run()

