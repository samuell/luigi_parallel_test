# -*- coding: utf-8 -*-

import luigi
import time

class InData(luigi.ExternalTask):
    
    chunk_no = luigi.IntParameter(default=0)
    
    def output(self):
        return luigi.LocalTarget("indata_%d.txt" % self.chunk_no)
              

class Merged(luigi.Task):
        
    def requires(self):
        return [InData(i) for i in xrange(4)]
    
    def output(self):
        return luigi.LocalTarget("output.txt")
        
    def run(self):
        datas = []
        
        for input in self.input():
            with input.open('r') as in_file:
                for line in in_file:
                    time.sleep(0.001)
                    datas.append(line.strip())
                    
        with self.output().open('w') as out_file:
            for data in datas:
                print >> out_file, data
                
if __name__ == '__main__':
    luigi.run(main_task_cls=Merged)
                