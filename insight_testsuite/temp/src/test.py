# -*- coding: utf-8 -*-
"""
Created on Fri Mar 31 00:52:52 2017

@author: Parimala Killada
"""
import re,pandas as pd
import operator
from datetime import datetime,timedelta
from collections import defaultdict, Counter
#from itertools import chain
class LogAnalyzer():
    def __init__(self):
        self.linecount = 0
        self.counters = defaultdict(Counter)
        self.df =pd.DataFrame(columns=['host','timestamp','request','status','bytes'])
        self.loglist =[]
        self.top_bandwidth= defaultdict(Counter)
        self.dt_start=datetime.now()
        self.requests=[]
        self.current_counter =1
        self.start =None
    def analyze(self, logfile):
        with open(logfile,'r',encoding="ISO-8859-1") as f:
            for line in f:
                kwargs=self._parse(line)
                self._update(**kwargs) 
                #self.df =pd.DataFrame.from_records(self.loglist)
    def summarize(self,writefile1,writefile2,topcount=10):
        
        #Lists topcount for all variables        
        #for key in self.counters:
        #list = sorted(self.counters[key].items(), key=lambda x: x[1], reverse=True)
        #list = list[:topcount]
 
        #we need hosts count only        
        #print(self.df.head(10))
        #print(len(self.loglist))
        top_hosts = self.counters['host'].most_common(topcount)
        hosts = open(writefile1, 'w')
        for l in top_hosts:
            hosts.write(l[0]+','+str(l[1])+'\n')
        hosts.close() 
        
        #top 10 bandwidth consumption resources
        
        test =[]
        for key, c in self.top_bandwidth.items():
            test.append([key,c['bytes']/(sum(c.values())-c['bytes'])])
        test.sort(key=operator.itemgetter(1),reverse=True)
        top_resources = test[:topcount]
        
        resources = open(writefile2, 'w')
        for l in top_resources:
            resources.write(l[0].split(' ')[1]+'\n')
        resources.close()
        
        print(self.requests)
        #chain.from_iterable(top_bandwidth.itervalues())
        #for key in top_bandwidth:
             #top_resources = sorted(top_bandwidth[key].items(), key=lambda x: x[1], reverse=True)
             #top_resources = top_resources[:topcount]
        #top_resources = top_bandwidth.most_common(topcount)
        #print(top_resources)
    
   #@staticmethod
    def _parse(self,line):
        #regex = r'^([^\s]+)\s[\s-]+.*\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]\s(.*\"\w+\s+[^\s]+\s+.*\w\")\s(\d{3})\s([\d]+|[\-]$)'
        #r'(?P<request>.*\"\w+\s+[^\s]+\s+.*\w\")\s'
        pattern = re.compile(
        r'(?P<host>[^\s]+|)\s[\s-]+'
        r'.*\[(?P<timestamp>\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]\s'
        r'"(?P<request>.*?)"\s'
        r'(?P<status>\d{3})\s'
        r'(?P<bytes>[\d]+|[\-]$)'
        )
        
        #match() return 'None' if no match is found
        m = pattern.match(line)
        if m != None:
           #res = m.groups()
           #self.df =self.df.append({'host':res[0],
           #'timestamp':res[1],
           #'request':res[2],'status':res[3],'bytes':res[4]},ignore_index=True)
           #self.df =pd.concat([self.df,newentry])
           
           fsent=m.group('bytes').strip().replace('-','0')
           return {'host':m.group('host'), 'timestamp':m.group('timestamp'), \
           'request':m.group('request'),\
                'status':m.group('status'),'bytes':fsent}
        #return {'host':res[0]}
        else:
            print(line)
            return {'host':'Wrong_entry'}
    
    def _update(self, **kwargs):
          self.loglist.append(kwargs.copy())
          #self.df=pd.DataFrame.from_dict(kwargs,orient='columns')
          
          for key, value in kwargs.items():
             self.counters[key][value] += 1
          
          #updating top resources default counter
          key =kwargs['request']
          self.top_bandwidth[key][kwargs['host']] += 1
          self.top_bandwidth[key]['bytes'] += int(kwargs['bytes'])
          
          #Updating 60-second window 
          key1 = kwargs['timestamp']
          time_format ='%d/%b/%Y:%H:%M:%S %z'
          
          
          window_size =timedelta(minutes=60)
          if self.linecount == 0:
              self.dt_start = datetime.strptime(key1,time_format)
              self.start =key1
          else:
              new_time = datetime.strptime(key1,time_format)
              if new_time - self.dt_start < window_size and \
                    new_time.minute == self.dt_start.minute:
                          self.current_counter += 1
              else:
                  self.requests.append([self.start,self.current_counter])
                  self.dt_start = new_time
                  self.current_counter =1
                  self.start = key
          
           
          self.linecount += 1
          #dt_end=dt+datetime.timedelta(minutes =60)
          #self.top_busiest[key][kwargs['request']] +=1
          
if __name__ == '__main__':
    logfile = 'I:/Parimala/Coding Challenge 2017/fansite-analytics-challenge-master/log_input/log.txt'
    testlogfile = 'I:/Parimala/Coding Challenge 2017/fansite-analytics-challenge-master/insight_testsuite/tests/test_features/log_input/log.txt'
    writefile1 ='I:/Parimala/Coding Challenge 2017/fansite-analytics-challenge-master/log_output/hosts.txt'
    writefile2 ='I:/Parimala/Coding Challenge 2017/fansite-analytics-challenge-master/log_output/resources.txt'
    summary = LogAnalyzer()
    summary.analyze(testlogfile)
    summary.summarize(writefile1,writefile2)       