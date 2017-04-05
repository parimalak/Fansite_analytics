
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 31 00:52:52 2017

@author: Parimala Killada
"""
import re,operator,sys
from datetime import datetime,timedelta
from collections import defaultdict, Counter
'''
Class has methods that performs tasks like
parsing,analyzing,extracting features and finally writing to specified file locations

1.'analyze' method input file is read line by line,calls 'parse' and 'update' methods
2.'parse' method uses regular expressions to group the input line and returns the results
in kwargs(key,value )format which were used in 'update' method
3.'update' method updates the data structures for four features
4. For Feature 3 and Feature 4 two more methods('time_converter,update_record)are also used.
5.Finally the method 'summarize' writes to the output files
'''

class LogAnalyzer():
    
    #initializing variables 
    def __init__(self):
        self.linecount = 0
        
        #feature1 Variables;
        #counters holds counts of hosts,statutes,bytes,requests and timestamps
        self.counters = defaultdict(Counter)
        
        #feature2 variables
        #Contains counts of total bytes,host counts for each request
        self.top_bandwidth= defaultdict(Counter)
        
        #feature3 Variables
        self.dt_start=None
        self.start_time=None
        #stores start of time stamp and total no of requests within 60 minutes period
        self.requests=[]
        self.current_counter =1
        self.start =None
        self.time_format ='%d/%b/%Y:%H:%M:%S %z'
        
        #Feature 4 Variables
        #blocked_id stores host name and its timestamp as key value pair
        self.blocked_ip={}
        #login_info stores time stamp and failed login count for host
        self.login_info={}
        #Flag is set only when the host has failed 3 login attempts 
        #and host is trying login during  block period
        self.line_flag = False
        
    #To avoid unneccesary looping to write records for feature4,
    #the incoming lines are captured to 'blocked.txt' file based on the flag
    def analyze(self, logfile,writefile4):
        with open(logfile,'r',encoding="ISO-8859-1") as f:
            blocked = open(writefile4, 'w',encoding="ISO-8859-1")
            #reading line by line from log file
            for line in f:
                #parsing to get host,timestamp,request,status,bytes sents in dictionary 
                kwargs=self._parse(line)
                #updates necessary datastructures for features
                self._update(**kwargs) 
                
                #writing line for feature4
                if self.line_flag ==True:
                   
                   blocked.write(line+'\n')
                   self.line_flag = False
            blocked.close()
            if self.current_counter != 1:
               self.requests.append([self.start,self.current_counter])
            
    def summarize(self,writefile1,writefile2,writefile3,topcount=10):
        
        #Lists topcount for all variables        
        #for key in self.counters:
        #list = sorted(self.counters[key].items(), key=lambda x: x[1], reverse=True)
        #list = list[:topcount]
 
        #counters has counts for all the variables
        
        #for feature 1 
        #top 10 most active host/ip address that accesed site   
        #written to the file in the format host,#of acceses
        top_hosts = self.counters['host'].most_common(topcount)
        hosts = open(writefile1, 'w')
        for l in top_hosts:
            hosts.write(l[0]+','+str(l[1])+'\n')
        hosts.close() 
        
        #feature2
        #top 10 bandwidth consumption resources
        #for each site request,
        #bandwidth is calculated as number of bytes sent/no.of acceses
        test =[]
        for key, c in self.top_bandwidth.items():
            #test.append([key,c['bytes']/(sum(c.values())-c['bytes'])])
            test.append([key,c['bytes']])
        test.sort(key=operator.itemgetter(1),reverse=True)
        top_resources = test[:topcount]
        
        #writing request to the file
        resources = open(writefile2, 'w')
        for l in top_resources:
            resources.write(l[0].split(' ')[1]+'\n')
        resources.close()
        
        #feature3
        #top 10 busiest/most frequent vistors in 60-minutes period
        self.requests.sort(key=operator.itemgetter(1),reverse=True)
        top_hours = self.requests[:topcount]
        
        #writing to file in the format timestamp,#of acceses
        hours = open(writefile3,'w')
        for h in top_hours:
            hours.write(h[0]+','+str(h[1])+'\n')
        hours.close()
        
    
   
    def _parse(self,line):
        #regular expression to match the foramt
        #'host' - - [DD/MM/YYYY :HH:MM:SS -tz] "request" status_code bytes_send 
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
           #replacing '-' to 0 for bytes 
           fsent=m.group('bytes').strip().replace('-','0')
           return {'host':m.group('host'), 'timestamp':m.group('timestamp'), \
           'request':m.group('request'),\
                'status':m.group('status'),'bytes':fsent}
        
        else:
            return {'host':'Wrong_entry'}
    
    #for Feature 3 &4
    #converts timestamp to datetime format 
    def time_converter(self,timestamp):
        return datetime.strptime(timestamp,self.time_format)
    
     
    #for Feature 4
    #records the entries during blocked time of 5 minutes and watch time of 20-seconds
    #login_info stores timestamp and failed login count 
    #for each hosts in watch time and block time
    def update_record(self,login_info,host,status,timestamp):
         record = self.login_info.get(host, None)
         if status == "401":
             #first login fail
             if not record:
                  self.login_info[host] = [self.time_converter(timestamp), 1]
             else:
                 #fail with in watch time of 20 seconds
                 if self.time_converter(timestamp) - record[0] > timedelta(seconds=20):
                     record = [self.time_converter(timestamp), 1]
                 else:
                     #increment failed login count
                     record[1] += 1
                 #3 login failures store the host in blocked_ip with the time stamp
                 if record[1] >= 3:
                    
                    self.blocked_ip[host] = self.time_converter(timestamp)
                    self.login_info.pop(host,None)
         elif status == "200":
             #login success before reaching failed login count 3
             #remove host and count from the record
             if record:
                 self.login_info.pop(host,None)
    
    #updates data stuctures for all features
    def _update(self, **kwargs):
          
          #feature 1
          #example counters will be{'112.67.89.09':7,'304':3,'401':3,'1420':6,'0':1 and so on}
          for key, value in kwargs.items():
             self.counters[key][value] += 1
          
          #feature 2
          #updating top resources default counter
          #example topband_width looks like
          #{'POST /login HTTP/1.0':{'112.67.45.78':5,'bytes':1420 and ...} ...}
          request =kwargs['request']
          self.top_bandwidth[request][kwargs['host']] += 1
          self.top_bandwidth[request]['bytes'] += int(kwargs['bytes'])
          
          #feature 3
          #Updating 60-second window 
          timestamp = kwargs['timestamp']
          
          window_size =timedelta(minutes=60)
          #start with the first record
          if self.linecount == 0:
              self.dt_start = self.time_converter(timestamp)
              self.start =timestamp
              
          else:
              #for each new_time if it is within the window_size
              #increment the counter
              new_time = self.time_converter(timestamp)
              if new_time - self.dt_start <= window_size:
                          self.current_counter += 1
              #passes the window_size ,append the start of window_size to requests 
              #and make new time as the start time          
              else:
                  self.requests.append([self.start,self.current_counter])
                  self.dt_start = new_time
                  self.current_counter =1
                  self.start = timestamp
          
          #Updating variables for feature 4
          #Identifying Login Fail Patterns
          host = kwargs['host']
          status=kwargs['status']
          #if host is already in blocked_ip i.e reached failed login count 3
          #log any new succeses or failures for 5 minutes
          #5 minutes are passed host can be removed from blocked_ip
          if host in self.blocked_ip:
                    if self.time_converter(timestamp) - self.blocked_ip[host] <= timedelta(seconds=300):
                        self.line_flag =True
                    else:
                        self.blocked_ip.pop(host,None)
                        if status in ['200','401']:
                            self.update_record(self,host,status,timestamp) 
          #first login success or failure for host
          #record the entry
          elif status in ['200','401']:
                     self.update_record(self,host,status,timestamp)   
          self.linecount += 1
          
    
                 
if __name__ == '__main__':
    
    logfile =sys.argv[1]
    writefile1=sys.argv[2]
    writefile2=sys.argv[3]
    writefile3=sys.argv[4]
    writefile4=sys.argv[5]
    summary = LogAnalyzer()
    summary.analyze(logfile,writefile4)
    summary.summarize(writefile1,writefile2,writefile3)       