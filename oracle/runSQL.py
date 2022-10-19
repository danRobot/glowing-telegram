from database import Database
from multiprocessing import Pool
from sys import argv
from os.path import isdir,join
from os import listdir,name,system
from numpy import tile,array

def run_script(sentence,username,userpwd,host,port,service_name,initcon=True):
    sop=Database(username,userpwd,host,port,service_name,initcon)
    try:
        sop.try_connect()
    except Exception as e:
        return e
    if type(sentence)==type([]):
        for sql in sentence:
            result=sop.basic_query(sql)
    else:
        result=sop.basic_query(sql)
    sop.release()
    return result
def runSQL(line):
    username, userpwd, host, port, service_name, sql_dir=line.split('\n')[0].split(' ')
    run_sql=runQuery(sql_dir)
    run_sql.connect(username, userpwd, host, port, service_name)
    return run_sql.run()

class runQuery:
    def __init__(self,sql_file:str) -> None:
        if isdir(sql_file):
            scripts=listdir(sql_file)
            scripts.sort()
            self.sql_file=[join(sql_file,script) for script in scripts]
        else:
            self.sql_file=sql_file
        
        pass
    def __read__(self,sql_file):
        try:
            sql=open(sql_file,'r').read()
        except:
            sql=open(sql_file,'r',encoding='latin-1').read()
        return sql
    def __get_sql__(self):
        if type(self.sql_file)==type([]):
            self.sql=list(map(self.__read__,self.sql_file))
        else:
            self.sql=self.__read__(self.sql_file)
        self.sql=self.sql
    def connect(self,username, userpwd, host, port, service_name):
        try:
            sop=Database(username,userpwd,host,int(port),service_name)
            self.result_con=sop.try_connect()
        except Exception as e:
            self.result_con=e
        self.connection=sop
    def run(self):
        if self.result_con!='OK':
            return self.result_con
        self.__get_sql__()
        results=[]
        if type(self.sql)==type([]):
            for sql in self.sql:
                results.append(self.connection.basic_query(sql))
        else:
            results.append(self.connection.basic_query(self.sql))
        return results
    pass
if name == 'posix':
    _ = system('clear')
else:
    # for windows platfrom
    _ = system('cls')
sites=open(argv[1],'r').readlines()
#p = Process(target=runSQL, args=(sites, ))
#p.start()
#p.join()
with Pool(5) as p:
    r=p.map(runSQL,sites)
    print(r)
