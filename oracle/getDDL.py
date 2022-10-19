from database import Database
from sys import argv

username=argv[1]
userpwd = argv[2]
host = argv[3]
port = 1521
service_name = "XE"
print(username,userpwd,host,port,service_name)
sop=Database(username,userpwd,host,port,service_name,initcon=True)
try:
    sop.try_connect()
except Exception as e:
    print(e)
    exit()
ddl=sop.getDDL(argv[4],argv[5])
filesql=open(argv[5]+'.sql','w',encoding='utf-8')
filesql.write(ddl)
filesql.close()
sop.release()