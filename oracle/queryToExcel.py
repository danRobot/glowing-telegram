from numpy import block
from database import Database
from sys import argv
import pandas as pd

def save(op,name):
    operaciones=op.copy()
    writer = pd.ExcelWriter(name+'.xlsx') 

    operaciones.to_excel(writer,sheet_name='name',index=False,na_rep='NaN')

    for column in operaciones:
        column_width = max(operaciones[column].astype(str).map(len).max(), len(column))
        col_idx = operaciones.columns.get_loc(column)
        writer.sheets['name'].set_column(col_idx, col_idx, column_width)
    writer.save()

username=''
userpwd = ''
host = ''
port = 0
service_name = ""
print(username,userpwd,host,port,service_name)
local=Database(username,userpwd,host,port,service_name,initcon=True)

try:
    sql=open(argv[1],'r').read()
except:
    sql=open(argv[1],'r',encoding='latin-1').read()

panda=local.query2pandas(sql)
save(panda,'file')

