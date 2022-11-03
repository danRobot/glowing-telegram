from typing import Any, Union
from matplotlib.pyplot import table
import oracledb as db
import pandas as pd
import datetime as dt
from sys import platform
import os
from os.path import join
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

class Database:
    def __init__(self,
            username:str,
            password:str,
            host:str,
            port:int,
            service:str,
            initcon:bool=False):
        self.username=username
        self.password=password
        self.host=host
        self.port=port
        self.service=service
        self.db=None
        self.cur=None
        self.initcon=initcon
        result_conection=self.try_connect()
        print("Conección {}".format(result_conection))
        self.sqls_dir=join(BASE_DIR,'sql_querys')
        self.put_alias=lambda x:x+'.' if x!=None else ''
        pass
    def try_connect(self,username:str=None,
                    password:str=None,
                    host:int=None,
                    port:str=None,
                    service:str=None,
                    initcon:bool=False):
        if username!=None and password!=None and host!=None and port!=None and service!=None:
            self.username=username
            self.password=password
            self.host=host
            self.port=port
            self.service=service
            self.initcon=initcon
        self.release()
        try:
            if self.initcon:
                if platform=='linux':
                    db.init_oracle_client()
                else:
                    db.init_oracle_client(lib_dir=r"C:/instantclient_21_6")
            dsn = f'{self.username}/{self.password}@{self.host}:{self.port}/{self.service}'
            if self.db is not None:
                self.db.close()
                self.db.connect(dsn)
            else:
                self.db=db.connect(dsn)
            self.cur=self.db.cursor()
            result='OK'
        except Exception as e:
            try:
                result=e.args[0].message
            except:
                result=e
        return result
    def _get_sqlquery(self,query_name:str):
        path=join(self.sqls_dir,query_name+'.sql')
        archivo=open(path,'r')
        query=archivo.read()
        archivo.close()
        return query
    def _execute(self,sql:str,
                 values:Union[list, tuple, dict]=None):
        try:
            if values==None:
                result=self.cur.execute(sql)
            else:
                result=self.cur.execute(sql,values)
        except Exception as e:
            try:
                result=(sql,e.args[0].message)
            except:
                result=(sql,e)
        return result
    def _execcutemany(self,sql,values):
        try:
            error_log=[]
            self.cur.executemany(sql, values,batcherrors=True)
            errors=self.cur.getbatcherrors()
            for error in errors:
                error_log.append("Error", error.message, "at row offset", error.offset)
            if len(errors)==0:
                result=None
            else:
                result=error_log
        except Exception as e:
            try:
                result=(sql,e.args[0].message)
            except:
                result=(sql,e)
        return result
    def _callfn(self,fn,typ,param):
        try:
            result=self.cur.callfunc(fn,typ,param)
        except Exception as e:
            try:
                result=e.args[0].message
            except:
                result=e
        return result
    def release(self):
        if self.cur is not None:
            self.cur.close()
        if self.db is not None:
            self.db.close()
        self.cur=None
        self.db=None
    def makefilter(self,
                   query_filter:list[Union[list,tuple]],
                   alias:str=None,
                   begin:int=0):
        datos=[]
        for fila in query_filter:
            data=fila[1]
            try:
                if '*' in data:
                    data=data.replace('*','%')
            except:
                pass
            datos.append(data)
        datos=self.flatten(datos)
        fil=' WHERE '
        for col,data in query_filter:
            col=col.upper()
            if type(data)==type([]):
                data=list(map(str,data))
                data="'"+"','".join(data)+"'"
                fil+=' '+self.put_alias(alias)+col+" IN("+data+')'+' AND'
            else:
                data=str(data)
                if '*' in data:
                    fil+=' '+self.put_alias(alias)+col+" LIKE '"+data.replace('*','%')+"' AND"
                else:
                    fil+=' '+self.put_alias(alias)+col+" = '"+data+"' AND"
        r_filtro=fil[:-4]
        for i,dat in enumerate(datos):
            dat=str(dat)
            r_filtro=r_filtro.replace("'"+dat+"'",':'+str(i+1+begin),1)
        return r_filtro,datos
    def basic_query(self,sql:str):
        rows=self._execute(sql)
        if rows is None:
            return 'OK'
        else:
            if type(rows)==type(('1','2')):
                return rows
            try:
                return rows.fetchall()
            except Exception as e:
                return e
    def common_sql(self,query_file,filtro,alias:str=None):
        sql=self._get_sqlquery(query_file)
        query_filter,params=self.makefilter(filtro,alias)
        sql=sql.replace('FILTRO',query_filter)
        rows=self._execute(sql,params)
        if type(rows)==type(('1','2')):
            return rows
        else:
            table_columns = rows.fetchall()
            table_columns.sort(key=lambda x: x[1])
            if len(table_columns)==0:
                table_columns='sin datos'
            return table_columns
    def get_table_columns(self, table:str):
        filtro=[['TABLE_NAME',table.upper()],
                ['OWNER',self.username]]
        return self.common_sql('get_table_columns',filtro)
    def get_relationship(self, table:str):
        filtro=[['TABLE_NAME',table.upper()],
                ['constraint_type','R'],
                ['OWNER',self.username]]
        return self.common_sql('get_relationship',filtro,'alls')
    def get_tables_by_column_name(self, name:str):
        filtro=[['COLUMN_NAME',name.upper()],
                ['OWNER',self.username]]
        return self.common_sql('get_tables_by_column_name',filtro)
    def getDDL(self,entity:str,name:str):
        result = self._callfn('dbms_metadata.get_ddl',db.DB_TYPE_CLOB,
                            [entity.upper(),name.upper()])
        try:
            value=result.read()
        except:
            value=result
        return value
    def search_table(self, regex):
        regex = regex.upper().replace('*','%')
        sql_chk = "select user_tables.table_name from user_tables where upper(TABLE_NAME) like '" + regex + "'"
        rows=self._execute(sql_chk)
        return rows.fetchall()
    def get_table_content(self, table, cols=None, query_filter=None):
        table_name = table.upper()
        if cols is None:
            cols = '*'
        else:
            if type(cols) == type([]):
                cols = ','.join(cols)
        if query_filter is None:
            sql_chk = "select " + cols + " from " + table_name
            rows=self._execute(sql_chk)
        else:
            filtro,values=self.makefilter(query_filter)
            sql_chk = "select " + cols + " from " + table_name + filtro
            rows=self._execute(sql_chk,values)
        if type(rows)==type(('1','2')):
            return rows
        else:
            table_name = rows.fetchall()
            return table_name
    def query2pandas(self,sql):
        try:
            cur=self.cur.execute(sql)
            cols=cur.description
            rows=cur.fetchall()
        except Exception as e:
            return (sql,e)
        content = list(map(list, rows))
        cols = list(map(lambda x:x[0], cols))
        pdd = pd.DataFrame(content)
        pdd.columns=cols
        return pdd
    def table2pandas(self, table, cols=None, query_filter=None):
        content = self.get_table_content(table=table, cols=cols, query_filter=query_filter)
        content = list(map(list, content))
        pdd = pd.DataFrame(content)
        try:
            pdd.columns = self.get_table_columns(table)
        except:
            pdd.columns=cols
            pass
        return pdd

    def copy_table(self, table, tableData, tableColumns, sort=True,clean=False):
        length = self.basic_query("select count(*) from " + table)[0][0]
        try:
            if length > 0 and clean is True:
                self.basic_query("DELETE FROM " + table.upper())
        except:
            print('No se pudo borrar datos de la tabla ' + table)
        if(sort):
            columns = self.get_table_columns(table=table)
        else:
            columns = tableColumns.copy()
        col2 = columns.copy()
        col2.sort()
        if col2.sort() != tableColumns.sort():
            print("Tablas Diferentes")
            return
        assert col2.sort() == tableColumns.sort()
        values = [":" + str(i + 1) for i in range(len(tableColumns))]
        sql = "insert into " + table + "(" + ','.join(columns) + ") values(" + ','.join(values) + ")"
        try:
            self.cur.executemany(sql, tableData,batcherrors=True)
            self.db.commit()
            return "success"
        except Exception as e:
            print('test falla')
            if e.args[0].message == "ORA-00947: no hay suficientes valores":
                print('test entra')
                result=self.move_table(table, tableData, tableColumns)
            else:
                result=e.args[0].message+'falla move'
            return result

    def insert_data(self, table, tableData, tableColumns):
        values = [":" + str(i + 1) for i in range(len(tableColumns))]
        sql = "insert into " + table + "(" + ','.join(tableColumns) + ") values(" + ','.join(values) + ")"
        self.cur.executemany(sql, tableData,batcherrors=True)
        errors=self.cur.getbatcherrors()
        for error in errors:
            print("Error", error.message, "at row offset", error.offset)
        if len(errors)==0:
            result=True
        else:
            result=False
        return result
    def make_update(self,columns):
        cols=[]
        values=[]
        for i,val in enumerate(columns):
            cols.append('='.join([val[0].upper(),':'+str(i+1)]))
            values.append(val[1])
        return ','.join(cols),values
    def update_table(self, table, cols, query_filter,block=False):
        table_name = table.upper()
        if block:
            values=[]
            update_cols=cols[1]
            update_filt=query_filter[1]
            cols=[cols[0]]
            query_filter=[query_filter[0]]
            number,_=self.make_update(cols)
            filtro_upd,_=self.makefilter(query_filter,begin=len(cols))
            sql = "UPDATE " + table_name +" SET "+number+ filtro_upd
            for col,wh in zip(update_cols,update_filt):
                values.append(tuple(col+wh))
            print(sql,values)
            rows=self._execcutemany(sql,values)
            print(rows)
        else:
            number,values_column=self.make_update(cols)
            filtro_upd,values_filtro=self.makefilter(query_filter,begin=len(cols))
            sql = "UPDATE " + table_name +" SET "+number+ filtro_upd
            values=values_column+values_filtro
            rows=self._execute(sql,values)
        if rows is None :
            return 'OK'
        if type(rows)==type(('1','2')):
            return rows
        else:
            table_name = rows.fetchall()
            return table_name
    def commit(self):
        self.db.commit()
        print('comit')
    def rollback(self):
        self.db.rollback()
        print('rollback')
    def flatten(self,list_of_lists):
        if len(list_of_lists) == 0:
            return list_of_lists
        if isinstance(list_of_lists[0], list):
            return self.flatten(list_of_lists[0]) + self.flatten(list_of_lists[1:])
        return list_of_lists[:1] + self.flatten(list_of_lists[1:])
    pass

